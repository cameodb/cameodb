//! Admission at the HTTP door: how many requests may be in flight, and how many of one kind.
//!
//! One count of `max_concurrent_requests` bounds everything in flight. Beside it, searches and
//! writes each have a share of that count they cannot pass, so a flood of one kind leaves room
//! for the other: a storm of searches used to hold every permit, and the node answered its
//! writes and deletes `503` while its writer threads stood idle. Inside the node the worker
//! pool already keeps writes from taking every slot (`writer_slot_cap`); this is the same split
//! where a request first arrives, told from the method and path alone, before any body is read.

use std::sync::Arc;
use std::time::Duration;

use axum::http::{Method, header};
use futures::StreamExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Which side of the node a request's work lands on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DoorClass {
    /// A search, plain or streamed, and everything under `/mcp`: work for the read pool.
    Read,
    /// A document write or delete, a bulk request, a delete by query: work for a shard's
    /// writer thread.
    Write,
    /// Everything else — schemas, listings, keys, the admin API. Bounded by the total alone.
    Other,
}

impl DoorClass {
    /// The class of the request `method path`, with any trailing slash already removed.
    pub(crate) fn of(method: &Method, path: &str) -> Self {
        if path == "/mcp" || path.starts_with("/mcp/") {
            return Self::Read;
        }
        let Some(rest) = path.strip_prefix("/api/") else {
            return Self::Other;
        };
        // `rest` is `{index}/{what}`; an index name holds no slash.
        let Some((_index, what)) = rest.split_once('/') else {
            return Self::Other;
        };
        match what {
            "search" | "search/stream" => Self::Read,
            "document" if matches!(*method, Method::PUT | Method::DELETE) => Self::Write,
            "document/stream" | "_bulk" | "_bulk/delete" | "delete" => Self::Write,
            _ => Self::Other,
        }
    }
}

/// Why the door turned a request away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DoorFull {
    /// Every permit is held.
    Total,
    /// The request's class holds its whole share.
    Class(DoorClass),
}

impl DoorFull {
    /// The body of the `503`. The first is the text the node has always answered.
    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::Class(DoorClass::Read) => "Too many concurrent search requests",
            Self::Class(DoorClass::Write) => "Too many concurrent write requests",
            Self::Total | Self::Class(DoorClass::Other) => "Too many concurrent requests",
        }
    }
}

/// How many of `total` permits one class may hold at once: three quarters, leaving at least one
/// to the rest — the worker pool's `writer_slot_cap`, at the door. A total of one has nothing
/// to split.
pub(crate) fn class_share(total: usize) -> usize {
    if total <= 1 {
        return total.max(1);
    }
    total - (total / 4).max(1)
}

/// The door's permits: one total, and a share of it for each of searches and writes.
#[derive(Clone)]
pub(crate) struct DoorPermits {
    total: Arc<Semaphore>,
    reads: Arc<Semaphore>,
    writes: Arc<Semaphore>,
}

/// What an admitted request holds until its response — or its streamed body — ends.
pub(crate) struct DoorPermit {
    _class: Option<OwnedSemaphorePermit>,
    _total: OwnedSemaphorePermit,
}

impl DoorPermits {
    pub(crate) fn new(max_concurrent_requests: usize) -> Self {
        let total = max_concurrent_requests.max(1);
        let share = class_share(total);
        Self {
            total: Arc::new(Semaphore::new(total)),
            reads: Arc::new(Semaphore::new(share)),
            writes: Arc::new(Semaphore::new(share)),
        }
    }

    /// Admit a request of `class`, or say which bound it met. Never waits.
    pub(crate) fn try_admit(&self, class: DoorClass) -> Result<DoorPermit, DoorFull> {
        let share = match class {
            DoorClass::Read => Some(&self.reads),
            DoorClass::Write => Some(&self.writes),
            DoorClass::Other => None,
        };
        // The class first: a request its class has no room for must not take, even for an
        // instant, a permit another class could have used.
        let class_permit = match share {
            Some(share) => Some(
                Arc::clone(share)
                    .try_acquire_owned()
                    .map_err(|_| DoorFull::Class(class))?,
            ),
            None => None,
        };
        let total = Arc::clone(&self.total)
            .try_acquire_owned()
            .map_err(|_| DoorFull::Total)?;
        Ok(DoorPermit {
            _class: class_permit,
            _total: total,
        })
    }
}

/// The most of a refused request's body the door reads before it answers.
pub(crate) const REFUSED_BODY_DRAIN_BYTES: u64 = 4 * 1024 * 1024;

/// The longest the door waits on a refused request's body.
pub(crate) const REFUSED_BODY_DRAIN_TIME: Duration = Duration::from_millis(250);

/// Read and discard the body of a request the door is about to refuse; `true` when the whole
/// body was read.
///
/// A refusal answered while the client is still sending its body ends with the connection
/// closed under the client's write, and what the client then reports is whichever it noticed
/// first: the `503`, or a broken connection — a transport error where the node meant "busy,
/// retry after", and no `Retry-After` to pace the retry by. A bulk request is the one large
/// enough to meet it. Reading the body first costs a copy off the socket and nothing else: no
/// decompression, no parse, no permit. It is bounded in bytes and in time, so a body past the
/// bound, or a client that trickles one, is answered at once as before; the caller then marks
/// the response `Connection: close`.
pub(crate) async fn drain_refused_body(req: axum::extract::Request) -> bool {
    let declared = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    if declared.is_some_and(|length| length > REFUSED_BODY_DRAIN_BYTES) {
        return false;
    }
    let mut body = req.into_body().into_data_stream();
    let drain = async {
        let mut read = 0u64;
        while let Some(chunk) = body.next().await {
            let Ok(chunk) = chunk else {
                return false;
            };
            read += chunk.len() as u64;
            if read > REFUSED_BODY_DRAIN_BYTES {
                return false;
            }
        }
        true
    };
    tokio::time::timeout(REFUSED_BODY_DRAIN_TIME, drain)
        .await
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;

    #[test]
    fn a_request_is_classed_by_its_method_and_path() {
        let class = |method: Method, path: &str| DoorClass::of(&method, path);
        assert_eq!(class(Method::POST, "/api/logs/search"), DoorClass::Read);
        assert_eq!(
            class(Method::POST, "/api/logs/search/stream"),
            DoorClass::Read
        );
        assert_eq!(class(Method::POST, "/mcp"), DoorClass::Read);
        assert_eq!(class(Method::GET, "/mcp/sse"), DoorClass::Read);

        assert_eq!(class(Method::PUT, "/api/logs/document"), DoorClass::Write);
        assert_eq!(
            class(Method::DELETE, "/api/logs/document"),
            DoorClass::Write
        );
        assert_eq!(
            class(Method::POST, "/api/logs/document/stream"),
            DoorClass::Write
        );
        assert_eq!(class(Method::POST, "/api/logs/_bulk"), DoorClass::Write);
        assert_eq!(
            class(Method::POST, "/api/logs/_bulk/delete"),
            DoorClass::Write
        );
        assert_eq!(class(Method::POST, "/api/logs/delete"), DoorClass::Write);

        // An index may be named after an operation; the operation is what follows it.
        assert_eq!(class(Method::POST, "/api/search/_bulk"), DoorClass::Write);
        assert_eq!(class(Method::DELETE, "/api/search"), DoorClass::Other);

        assert_eq!(class(Method::GET, "/api/logs/_config"), DoorClass::Other);
        assert_eq!(class(Method::PUT, "/api/logs/_config"), DoorClass::Other);
        assert_eq!(class(Method::DELETE, "/api/logs"), DoorClass::Other);
        assert_eq!(class(Method::GET, "/_indexes"), DoorClass::Other);
        assert_eq!(class(Method::GET, "/_admin/workers"), DoorClass::Other);
        assert_eq!(class(Method::GET, "/mcpx"), DoorClass::Other);
    }

    #[test]
    fn a_class_share_leaves_at_least_one_permit_to_the_rest() {
        assert_eq!(class_share(1), 1);
        assert_eq!(class_share(2), 1);
        assert_eq!(class_share(4), 3);
        assert_eq!(class_share(8), 6);
        assert_eq!(class_share(128), 96);
    }

    /// A flood of searches stops at its share, and a write is still admitted — the case the
    /// one shared count refused: every permit held by a search, every write answered `503`.
    #[test]
    fn a_flood_of_one_class_leaves_room_for_the_other() {
        let door = DoorPermits::new(8);
        let searches: Vec<_> = (0..6)
            .map(|_| door.try_admit(DoorClass::Read).expect("within the share"))
            .collect();
        assert_eq!(
            door.try_admit(DoorClass::Read).err(),
            Some(DoorFull::Class(DoorClass::Read))
        );
        let first = door
            .try_admit(DoorClass::Write)
            .expect("a write is admitted");
        let second = door.try_admit(DoorClass::Write).expect("and a second");
        // The total still binds: eight are in flight.
        assert_eq!(
            door.try_admit(DoorClass::Write).err(),
            Some(DoorFull::Total)
        );
        assert_eq!(
            door.try_admit(DoorClass::Other).err(),
            Some(DoorFull::Total)
        );

        drop(first);
        door.try_admit(DoorClass::Other)
            .expect("a freed permit is anyone's");
        drop((second, searches));
        assert_eq!(door.total.available_permits(), 8);
        assert_eq!(door.reads.available_permits(), 6);
        assert_eq!(door.writes.available_permits(), 6);
    }

    /// A request refused by the total gives back the class permit it had taken.
    #[test]
    fn a_refusal_holds_nothing() {
        let door = DoorPermits::new(4);
        let held: Vec<_> = (0..3)
            .map(|_| door.try_admit(DoorClass::Read).expect("admitted"))
            .collect();
        let other = door.try_admit(DoorClass::Other).expect("admitted");
        assert_eq!(
            door.try_admit(DoorClass::Write).err(),
            Some(DoorFull::Total)
        );
        assert_eq!(door.writes.available_permits(), 3);
        drop((held, other));
    }

    #[tokio::test]
    async fn a_refused_body_is_read_up_to_the_bound() {
        let request = |body: Body, length: Option<u64>| {
            let mut builder = axum::http::Request::builder()
                .method(Method::POST)
                .uri("/x");
            if let Some(length) = length {
                builder = builder.header(header::CONTENT_LENGTH, length);
            }
            builder.body(body).expect("request")
        };
        assert!(drain_refused_body(request(Body::empty(), None)).await);
        assert!(drain_refused_body(request(Body::from(vec![b'x'; 64 * 1024]), None)).await);

        // Declared past the bound: not read at all.
        let declared = REFUSED_BODY_DRAIN_BYTES + 1;
        assert!(!drain_refused_body(request(Body::empty(), Some(declared))).await);

        // Undeclared and past the bound: given up on once the bound is passed.
        let chunks = (0..5).map(|_| Ok::<_, std::io::Error>(vec![b'x'; 1024 * 1024]));
        let long = Body::from_stream(futures::stream::iter(chunks));
        assert!(!drain_refused_body(request(long, None)).await);

        // A body that never arrives: given up on at the time bound.
        let stalled =
            Body::from_stream(futures::stream::pending::<Result<Vec<u8>, std::io::Error>>());
        let started = std::time::Instant::now();
        assert!(!drain_refused_body(request(stalled, None)).await);
        assert!(started.elapsed() < REFUSED_BODY_DRAIN_TIME * 4);
    }
}
