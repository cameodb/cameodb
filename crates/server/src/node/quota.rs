//! Per-tenant quotas (M4): what one tenant's indexes may add up to on this node.
//!
//! Two ceilings, enforced at two different points, because they are two different kinds of
//! number:
//!
//! - **Index count** is decided at the mint, and exactly. Minting is serialised on the
//!   orchestrator's mailbox — both the implicit mint in `staged_schema_validation` and the
//!   explicit one, a `PUT /_config` applied with `check_quota` (`orch_apply_schema`), run
//!   there — so counting the tenant's indexes from
//!   durable schemas and refusing at the cap cannot race another mint. Mints are rare, so the
//!   count is taken fresh each time rather than cached.
//! - **Bytes** are decided on every write to a tenanted index, against a cached reading. A fresh
//!   measurement is a directory walk per index, which is the cost `commit_index` had removed
//!   from the write path earlier in this cycle, so the reading is refreshed off the write path
//!   on a TTL instead. The trade is stated rather than hidden: a tenant can overshoot by up to
//!   one refresh interval's worth of ingest.
//!
//! Neither ceiling costs anything until an operator writes one. A node with no
//! `[security.tenants.*]` entry — every node before this — never measures and never refuses.

use super::*;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwapOption;
use futures::future::join_all;
use storage::HybridStore;

use crate::auth::TenantQuota;

/// How old a usage reading may be before a write asks for a new one.
///
/// This is the overshoot window: a tenant writing at full speed can land this many seconds of
/// ingest past their ceiling before the reading catches up. Ten seconds keeps that small against
/// any ceiling worth setting, and a refresh is a stats pass the listing endpoint already runs on
/// demand — sampled for redb, cached for tantivy — so taking one this often is cheap.
pub(super) const USAGE_TTL: Duration = Duration::from_secs(10);

/// Longest a refresh may run before it is abandoned.
///
/// A refresh that never returns must not leave the in-flight flag set for good, or every later
/// reading would be the last good one for ever and a tenant who deleted data would stay refused.
/// Abandoning it clears the flag and keeps the previous reading; the next stale check tries again.
const REFRESH_TIMEOUT: Duration = Duration::from_secs(30);

/// One usage reading: bytes per tenant, and when it was taken.
#[derive(Debug)]
struct UsageReading {
    taken: Instant,
    bytes: HashMap<String, u64>,
}

/// The configured ceilings and the cached usage they are checked against.
///
/// Shared by the actor and every worker through an `Arc`: both write lanes check bytes, and a
/// reading taken by one serves the other.
#[derive(Debug)]
pub(crate) struct TenantQuotas {
    limits: HashMap<String, TenantQuota>,
    usage: Arc<ArcSwapOption<UsageReading>>,
    /// Set while a refresh is running, so a burst of writes past the TTL starts one, not many.
    refreshing: Arc<AtomicBool>,
    ttl: Duration,
}

impl TenantQuotas {
    pub(crate) fn new(limits: HashMap<String, TenantQuota>) -> Self {
        Self::with_ttl(limits, USAGE_TTL)
    }

    pub(super) fn with_ttl(limits: HashMap<String, TenantQuota>, ttl: Duration) -> Self {
        Self {
            limits,
            usage: Arc::new(ArcSwapOption::empty()),
            refreshing: Arc::new(AtomicBool::new(false)),
            ttl,
        }
    }

    /// The index ceiling for `tenant`, if one is set.
    pub(super) fn max_indexes(&self, tenant: &str) -> Option<usize> {
        self.limits
            .get(tenant)
            .map(|quota| quota.max_indexes)
            .filter(|&max| max > 0)
    }

    fn max_bytes(&self, tenant: &str) -> Option<u64> {
        self.limits
            .get(tenant)
            .map(|quota| quota.max_bytes)
            .filter(|&max| max > 0)
    }

    /// Refuse a mint that would take `tenant` past their index ceiling.
    ///
    /// `owned` is how many live indexes the tenant holds *before* this one — the caller counts
    /// them, from durable schemas, only when [`Self::max_indexes`] says there is a ceiling to
    /// count against.
    pub(super) fn check_mint(&self, tenant: &str, owned: usize) -> Result<(), OrchestratorError> {
        match self.max_indexes(tenant) {
            Some(max) if owned >= max => Err(OrchestratorError::QuotaExceeded {
                tenant: tenant.to_string(),
                detail: format!(
                    "tenant already owns {owned} of {max} permitted indexes; \
                     delete one or ask the operator to raise max_indexes"
                ),
            }),
            _ => Ok(()),
        }
    }

    /// Refuse a write to an index `owner` holds when the last reading has them at or over their
    /// byte ceiling.
    ///
    /// `owner` is the index's tenant — the one stamped on its schema, or the one about to be
    /// stamped for a write that mints it — and never the writing key's: bytes are charged to
    /// whoever owns the index they land in. An unowned index, or an owner with no byte ceiling,
    /// returns at once without touching the reading.
    ///
    /// A stale reading starts a refresh and is still used for this decision, so no write ever
    /// waits on a measurement. With no reading yet, the write is allowed: that is the first
    /// interval of the overshoot window, not a hole in it.
    pub(super) fn check_write(
        &self,
        owner: Option<&str>,
        shards: &HashMap<Uuid, MicroshardActor>,
    ) -> Result<(), OrchestratorError> {
        let Some(owner) = owner else {
            return Ok(());
        };
        let Some(max) = self.max_bytes(owner) else {
            return Ok(());
        };

        let reading = self.usage.load_full();
        let stale = reading
            .as_ref()
            .is_none_or(|reading| reading.taken.elapsed() >= self.ttl);
        if stale {
            self.refresh_in_background(shards);
        }

        let used = reading
            .as_ref()
            .and_then(|reading| reading.bytes.get(owner).copied())
            .unwrap_or(0);
        if used >= max {
            return Err(OrchestratorError::QuotaExceeded {
                tenant: owner.to_string(),
                detail: format!(
                    "tenant's indexes occupy {used} of {max} permitted bytes; \
                     delete data or ask the operator to raise max_bytes"
                ),
            });
        }
        Ok(())
    }

    /// Start one refresh unless one is already running.
    fn refresh_in_background(&self, shards: &HashMap<Uuid, MicroshardActor>) {
        if self
            .refreshing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        // Only reachable inside a runtime — every write path is — but a missing one must clear
        // the flag rather than panic with it set.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.refreshing.store(false, Ordering::Release);
            return;
        };

        let shards: Vec<MicroshardActor> = shards.values().cloned().collect();
        let refreshing = Arc::clone(&self.refreshing);
        let usage = Arc::clone(&self.usage);
        runtime.spawn(async move {
            match tokio::time::timeout(REFRESH_TIMEOUT, measure_usage(shards)).await {
                Ok(Ok(bytes)) => usage.store(Some(Arc::new(UsageReading {
                    taken: Instant::now(),
                    bytes,
                }))),
                Ok(Err(err)) => {
                    tracing::warn!(error = %err, "Tenant usage refresh failed; keeping the last reading");
                }
                Err(_) => {
                    tracing::warn!(
                        timeout_secs = REFRESH_TIMEOUT.as_secs(),
                        "Tenant usage refresh timed out; keeping the last reading"
                    );
                }
            }
            refreshing.store(false, Ordering::Release);
        });
    }
}

/// Whose bytes a write to an index with `schema` lands in.
///
/// The stamped owner for an index that exists. For one this write is about to mint, the tenant
/// the mint will stamp — so a tenant already at their byte ceiling cannot sidestep it by writing
/// into a new index, and the refusal comes before the mint rather than after it has left an
/// empty index behind.
pub(super) fn owner_of<'a>(
    schema: &'a IndexSchema,
    minting_tenant: Option<&'a str>,
) -> Option<&'a str> {
    if schema.fields.is_empty() || schema.state == storage::SchemaState::Dropped {
        minting_tenant
    } else {
        schema.tenant.as_deref()
    }
}

/// Which tenant owns each live index in `store`, read from durable schemas.
///
/// One store answers for the node: every schema is persisted to every local store, and the first
/// is what `schema_from_shards` reads too. Deletion records are skipped by `get_index_names`, so a
/// dropped index is no one's. A blocking redb read — callers run it off the runtime.
pub(super) fn tenant_owners(store: &HybridStore) -> Result<HashMap<String, String>, StoreError> {
    let mut owners = HashMap::new();
    for index in store.get_index_names()? {
        if let Some(tenant) = store
            .get_schema_cached(&index)?
            .and_then(|schema| schema.tenant.clone())
        {
            owners.insert(index, tenant);
        }
    }
    Ok(owners)
}

/// How many live indexes `tenant` owns on this node.
pub(super) async fn owned_index_count(
    shards: &HashMap<Uuid, MicroshardActor>,
    tenant: &str,
) -> Result<usize, OrchestratorError> {
    let Some(store) = shards.values().find_map(|shard| shard.store.clone()) else {
        return Ok(0);
    };
    let tenant = tenant.to_string();
    let owners = tokio::task::spawn_blocking(move || tenant_owners(&store))
        .await
        .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))??;
    Ok(owners.values().filter(|owner| **owner == tenant).count())
}

/// Bytes per tenant across this node's shards: the listing's `total_size_bytes` (tantivy plus
/// redb) for each index, summed under the index's owner.
///
/// A shard that cannot answer fails the whole reading rather than contributing nothing — a
/// reading missing a shard would under-count every tenant on it, which is the one direction a
/// quota must not err in silently. The caller keeps the previous reading instead.
async fn measure_usage(
    shards: Vec<MicroshardActor>,
) -> Result<HashMap<String, u64>, OrchestratorError> {
    let Some(store) = shards.iter().find_map(|shard| shard.store.clone()) else {
        return Ok(HashMap::new());
    };
    let owners = tokio::task::spawn_blocking(move || tenant_owners(&store))
        .await
        .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))??;
    if owners.is_empty() {
        return Ok(HashMap::new());
    }

    let msg = GetShardStats {
        include_data_size: true,
    };
    let snapshots = join_all(
        shards
            .iter()
            .map(|shard| shard.handle_get_stats(msg.clone())),
    )
    .await;

    let mut bytes: HashMap<String, u64> = HashMap::new();
    for snapshot in snapshots {
        for (index, stats) in snapshot?.per_index {
            if let Some(owner) = owners.get(&index) {
                *bytes.entry(owner.clone()).or_default() += stats.tantivy_bytes + stats.redb_bytes;
            }
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quotas(entries: &[(&str, usize, u64)]) -> TenantQuotas {
        TenantQuotas::new(
            entries
                .iter()
                .map(|&(tenant, max_indexes, max_bytes)| {
                    (
                        tenant.to_string(),
                        TenantQuota {
                            max_indexes,
                            max_bytes,
                        },
                    )
                })
                .collect(),
        )
    }

    fn with_reading(quotas: &TenantQuotas, bytes: &[(&str, u64)]) {
        quotas.usage.store(Some(Arc::new(UsageReading {
            taken: Instant::now(),
            bytes: bytes.iter().map(|&(t, b)| (t.to_string(), b)).collect(),
        })));
    }

    /// Nothing configured is nothing enforced: the default a patch upgrade lands on.
    #[test]
    fn no_entry_is_no_ceiling() {
        let quotas = quotas(&[]);
        assert_eq!(quotas.max_indexes("acme"), None);
        assert!(quotas.check_mint("acme", 10_000).is_ok());
        assert!(quotas.check_write(Some("acme"), &HashMap::new()).is_ok());
    }

    /// `0` is unlimited for each ceiling independently, so setting one leaves the other off.
    #[test]
    fn a_zero_ceiling_is_unlimited() {
        let quotas = quotas(&[("acme", 0, 100)]);
        assert_eq!(quotas.max_indexes("acme"), None);
        assert!(quotas.check_mint("acme", 10_000).is_ok());
    }

    /// Refused *at* the cap, not past it: `owned` is the count before this mint.
    #[test]
    fn a_mint_is_refused_at_the_cap_and_only_for_that_tenant() {
        let quotas = quotas(&[("acme", 2, 0)]);
        assert!(quotas.check_mint("acme", 1).is_ok());
        let err = quotas.check_mint("acme", 2).unwrap_err();
        assert_eq!(err.verdict(), RemoteVerdict::QuotaExceeded);
        assert!(err.to_string().contains("acme"), "{err}");
        assert!(err.to_string().contains("max_indexes"), "{err}");
        assert!(quotas.check_mint("globex", 2).is_ok());
    }

    /// An unowned index is charged to no one, whatever the ceilings say.
    #[test]
    fn an_unowned_write_is_never_refused() {
        let quotas = quotas(&[("acme", 0, 1)]);
        with_reading(&quotas, &[("acme", 1_000)]);
        assert!(quotas.check_write(None, &HashMap::new()).is_ok());
    }

    /// With no reading yet the write is allowed — the first interval of the overshoot window.
    #[tokio::test]
    async fn no_reading_yet_allows_the_write() {
        let quotas = quotas(&[("acme", 0, 1)]);
        assert!(quotas.check_write(Some("acme"), &HashMap::new()).is_ok());
    }

    #[test]
    fn a_write_is_refused_at_the_byte_ceiling_and_only_for_that_tenant() {
        let quotas = quotas(&[("acme", 0, 100), ("globex", 0, 100)]);
        with_reading(&quotas, &[("acme", 99), ("globex", 100)]);
        assert!(quotas.check_write(Some("acme"), &HashMap::new()).is_ok());
        let err = quotas
            .check_write(Some("globex"), &HashMap::new())
            .unwrap_err();
        assert_eq!(err.verdict(), RemoteVerdict::QuotaExceeded);
        assert!(err.to_string().contains("max_bytes"), "{err}");
    }

    /// A stale reading still decides the write that found it stale, and is then replaced.
    #[tokio::test]
    async fn a_stale_reading_still_decides_and_is_replaced() {
        let quotas = TenantQuotas::with_ttl(
            [(
                "acme".to_string(),
                TenantQuota {
                    max_indexes: 0,
                    max_bytes: 10,
                },
            )]
            .into_iter()
            .collect(),
            Duration::ZERO,
        );
        with_reading(&quotas, &[("acme", 10)]);
        for _ in 0..5 {
            assert!(quotas.check_write(Some("acme"), &HashMap::new()).is_err());
        }
        // The refresh over no shards publishes an empty reading and clears the flag.
        let deadline = Instant::now() + Duration::from_secs(5);
        while quotas.refreshing.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "the refresh never finished");
            tokio::task::yield_now().await;
        }
        assert!(
            quotas.check_write(Some("acme"), &HashMap::new()).is_ok(),
            "the refreshed reading holds nothing for acme"
        );
    }

    /// The mint's tenant owns a new or dropped index; the stamp owns a live one.
    #[test]
    fn owner_follows_the_stamp_or_the_mint() {
        let mut schema = IndexSchema::default();
        assert_eq!(owner_of(&schema, Some("acme")), Some("acme"));

        schema.fields.insert(
            "id".to_string(),
            storage::FieldDef::new("id".to_string(), TantivyFieldType::Text),
        );
        assert_eq!(
            owner_of(&schema, Some("acme")),
            None,
            "a live unowned index"
        );
        schema.tenant = Some("globex".to_string());
        assert_eq!(owner_of(&schema, Some("acme")), Some("globex"));

        schema.state = storage::SchemaState::Dropped;
        assert_eq!(owner_of(&schema, Some("acme")), Some("acme"));
    }
}
