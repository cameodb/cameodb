//! Shared helpers for the server integration tests.
//!
//! These live in a `tests/common/mod.rs` module so every integration-test binary can include
//! them without introducing a new crate. Each binary compiles its own copy, which is fine: the
//! helpers are tiny and the synchronization uses an OS-level file lock, not in-process state.

use std::net::TcpListener;
use std::sync::atomic::{AtomicU16, Ordering};

/// Ports handed out here, all below every OS's ephemeral range (macOS and Windows start at
/// 49152, Linux at 32768).
const PORT_RANGE: std::ops::Range<u16> = 20000..32000;

/// Reserve a port for a node this test is about to spawn.
///
/// The port is free when this returns, but the node binds it only once it has started, which
/// under load takes seconds. A port taken with `bind(0)` comes from the ephemeral range, the
/// same pool every outbound connection draws from, and the tests around this one open
/// thousands of those: one of them landed on a reserved port and the node exited with
/// "Address already in use" (seen under a saturated CPU). Below the ephemeral range nothing
/// is handed out implicitly, so only another listener can collide, and the bind below skips
/// any port one already holds.
///
/// Within a binary a counter keeps concurrent tests apart; between binaries the start is
/// offset by the process id and the cross-process file lock serializes the bind check.
pub fn reserve_port() -> u16 {
    static NEXT: AtomicU16 = AtomicU16::new(0);

    #[cfg(unix)]
    let _lock = PortLock::acquire();

    let span = PORT_RANGE.end - PORT_RANGE.start;
    let base = (std::process::id() % u32::from(span)) as u16;
    for _ in 0..span {
        let offset = (base + NEXT.fetch_add(1, Ordering::Relaxed) % span) % span;
        let port = PORT_RANGE.start + offset;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("no free port in {PORT_RANGE:?}");
}

#[cfg(unix)]
struct PortLock(std::fs::File);

#[cfg(unix)]
impl PortLock {
    fn acquire() -> Self {
        let path = std::env::temp_dir().join("cameodb_test_port.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open port reservation lock file");
        file.lock().expect("acquire port reservation lock");
        Self(file)
    }
}

#[cfg(unix)]
impl Drop for PortLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}
