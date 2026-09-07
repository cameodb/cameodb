//! Shared helpers for the server integration tests.
//!
//! These live in a `tests/common/mod.rs` module so every integration-test binary can include
//! them without introducing a new crate. Each binary compiles its own copy, which is fine: the
//! helpers are tiny and the synchronization uses an OS-level file lock, not in-process state.

use std::net::TcpListener;

/// Reserve an ephemeral port in a way that is safe to use across concurrent test binaries.
///
/// Every test binary that calls this competes for a cross-process file lock. While the lock is
/// held, the function binds an ephemeral port, reads the number, and immediately closes the
/// socket. Because only one test process is in the bind/release window at a time, the chance of
/// another test process claiming the same port before the spawned server binds is reduced to the
/// tiny external race between this function returning and the server process starting.
///
/// On non-Unix platforms the lock is skipped and the function falls back to a plain ephemeral
/// bind, which is no worse than the previous behaviour on those platforms.
pub fn reserve_port() -> u16 {
    #[cfg(unix)]
    let _lock = PortLock::acquire();

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local addr").port()
}

#[cfg(unix)]
struct PortLock(std::fs::File);

#[cfg(unix)]
impl PortLock {
    fn acquire() -> Self {
        use std::os::fd::AsRawFd;

        let path = std::env::temp_dir().join("cameodb_test_port.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open port reservation lock file");
        let fd = file.as_raw_fd();
        let rc = unsafe { libc::flock(fd, libc::LOCK_EX) };
        if rc != 0 {
            panic!("failed to acquire port reservation lock");
        }
        Self(file)
    }
}

#[cfg(unix)]
impl Drop for PortLock {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;

        unsafe {
            let _ = libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}
