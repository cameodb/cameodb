//! `cameodb service run`: the node as a Windows service.
//!
//! Windows starts a service by running its command line and waiting for the process to hand its
//! main thread to the service control manager, which then calls [`service_main`] on a thread of
//! its own. A console program never does that, and the manager gives up on it after 30 seconds
//! with error 1053, so a node cannot be registered as a service without this entry point.
//!
//! What it reports, and when:
//! - *start pending* until the node has bound its port, with progress every few seconds, so
//!   the minutes a large node can take to open its shards are not taken for a hang;
//! - *running* once the port is bound, which is when `Start-Service` returns;
//! - *stop pending* from a stop — `Stop-Service`, or the machine shutting down — until the node
//!   has finished the same graceful shutdown Ctrl+C runs, again with progress;
//! - *stopped*, with a service-specific exit code of 1 if the node failed. Windows' recovery
//!   settings restart it on that, as `Restart=on-failure` does under systemd.
//!
//! A failure is logged before it is reported, to the node's log file: a service has no console,
//! so `[logging] directory` is the only place it can be read. A config that cannot be read at
//! all fails before there is a log file; `cameodb check-config -c <PATH>` says why.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use windows_service::service::{
    ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus, ServiceType,
};
use windows_service::service_control_handler::{
    self, ServiceControlHandlerResult, ServiceStatusHandle,
};

/// The name the service is installed under, and the one the manager starts this process as.
pub const SERVICE_NAME: &str = "CameoDB";

/// Set when Windows asks the service to stop; the node's shutdown waits on it beside Ctrl+C.
/// A stop that arrives while the node is still starting is kept until it gets there.
pub static STOP: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// How long the manager is told to wait between two reports before it takes the service for
/// hung; progress is reported well inside it.
const WAIT_HINT: Duration = Duration::from_secs(30);
const PROGRESS_EVERY: Duration = Duration::from_secs(5);

const STARTING: u8 = 0;
const RUNNING: u8 = 1;
const STOPPING: u8 = 2;
const STOPPED: u8 = 3;

static STATUS: OnceLock<ServiceStatusHandle> = OnceLock::new();
static PHASE: AtomicU8 = AtomicU8::new(STARTING);
static CHECKPOINT: AtomicU32 = AtomicU32::new(0);
/// The node's command line: this program's path, then everything after `service run`.
static NODE_ARGS: OnceLock<Vec<String>> = OnceLock::new();

const USAGE: &str = "\
cameodb service — run the node as a Windows service

Usage:
  cameodb service run [OPTIONS]

`service run` is the command line Windows starts the CameoDB service with; the installer
registers it. Every node option (see `cameodb --help`) can follow it, as on the node's own
command line: the installed service runs `service run --config <PATH>`.

It cannot be started from a console. To run the node in a console instead:
  cameodb --config <PATH>

Manage the installed service with the usual tools:
  Start-Service CameoDB      Stop-Service CameoDB      Get-Service CameoDB

Stopping it runs the same graceful shutdown as Ctrl+C. Logs go to [logging] directory in the
config; the service has no console to write to.";

// The dispatcher's callback, which the manager calls with the start parameters. The macro's
// `unsafe` converts the manager's argument array; it is the crate's code, which the workspace's
// `unsafe_code` lint does not count as ours.
mod entry {
    windows_service::define_windows_service!(ffi_service_main, service_main);

    fn service_main(_start_parameters: Vec<std::ffi::OsString>) {
        super::service_main();
    }

    /// Hand this thread to the manager until the service has stopped.
    pub(super) fn dispatch() -> windows_service::Result<()> {
        windows_service::service_dispatcher::start(super::SERVICE_NAME, ffi_service_main)
    }
}

/// `cameodb service …`. `args` is everything after `service`.
pub fn run(program: &str, args: &[String]) -> Result<()> {
    match args.first().map(String::as_str) {
        Some("run") => {}
        None | Some("-h") | Some("--help") => {
            println!("{USAGE}");
            return Ok(());
        }
        Some(other) => bail!("cameodb service {other}: unknown; the only command is `service run`"),
    }
    let node_args = std::iter::once(program.to_string())
        .chain(args[1..].iter().cloned())
        .collect();
    let _ = NODE_ARGS.set(node_args);

    match entry::dispatch() {
        Ok(()) => Ok(()),
        // ERROR_FAILED_SERVICE_CONTROLLER_CONNECT: run from a console, not by the manager.
        Err(windows_service::Error::Winapi(e)) if e.raw_os_error() == Some(1063) => bail!(
            "`cameodb service run` is started by Windows, not from a console: use \
             `Start-Service CameoDB`, or `cameodb --config <PATH>` to run the node here"
        ),
        Err(e) => Err(e).context("connecting to the Windows service manager"),
    }
}

/// Called by the node once its port is bound: the service is running.
pub fn node_started() {
    if PHASE
        .compare_exchange(STARTING, RUNNING, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        report(ServiceState::Running, ServiceExitCode::Win32(0));
    }
}

fn service_main() {
    let handler = |control| match control {
        ServiceControl::Stop | ServiceControl::Preshutdown => {
            // Once: a second stop, or one after the node has ended, changes nothing.
            let first = PHASE
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |phase| {
                    matches!(phase, STARTING | RUNNING).then_some(STOPPING)
                })
                .is_ok();
            if first {
                report(ServiceState::StopPending, ServiceExitCode::Win32(0));
                STOP.notify_one();
            }
            ServiceControlHandlerResult::NoError
        }
        ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
        _ => ServiceControlHandlerResult::NotImplemented,
    };
    let Ok(handle) = service_control_handler::register(SERVICE_NAME, handler) else {
        // Nothing to report to; the manager times the start out.
        return;
    };
    let _ = STATUS.set(handle);
    report(ServiceState::StartPending, ServiceExitCode::Win32(0));

    // Progress while starting or stopping, from a thread of its own: the node's runtime may be
    // busy for minutes opening shards, and a report must still go out every few seconds.
    std::thread::spawn(|| {
        loop {
            std::thread::sleep(PROGRESS_EVERY);
            match PHASE.load(Ordering::SeqCst) {
                STARTING => report(ServiceState::StartPending, ServiceExitCode::Win32(0)),
                STOPPING => report(ServiceState::StopPending, ServiceExitCode::Win32(0)),
                STOPPED => return,
                _ => {}
            }
        }
    });

    let args = NODE_ARGS.get().cloned().unwrap_or_default();
    let result = crate::runtime()
        .context("starting the node's runtime")
        .and_then(|runtime| runtime.block_on(crate::run(args)));
    let exit = match &result {
        Ok(()) => ServiceExitCode::Win32(0),
        Err(e) => {
            // Into the log file, if the node got as far as opening one.
            tracing::error!("CameoDB service stopped on an error: {e:#}");
            ServiceExitCode::ServiceSpecific(1)
        }
    };
    PHASE.store(STOPPED, Ordering::SeqCst);
    report(ServiceState::Stopped, exit);
}

/// Tell the manager where the service is. Pending states carry a rising checkpoint, which is
/// how it tells progress from a hang.
fn report(state: ServiceState, exit_code: ServiceExitCode) {
    let Some(handle) = STATUS.get() else {
        return;
    };
    let pending = matches!(
        state,
        ServiceState::StartPending | ServiceState::StopPending
    );
    let controls_accepted = match state {
        // Accepted while starting too: a stop is kept until the node can act on it, and a
        // machine shutting down during a long start still gets the graceful shutdown.
        ServiceState::StartPending | ServiceState::Running => {
            ServiceControlAccept::STOP | ServiceControlAccept::PRESHUTDOWN
        }
        _ => ServiceControlAccept::empty(),
    };
    let _ = handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: state,
        controls_accepted,
        exit_code,
        checkpoint: if pending {
            CHECKPOINT.fetch_add(1, Ordering::SeqCst) + 1
        } else {
            0
        },
        wait_hint: if pending { WAIT_HINT } else { Duration::ZERO },
        process_id: None,
    });
}
