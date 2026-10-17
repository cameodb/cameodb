//! The node's log: standard output always, and a daily file when `[logging] directory` says.
//!
//! Logging starts before the config is read, so that which file was chosen — and any complaint
//! about it — is logged at all. The config then says at what level, and where else the log goes.
//! Both arrive through `reload` handles rather than by starting logging a second time, which
//! `tracing` does not allow.
//!
//! The file is written synchronously, on the thread that logs. A background writer would be
//! faster, but its buffer is lost when the process exits without unwinding — on a second
//! Ctrl+C, an emergency shutdown, a service stopped by Windows — which are the moments the log
//! is read afterwards to explain.

use anyhow::{Context, Result};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::layer::{Layered, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry, fmt, reload};

use crate::config::LoggingConfig;

type FileLayer = Option<Box<dyn Layer<Registry> + Send + Sync>>;
type WithFile = Layered<reload::Layer<FileLayer, Registry>, Registry>;

/// What can still be changed about the log once it has started.
pub struct Logging {
    filter: reload::Handle<EnvFilter, WithFile>,
    file: reload::Handle<FileLayer, Registry>,
}

/// Start logging to standard output, at `RUST_LOG`'s level or errors only, as before any
/// config is read.
pub fn init() -> Logging {
    let (subscriber, logging) = subscriber();
    subscriber.init();
    logging
}

/// The subscriber [`init`] installs, for a test to install for itself instead.
fn subscriber() -> (impl tracing::Subscriber + Send + Sync, Logging) {
    let (file, file_handle) = reload::Layer::new(None);
    let (filter, filter_handle) = reload::Layer::new(EnvFilter::from_default_env());
    // The filter is outermost and so global: an event it refuses reaches neither the file
    // nor standard output.
    let subscriber = tracing_subscriber::registry()
        .with(file)
        .with(filter)
        .with(fmt::layer());
    (
        subscriber,
        Logging {
            filter: filter_handle,
            file: file_handle,
        },
    )
}

impl Logging {
    /// Apply `[logging]`: its level, unless `RUST_LOG` is set, and its log file.
    pub fn apply(&self, config: &LoggingConfig) -> Result<()> {
        let rust_log = std::env::var("RUST_LOG").is_ok_and(|v| !v.trim().is_empty());
        if let (Some(level), false) = (&config.level, rust_log) {
            let filter = EnvFilter::try_new(level)
                .with_context(|| format!("logging.level {level:?} is not a valid filter"))?;
            self.filter.reload(filter)?;
        }

        if let Some(directory) = &config.directory {
            let appender = RollingFileAppender::builder()
                .rotation(Rotation::DAILY)
                .filename_prefix("cameodb")
                .filename_suffix("log")
                .max_log_files(config.max_files)
                .build(directory)
                .with_context(|| format!("cannot write a log file in {}", directory.display()))?;
            let layer = fmt::layer().with_ansi(false).with_writer(appender).boxed();
            self.file.reload(Some(layer))?;
            tracing::info!(directory = %directory.display(), "Logging to a daily file");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_files(dir: &std::path::Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn the_file_gets_what_the_level_lets_through() {
        let dir = tempfile::tempdir().unwrap();
        let logs = dir.path().join("logs");
        let (subscriber, logging) = subscriber();
        let _guard = tracing::subscriber::set_default(subscriber);

        tracing::error!("before the config: standard output only");
        logging
            .apply(&LoggingConfig {
                directory: Some(logs.clone()),
                max_files: 3,
                level: Some("info".into()),
            })
            .unwrap();
        tracing::info!("an info line, let through by the level");
        tracing::debug!("a debug line, held back by it");

        let files = log_files(&logs);
        assert_eq!(files.len(), 1, "{files:?}");
        assert!(files[0].starts_with("cameodb.") && files[0].ends_with(".log"));
        let text = std::fs::read_to_string(logs.join(&files[0])).unwrap();
        assert!(text.contains("an info line"), "{text}");
        assert!(!text.contains("a debug line"), "{text}");
        assert!(!text.contains("before the config"), "{text}");
        assert!(
            !text.contains('\u{1b}'),
            "no colour codes in a file: {text:?}"
        );
    }

    #[test]
    fn a_directory_that_cannot_be_written_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        // A file where the directory should be.
        let blocked = dir.path().join("logs");
        std::fs::write(&blocked, b"").unwrap();
        let (subscriber, logging) = subscriber();
        let _guard = tracing::subscriber::set_default(subscriber);

        let err = logging
            .apply(&LoggingConfig {
                directory: Some(blocked),
                ..LoggingConfig::default()
            })
            .unwrap_err();

        assert!(
            format!("{err:#}").contains("cannot write a log file"),
            "{err:#}"
        );
    }
}
