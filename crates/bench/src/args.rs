//! Argument parsing, done by hand.
//!
//! No `clap` here deliberately: this binary doubles as a worked example of consuming the
//! SDK, and a reader following it should meet `CameoClient` on the second screen rather than
//! a derive macro. The parsing is dull on purpose.

use anyhow::{Result, anyhow, bail};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// One document per request. This is the operation Phase 13's p99 target is about.
    Write,
    /// Documents per request, via `_bulk`. The comparison against `write` is the whole
    /// point: it shows what batching buys, in throughput and in per-document cost.
    Bulk,
    /// Search only, against whatever the index already holds.
    Search,
    /// Both at once, which is the only way to see writes and merges interfere with reads.
    Mixed,
}

impl Mode {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "write" => Ok(Mode::Write),
            "bulk" => Ok(Mode::Bulk),
            "search" => Ok(Mode::Search),
            "mixed" => Ok(Mode::Mixed),
            other => bail!("unknown mode '{other}' (expected write, bulk, search or mixed)"),
        }
    }

    /// Single-document writes.
    pub fn writes(&self) -> bool {
        matches!(self, Mode::Write | Mode::Mixed)
    }

    /// Batched writes.
    pub fn bulk(&self) -> bool {
        matches!(self, Mode::Bulk)
    }

    pub fn searches(&self) -> bool {
        matches!(self, Mode::Search | Mode::Mixed)
    }
}

#[derive(Debug)]
pub struct Args {
    pub url: String,
    pub index: String,
    pub mode: Mode,
    /// Concurrent in-flight requests per workload. Closed-loop: each one issues a request,
    /// waits for the answer, then issues the next.
    pub concurrency: usize,
    pub duration: Duration,
    pub warmup: Duration,
    /// Documents pre-loaded before the run so searches have something to match.
    pub seed_docs: usize,
    /// Documents per request in bulk mode.
    pub batch_size: usize,
    pub api_key: Option<String>,
    pub insecure: bool,
    pub keep_index: bool,
}

const USAGE: &str = "\
cameodb-bench — latency harness for CameoDB, and a worked example of the client SDK

USAGE:
    cameodb-bench [OPTIONS]

OPTIONS:
    --url <URL>            Node to test            (default: http://localhost:9480)
    --index <NAME>         Index to use            (default: bench)
    --mode <MODE>          write | bulk | search | mixed  (default: mixed)
    --concurrency <N>      In-flight requests per workload (default: 8)
    --duration <SECS>      Measured run length     (default: 20)
    --warmup <SECS>        Unmeasured run first    (default: 5)
    --seed-docs <N>        Documents pre-loaded before measuring (default: 5000)
    --batch-size <N>       Documents per request in bulk mode (default: 500)
    --api-key <KEY>        Bearer key, or set CAMEODB_API_KEY
    --insecure             Accept an invalid TLS certificate
    --keep-index           Leave the index behind instead of deleting it
    -h, --help             This text

NOTES:
    Closed-loop by design: N workers each issue one request at a time. That measures service
    time under a fixed concurrency, not behaviour under a fixed arrival rate — a saturated
    node shows up as rising latency rather than a growing queue. Compare runs at equal
    concurrency; do not read these numbers as an open-loop SLA.
";

pub fn parse() -> Result<Option<Args>> {
    let mut url = "http://localhost:9480".to_string();
    let mut index = "bench".to_string();
    let mut mode = Mode::Mixed;
    let mut concurrency = 8usize;
    let mut duration = 20u64;
    let mut warmup = 5u64;
    let mut seed_docs = 5_000usize;
    let mut batch_size = 500usize;
    let mut api_key = std::env::var("CAMEODB_API_KEY").ok();
    let mut insecure = false;
    let mut keep_index = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or_else(|| anyhow!("{arg} needs a value"));
        match arg.as_str() {
            "-h" | "--help" => {
                print!("{USAGE}");
                return Ok(None);
            }
            "--url" => url = value()?,
            "--index" => index = value()?,
            "--mode" => mode = Mode::parse(&value()?)?,
            "--concurrency" => concurrency = value()?.parse()?,
            "--duration" => duration = value()?.parse()?,
            "--warmup" => warmup = value()?.parse()?,
            "--seed-docs" => seed_docs = value()?.parse()?,
            "--batch-size" => batch_size = value()?.parse()?,
            "--api-key" => api_key = Some(value()?),
            "--insecure" => insecure = true,
            "--keep-index" => keep_index = true,
            other => bail!("unknown argument '{other}' (try --help)"),
        }
    }

    if concurrency == 0 {
        bail!("--concurrency must be at least 1");
    }
    if duration == 0 {
        bail!("--duration must be at least 1 second");
    }
    if batch_size == 0 {
        bail!("--batch-size must be at least 1");
    }

    Ok(Some(Args {
        url,
        index,
        mode,
        concurrency,
        duration: Duration::from_secs(duration),
        warmup: Duration::from_secs(warmup),
        seed_docs,
        batch_size,
        api_key,
        insecure,
        keep_index,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_controls_which_workloads_run() {
        assert!(Mode::Write.writes() && !Mode::Write.searches());
        assert!(Mode::Search.searches() && !Mode::Search.writes());
        assert!(Mode::Mixed.writes() && Mode::Mixed.searches());
    }

    #[test]
    fn an_unknown_mode_is_refused_by_name() {
        let err = Mode::parse("read").unwrap_err().to_string();
        assert!(
            err.contains("read"),
            "the message should name what was given"
        );
    }
}
