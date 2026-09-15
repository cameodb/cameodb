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

/// How request start times are drawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arrival {
    /// Exponential gaps: a Poisson process, the default.
    ///
    /// The bursts are the point, not an artefact. Several requests genuinely coexisting at
    /// one shard is the condition a write-side batching optimisation needs in order to pay,
    /// and a perfectly spaced arrival stream never produces it — which is part of why the
    /// bounded linger could not be evaluated against the closed-loop harness.
    Poisson,
    /// Exactly `1/rate` apart. Not realistic, but it isolates the node from arrival variance
    /// when the question is about the node.
    Uniform,
}

impl Arrival {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "poisson" => Ok(Arrival::Poisson),
            "uniform" => Ok(Arrival::Uniform),
            other => bail!("unknown arrival process '{other}' (expected poisson or uniform)"),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Arrival::Poisson => "poisson",
            Arrival::Uniform => "uniform",
        }
    }
}

/// One offered-rate point, held for `duration`.
///
/// A run is a list of these. One entry is a flat run at a fixed rate; several are a ramp,
/// which is how the knee gets found in a single pass instead of by re-running by hand.
#[derive(Debug, Clone, Copy)]
pub struct Step {
    /// Searches offered per second.
    pub search: f64,
    /// Writes offered per second — single writes or bulk requests, whichever the mode runs.
    pub write: f64,
    pub duration: Duration,
}

impl Step {
    /// The rate for a workload that is not running is zero, so this is what decides whether a
    /// scheduler is started at all.
    pub fn rate_for(&self, mode: Mode, workload: Workload) -> f64 {
        match workload {
            Workload::Search if mode.searches() => self.search,
            Workload::Write if mode.writes() => self.write,
            Workload::Bulk if mode.bulk() => self.write,
            _ => 0.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Workload {
    Search,
    Write,
    Bulk,
}

#[derive(Debug)]
pub struct OpenLoop {
    pub steps: Vec<Step>,
    pub arrival: Arrival,
    /// Ceiling on requests outstanding at once.
    ///
    /// Not a concurrency limit in the closed-loop sense — the generator never *waits* for a
    /// permit, because waiting is precisely what makes a harness closed-loop. It drops the
    /// arrival and counts it, and a run that drops any is reported as invalid rather than
    /// quietly rescaled.
    pub max_in_flight: usize,
    /// Seeds the arrival stream. Fixed by default so two runs offer the same load.
    pub seed: u64,
}

impl OpenLoop {
    pub fn total_duration(&self) -> Duration {
        self.steps.iter().map(|step| step.duration).sum()
    }
}

/// Which load model the run uses. The closed-loop path is unchanged and still the default:
/// every performance figure this repository has published was taken with it, and they stay
/// comparable only if it keeps behaving exactly as it did.
#[derive(Debug)]
pub enum Load {
    Closed { concurrency: usize },
    Open(OpenLoop),
}

#[derive(Debug)]
pub struct Args {
    pub url: String,
    pub index: String,
    pub mode: Mode,
    pub load: Load,
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
cameodb-bench — load harness for CameoDB, and a worked example of the client SDK

USAGE:
    cameodb-bench [OPTIONS]

OPTIONS:
    --url <URL>            Node to test            (default: http://localhost:9480)
    --index <NAME>         Index to use            (default: bench)
    --mode <MODE>          write | bulk | search | mixed  (default: mixed)
    --duration <SECS>      Measured run length, or length of each rate step (default: 20)
    --warmup <SECS>        Unmeasured run first    (default: 5)
    --seed-docs <N>        Documents pre-loaded before measuring (default: 5000)
    --batch-size <N>       Documents per request in bulk mode (default: 500)
    --api-key <KEY>        Bearer key, or set CAMEODB_API_KEY
    --insecure             Accept an invalid TLS certificate
    --keep-index           Leave the index behind instead of deleting it
    -h, --help             This text

CLOSED LOOP (default):
    --concurrency <N>      In-flight requests per workload (default: 8)

    N workers each issue one request at a time and wait for the answer. Measures service
    time at a fixed concurrency. A saturated node shows up as rising latency rather than a
    growing queue, because the harness stops offering load while it waits. Compare runs at
    equal concurrency; these numbers are not an SLA.

OPEN LOOP (--rate):
    --rate <PER_SEC>       Requests offered per second, per workload
    --search-rate <N>      Override --rate for searches
    --write-rate <N>       Override --rate for writes (single or bulk)
    --rate-steps <a,b,c>   Ramp through these rates, --duration seconds each
    --arrival <PROCESS>    poisson | uniform       (default: poisson)
    --max-in-flight <N>    Harness safety ceiling  (default: 50000)
    --seed <N>             Arrival-stream seed     (default: 1)

    Arrivals are generated from the rate and do not wait for responses, so the node's queue
    is allowed to grow — which is the measurement. Latency is reported twice: `service` is
    sent-to-answered, `total` is intended-send-to-answered. The second is the one an SLA is
    written against; where they diverge, the gap is queueing the closed-loop mode cannot see.

    The generator spins to hit sub-millisecond arrival times, so it reserves a core. Run it
    off-box where the result matters, and read the reported harness lag before believing any
    number: if lag is climbing while in-flight is below the ceiling, the measurement is of
    this harness and not of the node.

    --rate and --concurrency are mutually exclusive.
";

pub fn parse() -> Result<Option<Args>> {
    let mut url = "http://localhost:9480".to_string();
    let mut index = "bench".to_string();
    let mut mode = Mode::Mixed;
    let mut concurrency: Option<usize> = None;
    let mut duration = 20u64;
    let mut warmup = 5u64;
    let mut seed_docs = 5_000usize;
    let mut batch_size = 500usize;
    let mut api_key = std::env::var("CAMEODB_API_KEY").ok();
    let mut insecure = false;
    let mut keep_index = false;

    let mut rate: Option<f64> = None;
    let mut search_rate: Option<f64> = None;
    let mut write_rate: Option<f64> = None;
    let mut rate_steps: Option<Vec<f64>> = None;
    let mut arrival = Arrival::Poisson;
    let mut max_in_flight = 50_000usize;
    let mut seed = 1u64;

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
            "--concurrency" => concurrency = Some(value()?.parse()?),
            "--duration" => duration = value()?.parse()?,
            "--warmup" => warmup = value()?.parse()?,
            "--seed-docs" => seed_docs = value()?.parse()?,
            "--batch-size" => batch_size = value()?.parse()?,
            "--api-key" => api_key = Some(value()?),
            "--insecure" => insecure = true,
            "--keep-index" => keep_index = true,
            "--rate" => rate = Some(parse_rate(&value()?)?),
            "--search-rate" => search_rate = Some(parse_rate(&value()?)?),
            "--write-rate" => write_rate = Some(parse_rate(&value()?)?),
            "--rate-steps" => rate_steps = Some(parse_rate_steps(&value()?)?),
            "--arrival" => arrival = Arrival::parse(&value()?)?,
            "--max-in-flight" => max_in_flight = value()?.parse()?,
            "--seed" => seed = value()?.parse()?,
            other => bail!("unknown argument '{other}' (try --help)"),
        }
    }

    if duration == 0 {
        bail!("--duration must be at least 1 second");
    }
    if batch_size == 0 {
        bail!("--batch-size must be at least 1");
    }

    let load = resolve_load(
        concurrency,
        rate,
        search_rate,
        write_rate,
        rate_steps,
        arrival,
        max_in_flight,
        seed,
        Duration::from_secs(duration),
    )?;

    Ok(Some(Args {
        url,
        index,
        mode,
        load,
        duration: Duration::from_secs(duration),
        warmup: Duration::from_secs(warmup),
        seed_docs,
        batch_size,
        api_key,
        insecure,
        keep_index,
    }))
}

fn parse_rate(raw: &str) -> Result<f64> {
    let rate: f64 = raw
        .parse()
        .map_err(|_| anyhow!("'{raw}' is not a rate (requests per second)"))?;
    if !rate.is_finite() || rate <= 0.0 {
        bail!("a rate must be a positive number of requests per second, got '{raw}'");
    }
    Ok(rate)
}

fn parse_rate_steps(raw: &str) -> Result<Vec<f64>> {
    let steps: Vec<f64> = raw
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(parse_rate)
        .collect::<Result<_>>()?;
    if steps.is_empty() {
        bail!("--rate-steps needs at least one rate, e.g. --rate-steps 1000,2000,4000");
    }
    Ok(steps)
}

/// Decide which load model the flags describe, and refuse the combinations that would mean
/// two things at once.
#[allow(clippy::too_many_arguments)]
fn resolve_load(
    concurrency: Option<usize>,
    rate: Option<f64>,
    search_rate: Option<f64>,
    write_rate: Option<f64>,
    rate_steps: Option<Vec<f64>>,
    arrival: Arrival,
    max_in_flight: usize,
    seed: u64,
    step_duration: Duration,
) -> Result<Load> {
    let wants_open =
        rate.is_some() || search_rate.is_some() || write_rate.is_some() || rate_steps.is_some();

    if !wants_open {
        let concurrency = concurrency.unwrap_or(8);
        if concurrency == 0 {
            bail!("--concurrency must be at least 1");
        }
        return Ok(Load::Closed { concurrency });
    }

    if concurrency.is_some() {
        bail!(
            "--concurrency and the open-loop rate flags describe different load models and \
             cannot be combined. --concurrency holds a fixed number of requests in flight; \
             --rate offers requests on a schedule regardless of how many are outstanding"
        );
    }
    if max_in_flight == 0 {
        bail!("--max-in-flight must be at least 1");
    }

    let steps = match rate_steps {
        Some(values) => {
            if search_rate.is_some() || write_rate.is_some() {
                bail!(
                    "--rate-steps offers the same rate to every workload, so it cannot be \
                     combined with --search-rate or --write-rate. Run one rate at a time to \
                     vary them independently"
                );
            }
            values
                .into_iter()
                .map(|rate| Step {
                    search: rate,
                    write: rate,
                    duration: step_duration,
                })
                .collect()
        }
        None => {
            // A per-workload override stands on its own: `--search-rate 5000` with no `--rate`
            // means searches at 5000/s and no writes offered, which is what the mode would
            // have to say twice otherwise.
            let base = rate.unwrap_or(0.0);
            vec![Step {
                search: search_rate.unwrap_or(base),
                write: write_rate.unwrap_or(base),
                duration: step_duration,
            }]
        }
    };

    Ok(Load::Open(OpenLoop {
        steps,
        arrival,
        max_in_flight,
        seed,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open(
        rate: Option<f64>,
        search: Option<f64>,
        write: Option<f64>,
        steps: Option<Vec<f64>>,
    ) -> Result<Load> {
        resolve_load(
            None,
            rate,
            search,
            write,
            steps,
            Arrival::Poisson,
            50_000,
            1,
            Duration::from_secs(20),
        )
    }

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

    /// The default has to stay exactly what it was. Every published figure in the ROADMAP was
    /// taken closed-loop at a concurrency, and they stop being comparable the moment a bare
    /// invocation means something else.
    #[test]
    fn no_rate_flag_leaves_the_closed_loop_default_alone() {
        let load = resolve_load(
            None,
            None,
            None,
            None,
            None,
            Arrival::Poisson,
            50_000,
            1,
            Duration::from_secs(20),
        )
        .unwrap();
        match load {
            Load::Closed { concurrency } => assert_eq!(concurrency, 8),
            Load::Open(_) => panic!("a bare invocation must stay closed-loop"),
        }
    }

    /// The two models cannot be blended, and saying so is better than silently honouring one.
    #[test]
    fn concurrency_and_rate_are_refused_together() {
        let err = resolve_load(
            Some(8),
            Some(1_000.0),
            None,
            None,
            None,
            Arrival::Poisson,
            50_000,
            1,
            Duration::from_secs(20),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("different load models"), "{err}");
    }

    #[test]
    fn a_bare_rate_is_offered_to_every_running_workload() {
        let Load::Open(open) = open(Some(1_000.0), None, None, None).unwrap() else {
            panic!("expected an open-loop run");
        };
        assert_eq!(open.steps.len(), 1);
        assert_eq!(open.steps[0].search, 1_000.0);
        assert_eq!(open.steps[0].write, 1_000.0);
    }

    /// Reads and writes interfere, and the ratio between them is the variable. Pinning one
    /// side while sweeping the other is the reason the overrides exist.
    #[test]
    fn per_workload_rates_override_the_base() {
        let Load::Open(open) = open(Some(1_000.0), Some(5_000.0), None, None).unwrap() else {
            panic!("expected an open-loop run");
        };
        assert_eq!(open.steps[0].search, 5_000.0);
        assert_eq!(open.steps[0].write, 1_000.0);
    }

    /// An override on its own offers nothing to the other workload, rather than falling back
    /// to some default rate nobody asked for.
    #[test]
    fn an_override_alone_leaves_the_other_workload_at_zero() {
        let Load::Open(open) = open(None, Some(5_000.0), None, None).unwrap() else {
            panic!("expected an open-loop run");
        };
        assert_eq!(open.steps[0].search, 5_000.0);
        assert_eq!(open.steps[0].write, 0.0);
    }

    #[test]
    fn a_ramp_becomes_one_step_per_rate_and_sums_its_duration() {
        let Load::Open(open) =
            open(None, None, None, Some(vec![1_000.0, 2_000.0, 4_000.0])).unwrap()
        else {
            panic!("expected an open-loop run");
        };
        assert_eq!(open.steps.len(), 3);
        assert_eq!(open.steps[2].search, 4_000.0);
        assert_eq!(open.total_duration(), Duration::from_secs(60));
    }

    #[test]
    fn a_ramp_refuses_per_workload_overrides() {
        let err = open(None, Some(100.0), None, Some(vec![1_000.0]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("--rate-steps"), "{err}");
    }

    #[test]
    fn a_rate_must_be_a_positive_number() {
        assert!(parse_rate("0").is_err());
        assert!(parse_rate("-5").is_err());
        assert!(parse_rate("fast").is_err());
        assert_eq!(parse_rate("1500.5").unwrap(), 1500.5);
    }

    /// A workload the mode does not run is offered nothing, whatever the rate says — so a
    /// `--mode search --rate 1000` run does not quietly start a write scheduler.
    #[test]
    fn a_step_offers_nothing_to_a_workload_the_mode_excludes() {
        let step = Step {
            search: 1_000.0,
            write: 1_000.0,
            duration: Duration::from_secs(1),
        };
        assert_eq!(step.rate_for(Mode::Search, Workload::Search), 1_000.0);
        assert_eq!(step.rate_for(Mode::Search, Workload::Write), 0.0);
        assert_eq!(step.rate_for(Mode::Search, Workload::Bulk), 0.0);
        assert_eq!(step.rate_for(Mode::Bulk, Workload::Bulk), 1_000.0);
        assert_eq!(step.rate_for(Mode::Bulk, Workload::Write), 0.0);
        assert_eq!(step.rate_for(Mode::Mixed, Workload::Write), 1_000.0);
    }
}
