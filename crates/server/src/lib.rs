//! Trail's server: the HTTP API, the rollup aggregator, alerts and the housekeeping timers.
//!
//! [`App`] is everything except sockets and clocks: each of its entry points takes `now`, so tests
//! drive it directly. [`Server`] wraps an `App` with HTTP workers and timer threads.
//!
//! Trail is crash-only: there is no shutdown step to get right. SQLite's log makes writes safe to
//! interrupt, open rollup buckets are rebuilt from raw events at start, and senders resend what
//! was not acknowledged (ids make that a no-op), so killing the process loses nothing it had
//! answered for.
//!
//! - `ingest`: the write path.
//! - `rollup`, `sketch`: declared aggregations, computed as events arrive.
//! - `alerts`: rules and the webhook.
//! - `api`, `http`: the read API and request plumbing.
//! - `meters`: Trail's own metrics.

pub mod alerts;
pub mod api;
mod http;
pub mod ingest;
pub mod meters;
pub mod rollup;
pub mod sketch;

pub use http::{MAX_CONNECTIONS, Patience, split_url};

use alerts::Alerts;
use meters::{Meters, gauge};
use rollup::Aggregator;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use trail_core::config::Config;
use trail_core::decl::{self, Declarations, Files, Retention};
use trail_core::envelope::{Level, LogRecord, Object};
use trail_core::registry::Registry;
use trail_core::{Problem, Ts};
use trail_store::{Batch, Store};

/// The source Trail reports about itself under. Nobody else may publish or write as it.
pub const OWN_SOURCE: &str = "trail";
/// The state key under which the rollup timer saves how far it has got (ms).
const ROLLUPS_THROUGH: &str = "rollups_through";
/// How far before a saved mark the catch-up at start looks: an event's arrival time is taken
/// before it is stored, so one stored just after a mark may carry a time just before it.
const CATCH_UP_MARGIN: i64 = 10_000;

/// Locks a mutex. A thread that panicked while holding it does not take the server down with it:
/// everything behind these locks is rebuilt from the store.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Trail's own declarations, built into the binary.
pub fn own_declarations() -> Result<Declarations, Vec<Problem>> {
    decl::parse(&Files {
        events: include_str!("../trail/events.toml").to_string(),
        metrics: Some(include_str!("../trail/metrics.toml").to_string()),
        rollups: None,
        alerts: None,
    })
}

/// The server without its sockets and timers.
pub struct App {
    pub config: Config,
    pub store: Store,
    registry: RwLock<Registry>,
    pub(crate) rollups: Mutex<Aggregator>,
    pub(crate) alerts: Mutex<Alerts>,
    /// The alert state as last saved, to skip writes when nothing changed.
    pub(crate) alerts_saved: Mutex<String>,
    /// How many days the last archive and retention run could not finish. -1 before the first run.
    pub(crate) archive_failing: std::sync::atomic::AtomicI64,
    /// New events stored since start. The rollup timer compares it with what it last saw, to
    /// move its mark only when something arrived.
    pub(crate) events_seen: std::sync::atomic::AtomicU64,
    pub(crate) events_rolled: std::sync::atomic::AtomicU64,
    pub meters: Meters,
    pub started: Ts,
    /// Whether `log` also prints to stderr. Off in tests.
    pub quiet: bool,
}

/// Why declarations were not published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unpublished {
    /// They fail the check (with `keep`, once added to what the source already declares): 400.
    Invalid(Vec<Problem>),
    /// A name belongs to another source or to Trail, or the store failed: 409.
    Conflict(Vec<Problem>),
}

/// What `rebuild_rollup` did.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Rebuilt {
    pub rollup: String,
    pub events: u64,
    pub buckets: u64,
    pub samples: u64,
    /// The first bucket rebuilt (ms). None when the event has never been sent.
    pub from: Option<Ts>,
}

impl App {
    /// Opens the store, loads the registry and rebuilds the open rollup buckets.
    pub fn open(config: Config, now: Ts) -> Result<App, Problem> {
        let store = Store::open(&config).map_err(|e| e.problem())?;
        let (sources, unreadable) = store.registry().map_err(|e| e.problem())?;
        let mut registry = Registry::from_sources(sources);
        match own_declarations() {
            Ok(own) => registry.set(own),
            Err(problems) => {
                return Err(problems
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| Problem::new("trail", "the built-in declarations did not load", "")));
            }
        }
        let alerts = Alerts::load(&store);
        let app = App {
            config,
            store,
            registry: RwLock::new(registry),
            rollups: Mutex::new(Aggregator::new()),
            alerts: Mutex::new(alerts),
            alerts_saved: Mutex::new(String::new()),
            archive_failing: std::sync::atomic::AtomicI64::new(-1),
            events_seen: std::sync::atomic::AtomicU64::new(0),
            events_rolled: std::sync::atomic::AtomicU64::new(0),
            meters: Meters::default(),
            started: now,
            quiet: false,
        };
        for error in unreadable {
            app.log(Level::Error, &error.to_string());
        }
        app.sync_rollups(now)?;
        app.catch_up();
        app.start_marks(now)?;
        Ok(app)
    }

    /// On a first start there is nothing to catch up on, but there will be at the next one:
    /// save where the timers start from, in case Trail stops before they first run. The event
    /// rules start after the last event stored, so a first start, or the first start of a Trail
    /// that keeps that mark, raises no notices for events from before it.
    fn start_marks(&self, now: Ts) -> Result<(), Problem> {
        if !matches!(self.store.state(ROLLUPS_THROUGH), Ok(Some(_))) {
            let _ = self.store.set_state(ROLLUPS_THROUGH, &now.ms().to_string());
        }
        let mut alerts = lock(&self.alerts);
        if alerts.noticed_seq.is_none() {
            alerts.noticed_seq = Some(self.store.last_seq().map_err(|e| e.problem())?);
            drop(alerts);
            self.save_alerts();
        }
        Ok(())
    }

    /// Acts on events that were stored shortly before Trail last stopped but not yet rolled up:
    /// the rollup timer saves how far it had got, and this looks again at what arrived after
    /// that. (Event rules need no catching up: they carry on after the last seq they saw.)
    fn catch_up(&self) {
        let Some(since) = self
            .store
            .state(ROLLUPS_THROUGH)
            .ok()
            .flatten()
            .and_then(|text| text.parse::<i64>().ok())
        else {
            return;
        };
        let events = match self.store.events_received_since(Ts(since - CATCH_UP_MARGIN)) {
            Ok(events) => events,
            Err(error) => {
                self.log(
                    Level::Error,
                    &format!("could not look for events stored just before the last stop: {error}"),
                );
                return;
            }
        };
        let mut rollups = lock(&self.rollups);
        for event in &events {
            rollups.catch_up(event, &self.config.zone);
        }
    }

    /// The registry, for reading.
    pub fn registry(&self) -> RwLockReadGuard<'_, Registry> {
        self.registry.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn sync_rollups(&self, now: Ts) -> Result<(), Problem> {
        let mut rollups = lock(&self.rollups);
        let registry = self.registry();
        rollups
            .sync(
                &registry,
                &self.store,
                &self.config.zone,
                now,
                self.config.limits.rollup_groups,
            )
            .map_err(|e| e.problem())
    }

    /// Loads one source's declarations (`PUT /v1/registry/{source}`), replacing all it declared.
    /// Its drift report starts again, since the new declarations may have settled the old
    /// mismatches.
    pub fn publish(&self, decl: Declarations, now: Ts) -> Result<(), Vec<Problem>> {
        self.publish_with(decl, false, now)
            .map(|_| ())
            .map_err(|refused| match refused {
                Unpublished::Invalid(problems) | Unpublished::Conflict(problems) => problems,
            })
    }

    /// Like [`App::publish`]; with `keep`, adds to what the source already declares instead of
    /// replacing it (`?keep=true`, see [`Declarations::keeping`]). Returns what the source
    /// declares now.
    pub fn publish_with(&self, decl: Declarations, keep: bool, now: Ts) -> Result<Declarations, Unpublished> {
        if decl.source == OWN_SOURCE {
            return Err(Unpublished::Conflict(vec![Problem::new(
                "source",
                "`trail` is the server's own source",
                "name your service something else in events.toml",
            )]));
        }
        let decl = {
            let mut registry = self.registry.write().unwrap_or_else(PoisonError::into_inner);
            let decl = match registry.get(&decl.source) {
                Some(existing) if keep => {
                    let merged = decl.keeping(existing);
                    let problems = decl::validate(&merged);
                    if !problems.is_empty() {
                        return Err(Unpublished::Invalid(problems));
                    }
                    merged
                }
                _ => decl,
            };
            let conflicts = registry.conflicts(&decl);
            if !conflicts.is_empty() {
                return Err(Unpublished::Conflict(conflicts));
            }
            self.store
                .put_declarations(&decl)
                .map_err(|e| Unpublished::Conflict(vec![e.problem()]))?;
            registry.set(decl.clone());
            decl
        };
        // Best effort: a failure here only delays the report's reset.
        let _ = self.store.clear_problems(&decl.source);
        self.sync_rollups(now)
            .map_err(|p| Unpublished::Conflict(vec![p]))?;
        Ok(decl)
    }

    /// Removes one source's declarations. Its records stay; they are no longer checked.
    pub fn unpublish(&self, source: &str, now: Ts) -> Result<bool, Problem> {
        if source == OWN_SOURCE {
            return Err(Problem::new(
                "source",
                "`trail` is the server's own source and cannot be removed",
                "",
            ));
        }
        let removed = {
            let mut registry = self.registry.write().unwrap_or_else(PoisonError::into_inner);
            let removed = self.store.delete_declarations(source).map_err(|e| e.problem())?;
            registry.remove(source);
            removed
        };
        self.sync_rollups(now)?;
        Ok(removed)
    }

    /// Records something Trail itself wants known: to stderr, and to its own log store.
    pub fn log(&self, level: Level, message: &str) {
        if !self.quiet {
            eprintln!("trail {}: {message}", level.as_str());
        }
        let now = Ts::now();
        let record = LogRecord {
            id: trail_core::ids::uuid7(now),
            ts: now,
            received_at: now,
            source: OWN_SOURCE.to_string(),
            host: None,
            stream: None,
            level,
            message: message.to_string(),
            fields: Object::new(),
            seq: None,
        };
        let batch = Batch {
            logs: vec![record],
            ..Batch::default()
        };
        if let Err(error) = self.store.write(&batch)
            && !self.quiet
        {
            eprintln!("trail error: could not store that log line: {error}");
        }
    }

    /// Writes the rollup buckets that changed and recomputes the ones late events touched.
    pub fn tick_rollups(&self, now: Ts) {
        use std::sync::atomic::Ordering;
        let (flushed, dirty, events) = {
            let mut rollups = lock(&self.rollups);
            // Read under the lock: every event counted here has been seen by the rollups.
            let events = self.events_seen.load(Ordering::Relaxed);
            (
                rollups.flush(now, &self.config.zone),
                rollups.take_dirty(),
                events,
            )
        };
        let mut complete = true;
        for (id, start, points) in flushed {
            let batch = Batch {
                points,
                ..Batch::default()
            };
            if let Err(problem) = self.commit(&batch) {
                complete = false;
                lock(&self.rollups).mark_dirty(&id, start);
                self.log(
                    Level::Error,
                    &format!("rollup `{id}` could not be written: {problem}"),
                );
            }
        }
        // A phone back from a week offline dirties thousands of buckets. They are redone a
        // rollup and a local day at a time: one read of that day's events, not one per bucket.
        let mut groups: BTreeMap<(String, trail_core::time::Day), (rollup::Spec, Vec<Ts>)> = BTreeMap::new();
        for (spec, start) in dirty {
            groups
                .entry((spec.id.clone(), self.config.zone.day_of(start)))
                .or_insert_with(|| (spec, Vec::new()))
                .1
                .push(start);
        }
        for (spec, starts) in groups.into_values() {
            let (Some(first), Some(last)) = (starts.iter().min(), starts.iter().max()) else {
                continue;
            };
            let end = self.config.zone.next(*last, spec.decl.every);
            let computed = rollup::compute_range(
                &self.store,
                &self.config.zone,
                &spec,
                *first,
                end,
                self.config.limits.rollup_groups,
            )
            .map_err(|e| e.problem());
            let mut failed: Option<Problem> = None;
            match computed {
                Ok((_, mut buckets)) => {
                    for start in &starts {
                        let bucket = buckets.remove(start).unwrap_or_default();
                        let batch = Batch {
                            points: rollup::points_of(&spec, *start, &bucket),
                            ..Batch::default()
                        };
                        if let Err(problem) = self.commit(&batch) {
                            lock(&self.rollups).mark_dirty(&spec.id, *start);
                            failed = Some(problem);
                        }
                    }
                }
                Err(problem) => {
                    let mut rollups = lock(&self.rollups);
                    for start in &starts {
                        rollups.mark_dirty(&spec.id, *start);
                    }
                    failed = Some(problem);
                }
            }
            if let Some(problem) = failed {
                complete = false;
                self.log(
                    Level::Error,
                    &format!("rollup `{}` could not be recomputed: {problem}", spec.id),
                );
            }
        }
        // Everything that arrived before this tick is now in the stored samples. Saved only
        // when something arrived, so an idle Trail writes nothing.
        if complete
            && events != self.events_rolled.load(Ordering::Relaxed)
            && self
                .store
                .set_state(ROLLUPS_THROUGH, &now.ms().to_string())
                .is_ok()
        {
            self.events_rolled.store(events, Ordering::Relaxed);
        }
    }

    /// Recomputes one rollup from the raw events, a local day at a time, as far back as they are
    /// kept (or from `since`).
    pub fn rebuild_rollup(&self, id: &str, since: Option<Ts>, now: Ts) -> Result<Rebuilt, Problem> {
        let spec = {
            let rollups = lock(&self.rollups);
            match rollups.spec(id) {
                Some(spec) => spec.clone(),
                None => {
                    let known: Vec<String> = rollups.specs().map(|s| s.id.clone()).collect();
                    let hint = trail_core::problem::closest(id, known.iter().map(String::as_str))
                        .map(|c| format!("did you mean `{c}`? "))
                        .unwrap_or_default();
                    return Err(Problem::new(
                        "rollup",
                        format!("no rollup `{id}` is published"),
                        format!(
                            "{hint}`GET /v1/rollups` lists them; publish the source's declarations first"
                        ),
                    ));
                }
            }
        };
        let zone = &self.config.zone;
        let end = now.plus(1);
        let mut first: Option<i64> = None;
        self.store
            .scan_events(&spec.decl.from, since.unwrap_or(Ts(0)), end, &mut |event| {
                first = Some(first.map_or(event.ts.ms(), |ts: i64| ts.min(event.ts.ms())));
            })
            .map_err(|e| e.problem())?;
        let mut report = Rebuilt {
            rollup: id.to_string(),
            events: 0,
            buckets: 0,
            samples: 0,
            from: None,
        };
        let Some(first) = first else {
            return Ok(report);
        };
        let first = zone.floor(Ts(first), spec.decl.every);
        report.from = Some(first);
        let mut day = zone.day_of(first);
        let last = zone.day_of(now);
        while day <= last {
            let (lo, hi) = (zone.day_start(day), zone.day_end(day));
            let (events, buckets) = rollup::compute_range(
                &self.store,
                zone,
                &spec,
                lo,
                hi.min(end),
                self.config.limits.rollup_groups,
            )
            .map_err(|e| e.problem())?;
            report.events += events;
            for (start, bucket) in buckets {
                let points = rollup::points_of(&spec, start, &bucket);
                report.buckets += 1;
                report.samples += points.len() as u64;
                self.commit(&Batch {
                    points,
                    ..Batch::default()
                })?;
            }
            let next = zone.next_day(day);
            if next == day {
                break;
            }
            day = next;
        }
        // Events that arrived during the scan are in the open buckets and maybe not in what
        // was just written: the next flush writes the open buckets again, from memory.
        lock(&self.rollups).rewrite_open(id);
        Ok(report)
    }

    /// Writes Trail's own metrics and keeps the metric tiers up to date.
    pub fn tick_metrics(&self, now: Ts) {
        let mut points = self.meters.readings(now);
        #[allow(clippy::cast_precision_loss)]
        match self.store.stats() {
            Ok(stats) => {
                for store in &stats.stores {
                    let label = [("store", store.store.as_str())];
                    points.push(gauge("trail.store.rows", &label, store.rows as f64, now));
                    points.push(gauge("trail.archive.files", &label, store.files as f64, now));
                    points.push(gauge("trail.archive.bytes", &label, store.file_bytes as f64, now));
                    let caps = &self.config.caps;
                    let cap = match store.store.as_str() {
                        "events" => caps.events,
                        "samples" => caps.metrics,
                        "errors" => caps.errors,
                        "logs" => caps.logs,
                        _ => None,
                    };
                    if let Some(cap) = cap.filter(|cap| *cap > 0) {
                        let used = store.file_bytes as f64 / cap as f64;
                        points.push(gauge("trail.archive.cap_used", &label, used, now));
                    }
                }
                points.push(gauge("trail.db.bytes", &[], stats.db_bytes as f64, now));
                points.push(gauge("trail.series", &[], stats.series as f64, now));
            }
            Err(error) => self.log(
                Level::Error,
                &format!("could not read the store's sizes: {error}"),
            ),
        }
        {
            let rollups = lock(&self.rollups);
            let tally = rollups.tally();
            #[allow(clippy::cast_precision_loss)]
            for (name, counts) in [
                ("trail.rollup.late", &tally.late),
                ("trail.rollup.overflow", &tally.overflow),
            ] {
                for (rollup, n) in counts {
                    let mut point = gauge(name, &[("rollup", rollup)], *n as f64, now);
                    point.counter = true;
                    point.point.kind = Some(trail_core::envelope::MetricKind::Counter);
                    points.push(point);
                }
            }
        }
        let failing = self.archive_failing.load(std::sync::atomic::Ordering::Relaxed);
        if failing >= 0 {
            #[allow(clippy::cast_precision_loss)]
            points.push(gauge("trail.archive.failing", &[], failing as f64, now));
        }
        #[allow(clippy::cast_precision_loss)]
        points.push(gauge(
            "trail.alerts.firing",
            &[],
            lock(&self.alerts).firing.len() as f64,
            now,
        ));
        let batch = Batch {
            points,
            ..Batch::default()
        };
        if let Err(problem) = self.commit(&batch) {
            self.log(
                Level::Error,
                &format!("could not write Trail's own metrics: {problem}"),
            );
        }
        if let Err(error) = self.store.maintain_tiers(now) {
            self.log(
                Level::Error,
                &format!("the metric tiers could not be updated: {error}"),
            );
        }
    }

    /// Writes day files for finished days, lets old rows leave SQLite, and applies retention.
    pub fn tick_archive(&self, now: Ts) {
        // A copy, so a long archive run never holds the registry lock.
        let retention: HashMap<String, Retention> = {
            let registry = self.registry();
            registry
                .sources()
                .flat_map(|d| {
                    d.events
                        .iter()
                        .map(|(name, event)| (name.clone(), event.retention))
                })
                .collect()
        };
        let mut failing = 0usize;
        match self.store.archive(now, &|name| {
            retention.get(name).copied().unwrap_or(Retention::Forever)
        }) {
            Ok(report) => {
                for file in &report.written {
                    self.log(
                        Level::Info,
                        &format!(
                            "archived {} {}: {} rows, {} bytes",
                            file.class, file.day, file.rows, file.bytes
                        ),
                    );
                }
                failing += report.failures.len();
                for failure in &report.failures {
                    self.log(Level::Error, &format!("a day could not be archived: {failure}"));
                }
            }
            Err(error) => {
                failing += 1;
                self.log(Level::Error, &format!("the archive run failed: {error}"));
            }
        }
        match self.store.expire(now) {
            Ok(report) => {
                for file in &report.deleted {
                    self.log(
                        Level::Info,
                        &format!("deleted {} {} (retention or cap)", file.class, file.day),
                    );
                }
                failing += report.failures.len();
                for failure in &report.failures {
                    self.log(
                        Level::Error,
                        &format!("retention could not be applied: {failure}"),
                    );
                }
            }
            Err(error) => {
                failing += 1;
                self.log(Level::Error, &format!("the retention run failed: {error}"));
            }
        }
        self.archive_failing.store(
            i64::try_from(failing).unwrap_or(i64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

/// How often each timer runs.
#[derive(Debug, Clone, Copy)]
pub struct Timing {
    pub rollups: Duration,
    pub metrics: Duration,
    pub archive: Duration,
    pub alerts: Duration,
    /// Queries worked on at once, beside the one writer.
    pub workers: usize,
    /// How long a connection may take to send a request or take its answer.
    pub patience: Patience,
}

impl Timing {
    pub fn from_config(config: &Config) -> Timing {
        let ms = |value: i64| Duration::from_millis(u64::try_from(value.max(1000)).unwrap_or(1000));
        Timing {
            rollups: Duration::from_secs(5),
            metrics: ms(config.min_step_ms),
            archive: Duration::from_secs(600),
            alerts: ms(config.alerts.interval_ms),
            workers: config.workers,
            patience: Patience::default(),
        }
    }
}

/// One timer's work.
type Job = fn(&App, Ts);

/// A running Trail.
pub struct Server {
    app: Arc<App>,
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    open: Arc<http::Open>,
    threads: Vec<JoinHandle<()>>,
}

impl Server {
    /// Opens the store and starts listening, with the timers the config asks for.
    pub fn start(config: Config) -> Result<Server, Problem> {
        let timing = Timing::from_config(&config);
        Server::start_with(config, timing, false)
    }

    pub fn start_with(config: Config, timing: Timing, quiet: bool) -> Result<Server, Problem> {
        let listen = config.listen.clone();
        let mut app = App::open(config, Ts::now())?;
        app.quiet = quiet;
        let app = Arc::new(app);
        let cant_listen = |e: &dyn std::fmt::Display| {
            Problem::new(
                "listen",
                format!("can't listen on {listen}: {e}"),
                "another program has the port (is Trail already running?), or the address is not this machine's; change `listen` in trail.toml to an IP address and a port, like 127.0.0.1:8200",
            )
        };
        let listener = std::net::TcpListener::bind(&listen).map_err(|e| cant_listen(&e))?;
        let addr = listener.local_addr().map_err(|e| cant_listen(&e))?;
        let stop = Arc::new(AtomicBool::new(false));
        let open = Arc::new(http::Open::default());
        let mut threads = Vec::new();
        threads.push(
            http::serve(
                app.clone(),
                listener,
                Arc::new(http::Gate::new(timing.workers)),
                timing.patience,
                open.clone(),
                stop.clone(),
            )
            .map_err(|e| cant_listen(&e))?,
        );
        let timers: [(Duration, Duration, Job); 4] = [
            (timing.rollups, timing.rollups, App::tick_rollups),
            (timing.metrics, timing.metrics, App::tick_metrics),
            (timing.alerts, timing.alerts, App::tick_alerts),
            // The first archive run is soon after start, so a restart after midnight catches up.
            (
                timing.archive,
                Duration::from_secs(5).min(timing.archive),
                App::tick_archive,
            ),
        ];
        for (period, first, job) in timers {
            let (app, stop) = (app.clone(), stop.clone());
            threads.push(std::thread::spawn(move || {
                let mut wait = first;
                loop {
                    let until = Instant::now() + wait;
                    while Instant::now() < until {
                        if stop.load(Ordering::Relaxed) {
                            return;
                        }
                        std::thread::sleep(Duration::from_millis(100).min(wait));
                    }
                    job(&app, Ts::now());
                    wait = period;
                }
            }));
        }
        Ok(Server {
            app,
            addr,
            stop,
            open,
            threads,
        })
    }

    /// The address it listens on (useful when the config asked for port 0).
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn app(&self) -> &Arc<App> {
        &self.app
    }

    /// Stops the workers and timers and waits for them.
    pub fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        // The accepting thread is waiting for a caller: be one, so it sees the flag.
        let _ = std::net::TcpStream::connect(self.addr);
        self.open.close_all();
        for thread in self.threads {
            let _ = thread.join();
        }
    }

    /// Runs until the process is killed.
    pub fn wait(self) {
        for thread in self.threads {
            let _ = thread.join();
        }
    }
}
