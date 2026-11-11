//! A load generator for a running Trail: writer threads post batches of the shop example's
//! `shop.order.placed` events while reader threads run the three common lookups, for a fixed
//! time. Prints one line of JSON. `scripts/bench` starts the servers and calls this.
//!
//!   cargo run --release -p trail-server --example load -- --url http://127.0.0.1:8200 \
//!       --seconds 60 --writers 8 --readers 8 --batch 100

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::print_stdout)]
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const READS: [(&str, &str); 3] = [
    ("newest_events", "/v1/events?name=shop.order.placed&limit=100"),
    (
        "rollup_query",
        "/v1/metrics/query?name=shop.orders.count&since=-15m&step=1m&agg=sum",
    ),
    ("latest", "/v1/metrics/latest?name=shop.orders.count"),
];

fn now_ms() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
}

fn agent() -> ureq::Agent {
    ureq::Agent::new_with_config(
        ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(30)))
            .build(),
    )
}

/// The value at a share of a sorted list of microseconds, in milliseconds.
fn at(sorted: &[u32], share: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() - 1) as f64 * share).round() as usize;
    f64::from(sorted[index]) / 1000.0
}

fn summary(mut micros: Vec<u32>, seconds: f64) -> String {
    micros.sort_unstable();
    format!(
        r#"{{"per_s": {:.0}, "p50_ms": {:.2}, "p99_ms": {:.2}, "max_ms": {:.1}}}"#,
        micros.len() as f64 / seconds,
        at(&micros, 0.5),
        at(&micros, 0.99),
        at(&micros, 1.0)
    )
}

fn main() {
    let mut url = "http://127.0.0.1:8200".to_string();
    let (mut seconds, mut writers, mut readers, mut batch) = (60u64, 8usize, 8usize, 100usize);
    let mut args = std::env::args().skip(1);
    while let Some(name) = args.next() {
        let value = args.next().unwrap_or_else(|| panic!("{name} needs a value"));
        match name.as_str() {
            "--url" => url = value,
            "--seconds" => seconds = value.parse().unwrap(),
            "--writers" => writers = value.parse().unwrap(),
            "--readers" => readers = value.parse().unwrap(),
            "--batch" => batch = value.parse().unwrap(),
            other => panic!("unknown option {other}: use --url --seconds --writers --readers --batch"),
        }
    }
    let run = now_ms();
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let failed = Arc::new(AtomicU64::new(0));
    // The first few failures, with how long each took, to say what went wrong.
    let why: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let note = |why: &Mutex<Vec<String>>, what: String| {
        let mut why = why.lock().unwrap();
        if why.len() < 5 {
            why.push(what);
        }
    };

    let mut write_threads = Vec::new();
    for writer in 0..writers {
        let (url, failed, why) = (format!("{url}/v1/ingest"), failed.clone(), why.clone());
        write_threads.push(std::thread::spawn(move || {
            let agent = agent();
            let mut took = Vec::new();
            let mut n = 0u64;
            while Instant::now() < deadline {
                let ts = now_ms();
                let mut body = String::with_capacity(batch * 200);
                body.push_str(r#"{"source":"shop","version":"bench","host":"load","events":["#);
                for i in 0..batch {
                    n += 1;
                    if i > 0 {
                        body.push(',');
                    }
                    let method = if n.is_multiple_of(3) { "cash" } else { "card" };
                    body.push_str(&format!(
                        r#"{{"id":"b{run}-{writer}-{n}","name":"shop.order.placed","ts":{ts},"props":{{"order_id":"o-{writer}-{n}","total_cents":{},"method":"{method}","items":["tea","mug"],"latency_ms":{}}}}}"#,
                        500 + n % 4000,
                        20 + n % 300
                    ));
                }
                body.push_str("]}");
                let started = Instant::now();
                match agent.post(&url).send(body) {
                    Ok(mut response) if response.status().as_u16() == 200 => {
                        let _ = response.body_mut().read_to_string();
                        took.push(started.elapsed().as_micros() as u32);
                    }
                    other => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        let what = match other {
                            Ok(response) => format!("status {}", response.status()),
                            Err(error) => error.to_string(),
                        };
                        note(&why, format!("write after {} ms: {what}", started.elapsed().as_millis()));
                    }
                }
            }
            took
        }));
    }

    let mut read_threads = Vec::new();
    for reader in 0..readers {
        let (url, failed, why) = (url.clone(), failed.clone(), why.clone());
        read_threads.push(std::thread::spawn(move || {
            let agent = agent();
            let mut took: [Vec<u32>; 3] = [Vec::new(), Vec::new(), Vec::new()];
            let mut turn = reader;
            while Instant::now() < deadline {
                let kind = turn % READS.len();
                turn += 1;
                let started = Instant::now();
                match agent.get(&format!("{url}{}", READS[kind].1)).call() {
                    Ok(mut response) if response.status().as_u16() == 200 => {
                        let _ = response.body_mut().read_to_string();
                        took[kind].push(started.elapsed().as_micros() as u32);
                    }
                    other => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        let what = match other {
                            Ok(response) => format!("status {}", response.status()),
                            Err(error) => error.to_string(),
                        };
                        note(
                            &why,
                            format!("read after {} ms: {what}", started.elapsed().as_millis()),
                        );
                    }
                }
            }
            took
        }));
    }

    let mut writes = Vec::new();
    for thread in write_threads {
        writes.extend(thread.join().unwrap());
    }
    let mut reads: [Vec<u32>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    for thread in read_threads {
        for (kind, took) in thread.join().unwrap().into_iter().enumerate() {
            reads[kind].extend(took);
        }
    }
    let secs = seconds as f64;
    let events = writes.len() * batch;
    let all_reads: Vec<u32> = reads.iter().flatten().copied().collect();
    let by_kind: Vec<String> = READS
        .iter()
        .zip(reads)
        .map(|((name, _), took)| format!(r#""{name}": {}"#, summary(took, secs)))
        .collect();
    println!(
        r#"{{"seconds": {seconds}, "writers": {writers}, "readers": {readers}, "batch": {batch}, "events": {events}, "events_per_s": {:.0}, "writes": {}, "reads": {}, {}, "failed": {}, "why": {:?}}}"#,
        events as f64 / secs,
        summary(writes, secs),
        summary(all_reads, secs),
        by_kind.join(", "),
        failed.load(Ordering::Relaxed),
        why.lock().unwrap()
    );
}
