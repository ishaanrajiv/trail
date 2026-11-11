//! `trail serve`, and the made-up data behind `--seed`.

use crate::args::Args;
use crate::{Done, out};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use trail_core::config::Config;
use trail_core::decl::{self, Files};
use trail_core::time::{HOUR, MINUTE, SECOND};
use trail_core::{Problem, Ts};
use trail_server::{App, Server};

/// `trail serve [--config FILE] [--seed]`
pub fn serve(raw: Vec<String>) -> Done {
    let args = Args::parse("serve", raw, &["config"], &["seed"])?;
    let seed = args.flag("seed");
    let config = match args.value("config") {
        Some(path) => Config::load(Path::new(path))?,
        // A seeded server with no config gets a folder of its own, so it never touches real data.
        None if seed => {
            let dir: PathBuf = std::env::temp_dir().join(format!("trail-seed-{}", std::process::id()));
            Config::default_in(&dir)
        }
        None => Config::default_in(Path::new("./trail-data")),
    };
    let data_dir = config.data_dir.display().to_string();
    let server = Server::start(config).map_err(|p| vec![p])?;
    eprintln!("trail: listening on {}, data in {data_dir}", server.url());
    if seed {
        fill(server.app()).map_err(|p| vec![p])?;
        out(&format!(
            "seeded: three hours of the example `shop` service. Try:\n  \
             trail get /v1 --url {url}\n  \
             trail get '/v1/events?name=shop.order.*&limit=3' --url {url}\n  \
             trail get '/v1/metrics/query?name=shop.orders.count&since=-3h&step=15m&agg=sum' --url {url}\n  \
             trail get /v1/alerts --url {url}",
            url = server.url()
        ));
    }
    server.wait();
    Ok(())
}

/// The example project's declarations, built into the binary.
fn example() -> Result<decl::Declarations, Problem> {
    decl::parse(&Files {
        events: include_str!("../../../examples/shop/trail/events.toml").to_string(),
        metrics: Some(include_str!("../../../examples/shop/trail/metrics.toml").to_string()),
        rollups: Some(include_str!("../../../examples/shop/trail/rollups.toml").to_string()),
        alerts: Some(include_str!("../../../examples/shop/trail/alerts.toml").to_string()),
    })
    .map_err(|problems| {
        problems
            .into_iter()
            .next()
            .unwrap_or_else(|| Problem::new("example", "the example declarations did not load", ""))
    })
}

/// A small repeatable generator, so seeded data looks the same from run to run.
struct Dice(u64);

impl Dice {
    fn roll(&mut self, sides: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % sides.max(1)
    }
}

/// Publishes the example declarations and sends three hours of made-up traffic ending now.
fn fill(app: &App) -> Result<(), Problem> {
    let now = Ts::now();
    app.publish(example()?, now).map_err(|problems| {
        problems
            .into_iter()
            .next()
            .unwrap_or_else(|| Problem::new("seed", "publish failed", ""))
    })?;
    let start = now.ms() - 3 * HOUR;
    let mut dice = Dice(42);
    let mut emails = 0u64;
    let mut order = 0u64;
    for block in 0..18i64 {
        let mut events: Vec<Value> = Vec::new();
        let mut metrics: Vec<Value> = Vec::new();
        let mut errors: Vec<Value> = Vec::new();
        let mut logs: Vec<Value> = Vec::new();
        for minute in block * 10..(block + 1) * 10 {
            let at = start + minute * MINUTE;
            #[allow(clippy::cast_possible_wrap)]
            for i in 0..dice.roll(5) as i64 {
                order += 1;
                let method = if dice.roll(4) == 0 { "cash" } else { "card" };
                events.push(json!({
                    "id": format!("seed-order-{order}"), "name": "shop.order.placed", "ts": at + i * 7 * SECOND,
                    "actor": "user", "client": if dice.roll(3) == 0 { "ios" } else { "web" },
                    "props": {"order_id": format!("o-{order}"), "total_cents": 500 + dice.roll(9500), "method": method,
                              "items": ["tea", "cake"], "latency_ms": 40 + dice.roll(400)},
                }));
            }
            if dice.roll(12) == 0 {
                let reason = if dice.roll(2) == 0 {
                    "payment"
                } else {
                    "out-of-stock"
                };
                events.push(json!({
                    "id": format!("seed-failed-{minute}"), "name": "shop.order.failed", "ts": at + 30 * SECOND,
                    "props": {"order_id": format!("o-x{minute}"), "reason": reason},
                }));
                errors.push(json!({
                    "id": format!("seed-error-{minute}"), "ts": at + 30 * SECOND, "kind": "PaymentError",
                    "message": format!("card declined for order o-x{minute}"),
                    "stack": "File \"shop/pay.py\", line 41, in charge\n    raise PaymentError(code)",
                    "context": {"route": "/checkout"},
                }));
            }
            for (i, path) in (0i64..).zip(["/", "/menu", "/checkout"]) {
                events.push(json!({
                    "id": format!("seed-view-{minute}-{i}"), "name": "shop.page.viewed", "ts": at + i * 19 * SECOND,
                    "props": {"path": path, "signed_in": i > 0,
                              "load_ms": 80.5 + f64::from(u32::try_from(dice.roll(300)).unwrap_or(0))},
                }));
            }
            for half in 0..2i64 {
                let ts = at + half * 30 * SECOND;
                // The email queue backs up over the last quarter of an hour, so an alert is firing.
                let backlog = if minute >= 165 {
                    120 + (minute - 165) * 4
                } else {
                    0
                };
                #[allow(clippy::cast_possible_wrap)]
                let depth = backlog + dice.roll(30) as i64;
                emails += dice.roll(4);
                metrics.push(json!({"name": "shop.queue.depth", "value": depth, "ts": ts, "labels": {"queue": "email"}}));
                metrics.push(json!({"name": "shop.queue.depth", "value": dice.roll(10), "ts": ts, "labels": {"queue": "sms"}}));
                metrics.push(json!({"name": "shop.emails.sent", "value": emails, "ts": ts}));
            }
            logs.push(json!({
                "id": format!("seed-log-{minute}"), "ts": at, "level": if minute % 37 == 0 { "warn" } else { "info" },
                "stream": "worker", "message": format!("processed batch {minute}"), "fields": {"jobs": dice.roll(40)},
            }));
        }
        let body = json!({"source": "shop", "version": "seed", "host": "web-1",
                          "events": events, "metrics": metrics, "errors": errors, "logs": logs});
        app.ingest(body.to_string().as_bytes(), None, now)
            .map_err(|refusal| refusal.problem().clone())?;
    }
    for rollup in ["shop.orders", "shop.failures"] {
        app.rebuild_rollup(rollup, None, now)?;
    }
    app.tick_rollups(now);
    app.tick_metrics(now);
    app.tick_alerts(now);
    Ok(())
}
