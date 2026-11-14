//! Trail from the outside: requests in, stored records, rollups, alerts and the read API out.
//! Each test runs an `App` in its own temp folder and drives time itself.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use serde_json::{Value, json};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use trail_core::Ts;
use trail_core::config::Config;
use trail_core::decl::{self, Declarations};
use trail_core::time::{DAY, Every, HOUR, MINUTE, SECOND};
use trail_server::api::{ROUTES, Request, route};
use trail_server::{App, Patience, Server, Timing, split_url};

/// 2026-11-10 12:00:00 UTC: on the hour, so buckets are easy to reason about.
const T0: i64 = 1_794_312_000_000;

struct Temp(PathBuf);

impl Temp {
    fn new(name: &str) -> Temp {
        static N: AtomicU32 = AtomicU32::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("trail-server-{name}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Temp(dir)
    }

    fn config(&self) -> Config {
        Config::default_in(&self.0.join("data"))
    }
}

impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn open(config: Config, now: i64) -> App {
    let mut app = App::open(config, Ts(now)).unwrap();
    app.quiet = true;
    app
}

fn shop() -> Declarations {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/shop/trail");
    decl::load_dir(&dir).unwrap()
}

fn call(app: &App, method: &str, url: &str, token: Option<&str>, body: Vec<u8>, now: i64) -> (u16, Value) {
    let (path, query) = split_url(url);
    let reply = route(
        app,
        &Request {
            method: method.to_string(),
            path,
            query,
            token: token.map(str::to_string),
            body,
        },
        Ts(now),
    );
    (reply.status, reply.body.unwrap_or(Value::Null))
}

fn get(app: &App, url: &str, now: i64) -> Value {
    let (status, body) = call(app, "GET", url, None, Vec::new(), now);
    assert_eq!(status, 200, "GET {url}: {body}");
    body
}

fn ingest(app: &App, body: &Value, now: i64) -> Value {
    let (status, reply) = call(
        app,
        "POST",
        "/v1/ingest",
        None,
        body.to_string().into_bytes(),
        now,
    );
    assert_eq!(status, 200, "{reply}");
    reply
}

fn order(id: &str, ts: i64, method: &str, total: i64, latency: i64) -> Value {
    json!({"id": id, "name": "shop.order.placed", "ts": ts,
           "props": {"order_id": id, "total_cents": total, "method": method, "items": ["a"], "latency_ms": latency}})
}

fn orders(events: Vec<Value>) -> Value {
    json!({"source": "shop", "version": "abc123", "host": "web-1", "events": events})
}

/// The newest value of each series of a metric, by one label.
fn latest(app: &App, name: &str, label: &str, now: i64) -> Vec<(String, i64, f64)> {
    let body = get(app, &format!("/v1/metrics/latest?name={name}"), now);
    body["series"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| {
            (
                s["labels"][label].as_str().unwrap_or("").to_string(),
                s["ts"].as_i64().unwrap(),
                s["value"].as_f64().unwrap(),
            )
        })
        .collect()
}

#[test]
fn rollups_are_live_exact_on_resend_and_redone_for_late_events() {
    let temp = Temp::new("rollups");
    let app = open(temp.config(), T0);
    app.publish(shop(), Ts(T0)).unwrap();

    let batch = orders(vec![
        order("o-1", T0 + 5 * SECOND, "card", 1000, 100),
        order("o-2", T0 + 10 * SECOND, "card", 2000, 200),
        order("o-3", T0 + 15 * SECOND, "card", 3000, 300),
        order("o-4", T0 + 20 * SECOND, "cash", 500, 50),
    ]);
    let reply = ingest(&app, &batch, T0 + 30 * SECOND);
    assert_eq!(reply["accepted"]["events"], 4);
    assert_eq!(reply["mismatched"], json!([]));

    // The bucket is still open, and already readable.
    app.tick_rollups(Ts(T0 + 35 * SECOND));
    assert_eq!(
        latest(&app, "shop.orders.count", "method", T0 + 35 * SECOND),
        vec![("card".to_string(), T0, 3.0), ("cash".to_string(), T0, 1.0)]
    );
    let sums = latest(&app, "shop.orders.total_cents.sum", "method", T0 + 35 * SECOND);
    assert_eq!(sums[0], ("card".to_string(), T0, 6000.0));
    let p95 = latest(&app, "shop.orders.latency_ms.p95", "method", T0 + 35 * SECOND);
    assert!((p95[0].2 - 300.0).abs() / 300.0 < 0.021, "{p95:?}");

    // A resend changes nothing.
    let reply = ingest(&app, &batch, T0 + 40 * SECOND);
    assert_eq!(reply["accepted"]["events"], 0);
    assert_eq!(reply["duplicates"], 4);
    app.tick_rollups(Ts(T0 + 45 * SECOND));
    assert_eq!(
        latest(&app, "shop.orders.count", "method", T0 + 45 * SECOND)[0].2,
        3.0
    );

    // The next minute fills, and the first bucket leaves memory after its grace.
    ingest(
        &app,
        &orders(vec![order("o-5", T0 + 70 * SECOND, "card", 100, 10)]),
        T0 + 70 * SECOND,
    );
    app.tick_rollups(Ts(T0 + 3 * MINUTE));
    assert_eq!(
        latest(&app, "shop.orders.count", "method", T0 + 3 * MINUTE)[0],
        ("card".to_string(), T0 + MINUTE, 1.0)
    );

    // A late order for the first minute: its bucket is recomputed from the raw events.
    ingest(
        &app,
        &orders(vec![order("o-late", T0 + 25 * SECOND, "card", 4000, 400)]),
        T0 + 4 * MINUTE,
    );
    app.tick_rollups(Ts(T0 + 4 * MINUTE + SECOND));
    let body = get(
        &app,
        &format!(
            "/v1/metrics/query?name=shop.orders.count&label.method=card&since={T0}&until={}&step=1m&agg=sum",
            T0 + 2 * MINUTE
        ),
        T0 + 5 * MINUTE,
    );
    assert_eq!(body["tier"], "raw");
    assert_eq!(
        body["series"][0]["points"],
        json!([[T0, 4.0], [T0 + MINUTE, 1.0]])
    );

    // An hourly rollup is stamped with the hour and counts as it goes.
    let failed = json!({"source": "shop", "events": [
        {"id": "f-1", "name": "shop.order.failed", "ts": T0 + 6 * MINUTE, "props": {"order_id": "o-9", "reason": "payment"}},
        {"id": "f-2", "name": "shop.order.failed", "ts": T0 + 7 * MINUTE, "props": {"order_id": "o-10", "reason": "payment"}},
    ]});
    ingest(&app, &failed, T0 + 7 * MINUTE);
    app.tick_rollups(Ts(T0 + 7 * MINUTE + SECOND));
    assert_eq!(
        latest(&app, "shop.failures.count", "reason", T0 + 8 * MINUTE),
        vec![("payment".to_string(), T0, 2.0)]
    );

    // What it computes is listed.
    let rollups = get(&app, "/v1/rollups", T0);
    assert_eq!(rollups["rollups"].as_array().unwrap().len(), 2);
    assert_eq!(
        rollups["rollups"][1]["series"][2]["name"],
        "shop.orders.latency_ms.p95"
    );
}

#[test]
fn a_rollup_republished_with_wider_buckets_recomputes_the_ones_it_owed() {
    // The second one-minute bucket is no bucket's start once the rollup counts in five minutes.
    // It is owed either because a late order marked it after it had left memory (the first
    // order was in it, and it was written and closed), or because it was still open with an
    // order not yet written (the first order was in the minute before).
    for (name, first) in [("republish-wider-late", 65), ("republish-wider-open", 5)] {
        let temp = Temp::new(name);
        let app = open(temp.config(), T0);
        app.publish(shop(), Ts(T0)).unwrap();
        ingest(
            &app,
            &orders(vec![order("o-1", T0 + first * SECOND, "card", 1000, 100)]),
            T0 + 70 * SECOND,
        );
        app.tick_rollups(Ts(T0 + 4 * MINUTE));
        ingest(
            &app,
            &orders(vec![order("o-late", T0 + 70 * SECOND, "card", 1000, 100)]),
            T0 + 10 * MINUTE,
        );
        let mut edited = shop();
        edited.rollups.get_mut("orders").unwrap().every = Every::FiveMinutes;
        app.publish(edited, Ts(T0 + 10 * MINUTE + 2 * SECOND)).unwrap();
        let now = T0 + 10 * MINUTE + 5 * SECOND;
        app.tick_rollups(Ts(now));
        let body = get(
            &app,
            &format!(
                "/v1/metrics/query?name=shop.orders.count&label.method=card&since={T0}&until={}&step=1m&agg=sum",
                T0 + MINUTE
            ),
            now,
        );
        // The five-minute bucket that holds it is recomputed from both orders and written at
        // its own start.
        assert_eq!(body["series"][0]["points"], json!([[T0, 2.0]]), "{name}");
    }
}

#[test]
fn a_publish_that_keeps_a_rollup_keeps_the_buckets_it_owes() {
    // A late batch dirties an old bucket; the source republishes, changing only a description.
    let late_then_republish = |temp: &Temp| -> App {
        let app = open(temp.config(), T0);
        app.publish(shop(), Ts(T0)).unwrap();
        ingest(
            &app,
            &orders(vec![order("o-1", T0 + 5 * SECOND, "card", 1000, 100)]),
            T0 + 10 * SECOND,
        );
        app.tick_rollups(Ts(T0 + 3 * MINUTE));
        ingest(
            &app,
            &orders(vec![order("o-late", T0 + 25 * SECOND, "card", 1000, 100)]),
            T0 + 10 * MINUTE,
        );
        let mut edited = shop();
        edited.rollups.get_mut("orders").unwrap().description =
            "Orders, counted by how they were paid".to_string();
        app.publish(edited, Ts(T0 + 10 * MINUTE + 2 * SECOND)).unwrap();
        app
    };
    let first_minute = |app: &App, now: i64| {
        let body = get(
            app,
            &format!(
                "/v1/metrics/query?name=shop.orders.count&label.method=card&since={T0}&until={}&step=1m&agg=sum",
                T0 + MINUTE
            ),
            now,
        );
        body["series"][0]["points"].clone()
    };

    // The next tick recomputes the bucket the late order fell in.
    let temp = Temp::new("republish-dirty");
    let app = late_then_republish(&temp);
    let now = T0 + 10 * MINUTE + 5 * SECOND;
    app.tick_rollups(Ts(now));
    assert_eq!(first_minute(&app, now), json!([[T0, 2.0]]));

    // Killed before that tick, the next start finds it again from what is stored.
    let temp = Temp::new("republish-dirty-restart");
    drop(late_then_republish(&temp));
    let app = open(temp.config(), now);
    app.tick_rollups(Ts(now + SECOND));
    assert_eq!(first_minute(&app, now + SECOND), json!([[T0, 2.0]]));
}

#[test]
fn a_restart_rebuilds_the_open_bucket() {
    let temp = Temp::new("restart");
    let now = T0 + 30 * SECOND;
    {
        let app = open(temp.config(), T0);
        app.publish(shop(), Ts(T0)).unwrap();
        ingest(
            &app,
            &orders(vec![
                order("o-1", T0 + SECOND, "card", 1000, 100),
                order("o-2", T0 + 2 * SECOND, "card", 1000, 100),
            ]),
            now,
        );
        // Killed before any flush: nothing was written for the bucket.
    }
    let app = open(temp.config(), now);
    assert_eq!(
        get(&app, "/v1/registry", now)["sources"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    ingest(
        &app,
        &orders(vec![order("o-3", T0 + 40 * SECOND, "card", 1000, 100)]),
        T0 + 40 * SECOND,
    );
    app.tick_rollups(Ts(T0 + 41 * SECOND));
    assert_eq!(
        latest(&app, "shop.orders.count", "method", T0 + 41 * SECOND)[0].2,
        3.0
    );
}

/// Three hours of orders, a few a minute.
fn three_hours() -> Vec<(i64, Value)> {
    let mut out = Vec::new();
    for minute in 0..180i64 {
        for i in 0..=(minute % 3) {
            let ts = T0 + minute * MINUTE + i * SECOND;
            let method = if (minute + i) % 4 == 0 { "cash" } else { "card" };
            out.push((
                ts,
                order(
                    &format!("o-{minute}-{i}"),
                    ts,
                    method,
                    100 * (i + 1) + minute,
                    20 + minute * 3 + i,
                ),
            ));
        }
    }
    out
}

fn series_of(app: &App, name: &str, now: i64) -> Value {
    let url = format!(
        "/v1/metrics/query?name={name}&since={T0}&until={}&step=1m&agg=sum",
        T0 + 3 * HOUR
    );
    let body = get(app, &url, now);
    let series: Vec<Value> = body["series"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| json!({"labels": s["labels"], "points": s["points"]}))
        .collect();
    json!(series)
}

#[test]
fn a_rebuild_gives_what_live_ingest_gave() {
    let end = T0 + 3 * HOUR + 5 * MINUTE;

    // Live: declared first, events arrive in order, the timer ticks every minute.
    let live_dir = Temp::new("live");
    let live = open(live_dir.config(), T0);
    live.publish(shop(), Ts(T0)).unwrap();
    let mut ticked = T0;
    for (ts, event) in three_hours() {
        ingest(&live, &orders(vec![event]), ts);
        if ts - ticked >= MINUTE {
            live.tick_rollups(Ts(ts));
            ticked = ts;
        }
    }
    live.tick_rollups(Ts(end));

    // Rebuilt: the same events were stored before anything was declared.
    let rebuilt_dir = Temp::new("rebuilt");
    let rebuilt = open(rebuilt_dir.config(), T0);
    for (ts, event) in three_hours() {
        ingest(&rebuilt, &orders(vec![event]), ts);
    }
    assert_eq!(series_of(&rebuilt, "shop.orders.count", end), json!([]));
    rebuilt.publish(shop(), Ts(end)).unwrap();
    let (status, report) = call(
        &rebuilt,
        "POST",
        "/v1/rollups/shop/orders/rebuild",
        None,
        Vec::new(),
        end,
    );
    assert_eq!(status, 200, "{report}");
    assert_eq!(report["events"], 360);
    assert_eq!(report["buckets"], 180);
    assert_eq!(report["from"], T0);
    rebuilt.tick_rollups(Ts(end));

    for name in [
        "shop.orders.count",
        "shop.orders.total_cents.sum",
        "shop.orders.latency_ms.p95",
    ] {
        let (a, b) = (series_of(&live, name, end), series_of(&rebuilt, name, end));
        assert_eq!(a.as_array().unwrap().len(), 2, "{name}");
        assert_eq!(a, b, "{name}");
    }

    let (status, body) = call(
        &rebuilt,
        "POST",
        "/v1/rollups/shop/order/rebuild",
        None,
        Vec::new(),
        end,
    );
    assert_eq!(status, 404);
    assert!(
        body["error"]["fix"]
            .as_str()
            .unwrap()
            .contains("did you mean `shop.orders`"),
        "{body}"
    );
}

#[test]
fn a_mismatch_is_stored_and_reported_and_an_unreadable_record_is_refused() {
    let temp = Temp::new("drift");
    let app = open(temp.config(), T0);
    app.publish(shop(), Ts(T0)).unwrap();
    let body = json!({"source": "shop", "events": [
        {"id": "m-1", "name": "shop.order.placed", "ts": T0,
         "props": {"order_id": "o-1", "total_cents": 1200, "method": "card", "items": [], "latncy_ms": 5}},
        {"id": "m-2", "name": "Order", "ts": T0},
        {"id": "m-3", "name": "shop.order.placd", "ts": T0, "props": {}},
    ], "metrics": [
        {"name": "shop.queue.depth", "value": 3, "ts": T0},
        {"name": "shop.queue.depth", "value": "three", "ts": T0, "labels": {"queue": "email"}},
    ]});
    let reply = ingest(&app, &body, T0);
    assert_eq!(
        reply["accepted"],
        json!({"events": 2, "metrics": 1, "errors": 0, "logs": 0})
    );
    let rejected = reply["rejected"].as_array().unwrap();
    assert_eq!(rejected.len(), 2, "{reply}");
    assert_eq!(rejected[0]["at"], "events[1]");
    assert!(!rejected[0]["fix"].as_str().unwrap().is_empty());
    assert_eq!(rejected[1]["at"], "metrics[1]");
    let marked: Vec<&str> = reply["mismatched"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["message"].as_str().unwrap())
        .collect();
    assert!(marked.iter().any(|m| m.contains("latncy_ms")), "{marked:?}");
    assert!(
        marked
            .iter()
            .any(|m| m.contains("shop.order.placd") && m.contains("shop.order.placed")),
        "{marked:?}"
    );
    assert!(
        marked.iter().any(|m| m.contains("missing label `queue`")),
        "{marked:?}"
    );

    // The marked event is kept, with what was wrong.
    let events = get(&app, "/v1/events?mismatched=1&name=shop.order.placed", T0);
    assert_eq!(events["events"][0]["id"], "m-1");
    assert!(
        events["events"][0]["problems"][0]
            .as_str()
            .unwrap()
            .contains("latncy_ms")
    );

    // The drift report groups repeats.
    ingest(
        &app,
        &json!({"source": "shop", "metrics": [{"name": "shop.queue.depth", "value": 4, "ts": T0 + SECOND}]}),
        T0 + SECOND,
    );
    let problems = get(&app, "/v1/problems?source=shop", T0 + SECOND);
    let row = problems["problems"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["kind"] == "metric")
        .unwrap();
    assert_eq!(row["count"], 2);
    assert_eq!(row["name"], "shop.queue.depth");

    // A source that has declared nothing is not checked.
    let reply = ingest(
        &app,
        &json!({"source": "cron", "events": [{"name": "cron.job.ran", "props": {"anything": 1}}]}),
        T0,
    );
    assert_eq!(reply["mismatched"], json!([]));
    assert_eq!(reply["accepted"]["events"], 1);

    // Publishing again starts the report over.
    app.publish(shop(), Ts(T0)).unwrap();
    assert_eq!(get(&app, "/v1/problems?source=shop", T0)["problems"], json!([]));

    // An event name belongs to one source.
    let mut thief = shop();
    thief.source = "till".to_string();
    thief.rollups.clear();
    thief.alerts.clear();
    let (status, body) = call(
        &app,
        "PUT",
        "/v1/registry/till",
        None,
        thief.to_json().to_string().into_bytes(),
        T0,
    );
    assert_eq!(status, 409, "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("already declared by source `shop`")
    );
    let (status, _) = call(
        &app,
        "PUT",
        "/v1/registry/shop",
        None,
        thief.to_json().to_string().into_bytes(),
        T0,
    );
    assert_eq!(status, 400);
}

/// A webhook receiver: collects what is posted to it, and can be told to fail.
struct Hook {
    url: String,
    got: Arc<Mutex<Vec<Value>>>,
    down: Arc<Mutex<bool>>,
    /// A post whose body contains this text is answered 400.
    unwanted: Arc<Mutex<Option<String>>>,
}

fn hook() -> Hook {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/hooks/trail", listener.local_addr().unwrap());
    let got = Arc::new(Mutex::new(Vec::new()));
    let down = Arc::new(Mutex::new(false));
    let unwanted = Arc::new(Mutex::new(None::<String>));
    let (seen, failing, refusing) = (got.clone(), down.clone(), unwanted.clone());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let (_, body) = read_http(&mut stream);
            if *failing.lock().unwrap() {
                let _ = stream.write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
                );
                continue;
            }
            let refused = refusing
                .lock()
                .unwrap()
                .as_deref()
                .is_some_and(|text| String::from_utf8_lossy(&body).contains(text));
            if refused {
                let _ = stream
                    .write_all(b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
                continue;
            }
            seen.lock().unwrap().push(serde_json::from_slice(&body).unwrap());
            let _ = stream.write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n");
        }
    });
    Hook {
        url,
        got,
        down,
        unwanted,
    }
}

/// Reads one HTTP message with a Content-Length from a socket: its head and its body. It takes
/// no byte beyond the message, so the next one on the same connection can be read after it.
fn read_http(stream: &mut std::net::TcpStream) -> (String, Vec<u8>) {
    let mut bytes = Vec::new();
    let mut byte = [0u8; 1];
    while !bytes.ends_with(b"\r\n\r\n") {
        let n = stream.read(&mut byte).unwrap();
        assert!(
            n > 0,
            "closed before the headers ended: {:?}",
            String::from_utf8_lossy(&bytes)
        );
        bytes.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&bytes).into_owned();
    let length: usize = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().unwrap())
        })
        .unwrap_or(0);
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).unwrap();
    (head, body)
}

fn depth(app: &App, value: f64, now: i64) {
    let body = json!({"source": "shop", "metrics": [{"name": "shop.queue.depth", "value": value, "ts": now, "labels": {"queue": "email"}}]});
    assert_eq!(ingest(app, &body, now)["accepted"]["metrics"], 1);
}

#[test]
fn alerts_fire_hold_clear_and_reach_the_webhook() {
    let temp = Temp::new("alerts");
    let hook = hook();
    let mut config = temp.config();
    config.alerts.webhook = Some(hook.url.clone());
    let app = open(config.clone(), T0);
    app.publish(shop(), Ts(T0)).unwrap();
    let posts = || hook.got.lock().unwrap().clone();

    // Above the threshold: fires, with the labels and value filled in.
    depth(&app, 150.0, T0);
    app.tick_alerts(Ts(T0 + SECOND));
    let first = posts();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0]["type"], "snapshot");
    let firing = first[0]["firing"].as_array().unwrap();
    assert_eq!(firing.len(), 1);
    assert_eq!(firing[0]["key"], "shop.queue_backlog{queue=email}");
    assert_eq!(firing[0]["title"], "The email queue is backing up");
    assert_eq!(firing[0]["body"], "150 jobs are waiting.");
    assert_eq!(firing[0]["severity"], "warning");
    assert_eq!(firing[0]["since"], T0 + SECOND);

    // Nothing changed a few seconds later: nothing is posted.
    app.tick_alerts(Ts(T0 + 31 * SECOND));
    assert_eq!(posts().len(), 1);

    // Back under the threshold but not yet at `clear`: it holds, and keeps its start time.
    depth(&app, 90.0, T0 + 11 * MINUTE);
    app.tick_alerts(Ts(T0 + 11 * MINUTE + SECOND));
    let held = get(&app, "/v1/alerts", T0 + 11 * MINUTE + SECOND);
    assert_eq!(held["firing"][0]["since"], T0 + SECOND);
    assert_eq!(held["firing"][0]["body"], "90 jobs are waiting.");
    // The unchanged state is posted again after five minutes, for a receiver that missed it.
    assert_eq!(posts().len(), 2);

    // At `clear`: an empty snapshot says it recovered.
    depth(&app, 70.0, T0 + 22 * MINUTE);
    app.tick_alerts(Ts(T0 + 22 * MINUTE + SECOND));
    assert_eq!(posts().last().unwrap()["firing"], json!([]));

    // The worker goes quiet: the absent rule fires.
    app.tick_alerts(Ts(T0 + 25 * MINUTE));
    let quiet = posts().last().unwrap().clone();
    assert_eq!(quiet["firing"][0]["key"], "shop.worker_quiet{queue=email}");
    assert_eq!(quiet["firing"][0]["severity"], "severe");
    assert_eq!(quiet["firing"][0]["value"], Value::Null);

    // An event rule is a one-off notice; the webhook being down only delays it.
    *hook.down.lock().unwrap() = true;
    let now = T0 + 26 * MINUTE;
    let failed = json!({"source": "shop", "events": [
        {"id": "f-1", "name": "shop.order.failed", "ts": now, "props": {"order_id": "o-9", "reason": "payment"}},
        {"id": "f-2", "name": "shop.order.failed", "ts": now, "props": {"order_id": "o-10", "reason": "out-of-stock"}},
        {"id": "f-3", "name": "shop.order.failed", "ts": now - 2 * HOUR, "props": {"order_id": "o-old", "reason": "payment"}},
    ]});
    ingest(&app, &failed, now);
    ingest(&app, &failed, now);
    let before = posts().len();
    app.tick_alerts(Ts(now + SECOND));
    assert_eq!(posts().len(), before);
    assert_eq!(
        get(&app, "/v1/alerts", now)["notices"].as_array().unwrap().len(),
        1
    );

    // A restart remembers what was firing and what is queued.
    drop(app);
    let app = open(config, now + 2 * SECOND);
    assert_eq!(
        get(&app, "/v1/alerts", now)["notices"].as_array().unwrap().len(),
        1
    );
    assert_eq!(
        get(&app, "/v1/alerts", now)["firing"][0]["since"],
        T0 + 25 * MINUTE
    );
    *hook.down.lock().unwrap() = false;
    app.tick_alerts(Ts(now + 3 * SECOND));
    let notice = posts().last().unwrap().clone();
    assert_eq!(notice["type"], "notice");
    assert_eq!(notice["id"], "shop.payment_failed:f-1");
    assert_eq!(notice["title"], "Payment failed for order o-9");
    assert_eq!(notice["event"]["props"]["order_id"], "o-9");
    assert_eq!(get(&app, "/v1/alerts", now)["notices"], json!([]));
    let total = posts().len();
    app.tick_alerts(Ts(now + 4 * SECOND));
    assert_eq!(posts().len(), total, "a delivered notice is not sent twice");
}

#[test]
fn line_protocol_and_counters() {
    let temp = Temp::new("lines");
    let mut config = temp.config();
    config.counters = vec!["net.bytes_*".to_string()];
    let app = open(config, T0);
    let seconds = T0 / 1000;
    let body = format!(
        "cpu,host=web-1 usage=12.5,idle=80i {seconds}\nnet,iface=eth0 bytes_sent=1000i {seconds}\nbroken line\n"
    );
    let (status, reply) = call(
        &app,
        "POST",
        "/write?db=telegraf&precision=s",
        None,
        body.into_bytes(),
        T0,
    );
    assert_eq!(status, 400, "{reply}");
    assert_eq!(reply["lines"][0]["line"], 3);
    assert!(
        reply["error"]["message"]
            .as_str()
            .unwrap()
            .contains("were stored")
    );
    let body = format!("net,iface=eth0 bytes_sent=4000i {}\n", seconds + 30);
    let (status, _) = call(
        &app,
        "POST",
        "/api/v2/write?bucket=telegraf&precision=s",
        None,
        body.into_bytes(),
        T0 + 30 * SECOND,
    );
    assert_eq!(status, 204);

    let series = get(&app, "/v1/metrics/series?source=telegraf", T0);
    let names: Vec<&str> = series["series"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["cpu.idle", "cpu.usage", "net.bytes_sent"]);
    assert_eq!(series["series"][2]["kind"], "counter");
    // The counter is stored as a rate: 3000 bytes over 30 seconds.
    let rate = latest(&app, "net.bytes_sent", "iface", T0 + MINUTE);
    assert_eq!(rate, vec![("eth0".to_string(), T0 + 30 * SECOND, 100.0)]);
    assert_eq!(latest(&app, "cpu.*", "host", T0 + MINUTE).len(), 2);
}

#[test]
fn errors_and_logs_are_kept_grouped_and_searchable() {
    let temp = Temp::new("errors");
    let app = open(temp.config(), T0);
    let body = |id: &str, ts: i64| {
        json!({"source": "shop", "errors": [
            {"id": id, "ts": ts, "kind": "KeyError", "message": "missing key 42",
             "stack": "File \"pay.py\", line 10, in charge\n  raise KeyError(42)", "context": {"route": "/pay"}}
        ], "logs": [
            {"id": format!("l-{id}"), "ts": ts, "level": "warn", "stream": "worker", "message": "retrying payment", "fields": {"attempt": 2}},
            {"id": format!("d-{id}"), "ts": ts, "level": "debug", "message": "noise"},
        ]})
    };
    ingest(&app, &body("e-1", T0), T0);
    ingest(&app, &body("e-2", T0 + MINUTE), T0 + MINUTE);
    ingest(&app, &body("e-2", T0 + MINUTE), T0 + MINUTE);

    let groups = get(&app, "/v1/errors/groups?source=shop", T0 + MINUTE);
    assert_eq!(groups["groups"].as_array().unwrap().len(), 1);
    assert_eq!(groups["groups"][0]["count"], 2);
    let fingerprint = groups["groups"][0]["fingerprint"].as_str().unwrap().to_string();
    let errors = get(
        &app,
        &format!("/v1/errors?fingerprint={fingerprint}"),
        T0 + MINUTE,
    );
    assert_eq!(errors["errors"][0]["id"], "e-2");
    assert_eq!(errors["errors"][1]["context"]["route"], "/pay");

    let url = format!("/v1/errors/groups/{fingerprint}");
    let (status, group) = call(&app, "POST", &url, None, br#"{"status":"resolved"}"#.to_vec(), T0);
    assert_eq!((status, group["status"].as_str()), (200, Some("resolved")));
    let (status, reply) = call(&app, "POST", &url, None, br#"{"status":"gone"}"#.to_vec(), T0);
    assert_eq!(status, 400);
    assert!(reply["error"]["fix"].as_str().unwrap().contains("resolved"));
    // It happens again: the group reopens.
    ingest(&app, &body("e-3", T0 + 2 * MINUTE), T0 + 2 * MINUTE);
    assert_eq!(
        get(&app, "/v1/errors/groups?status=open", T0)["groups"][0]["count"],
        3
    );

    let logs = get(
        &app,
        "/v1/logs?source=shop&level=warn&q=retrying",
        T0 + 3 * MINUTE,
    );
    assert_eq!(logs["logs"].as_array().unwrap().len(), 3);
    assert_eq!(logs["logs"][0]["fields"]["attempt"], 2);
    let page = get(&app, "/v1/logs?source=shop&limit=2", T0 + 3 * MINUTE);
    let next = page["next"].as_str().unwrap();
    let rest = get(
        &app,
        &format!("/v1/logs?source=shop&limit=10&before={next}"),
        T0 + 3 * MINUTE,
    );
    assert_eq!(rest["logs"].as_array().unwrap().len(), 4);
    assert_eq!(rest["next"], Value::Null);
}

#[test]
fn old_days_move_to_files_and_stay_readable() {
    let temp = Temp::new("archive");
    let app = open(temp.config(), T0);
    app.publish(shop(), Ts(T0)).unwrap();
    for day in 0..3 {
        let ts = T0 + day * DAY;
        ingest(
            &app,
            &orders(vec![order(&format!("o-{day}"), ts, "card", 1000, 100)]),
            ts,
        );
        let viewed = json!({"source": "shop", "events": [{"id": format!("v-{day}"), "name": "shop.page.viewed", "ts": ts,
                             "props": {"path": "/", "signed_in": true}}]});
        ingest(&app, &viewed, ts);
    }
    let now = T0 + 5 * DAY;
    app.tick_archive(Ts(now));

    let stats = get(&app, "/v1/stats", now);
    let events = stats["stores"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["store"] == "events")
        .unwrap()
        .clone();
    assert_eq!(events["rows"], 0, "{stats}");
    // Three days of orders kept forever, three of page views in the 30-day class.
    assert_eq!(events["files"], 6, "{stats}");
    let data = temp.0.join("data/archive");
    assert!(data.join("events/2026-11-10.parquet").exists());
    assert!(data.join("events-30d/2026-11-12.parquet").exists());

    let found = get(&app, "/v1/events?name=shop.order.placed&since=-30d", now);
    let ids: Vec<&str> = found["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["o-2", "o-1", "o-0"]);
    assert_eq!(found["events"][2]["props"]["total_cents"], 1000);
    assert_eq!(found["events"][2]["version"], "abc123");

    // A rollup rebuilds across days that are only in files.
    let (status, report) = call(
        &app,
        "POST",
        "/v1/rollups/shop/orders/rebuild",
        None,
        Vec::new(),
        now,
    );
    assert_eq!((status, report["events"].as_i64()), (200, Some(3)), "{report}");

    // A week-old record from a phone that was offline lands on its own day.
    ingest(
        &app,
        &orders(vec![order("o-phone", T0 + HOUR, "cash", 700, 70)]),
        now,
    );
    app.tick_archive(Ts(now + HOUR));
    let found = get(
        &app,
        &format!("/v1/events?name=shop.order.placed&since={T0}&until={}", T0 + DAY),
        now + HOUR,
    );
    assert_eq!(found["events"].as_array().unwrap().len(), 2);

    // Page views are kept 30 days: their files go, the orders stay.
    app.tick_archive(Ts(T0 + 40 * DAY));
    assert!(!data.join("events-30d/2026-11-12.parquet").exists());
    assert!(data.join("events/2026-11-10.parquet").exists());
}

#[test]
fn after_seq_follows_what_arrives_even_late() {
    let temp = Temp::new("after-seq");
    let app = open(temp.config(), T0);
    app.publish(shop(), Ts(T0)).unwrap();
    ingest(&app, &orders(vec![order("o-0", T0, "card", 1000, 100)]), T0);
    let now = T0 + 5 * DAY;
    app.tick_archive(Ts(now));
    ingest(&app, &orders(vec![order("o-1", now, "card", 1000, 100)]), now);

    let ids = |body: &Value, key: &str| -> Vec<String> {
        body[key]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["id"].as_str().unwrap().to_string())
            .collect()
    };
    let first = get(&app, "/v1/events?after_seq=0&name=shop.order.placed", now);
    assert_eq!(ids(&first, "events"), ["o-0", "o-1"]);
    assert_eq!(first["next"], Value::Null);
    let at = first["next_seq"].as_i64().unwrap();
    assert_eq!(Some(at), first["events"][1]["seq"].as_i64());

    // A week-old order from a phone that was offline, for a day already in a file.
    ingest(
        &app,
        &orders(vec![order("o-phone", T0 + HOUR, "cash", 700, 70)]),
        now,
    );
    let next = get(
        &app,
        &format!("/v1/events?after_seq={at}&name=shop.order.placed"),
        now,
    );
    assert_eq!(ids(&next, "events"), ["o-phone"]);
    let at = next["next_seq"].as_i64().unwrap();
    app.tick_archive(Ts(now + HOUR));
    let again = get(&app, &format!("/v1/events?after_seq={at}"), now + HOUR);
    assert_eq!(ids(&again, "events"), Vec::<String>::new());
    assert_eq!(again["next_seq"].as_i64(), Some(at));

    // Newest-first pages carry seq on every record.
    let page = get(&app, "/v1/events?name=shop.order.placed", now + HOUR);
    assert!(
        page["events"]
            .as_array()
            .unwrap()
            .iter()
            .all(|e| e["seq"].is_i64()),
        "{page}"
    );

    // Errors and logs take it too.
    ingest(
        &app,
        &json!({"source": "shop", "errors": [{"kind": "KeyError", "message": "boom"}],
                "logs": [{"message": "started"}]}),
        now + HOUR,
    );
    let errors = get(&app, &format!("/v1/errors?after_seq={at}&source=shop"), now + HOUR);
    assert_eq!(errors["errors"].as_array().unwrap().len(), 1);
    let logs = get(&app, &format!("/v1/logs?after_seq={at}&level=info&source=shop"), now + HOUR);
    assert_eq!(logs["logs"].as_array().unwrap().len(), 1);

    // It doesn't combine with `before`, and must be a position.
    let (status, body) = call(
        &app,
        "GET",
        "/v1/events?after_seq=0&before=1:x",
        None,
        Vec::new(),
        now,
    );
    assert_eq!(status, 400, "{body}");
    assert!(body["error"]["fix"].as_str().unwrap().contains("alone"), "{body}");
    let (status, body) = call(&app, "GET", "/v1/logs?after_seq=-3", None, Vec::new(), now);
    assert_eq!(status, 400, "{body}");
    assert!(
        body["error"]["fix"].as_str().unwrap().contains("next_seq"),
        "{body}"
    );
}

#[test]
fn tokens_guard_writes_and_reads() {
    let temp = Temp::new("auth");
    let mut config = temp.config();
    config.auth.tokens.insert("shop".to_string(), "w-1".to_string());
    config.auth.read_token = Some("r-1".to_string());
    let app = open(config, T0);
    let body = orders(vec![order("o-1", T0, "card", 1, 1)])
        .to_string()
        .into_bytes();
    let (status, reply) = call(&app, "POST", "/v1/ingest", None, body.clone(), T0);
    assert_eq!(status, 401);
    assert!(reply["error"]["fix"].as_str().unwrap().contains("[auth.tokens]"));
    assert_eq!(
        call(&app, "POST", "/v1/ingest", Some("wrong"), body.clone(), T0).0,
        401
    );
    assert_eq!(call(&app, "POST", "/v1/ingest", Some("w-1"), body, T0).0, 200);
    // A source with no token configured writes freely.
    let other = json!({"source": "cron", "logs": [{"message": "ran"}]})
        .to_string()
        .into_bytes();
    assert_eq!(call(&app, "POST", "/v1/ingest", None, other, T0).0, 200);

    assert_eq!(call(&app, "GET", "/v1/events", None, Vec::new(), T0).0, 401);
    assert_eq!(
        call(&app, "GET", "/v1/events", Some("r-1"), Vec::new(), T0).0,
        200
    );
    assert_eq!(call(&app, "GET", "/v1/health", None, Vec::new(), T0).0, 200);
}

#[test]
fn a_wrong_request_says_what_would_work() {
    let temp = Temp::new("api");
    let app = open(temp.config(), T0);
    let fix = |method: &str, url: &str| {
        let (status, body) = call(&app, method, url, None, Vec::new(), T0);
        (status, body["error"]["fix"].as_str().unwrap_or("").to_string())
    };
    let (status, text) = fix("GET", "/v1/events?nme=shop.order.placed");
    assert_eq!(status, 400);
    assert!(text.starts_with("did you mean `name`?"), "{text}");
    let (status, text) = fix("GET", "/v1/events?since=1794312000");
    assert_eq!(status, 400);
    assert!(text.contains("1794312000000"), "{text}");
    assert!(fix("GET", "/v1/events?since=yesterday").1.contains("-1h"));
    assert!(fix("GET", "/v1/events?limit=5000").1.contains("1 to 1000"));
    assert!(fix("GET", "/v1/metrics/query").1.contains("/v1/metrics/series"));
    assert!(
        fix("GET", "/v1/metrics/query?name=x&agg=median")
            .1
            .contains("avg, min, max")
    );
    let (status, text) = fix("GET", "/v1/event");
    assert_eq!(status, 404);
    assert!(text.contains("did you mean `/v1/events`?"), "{text}");
    assert_eq!(fix("DELETE", "/v1/events").0, 405);
    let (status, body) = call(
        &app,
        "POST",
        "/v1/ingest",
        None,
        b"{\"sorce\": \"shop\"}".to_vec(),
        T0,
    );
    assert_eq!(status, 400);
    assert!(body["error"]["fix"].as_str().unwrap().contains("spec/ingest.md"));
    let (status, body) = call(
        &app,
        "PUT",
        "/v1/registry/trail",
        None,
        trail_server::own_declarations()
            .unwrap()
            .to_json()
            .to_string()
            .into_bytes(),
        T0,
    );
    assert_eq!(status, 409, "{body}");
}

#[test]
fn every_route_answers_and_is_documented() {
    let temp = Temp::new("routes");
    let app = open(temp.config(), T0);
    app.publish(shop(), Ts(T0)).unwrap();
    let doc =
        std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spec/api.md")).unwrap();
    let index = get(&app, "/v1", T0);
    assert_eq!(index["routes"].as_array().unwrap().len(), ROUTES.len());
    for r in ROUTES {
        let heading = format!("`{} {}`", r.method, r.path);
        assert!(
            doc.contains(&heading),
            "spec/api.md has no section for {heading}; add one"
        );
        for (param, _) in r.params {
            let section = doc.split(&heading).nth(1).unwrap_or("");
            let section = section.split("\n### ").next().unwrap_or("");
            assert!(
                section.contains(&format!("`{param}`")),
                "spec/api.md's section for {heading} does not mention the parameter `{param}`"
            );
        }
        let path = r
            .path
            .replace("{source}", "shop")
            .replace("{name}", "orders")
            .replace("{fingerprint}", "nope");
        let needs_name = r.path.starts_with("/v1/metrics/") && r.path != "/v1/metrics/series";
        let url = if needs_name {
            format!("{path}?name=shop.queue.depth")
        } else {
            path
        };
        let (status, body) = call(&app, r.method, &url, None, Vec::new(), T0);
        assert!(
            body["error"]["message"] != "this route is listed but not answered",
            "{} {} is in ROUTES but `answer` has no arm for it",
            r.method,
            r.path
        );
        assert!(status < 500, "{} {url}: {status} {body}", r.method);
    }
}

#[test]
fn trail_reports_on_itself() {
    let temp = Temp::new("self");
    let app = open(temp.config(), T0);
    ingest(
        &app,
        &orders(vec![
            order("o-1", T0, "card", 1, 1),
            order("o-2", T0, "card", 1, 1),
        ]),
        T0,
    );
    app.tick_metrics(Ts(T0));
    ingest(
        &app,
        &orders(vec![order("o-3", T0 + SECOND, "card", 1, 1)]),
        T0 + 10 * SECOND,
    );
    app.tick_metrics(Ts(T0 + 30 * SECOND));
    // One more event in 30 seconds.
    let rate = get(
        &app,
        "/v1/metrics/latest?name=trail.ingest.records&label.source=shop&label.kind=event",
        T0 + MINUTE,
    );
    let value = rate["series"][0]["value"].as_f64().unwrap();
    assert!((value - 1.0 / 30.0).abs() < 1e-9, "{rate}");
    let rows = get(
        &app,
        "/v1/metrics/latest?name=trail.store.rows&label.store=events",
        T0 + MINUTE,
    );
    assert_eq!(rows["series"][0]["value"], 3.0);
    // Its own metrics are declared, so nothing it sends is a mismatch.
    assert_eq!(get(&app, "/v1/problems?source=trail", T0)["problems"], json!([]));
    assert!(get(&app, "/v1/registry/trail", T0)["metrics"]["trail.archive.bytes"]["description"].is_string());
}

#[test]
fn a_failing_archive_and_a_filling_cap_show_in_its_metrics() {
    let temp = Temp::new("archive-health");
    let mut config = temp.config();
    config.caps.events = Some(1_000_000);
    config.caps.logs = None;
    let app = open(config, T0);
    let failing = |now: i64| {
        app.tick_metrics(Ts(now));
        get(&app, "/v1/metrics/latest?name=trail.archive.failing", now)["series"][0]["value"].clone()
    };
    // Nothing is said before the first run.
    assert_eq!(failing(T0), Value::Null);
    ingest(&app, &orders(vec![order("o-1", T0, "card", 1, 1)]), T0);

    // A stray file where the events folder belongs: the day can't be written.
    let folder = temp.0.join("data/archive/events");
    let _ = std::fs::remove_dir_all(&folder);
    std::fs::create_dir_all(folder.parent().unwrap()).unwrap();
    std::fs::write(&folder, "in the way").unwrap();
    let now = T0 + 2 * DAY;
    app.tick_archive(Ts(now));
    assert_eq!(failing(now), 1.0);
    // The event is still in SQLite, and still answers.
    assert_eq!(get(&app, "/v1/events?since=-7d", now)["events"][0]["id"], "o-1");

    // With the folder back, the next run writes the day and the gauge returns to 0.
    std::fs::remove_file(&folder).unwrap();
    let now = now + 10 * MINUTE;
    app.tick_archive(Ts(now));
    assert_eq!(failing(now), 0.0);
    assert!(folder.join("2026-11-10.parquet").exists());

    // The share of the cap the day files fill.
    let used = get(
        &app,
        "/v1/metrics/latest?name=trail.archive.cap_used&label.store=events",
        now,
    );
    let share = used["series"][0]["value"].as_f64().unwrap();
    let bytes = std::fs::metadata(folder.join("2026-11-10.parquet"))
        .unwrap()
        .len();
    let expected = bytes as f64 / 1_000_000.0;
    assert!(
        (share - expected).abs() < expected * 0.001,
        "{used} for {bytes} bytes"
    );
    // A store with no cap reports no share.
    let none = get(
        &app,
        "/v1/metrics/latest?name=trail.archive.cap_used&label.store=logs",
        now,
    );
    assert_eq!(none["series"], json!([]));
    assert_eq!(get(&app, "/v1/problems?source=trail", now)["problems"], json!([]));
}

#[test]
fn a_long_range_reads_the_tiers_and_its_first_bucket_is_whole() {
    let temp = Temp::new("tiers");
    let app = open(temp.config(), T0);
    // A reading of 10 every minute for three hours.
    for minute in 0..180 {
        let ts = T0 + minute * MINUTE;
        let body = json!({"source": "shop", "metrics": [{"name": "shop.queue.depth", "value": 10, "ts": ts, "labels": {"queue": "email"}}]});
        ingest(&app, &body, ts);
    }
    let now = T0 + 3 * HOUR + 10 * MINUTE;
    app.tick_metrics(Ts(now));
    // The range starts twenty minutes into an hour.
    let url = format!(
        "/v1/metrics/query?name=shop.queue.depth&since={}&until={}&step=1h&agg=count",
        T0 + 20 * MINUTE,
        T0 + 3 * HOUR
    );
    let body = get(&app, &url, now);
    assert_eq!(body["tier"], "1h");
    assert_eq!(
        body["series"][0]["points"],
        json!([[T0, 60.0], [T0 + HOUR, 60.0], [T0 + 2 * HOUR, 60.0]])
    );
    let url = format!(
        "/v1/metrics/query?name=shop.queue.depth&since={}&until={}&step=5m&agg=sum",
        T0 + 2 * MINUTE,
        T0 + 10 * MINUTE
    );
    let body = get(&app, &url, now);
    assert_eq!(body["tier"], "5m");
    assert_eq!(
        body["series"][0]["points"],
        json!([[T0, 50.0], [T0 + 5 * MINUTE, 50.0]])
    );
    let url = format!(
        "/v1/metrics/query?name=shop.queue.depth&since={}&until={}&step=1m",
        T0 + 90 * SECOND,
        T0 + 3 * MINUTE
    );
    let body = get(&app, &url, now);
    assert_eq!(body["tier"], "raw");
    assert_eq!(
        body["series"][0]["points"],
        json!([[T0 + MINUTE, 10.0], [T0 + 2 * MINUTE, 10.0]])
    );
}

#[test]
fn it_serves_over_http() {
    let temp = Temp::new("http");
    let mut config = temp.config();
    config.listen = "127.0.0.1:0".to_string();
    config.limits.body_bytes = 2000;
    let server = Server::start_with(config.clone(), Timing::from_config(&config), true).unwrap();
    let agent =
        ureq::Agent::new_with_config(ureq::Agent::config_builder().http_status_as_error(false).build());
    let now = Ts::now().ms();

    let body = orders(vec![order("o-1", now, "card", 1200, 80)]).to_string();
    let mut response = agent
        .post(&format!("{}/v1/ingest", server.url()))
        .send(body)
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let reply: Value = serde_json::from_str(&response.body_mut().read_to_string().unwrap()).unwrap();
    assert_eq!(reply["accepted"]["events"], 1);

    let mut response = agent
        .get(&format!(
            "{}/v1/events?name=shop.order.%2A&since=-1h",
            server.url()
        ))
        .call()
        .unwrap();
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/json"
    );
    let found: Value = serde_json::from_str(&response.body_mut().read_to_string().unwrap()).unwrap();
    assert_eq!(found["events"][0]["props"]["total_cents"], 1200);

    let big = "x".repeat(3000);
    let response = agent
        .post(&format!("{}/v1/ingest", server.url()))
        .send(big)
        .unwrap();
    assert_eq!(response.status().as_u16(), 413);

    // Telegraf streams its body in chunks, with no length up front.
    let lines = format!("cpu,host=web-1 usage=12.5 {now}\n");
    let response = agent
        .post(&format!("{}/v1/write?precision=ms&source=telegraf", server.url()))
        .send(ureq::SendBody::from_reader(&mut lines.as_bytes()))
        .unwrap();
    assert_eq!(response.status().as_u16(), 204);
    let mut response = agent
        .get(&format!("{}/v1/metrics/latest?name=cpu.usage", server.url()))
        .call()
        .unwrap();
    let found: Value = serde_json::from_str(&response.body_mut().read_to_string().unwrap()).unwrap();
    assert_eq!(found["series"][0]["value"], 12.5, "{found}");

    // A compressed body is refused with the setting that fixes it, not read as bad lines.
    let mut response = agent
        .post(&format!("{}/v1/write?source=telegraf", server.url()))
        .header("Content-Encoding", "gzip")
        .send(&[0x1f_u8, 0x8b, 0x08][..])
        .unwrap();
    assert_eq!(response.status().as_u16(), 415);
    let refused: Value = serde_json::from_str(&response.body_mut().read_to_string().unwrap()).unwrap();
    assert!(refused.to_string().contains("identity"), "{refused}");

    server.stop();
}

/// Sends raw bytes on a new connection and returns it.
fn raw(server: &Server, bytes: &[u8]) -> std::net::TcpStream {
    let mut stream = std::net::TcpStream::connect(server.addr()).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.write_all(bytes).unwrap();
    stream
}

/// The status of the next response on a connection, and its body.
fn response(stream: &mut std::net::TcpStream) -> (u16, String) {
    let (head, body) = read_http(stream);
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    (status, String::from_utf8(body).unwrap())
}

fn impatient(name: &str, workers: usize) -> (Temp, Server) {
    let temp = Temp::new(name);
    let mut config = temp.config();
    config.listen = "127.0.0.1:0".to_string();
    config.workers = workers;
    let timing = Timing {
        patience: Patience {
            // Long enough that a test thread held up between connecting and sending is not hung up on.
            idle: Duration::from_secs(2),
            head: Duration::from_millis(300),
            body: Duration::from_millis(300),
            write: Duration::from_secs(5),
            queue: Duration::from_secs(10),
        },
        ..Timing::from_config(&config)
    };
    let server = Server::start_with(config, timing, true).unwrap();
    (temp, server)
}

#[test]
fn the_wire_is_read_strictly_and_one_connection_serves_many_requests() {
    let (_temp, server) = impatient("wire", 4);
    let event = r#"{"source":"shop","events":[{"id":"w-1","name":"shop.page.viewed","props":{"path":"/","signed_in":true}}]}"#;

    // Two requests sent at once on one connection are answered in order, and it stays open.
    let post = format!(
        "POST /v1/ingest HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{event}",
        event.len()
    );
    let mut stream = raw(
        &server,
        format!("{post}GET /v1/health HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes(),
    );
    let (status, body) = response(&mut stream);
    assert_eq!((status, body.contains(r#""events":1"#)), (200, true), "{body}");
    assert_eq!(response(&mut stream).0, 200);
    // HEAD is GET without the body; the length is still stated.
    stream.write_all(b"HEAD /v1/health HTTP/1.1\r\n\r\n").unwrap();
    let mut head = [0u8; 512];
    let n = stream.read(&mut head).unwrap();
    let head = String::from_utf8_lossy(&head[..n]).into_owned();
    assert!(
        head.starts_with("HTTP/1.1 200") && head.ends_with("\r\n\r\n"),
        "{head}"
    );
    // An idle connection is hung up on, quietly.
    assert_eq!(stream.read(&mut [0u8; 16]).unwrap(), 0);

    // A chunked body with an extension and a trailer, after `Expect: 100-continue`.
    let mut stream = raw(
        &server,
        b"POST /v1/ingest HTTP/1.1\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
    );
    let mut go = [0u8; 25];
    stream.read_exact(&mut go).unwrap();
    assert_eq!(&go, b"HTTP/1.1 100 Continue\r\n\r\n");
    let event = event.replace("w-1", "w-2");
    let (first, second) = event.split_at(40);
    stream
        .write_all(
            format!(
                "{:x};note=1\r\n{first}\r\n{:X}\r\n{second}\r\n0\r\nChecksum: none\r\n\r\n",
                first.len(),
                second.len()
            )
            .as_bytes(),
        )
        .unwrap();
    let (status, body) = response(&mut stream);
    assert_eq!((status, body.contains(r#""events":1"#)), (200, true), "{body}");
    assert_eq!(
        stream.read(&mut [0u8; 16]).unwrap(),
        0,
        "`Connection: close` closes it"
    );

    // What can't be read safely is refused with what to send, and the connection closed.
    for (bytes, status, says) in [
        (&b"GET /v1/health\r\n\r\n"[..], 400, "METHOD /path HTTP/1.1"),
        (b"GET /v1/health HTTP/2\r\n\r\n", 400, "HTTP/1.1"),
        (
            b"POST /v1/ingest HTTP/1.1\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n",
            400,
            "both",
        ),
        (
            b"POST /v1/ingest HTTP/1.1\r\nContent-Length: 2\r\nContent-Length: 3\r\n\r\n",
            400,
            "disagree",
        ),
        (
            b"POST /v1/ingest HTTP/1.1\r\nTransfer-Encoding: gzip\r\n\r\n",
            501,
            "chunked",
        ),
        (
            b"POST /v1/ingest HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n",
            400,
            "hexadecimal",
        ),
    ] {
        let mut stream = raw(&server, bytes);
        let (got, body) = response(&mut stream);
        assert_eq!(got, status, "{}", String::from_utf8_lossy(bytes));
        assert!(body.contains(says), "{body}");
        assert_eq!(stream.read(&mut [0u8; 16]).unwrap_or(0), 0);
    }
    let long = format!("GET /v1/health HTTP/1.1\r\nX-Pad: {}\r\n\r\n", "x".repeat(20_000));
    assert_eq!(response(&mut raw(&server, long.as_bytes())).0, 431);

    server.stop();
}

#[test]
fn a_chunk_size_too_large_to_add_up_is_refused() {
    let (_temp, server) = impatient("chunk-wrap", 1);
    // One byte, then a chunk size that wraps a 64-bit sum back past the limit check.
    let mut stream = raw(
        &server,
        b"POST /v1/ingest HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\nffffffffffffffff\r\n",
    );
    let (status, body) = response(&mut stream);
    assert_eq!(status, 413, "{body}");
    assert!(body.contains("body_bytes"), "{body}");
    server.stop();
}

#[test]
fn a_caller_that_stalls_holds_no_worker() {
    // One worker, to make any hold-up show.
    let (_temp, server) = impatient("stall", 1);
    let agent =
        ureq::Agent::new_with_config(ureq::Agent::config_builder().http_status_as_error(false).build());
    let healthy = || {
        let started = Instant::now();
        let status = agent
            .get(&format!("{}/v1/health", server.url()))
            .call()
            .unwrap()
            .status()
            .as_u16();
        (status, started.elapsed())
    };

    // Several callers send part of a request and stop.
    let mut stalled: Vec<std::net::TcpStream> = Vec::new();
    for _ in 0..4 {
        stalled.push(raw(
            &server,
            b"POST /v1/ingest HTTP/1.1\r\nContent-Length: 500\r\n\r\n{\"source\":",
        ));
        stalled.push(raw(&server, b"GET /v1/health HTTP/1.1\r\nX-Half"));
    }
    // Everyone else is answered at once.
    let (status, took) = healthy();
    assert_eq!(status, 200);
    assert!(took < Duration::from_millis(200), "a health check took {took:?}");

    // Each stalled caller is told so once its time is up, and hung up on.
    for stream in &mut stalled {
        let (status, body) = response(stream);
        assert_eq!(status, 408, "{body}");
        assert!(body.contains("too long"), "{body}");
        assert_eq!(stream.read(&mut [0u8; 16]).unwrap_or(0), 0);
    }
    assert_eq!(healthy().0, 200);
    server.stop();
}

#[test]
fn a_large_answer_is_not_held_back() {
    let (_temp, server) = impatient("nodelay", 4);
    let events: Vec<Value> = (0..100)
        .map(|n| order(&format!("big-{n}"), Ts::now().ms(), "card", 1000, 50))
        .collect();
    let agent =
        ureq::Agent::new_with_config(ureq::Agent::config_builder().http_status_as_error(false).build());
    let status = agent
        .post(&format!("{}/v1/ingest", server.url()))
        .send(orders(events).to_string())
        .unwrap()
        .status();
    assert_eq!(status.as_u16(), 200);
    // Thirty answers of about 30 kB each on one connection. A server that writes its headers
    // and body separately loses 40 ms on every one of these to the kernel's delayed ACK.
    let started = Instant::now();
    for _ in 0..30 {
        let mut response = agent
            .get(&format!("{}/v1/events?limit=100", server.url()))
            .call()
            .unwrap();
        let text = response.body_mut().read_to_string().unwrap();
        assert!(text.len() > 20_000);
    }
    let each = started.elapsed() / 30;
    assert!(each < Duration::from_millis(20), "a 30 kB answer took {each:?}");
    server.stop();
}

fn post(app: &App, url: &str, token: Option<&str>, body: String, now: i64) -> (u16, Value) {
    call(app, "POST", url, token, body.into_bytes(), now)
}

#[test]
fn a_name_belongs_to_its_source() {
    // With a token on `shop`, nobody else may write what `shop` owns.
    let temp = Temp::new("owner-guarded");
    let mut config = temp.config();
    config.auth.tokens.insert("shop".to_string(), "w-1".to_string());
    let app = open(config, T0);
    app.publish(shop(), Ts(T0)).unwrap();
    let stranger = json!({"source": "other",
    "events": [order("x-1", T0, "card", 1, 1), {"id": "x-2", "name": "other.thing.done", "props": {}}],
    "metrics": [
        {"name": "shop.queue.depth", "value": 5, "ts": T0, "labels": {"queue": "email"}},
        {"name": "shop.orders.count", "value": 99, "ts": T0, "labels": {"method": "card"}},
        {"name": "trail.db.bytes", "value": 1, "ts": T0},
        {"name": "other.depth", "value": 2, "ts": T0},
    ]});
    let (status, reply) = post(&app, "/v1/ingest", None, stranger.to_string(), T0);
    assert_eq!(status, 200, "{reply}");
    assert_eq!(reply["accepted"]["events"], 1, "{reply}");
    assert_eq!(reply["accepted"]["metrics"], 1, "{reply}");
    let refused: Vec<&str> = reply["rejected"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["at"].as_str().unwrap())
        .collect();
    assert_eq!(
        refused,
        ["events[0]", "metrics[0]", "metrics[1]", "metrics[2]"],
        "{reply}"
    );
    assert!(
        reply["rejected"][0]["message"]
            .as_str()
            .unwrap()
            .contains("belongs to source `shop`")
    );
    assert!(
        reply["rejected"][3]["message"]
            .as_str()
            .unwrap()
            .contains("belongs to source `trail`")
    );
    assert_eq!(
        get(&app, "/v1/events?name=shop.order.placed", T0)["events"],
        json!([])
    );
    // The same goes for line protocol, which names the line.
    let (status, reply) = post(
        &app,
        "/v1/write?source=other&precision=ms",
        None,
        format!("shop.queue,queue=email depth=7 {T0}\nother load=1 {T0}\n"),
        T0,
    );
    assert_eq!(status, 400, "{reply}");
    assert_eq!(reply["lines"][0]["line"], 1, "{reply}");
    assert!(
        reply["lines"][0]["message"]
            .as_str()
            .unwrap()
            .contains("only `shop` may write it")
    );

    // Nobody sends as Trail itself.
    let own = json!({"source": "trail", "logs": [{"message": "hello"}]});
    let (status, reply) = post(&app, "/v1/ingest", None, own.to_string(), T0);
    assert_eq!(status, 400, "{reply}");
    assert!(
        reply["error"]["fix"]
            .as_str()
            .unwrap()
            .contains("your own source")
    );
    assert_eq!(
        post(
            &app,
            "/v1/write?source=trail",
            None,
            "cpu usage=1\n".to_string(),
            T0
        )
        .0,
        400
    );

    // With no token, a record under another source's name is kept and marked, and counts
    // towards nothing of the owner's: not its rollups, not its alerts.
    let temp = Temp::new("owner-open");
    let app = open(temp.config(), T0);
    app.publish(shop(), Ts(T0)).unwrap();
    let failed = json!({"id": "x-3", "name": "shop.order.failed", "ts": T0, "props": {"order_id": "x-3", "reason": "payment"}});
    let stranger = json!({"source": "other", "events": [order("x-1", T0, "card", 1, 1), failed]});
    let reply = ingest(&app, &stranger, T0);
    assert_eq!(reply["accepted"]["events"], 2, "{reply}");
    assert_eq!(reply["mismatched"].as_array().unwrap().len(), 2, "{reply}");
    assert!(
        reply["mismatched"][0]["message"]
            .as_str()
            .unwrap()
            .contains("declared by source `shop`")
    );
    app.tick_rollups(Ts(T0 + 5 * SECOND));
    assert_eq!(latest(&app, "shop.orders.count", "method", T0 + MINUTE), vec![]);
    assert_eq!(get(&app, "/v1/alerts", T0)["notices"], json!([]));
    // Rebuilt from the raw events, the answer is the same.
    let (status, _) = call(
        &app,
        "POST",
        "/v1/rollups/shop/orders/rebuild",
        None,
        Vec::new(),
        T0 + MINUTE,
    );
    assert_eq!(status, 200);
    assert_eq!(latest(&app, "shop.orders.count", "method", T0 + MINUTE), vec![]);
    // The owner's own event still counts.
    ingest(&app, &orders(vec![order("o-1", T0, "card", 1, 1)]), T0);
    app.tick_rollups(Ts(T0 + 10 * SECOND));
    assert_eq!(
        latest(&app, "shop.orders.count", "method", T0 + MINUTE),
        vec![("card".to_string(), T0, 1.0)]
    );
}

#[test]
fn a_reading_that_is_not_stored_is_reported() {
    let temp = Temp::new("unstored");
    let mut config = temp.config();
    config.counters = vec!["net.bytes_*".to_string()];
    config.limits.batch_items = 4;
    let app = open(config, T0);
    // Line protocol: a value that is not a number, among good lines.
    let body = format!("cpu usage=NaN {T0}\ncpu,host=a usage=2 {T0}\nnet bytes_sent=100i {T0}\n");
    let (status, reply) = post(&app, "/v1/write?source=telegraf&precision=ms", None, body, T0);
    assert_eq!(status, 400, "{reply}");
    assert_eq!(reply["lines"].as_array().unwrap().len(), 1, "{reply}");
    assert_eq!(reply["lines"][0]["line"], 1);
    assert!(
        reply["lines"][0]["message"].as_str().unwrap().contains("finite"),
        "{reply}"
    );
    assert!(
        reply["error"]["message"]
            .as_str()
            .unwrap()
            .contains("other 2 readings were stored"),
        "{reply}"
    );

    // A counter reading older than the last one can't become a rate: it is refused, by line...
    let body = format!("net bytes_sent=50i {}\n", T0 - 1000);
    let (status, reply) = post(&app, "/v1/write?source=telegraf&precision=ms", None, body, T0);
    assert_eq!(status, 400, "{reply}");
    assert!(
        reply["lines"][0]["message"]
            .as_str()
            .unwrap()
            .contains("time order"),
        "{reply}"
    );
    // ...and by index in a JSON batch.
    let batch = json!({"source": "telegraf", "metrics": [
        {"name": "net.bytes_sent", "value": 300, "ts": T0 + 1000},
        {"name": "net.bytes_sent", "value": 20, "ts": T0 - 2000},
    ]});
    let reply = ingest(&app, &batch, T0);
    assert_eq!(reply["accepted"]["metrics"], 1, "{reply}");
    assert_eq!(reply["rejected"][0]["at"], "metrics[1]", "{reply}");
    assert!(
        reply["rejected"][0]["fix"]
            .as_str()
            .unwrap()
            .contains("time order"),
        "{reply}"
    );

    // The record limit holds for line protocol too.
    let many = format!("cpu usage=1 {T0}\n").repeat(5);
    let (status, reply) = post(&app, "/v1/write?source=telegraf&precision=ms", None, many, T0);
    assert_eq!(status, 413, "{reply}");
    assert!(
        reply["error"]["fix"]
            .as_str()
            .unwrap()
            .contains("metric_batch_size"),
        "{reply}"
    );
}

#[test]
fn what_was_stored_just_before_a_stop_is_acted_on_at_the_next_start() {
    let temp = Temp::new("catch-up");
    let hook = hook();
    let mut config = temp.config();
    config.alerts.webhook = Some(hook.url.clone());
    let failed = |id: &str, ts: i64| {
        json!({"source": "shop", "events": [{"id": id, "name": "shop.order.failed", "ts": ts,
               "props": {"order_id": id, "reason": "payment"}}]})
    };
    {
        let app = open(config.clone(), T0);
        app.publish(shop(), Ts(T0)).unwrap();
        ingest(&app, &orders(vec![order("o-1", T0, "card", 100, 10)]), T0);
        app.tick_rollups(Ts(T0 + 5 * SECOND));
        app.tick_alerts(Ts(T0 + 5 * SECOND));
        assert_eq!(
            latest(&app, "shop.orders.count", "method", T0 + MINUTE),
            vec![("card".to_string(), T0, 1.0)]
        );
        // These arrive, are stored and answered, and then Trail dies before any timer runs:
        // one more order in the same minute, a week-old order from a phone, a failed payment.
        ingest(
            &app,
            &orders(vec![order("o-2", T0 + SECOND, "card", 100, 10)]),
            T0 + 6 * SECOND,
        );
        ingest(
            &app,
            &orders(vec![order("o-old", T0 - 7 * DAY, "cash", 100, 10)]),
            T0 + 6 * SECOND,
        );
        ingest(&app, &failed("f-1", T0 + 6 * SECOND), T0 + 6 * SECOND);
    }
    // It comes back three minutes later, in another bucket.
    let now = T0 + 3 * MINUTE;
    {
        let app = open(config.clone(), now);
        app.tick_rollups(Ts(now));
        let counts = get(
            &app,
            &format!(
                "/v1/metrics/query?name=shop.orders.count&since={}&until={}&step=1m&agg=sum",
                T0,
                T0 + MINUTE
            ),
            now,
        );
        assert_eq!(counts["series"][0]["points"][0][1], 2.0, "{counts}");
        let old = get(
            &app,
            &format!(
                "/v1/metrics/query?name=shop.orders.count&label.method=cash&since={}&until={}&step=1m&agg=sum",
                T0 - 7 * DAY,
                T0 - 7 * DAY + MINUTE
            ),
            now,
        );
        assert_eq!(old["series"][0]["points"][0][1], 1.0, "{old}");

        app.tick_alerts(Ts(now));
        let got = hook.got.lock().unwrap().clone();
        let notices: Vec<&Value> = got.iter().filter(|post| post["type"] == "notice").collect();
        assert_eq!(notices.len(), 1, "{got:?}");
        assert_eq!(notices[0]["event"]["id"], "f-1");
        // It dies again straight after sending, before... no: the send was saved by the tick.
    }
    // Another restart looks at the same events again and sends nothing twice.
    let app = open(config, now + MINUTE);
    app.tick_alerts(Ts(now + MINUTE));
    app.tick_rollups(Ts(now + MINUTE));
    let got = hook.got.lock().unwrap().clone();
    assert_eq!(
        got.iter().filter(|post| post["type"] == "notice").count(),
        1,
        "{got:?}"
    );
}

#[test]
fn a_rebuild_does_not_undo_what_arrived_while_it_ran() {
    let temp = Temp::new("rebuild-live");
    let app = open(temp.config(), T0);
    app.publish(shop(), Ts(T0)).unwrap();
    ingest(&app, &orders(vec![order("o-1", T0, "card", 100, 10)]), T0);
    app.tick_rollups(Ts(T0 + SECOND));
    // A sample written from an older scan, as a rebuild that started earlier would leave it.
    let stale = json!({"name": "shop.orders.count", "value": 0, "ts": T0, "labels": {"method": "card"}});
    app.commit(
        &trail_store::Batch {
            points: vec![trail_store::MetricPoint {
                source: "shop".to_string(),
                point: serde_json::from_value(stale).unwrap(),
                counter: false,
            }],
            ..trail_store::Batch::default()
        },
        Ts(T0 + SECOND),
    )
    .unwrap();
    let (status, _) = call(
        &app,
        "POST",
        "/v1/rollups/shop/orders/rebuild",
        None,
        Vec::new(),
        T0 + 2 * SECOND,
    );
    assert_eq!(status, 200);
    // An event lands in the open bucket; the next flush writes the bucket whole.
    ingest(
        &app,
        &orders(vec![order("o-2", T0 + SECOND, "card", 100, 10)]),
        T0 + 3 * SECOND,
    );
    app.tick_rollups(Ts(T0 + 5 * SECOND));
    assert_eq!(
        latest(&app, "shop.orders.count", "method", T0 + MINUTE),
        vec![("card".to_string(), T0, 2.0)]
    );
}

#[test]
fn a_notice_the_receiver_will_never_take_does_not_hold_back_the_rest() {
    let temp = Temp::new("notice-refused");
    let hook = hook();
    let mut config = temp.config();
    config.alerts.webhook = Some(hook.url.clone());
    let app = open(config, T0);
    app.publish(shop(), Ts(T0)).unwrap();
    let failed = |id: &str| {
        json!({"source": "shop", "events": [{"id": id, "name": "shop.order.failed", "ts": T0,
               "props": {"order_id": id, "reason": "payment"}}]})
    };
    *hook.unwanted.lock().unwrap() = Some("f-1".to_string());
    ingest(&app, &failed("f-1"), T0);
    ingest(&app, &failed("f-2"), T0);
    app.tick_alerts(Ts(T0 + SECOND));
    let got = hook.got.lock().unwrap().clone();
    let notices: Vec<&Value> = got.iter().filter(|post| post["type"] == "notice").collect();
    assert_eq!(notices.len(), 1, "{got:?}");
    assert_eq!(notices[0]["event"]["id"], "f-2");
    // The refused one is gone, not waiting for another try.
    *hook.unwanted.lock().unwrap() = None;
    app.tick_alerts(Ts(T0 + 2 * SECOND));
    let got = hook.got.lock().unwrap().clone();
    assert_eq!(
        got.iter().filter(|post| post["type"] == "notice").count(),
        1,
        "{got:?}"
    );
}

#[test]
fn what_a_source_declares_is_changed_with_its_write_token() {
    let temp = Temp::new("owned-registry");
    let mut config = temp.config();
    config.auth.tokens.insert("shop".to_string(), "w-1".to_string());
    config.auth.read_token = Some("r-1".to_string());
    let app = open(config, T0);
    let body = shop().to_json().to_string().into_bytes();
    for token in [None, Some("r-1"), Some("wrong")] {
        let (status, reply) = call(&app, "PUT", "/v1/registry/shop", token, body.clone(), T0);
        assert_eq!(status, 401, "{token:?}: {reply}");
        assert!(reply["error"]["fix"].as_str().unwrap().contains("[auth.tokens]"));
    }
    assert_eq!(
        call(&app, "PUT", "/v1/registry/shop", Some("w-1"), body, T0).0,
        200
    );
    // Reading them is still a read.
    assert_eq!(
        call(&app, "GET", "/v1/registry/shop", Some("r-1"), Vec::new(), T0).0,
        200
    );
    for (method, url) in [
        ("POST", "/v1/rollups/shop/orders/rebuild"),
        ("DELETE", "/v1/registry/shop"),
    ] {
        assert_eq!(
            call(&app, method, url, Some("r-1"), Vec::new(), T0).0,
            401,
            "{url}"
        );
        assert_eq!(
            call(&app, method, url, Some("w-1"), Vec::new(), T0).0,
            200,
            "{url}"
        );
    }
}

#[test]
fn changing_what_trail_keeps_takes_the_admin_token() {
    const ADMIN: &str = "admin-0123456789abcdef";
    let temp = Temp::new("admin");
    let mut config = temp.config();
    config.auth.admin_token = Some(ADMIN.to_string());
    let app = open(config, T0);
    let decl = shop().to_json().to_string().into_bytes();
    // Ingest and reads are as they were: no token.
    let error = json!({"source": "shop", "errors": [{"kind": "Timeout", "message": "upstream"}]});
    assert_eq!(ingest(&app, &error, T0)["accepted"]["errors"], 1);
    let groups = get(&app, "/v1/errors/groups", T0);
    let fingerprint = groups["groups"][0]["fingerprint"].as_str().unwrap().to_string();
    let status = br#"{"status": "resolved"}"#.to_vec();

    let group = format!("/v1/errors/groups/{fingerprint}");
    let changes: [(&str, &str, &[u8]); 4] = [
        ("PUT", "/v1/registry/shop", &decl),
        ("POST", "/v1/rollups/shop/orders/rebuild", b""),
        ("POST", &group, &status),
        ("DELETE", "/v1/registry/shop", b""),
    ];
    for (method, url, body) in changes {
        for token in [None, Some("wrong"), Some("admin-0123456789abcdeX")] {
            let (got, reply) = call(&app, method, url, token, body.to_vec(), T0);
            assert_eq!(got, 401, "{method} {url} with {token:?}: {reply}");
            let fix = reply["error"]["fix"].as_str().unwrap();
            assert!(
                fix.contains("admin_token") && fix.contains("TRAIL_ADMIN_TOKEN"),
                "{fix}"
            );
            assert!(!reply.to_string().contains(ADMIN), "{reply}");
        }
        let (got, reply) = call(&app, method, url, Some(ADMIN), body.to_vec(), T0);
        assert_eq!(got, 200, "{method} {url}: {reply}");
    }
    assert_eq!(call(&app, "GET", "/v1/events", None, Vec::new(), T0).0, 200);
    assert_eq!(call(&app, "GET", "/v1/registry", None, Vec::new(), T0).0, 200);

    // A source's own write token still changes what that source declares, and the admin token
    // does too; the read token does neither.
    let temp = Temp::new("admin-owned");
    let mut config = temp.config();
    config.auth.admin_token = Some(ADMIN.to_string());
    config.auth.read_token = Some("r-1".to_string());
    config.auth.tokens.insert("shop".to_string(), "w-1".to_string());
    let app = open(config, T0);
    for token in ["w-1", ADMIN] {
        for (method, url, body) in [
            ("PUT", "/v1/registry/shop", decl.clone()),
            ("POST", "/v1/rollups/shop/orders/rebuild", Vec::new()),
            ("DELETE", "/v1/registry/shop", Vec::new()),
        ] {
            assert_eq!(
                call(&app, method, url, Some("r-1"), body.clone(), T0).0,
                401,
                "{url}"
            );
            let (got, reply) = call(&app, method, url, Some(token), body, T0);
            assert_eq!(got, 200, "{method} {url} with {token}: {reply}");
        }
    }
    // A write token is no admin token for an error group.
    let error = json!({"source": "shop", "errors": [{"kind": "Timeout", "message": "upstream"}]});
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/ingest",
            Some("w-1"),
            error.to_string().into_bytes(),
            T0
        )
        .0,
        200
    );
    let (_, groups) = call(&app, "GET", "/v1/errors/groups", Some("r-1"), Vec::new(), T0);
    let group = format!(
        "/v1/errors/groups/{}",
        groups["groups"][0]["fingerprint"].as_str().unwrap()
    );
    let status = br#"{"status": "ignored"}"#.to_vec();
    for token in ["w-1", "r-1"] {
        assert_eq!(call(&app, "POST", &group, Some(token), status.clone(), T0).0, 401);
    }
    assert_eq!(call(&app, "POST", &group, Some(ADMIN), status, T0).0, 200);
}

#[test]
fn line_protocol_refuses_a_time_in_the_wrong_unit() {
    let temp = Temp::new("write-ts");
    let app = open(temp.config(), T0);
    // Seconds sent as milliseconds would land in January 1970 and overwrite each other there;
    // nanoseconds sent as milliseconds would be the series' newest reading for ever.
    let (status, reply) = post(
        &app,
        "/v1/write?source=host&precision=ms",
        None,
        format!(
            "cpu usage=1 {}\ncpu usage=2 {}\ncpu usage=3 {T0}\n",
            T0 / 1000,
            T0 * 1_000_000
        ),
        T0,
    );
    assert_eq!(status, 400, "{reply}");
    let lines: Vec<i64> = reply["lines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|line| line["line"].as_i64().unwrap())
        .collect();
    assert_eq!(lines, [1, 2], "{reply}");
    assert!(
        reply["lines"][0]["message"]
            .as_str()
            .unwrap()
            .contains("before the year 2000"),
        "{reply}"
    );
    let stored = get(&app, "/v1/metrics/latest?name=cpu.usage", T0);
    assert_eq!(stored["series"].as_array().map(Vec::len), Some(1), "{stored}");
}

#[test]
fn a_data_folder_keeps_the_timezone_it_was_written_with() {
    let temp = Temp::new("zone-guard");
    let mut config = temp.config();
    drop(open(config.clone(), T0));
    config.zone = trail_core::time::Zone::new("America/New_York").unwrap();
    let problem = match App::open(config, Ts(T0)) {
        Ok(_) => panic!("opened under another zone"),
        Err(problem) => problem,
    };
    assert!(problem.message.contains("`UTC`"), "{problem}");
    assert!(problem.fix.contains("timezone = \"UTC\""), "{problem}");
}
