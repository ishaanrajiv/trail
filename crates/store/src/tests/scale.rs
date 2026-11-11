use super::*;
use std::time::Instant;
use trail_core::decl::Retention;

/// Writes 1M events in batches of 10k, then archives their day. Run on request:
/// `cargo test -p trail-store --release -- --ignored scale --nocapture`.
#[test]
#[ignore = "a scale check: about a million rows"]
fn scale_one_million_events() {
    let (_dir, store) = store("scale");
    let total = 1_000_000i64;
    let batch = 10_000i64;
    let started = Instant::now();
    for b in 0..total / batch {
        let events = (0..batch)
            .map(|i| {
                let n = b * batch + i;
                let mut e = event(&format!("{n:016x}"), if n % 3 == 0 { "shop.order.placed" } else { "shop.page.viewed" }, T0 + n * 86);
                e.props = serde_json::from_value(serde_json::json!({"order_id": format!("o-{n}"), "total_cents": n % 5000, "method": "card"})).unwrap();
                e.actor = Some("user".into());
                e
            })
            .collect();
        write_events(&store, events);
    }
    let secs = started.elapsed().as_secs_f64();
    println!(
        "wrote {total} events in {secs:.1}s: {:.0} rows/s",
        total as f64 / secs
    );
    let started = Instant::now();
    let report = store.archive(Ts(T0 + D + H), &|_| Retention::Forever).unwrap();
    let file = &report.written[0];
    println!(
        "archived {} rows in {:.1}s: {} bytes ({:.1} bytes/row)",
        file.rows,
        started.elapsed().as_secs_f64(),
        file.bytes,
        file.bytes as f64 / file.rows as f64
    );
    assert_eq!(file.rows, total as u64);
    let db = store.stats().unwrap().db_bytes;
    println!("trail.db and its WAL: {db} bytes");
}
