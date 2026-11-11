use super::*;
use std::path::{Path, PathBuf};
use trail_core::decl::{Files, load_dir, parse};

fn shop_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/shop")
}

fn shop() -> Declarations {
    load_dir(&shop_dir().join("trail")).unwrap()
}

fn decl(events: &str, metrics: Option<&str>) -> Declarations {
    match parse(&Files {
        events: events.to_string(),
        metrics: metrics.map(str::to_string),
        rollups: None,
        alerts: None,
    }) {
        Ok(decl) => decl,
        Err(problems) => panic!("test declarations do not parse: {problems:#?}"),
    }
}

fn messages(problems: &[Problem]) -> String {
    problems
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------- golden files ----------

#[test]
fn generated_files_match_the_example() {
    let decl = shop();
    assert!(check(&decl).is_empty(), "{}", messages(&check(&decl)));
    let bless = std::env::var("TRAIL_BLESS").is_ok_and(|v| v == "1");
    let dir = shop_dir().join("generated");
    let mut stale = Vec::new();
    for (language, file) in [
        (Language::Python, "shop_events.py"),
        (Language::TypeScript, "shop_events.ts"),
        (Language::JavaScript, "shop_events.js"),
        (Language::Swift, "ShopEvents.swift"),
    ] {
        let path = dir.join(file);
        let text = generate(&decl, language);
        if bless {
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(&path, &text).unwrap();
        } else if std::fs::read_to_string(&path).ok().as_deref() != Some(text.as_str()) {
            stale.push(format!("examples/shop/generated/{file}"));
        }
    }
    assert!(
        stale.is_empty(),
        "these generated files differ from what `trail gen` writes now: {}\n\
         Check the difference is intended (generate and diff), then run \
         `TRAIL_BLESS=1 cargo test -p trail-codegen` to rewrite them.",
        stale.join(", ")
    );
}

#[test]
fn generation_is_deterministic() {
    let decl = shop();
    for language in Language::ALL {
        assert_eq!(generate(&decl, language), generate(&decl, language));
        assert_eq!(generate(&decl, language), generate(&decl.clone(), language));
    }
}

#[test]
fn shop_matches_the_spec_examples() {
    let decl = shop();
    let python = generate(&decl, Language::Python);
    assert!(python.contains(
        "    def shop_order_failed(
        self,
        *,
        order_id: str,
        reason: Literal[\"payment\", \"out-of-stock\"],
        detail: Any | None = None,
        context: Context | None = None,
    ) -> None:
        \"\"\"An order could not be completed\"\"\"
        props: dict[str, Any] = {\"order_id\": order_id, \"reason\": reason}
        if detail is not None:
            props[\"detail\"] = detail
        self._client.record_event(\"shop.order.failed\", props, context)
"
    ));
    assert!(python.contains(
        "    def shop_queue_depth(self, value: float, *, queue: str, ts: int | None = None) -> None:
        \"\"\"Jobs waiting in a queue (jobs)\"\"\"
        self._client.record_metric(\"shop.queue.depth\", value, {\"queue\": queue}, unit=\"jobs\", kind=\"gauge\", ts=ts)
"
    ));

    let ts = generate(&decl, Language::TypeScript);
    assert!(ts.contains(
        "/** An order could not be completed */
export type ShopOrderFailed = {
  order_id: string;
  reason: \"payment\" | \"out-of-stock\";
  detail?: JsonValue;
};
"
    ));
    assert!(ts.contains(
        "    shopEmailsSent(value: number, ts?: number): void {
      client.recordMetric(\"shop.emails.sent\", value, {}, { kind: \"counter\", ts });
    },
"
    ));

    let swift = generate(&decl, Language::Swift);
    assert!(swift.contains(
        "    public enum ShopOrderFailedReason: String, Sendable {
        case payment = \"payment\"
        case outOfStock = \"out-of-stock\"
    }

    /// An order could not be completed
    public func shopOrderFailed(
        orderId: String,
        reason: ShopOrderFailedReason,
        detail: TrailValue? = nil,
        context: EmitContext = EmitContext()
    ) {
        var props: [String: TrailValue] = [
            \"order_id\": .string(orderId),
            \"reason\": .string(reason.rawValue),
        ]
        if let detail { props[\"detail\"] = detail }
        client.recordEvent(\"shop.order.failed\", props, context)
    }
"
    ));
    assert!(swift.contains(".array(items.map { .string($0) })"));
    assert!(swift.contains("if let latencyMs { props[\"latency_ms\"] = .int(latencyMs) }"));
}

// ---------- check ----------

#[test]
fn check_finds_event_names_that_become_one_function() {
    let decl = decl(
        r#"source = "shop"
[events."shop.order_placed"]
description = "A"
[events."shop.order.placed"]
description = "B"
"#,
        None,
    );
    let problems = check(&decl);
    assert_eq!(problems.len(), 1, "{}", messages(&problems));
    let p = &problems[0];
    assert_eq!(p.at, "events.toml: events.\"shop.order_placed\"");
    assert!(p.message.contains("`shop_order_placed` in Python"), "{p}");
    assert!(
        p.message.contains("`shopOrderPlaced` in TypeScript and Swift"),
        "{p}"
    );
    assert_eq!(
        p.fix,
        "rename one of them so the names differ in more than `.`, `_` and `-`, like `shop.order_placed_2`"
    );
}

#[test]
fn check_finds_metric_names_that_become_one_function() {
    let decl = decl(
        "source = \"shop\"\n",
        Some(
            r#"[metrics."queue.depth"]
description = "A"
[metrics."queue-depth"]
description = "B"
[metrics."class"]
description = "C"
[metrics."class_"]
description = "D"
"#,
        ),
    );
    let problems = check(&decl);
    let text = messages(&problems);
    assert_eq!(problems.len(), 2, "{text}");
    assert!(text.contains("`queue-depth` and `queue.depth` both become `queue_depth` in Python and `queueDepth` in TypeScript and Swift"), "{text}");
    // `class` is a keyword in Python and Swift, so it becomes `class_`, which `class_` already is.
    assert!(
        text.contains("`class` and `class_` both become `class_` in Python and Swift"),
        "{text}"
    );
}

#[test]
fn check_finds_properties_that_become_one_parameter() {
    let decl = decl(
        r#"source = "shop"
[events."shop.order.placed"]
description = "A"
[events."shop.order.placed".props]
item_1 = "string"
item1 = "string"
class = "string"
class_ = "string"
"#,
        None,
    );
    let problems = check(&decl);
    let text = messages(&problems);
    assert_eq!(problems.len(), 2, "{text}");
    assert!(text.contains("events.toml: events.\"shop.order.placed\".props.item_1: properties `item1` and `item_1` both become the parameter `item1` in Swift"), "{text}");
    assert!(
        text.contains(
            "properties `class` and `class_` both become the parameter `class_` in Python and Swift"
        ),
        "{text}"
    );
    assert!(text.contains("like `class_2`"), "{text}");
}

#[test]
fn check_finds_labels_that_become_one_parameter() {
    let decl = decl(
        "source = \"shop\"\n",
        Some(
            r#"[metrics."queue.depth"]
description = "A"
labels = ["ts", "ts_", "a_1", "a1"]
"#,
        ),
    );
    let problems = check(&decl);
    let text = messages(&problems);
    assert_eq!(problems.len(), 2, "{text}");
    assert!(
        text.contains("labels `ts` and `ts_` both become the parameter `ts_` in Python and Swift"),
        "{text}"
    );
    assert!(
        text.contains("labels `a_1` and `a1` both become the parameter `a1` in Swift"),
        "{text}"
    );
}

#[test]
fn check_finds_enum_values_that_become_one_case() {
    let decl = decl(
        r#"source = "shop"
[events."shop.order.failed"]
description = "A"
[events."shop.order.failed".props]
reason = { enum = ["out-of-stock", "out_of_stock", "Out Of Stock", "payment"] }
"#,
        None,
    );
    let problems = check(&decl);
    let text = messages(&problems);
    assert_eq!(problems.len(), 2, "{text}");
    assert_eq!(
        problems[0].at,
        "events.toml: events.\"shop.order.failed\".props.reason"
    );
    assert!(
        text.contains("the values `out-of-stock` and `out_of_stock` both become the Swift case `outOfStock`"),
        "{text}"
    );
    assert!(
        text.contains("the values `out-of-stock` and `Out Of Stock` both become"),
        "{text}"
    );
}

#[test]
fn check_finds_enum_values_with_no_case_name() {
    let decl = decl(
        r#"source = "shop"
[events."shop.order.failed"]
description = "A"
[events."shop.order.failed".props]
reason = { enum = ["!!!", "日本", "ok"] }
"#,
        None,
    );
    let problems = check(&decl);
    let text = messages(&problems);
    assert_eq!(problems.len(), 2, "{text}");
    assert!(
        text.contains("the value `!!!` has no ASCII letter or digit"),
        "{text}"
    );
    assert!(
        text.contains("the value `日本` has no ASCII letter or digit"),
        "{text}"
    );
    assert!(problems.iter().all(|p| !p.fix.is_empty()));
}

#[test]
fn check_finds_enum_types_and_props_types_that_clash() {
    let decl = decl(
        r#"source = "shop"
[events."shop.order"]
description = "A"
[events."shop.order".props]
failed_reason = { enum = ["a"] }
[events."shop.order.failed"]
description = "B"
[events."shop.order.failed".props]
reason = { enum = ["b"] }
[events."json.value"]
description = "C"
"#,
        None,
    );
    let problems = check(&decl);
    let text = messages(&problems);
    assert_eq!(problems.len(), 2, "{text}");
    assert!(
        text.contains(
            "events.toml: events.\"json.value\": `json.value` gives the TypeScript props type `JsonValue`"
        ),
        "{text}"
    );
    assert!(
        text.contains(
            "this enum and `failed_reason` of `shop.order` both become the Swift enum `ShopOrderFailedReason`"
        ),
        "{text}"
    );
}

// ---------- awkward names ----------

const AWKWARD_EVENTS: &str = r#"source = "mail-worker"

[events."agent.login.done"]
description = "Has \"\"\" triple quotes, a \\ backslash,\na */ comment end and a newline, and ends in a \"quote\""
[events."agent.login.done".props]
class = "string"
from = "int?"
import = "bool"
default = "float?"
in = "string[]"
is = { enum = ["out-of-stock", "2fa", "a b", "default", "class", "self", "none", "x\"y\\z", "rawValue"], optional = true }
self = "json"
context = "string"
value = "int"
labels = "string"
func = "string?"
var = "string"
let = "string"
enum = { enum = ["case", "switch", "return", "true", "false", "nil", "None", "in", "init", "Type"] }
return = "string"
true = "bool?"
none = "string"
str = "string"
dict = "string[]"
float = "int"
int = "float?"
client_ = "string"
props_ = "string"
await = "json?"

[events."agent.ping"]
description = "No properties at all"

[events."agent.maybe"]
description = "Only optional properties"
[events."agent.maybe".props]
note = "string?"
level = { enum = ["low", "high"], optional = true }
"#;

const AWKWARD_METRICS: &str = r#"
[metrics."class"]
description = "A metric named like a keyword"
labels = ["class", "value", "ts", "labels", "client", "self", "from", "str", "float", "unit", "kind"]

[metrics."None"]
description = "Capitalised, and a Python keyword"
unit = "µs \"quoted\" \\ */"

[metrics."float"]
description = "Shadows a builtin type in Python"
kind = "counter"
labels = ["in"]

[metrics."init"]
description = "Swift's initialiser"

[metrics."client"]
description = "The struct's own property name"

[metrics."cpu.usage:1m"]
description = "Colon and digits\r\nover two lines"
unit = "%"
"#;

fn awkward() -> Declarations {
    // `dict = "string[]"` above is a typo on purpose? No: keep every type legal.
    decl(
        &AWKWARD_EVENTS.replace("\"list\"", "\"string[]\""),
        Some(AWKWARD_METRICS),
    )
}

#[test]
fn awkward_declarations_pass_check() {
    let decl = awkward();
    assert!(check(&decl).is_empty(), "{}", messages(&check(&decl)));
}

#[test]
fn awkward_declarations_generate_working_names() {
    let decl = awkward();
    if let Ok(dir) = std::env::var("TRAIL_AWKWARD_OUT") {
        // For type-checking the output by hand: TRAIL_AWKWARD_OUT=/tmp/x cargo test -p trail-codegen
        let dir = Path::new(&dir);
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("awkward.py"), generate(&decl, Language::Python)).unwrap();
        std::fs::write(dir.join("awkward.ts"), generate(&decl, Language::TypeScript)).unwrap();
        std::fs::write(dir.join("awkward.js"), generate(&decl, Language::JavaScript)).unwrap();
        std::fs::write(
            dir.join("MailWorkerEvents.swift"),
            generate(&decl, Language::Swift),
        )
        .unwrap();
    }

    let python = generate(&decl, Language::Python);
    for line in [
        "        class_: str,",
        "        from_: int | None = None,",
        "        self_: Any,",
        "        context_: str,",
        "        value: int,",
        "        str_: str,",
        "        dict_: list[str],",
        "        float: int,",
        "        await_: Any | None = None,",
        "        true: bool | None = None,",
        "        is_: Literal[\"out-of-stock\", \"2fa\", \"a b\", \"default\", \"class\", \"self\", \"none\", \"x\\\"y\\\\z\", \"rawValue\"] | None = None,",
        "        if from_ is not None:",
        "            props[\"from\"] = from_",
        "        \"\"\"Has \\\"\\\"\\\" triple quotes, a \\\\ backslash, a */ comment end and a newline, and ends in a \\\"quote\\\"\"\"\"",
        "    def class_(self, value: float, *, class_: str, value_: str, ts_: str, labels: str, client: str, self_: str, from_: str, str: str, float: str, unit: str, kind: str, ts: int | None = None) -> None:",
        "        self._client.record_metric(\"class\", value, {\"class\": class_, \"value\": value_, \"ts\": ts_, \"labels\": labels, \"client\": client, \"self\": self_, \"from\": from_, \"str\": str, \"float\": float, \"unit\": unit, \"kind\": kind}, unit=None, kind=\"gauge\", ts=ts)",
        "    def None_(self, value: float, *, ts: int | None = None) -> None:",
        "        \"\"\"Capitalised, and a Python keyword (µs \\\"quoted\\\" \\\\ */)\"\"\"",
        "        self._client.record_metric(\"None\", value, {}, unit=\"µs \\\"quoted\\\" \\\\ */\", kind=\"gauge\", ts=ts)",
        "    def float_(self, value: float, *, in_: str, ts: int | None = None) -> None:",
        "    def cpu_usage_1m(self, value: float, *, ts: int | None = None) -> None:",
        "        \"\"\"Colon and digits over two lines (%)\"\"\"",
        "        props: dict[str, Any] = {}",
    ] {
        assert!(python.contains(line), "missing in Python:\n{line}\n\n{python}");
    }

    let ts = generate(&decl, Language::TypeScript);
    for line in [
        "/** Has \"\"\" triple quotes, a \\ backslash, a *\\/ comment end and a newline, and ends in a \"quote\" */",
        "  class: string;",
        "  self: JsonValue;",
        "  is?: \"out-of-stock\" | \"2fa\" | \"a b\" | \"default\" | \"class\" | \"self\" | \"none\" | \"x\\\"y\\\\z\" | \"rawValue\";",
        "export type AgentPing = Record<string, never>;",
        "    agentPing(props: AgentPing = {}, context?: EmitContext): void {",
        "    agentMaybe(props: AgentMaybe = {}, context?: EmitContext): void {",
        "    class(value: number, labels: { class: string; value: string; ts: string; labels: string; client: string; self: string; from: string; str: string; float: string; unit: string; kind: string }, ts?: number): void {",
        "    /** Capitalised, and a Python keyword (µs \"quoted\" \\ *\\/) */",
        "      client.recordMetric(\"None\", value, {}, { unit: \"µs \\\"quoted\\\" \\\\ */\", kind: \"gauge\", ts });",
        "    cpuUsage1m(value: number, ts?: number): void {",
    ] {
        assert!(ts.contains(line), "missing in TypeScript:\n{line}\n\n{ts}");
    }

    let swift = generate(&decl, Language::Swift);
    for line in [
        "public struct MailWorkerEvents: Sendable {",
        "public struct MailWorkerMetrics: Sendable {",
        "    public enum AgentLoginDoneIs: String, Sendable {",
        "        case outOfStock = \"out-of-stock\"",
        "        case _2fa = \"2fa\"",
        "        case aB = \"a b\"",
        "        case `default` = \"default\"",
        "        case `class` = \"class\"",
        "        case `self` = \"self\"",
        "        case xYZ = \"x\\\"y\\\\z\"",
        "        case `init` = \"init\"",
        "        is_: AgentLoginDoneIs? = nil,",
        "        class_: String,",
        "        context_: String,",
        "        client_: String,",
        "        props_: String,",
        "        value: Int,",
        "        case type = \"Type\"",
        "        case none = \"None\"",
        "        case `nil` = \"nil\"",
        "        if let is_ { props[\"is\"] = .string(is_.rawValue) }",
        "            \"class\": .string(class_),",
        "            \"in\": .array(in_.map { .string($0) }),",
        "            \"self\": self_,",
        "        let props: [String: TrailValue] = [:]",
        "        var props: [String: TrailValue] = [:]",
        "    public func class_(_ value: Double, class_: String, value_: String, ts_: String, labels: String, client_: String, self_: String, from: String, str: String, float: String, unit: String, kind: String, ts: Int64? = nil) {",
        "    public func init_(_ value: Double, ts: Int64? = nil) {",
        "    public func client(_ value: Double, ts: Int64? = nil) {",
        "    public func none(_ value: Double, ts: Int64? = nil) {",
        "    /// Has \"\"\" triple quotes, a \\ backslash, a */ comment end and a newline, and ends in a \"quote\"",
    ] {
        assert!(swift.contains(line), "missing in Swift:\n{line}\n\n{swift}");
    }
}

#[test]
fn a_source_with_no_metrics_still_has_both_halves() {
    let decl = decl(
        r#"source = "shop"
[events."shop.page.viewed"]
description = "A page was shown"
[events."shop.page.viewed".props]
path = "string"
"#,
        None,
    );
    let python = generate(&decl, Language::Python);
    assert!(python.contains("class Events:") && python.contains("class Metrics:"));
    assert!(python.contains("from typing import Any\n"));
    let ts = generate(&decl, Language::TypeScript);
    assert!(ts.contains("export function metrics(client: TrailClient) {\n  return {};\n}\n"));
    assert!(ts.contains("import type { EmitContext, TrailClient } from \"trail-client\";"));
    let swift = generate(&decl, Language::Swift);
    assert!(swift.contains("public struct ShopMetrics: Sendable {"));
    assert!(swift.contains("let props: [String: TrailValue] = [\n"));
}

#[test]
fn a_source_with_only_metrics_imports_only_what_it_uses() {
    let decl = decl(
        "source = \"shop\"\n",
        Some("[metrics.\"shop.up\"]\ndescription = \"Up\"\n"),
    );
    let python = generate(&decl, Language::Python);
    assert!(!python.contains("from typing"));
    assert!(python.contains("from trail_client import Client\n"));
    let ts = generate(&decl, Language::TypeScript);
    assert!(ts.contains("import type { TrailClient } from \"trail-client\";"));
    assert!(ts.contains("export function events(client: TrailClient) {\n  return {};\n}\n"));
}
