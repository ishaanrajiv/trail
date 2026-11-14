//! The naming rules. Names are checked once, here, so every tool agrees on them.

/// A sender's name: lowercase letters, digits and hyphens, starting with a letter (`shop`, `mail-worker`).
pub fn is_source(text: &str) -> bool {
    let mut chars = text.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && text.len() <= 64
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// The service a source belongs to: lowercase letters, digits, `-` and `_`, starting with a
/// letter, at most 64 bytes (`shop`, `mail-worker`, `mail_worker`). Every source name is one.
pub fn is_service(text: &str) -> bool {
    let mut chars = text.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && text.len() <= 64
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

/// A property, label or rollup name: snake_case, starting with a letter.
pub fn is_ident(text: &str) -> bool {
    let mut chars = text.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && text.len() <= 64
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// An event name: two or more snake_case parts joined by dots (`shop.order.placed`).
pub fn is_event_name(text: &str) -> bool {
    text.len() <= 128 && text.split('.').count() >= 2 && text.split('.').all(is_ident)
}

/// An alert rule's `kind`, the name its receiver knows it by: written like an event name, two or
/// more snake_case parts joined by dots (`shop.queue_backlog`), at most 128 bytes.
pub fn is_alert_kind(text: &str) -> bool {
    is_event_name(text)
}

/// A metric name: letters, digits and `_ . : -`, starting with a letter (`cpu.usage_active`).
/// Looser than event names because collectors such as Telegraf choose their own.
pub fn is_metric_name(text: &str) -> bool {
    let mut chars = text.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic())
        && text.len() <= 200
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
}

/// A sender-chosen record id: 1 to 64 characters of letters, digits and `_ - : .`.
pub fn is_id(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= 64
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | ':' | '.'))
}

/// Fields every event carries. A property may not reuse one of these names.
pub const ENVELOPE_FIELDS: [&str; 11] = [
    "id",
    "name",
    "ts",
    "received_at",
    "source",
    "version",
    "host",
    "actor",
    "client",
    "trace_id",
    "props",
];

/// Envelope fields an aggregation may group or filter by.
pub const GROUPABLE_ENVELOPE_FIELDS: [&str; 3] = ["actor", "client", "host"];

/// Words that mark a property as likely to hold a secret. Trail keeps events for years, so a
/// declaration with one of these in a property name is refused.
const SECRET_WORDS: [&str; 10] = [
    "password",
    "passwd",
    "secret",
    "token",
    "otp",
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "credential",
];

/// The secret-like word in a property name, if any.
pub fn secret_word(name: &str) -> Option<&'static str> {
    SECRET_WORDS.into_iter().find(|word| name.contains(word))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert!(is_source("mail-worker"));
        assert!(!is_source("Home"));
        assert!(is_ident("latency_ms"));
        assert!(!is_ident("latencyMs"));
        assert!(is_event_name("shop.order.placed"));
        assert!(is_event_name("http.request"));
        assert!(!is_event_name("request"));
        assert!(!is_event_name("home..action"));
        assert!(is_metric_name("docker_container_mem.usage"));
        assert!(!is_metric_name("1bad"));
        assert!(is_id("0192f0c1-7a3e-7b1a-9c2d-acde48001122"));
        assert!(!is_id("has space"));
    }

    #[test]
    fn secrets() {
        assert_eq!(secret_word("otp_code"), Some("otp"));
        assert_eq!(secret_word("session_token"), Some("token"));
        assert_eq!(secret_word("device"), None);
    }
}
