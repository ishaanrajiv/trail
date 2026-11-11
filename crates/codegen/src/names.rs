//! Name conversion and text escaping shared by the three generators, and used by [`crate::check`]
//! so the check and the generated code always agree on what a name becomes.

/// The ASCII letter-and-digit runs of `text`. Everything else (`.`, `_`, `-`, `:`, spaces, other
/// punctuation, non-ASCII) separates words.
pub fn words(text: &str) -> Vec<&str> {
    text.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect()
}

/// `shop.order.placed` → `shopOrderPlaced`, `out-of-stock` → `outOfStock`. A word written all in
/// capitals is lowered first (`HTTP_STATUS` → `httpStatus`). Empty when `text` has no ASCII letter
/// or digit.
pub fn lower_camel(text: &str) -> String {
    let mut out = String::new();
    for (i, word) in words(text).into_iter().enumerate() {
        let word = if word.chars().any(|c| c.is_ascii_lowercase()) {
            word.to_string()
        } else {
            word.to_ascii_lowercase()
        };
        let mut chars = word.chars();
        if let Some(first) = chars.next() {
            if i == 0 {
                out.push(first.to_ascii_lowercase());
            } else {
                out.push(first.to_ascii_uppercase());
            }
            out.extend(chars);
        }
    }
    out
}

/// `shop.order.failed` → `ShopOrderFailed`, `mail-worker` → `MailWorker`.
pub fn upper_camel(text: &str) -> String {
    let lower = lower_camel(text);
    let mut chars = lower.chars();
    match chars.next() {
        Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

/// `name`, with a trailing `_` when it is in one of the `reserved` lists.
pub fn unreserve(name: String, reserved: &[&[&str]]) -> String {
    if reserved.iter().any(|list| list.contains(&name.as_str())) {
        name + "_"
    } else {
        name
    }
}

/// Text for a one-line comment or docstring: every run of whitespace or control characters
/// becomes one space, and the ends are trimmed.
pub fn flatten(text: &str) -> String {
    text.split(|c: char| c.is_whitespace() || c.is_control())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// A metric's one-line doc: its description, and its unit in brackets when it has one.
pub fn metric_doc(description: &str, unit: Option<&str>) -> String {
    match unit.map(flatten) {
        Some(unit) if !unit.is_empty() => format!("{} ({unit})", flatten(description)),
        _ => flatten(description),
    }
}

/// Which escape a control character gets in a string literal.
pub enum Escape {
    Python,
    TypeScript,
    Swift,
}

/// `text` as a double-quoted string literal in one of the three languages.
pub fn quote(text: &str, style: Escape) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            // Line terminators in JavaScript source before ES2019, and confusing everywhere.
            '\u{2028}' | '\u{2029}' | '\u{85}' => push_escape(&mut out, c, &style),
            c if c.is_control() => push_escape(&mut out, c, &style),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn push_escape(out: &mut String, c: char, style: &Escape) {
    let code = u32::from(c);
    match style {
        Escape::Python if code <= 0xff => out.push_str(&format!("\\x{code:02x}")),
        Escape::Python => out.push_str(&format!("\\u{code:04x}")),
        Escape::TypeScript => out.push_str(&format!("\\u{code:04x}")),
        Escape::Swift => out.push_str(&format!("\\u{{{code:x}}}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn camel_case() {
        assert_eq!(lower_camel("shop.order.placed"), "shopOrderPlaced");
        assert_eq!(lower_camel("out-of-stock"), "outOfStock");
        assert_eq!(lower_camel("a b"), "aB");
        assert_eq!(lower_camel("2fa"), "2fa");
        assert_eq!(lower_camel("HTTP_STATUS"), "httpStatus");
        assert_eq!(lower_camel("cpu.usage:1m"), "cpuUsage1m");
        assert_eq!(lower_camel("日本"), "");
        assert_eq!(upper_camel("mail-worker"), "MailWorker");
        assert_eq!(upper_camel("shop.order.failed"), "ShopOrderFailed");
    }

    #[test]
    fn flattens_text() {
        assert_eq!(flatten("  one\n two\t\tthree\u{0}  "), "one two three");
        assert_eq!(metric_doc("Jobs waiting", Some("jobs")), "Jobs waiting (jobs)");
        assert_eq!(metric_doc("Jobs waiting", None), "Jobs waiting");
    }

    #[test]
    fn quotes_literals() {
        let text = "a\"b\\c\nd\u{1}\u{2028}é";
        assert_eq!(quote(text, Escape::Python), "\"a\\\"b\\\\c\\nd\\x01\\u2028é\"");
        assert_eq!(
            quote(text, Escape::TypeScript),
            "\"a\\\"b\\\\c\\nd\\u0001\\u2028é\""
        );
        assert_eq!(quote(text, Escape::Swift), "\"a\\\"b\\\\c\\nd\\u{1}\\u{2028}é\"");
    }
}
