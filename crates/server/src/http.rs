//! Request plumbing: a `tiny_http` request in, `api::route`'s reply out.

use crate::App;
use crate::api::{self, Reply, Request};
use std::io::Read;
use trail_core::{Problem, Ts};

/// Decodes `%41` and `+` in a query string part.
fn decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|hex| u8::from_str_radix(hex, 16).ok()) {
                    Some(byte) => {
                        out.push(byte);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            other => out.push(other),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Splits a request URL into its path and decoded query parameters.
pub fn split_url(url: &str) -> (String, Vec<(String, String)>) {
    let (path, query) = url.split_once('?').unwrap_or((url, ""));
    let pairs = query
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            (decode(key), decode(value))
        })
        .collect();
    (decode(path), pairs)
}

fn bearer(request: &tiny_http::Request) -> Option<String> {
    request
        .headers()
        .iter()
        .find(|header| header.field.equiv("Authorization"))
        .and_then(|header| {
            let value = header.value.as_str();
            value
                .strip_prefix("Bearer ")
                .or_else(|| value.strip_prefix("bearer "))
                // InfluxDB clients send `Token <token>`.
                .or_else(|| value.strip_prefix("Token "))
                .map(|token| token.trim().to_string())
        })
}

/// Answers one HTTP request.
pub(crate) fn handle(app: &App, mut request: tiny_http::Request) {
    let limit = app.config.limits.body_bytes;
    let (path, query) = split_url(request.url());
    let mut body = Vec::new();
    let too_large = request.body_length().is_some_and(|length| length > limit)
        || request
            .as_reader()
            .take(limit as u64 + 1)
            .read_to_end(&mut body)
            .is_ok_and(|read| read > limit);
    let reply = if too_large {
        Reply::problem(
            413,
            &Problem::new(
                "body",
                format!("the body is over {limit} bytes"),
                "send smaller batches, or raise limits.body_bytes in trail.toml",
            ),
        )
    } else {
        api::route(
            app,
            &Request {
                method: request.method().as_str().to_string(),
                path,
                query,
                token: bearer(&request),
                body,
            },
            Ts::now(),
        )
    };
    let text = reply.body.map(|body| body.to_string()).unwrap_or_default();
    let mut response = tiny_http::Response::from_string(text).with_status_code(reply.status);
    if let Ok(header) = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]) {
        response.add_header(header);
    }
    // The caller hanging up is its own business.
    let _ = request.respond(response);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_split_and_decode() {
        let (path, query) = split_url("/v1/events?name=shop.order.%2A&q=a+b&flag&label.queue=e%20mail");
        assert_eq!(path, "/v1/events");
        assert_eq!(
            query,
            vec![
                ("name".to_string(), "shop.order.*".to_string()),
                ("q".to_string(), "a b".to_string()),
                ("flag".to_string(), String::new()),
                ("label.queue".to_string(), "e mail".to_string()),
            ]
        );
        assert_eq!(split_url("/v1").1, vec![]);
        assert_eq!(decode("100%"), "100%");
        assert_eq!(decode("%zz"), "%zz");
    }
}
