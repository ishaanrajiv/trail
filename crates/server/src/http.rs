//! What Trail puts behind the shared transport (`httpd.rs`): which slot each request waits for,
//! the bearer token, the `415` for a compressed body, and Trail's error body.

use crate::App;
use crate::api::{self, Reply};
use crate::httpd::{Handler, Lane, Refusal, Request, Response};
use std::sync::Arc;
use trail_core::{Problem, Ts};

/// The token in an `Authorization` header.
fn bearer(value: &str) -> Option<String> {
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        // InfluxDB clients send `Token <token>`.
        .or_else(|| value.strip_prefix("Token "))
        .map(|token| token.trim().to_string())
}

/// What a request waits for before it is worked on (`spec/config.md`, "How much of the machine").
fn lane_of(method: &str, path: &str) -> Lane {
    match (method, path.trim_end_matches('/')) {
        // The health check and the route list answer from memory, at once.
        ("GET", "/v1/health" | "/v1" | "") => Lane::Free,
        // Ingest is one writer at a time, and never waits behind a query.
        ("POST", "/v1/ingest" | "/v1/write" | "/api/v2/write" | "/write") => Lane::Write,
        // One of the `workers` query slots. Everything else.
        _ => Lane::Work,
    }
}

/// The `Content-Encoding` of a body Trail can't read: anything but none or `identity`.
fn encoding(request: &Request) -> Option<String> {
    request
        .header("content-encoding")
        .map(str::to_ascii_lowercase)
        .filter(|encoding| !encoding.is_empty() && encoding != "identity")
}

fn response(reply: Reply) -> Response {
    Response::json(
        reply.status,
        reply.body.map(|body| body.to_string()).unwrap_or_default(),
    )
}

/// Trail's routes, behind the transport.
pub(crate) struct Web {
    pub app: Arc<App>,
}

impl Handler for Web {
    fn lane(&self, request: &Request) -> Lane {
        if encoding(request).is_some() {
            // Refused at once below; no reason to wait for a slot first.
            return Lane::Free;
        }
        lane_of(&request.method, &request.path)
    }

    fn handle(&self, request: Request) -> Response {
        if let Some(encoding) = encoding(&request) {
            return response(Reply::problem(
                415,
                &Problem::new(
                    "Content-Encoding",
                    format!("the body is compressed with `{encoding}`, which Trail does not read"),
                    "send it uncompressed: in Telegraf set content_encoding = \"identity\" on the output",
                ),
            ));
        }
        let token = request.header("authorization").and_then(bearer);
        let reply = api::route(
            &self.app,
            &api::Request {
                method: request.method,
                path: request.path,
                query: request.query,
                token,
                body: request.body,
            },
            Ts::now(),
        );
        response(reply)
    }

    fn refused(&self, refusal: Refusal) -> Response {
        response(Reply::problem(
            refusal.status,
            &Problem::new(refusal.at, refusal.message, refusal.fix),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_each_request_waits_for() {
        assert_eq!(lane_of("GET", "/v1/health"), Lane::Free);
        assert_eq!(lane_of("GET", "/v1/"), Lane::Free);
        assert_eq!(lane_of("POST", "/v1/ingest"), Lane::Write);
        assert_eq!(lane_of("POST", "/api/v2/write"), Lane::Write);
        assert_eq!(lane_of("GET", "/v1/logs"), Lane::Work);
        assert_eq!(lane_of("PUT", "/v1/registry/shop"), Lane::Work);
        assert_eq!(lane_of("POST", "/v1/health"), Lane::Work);
    }

    #[test]
    fn tokens_are_read_from_either_scheme() {
        assert_eq!(bearer("Bearer abc ").as_deref(), Some("abc"));
        assert_eq!(bearer("bearer abc").as_deref(), Some("abc"));
        assert_eq!(bearer("Token abc").as_deref(), Some("abc"));
        assert_eq!(bearer("Basic abc"), None);
    }
}
