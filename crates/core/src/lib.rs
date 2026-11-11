//! Trail's shared types. Everything here is plain data and pure functions: the store, the server
//! and the command all build on it, and none of it opens a socket or a database.
//!
//! - [`time`]: timestamps, durations and day boundaries in a configured timezone.
//! - [`envelope`]: what a sender posts (`IngestBatch`) and the records the store keeps.
//! - [`decl`]: a service's declarations (`events.toml`, `metrics.toml`, `rollups.toml`,
//!   `alerts.toml`), parsed and validated.
//! - [`registry`]: every published declaration, and the check each incoming record gets.
//! - [`config`]: the server's `trail.toml`.

pub mod config;
pub mod decl;
pub mod envelope;
pub mod fingerprint;
pub mod ids;
pub mod lineproto;
pub mod names;
pub mod problem;
pub mod registry;
pub mod time;

pub use problem::Problem;
pub use time::Ts;
