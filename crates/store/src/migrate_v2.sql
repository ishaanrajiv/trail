-- Version 1 to 2 (PRAGMA user_version). spec/storage.md describes every table; schema.sql is
-- version 1 and never changes. This runs in one transaction, after `previous/v1/` is in place.

-- A record's identity is (source, id): two sources that use the same id, or one whose counter
-- restarts, keep both records. SQLite can't change a primary key in place, so each table is
-- rebuilt under the same name with the same columns, keeping every row and its rowid.
CREATE TABLE events_v2 (
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    ts INTEGER NOT NULL,
    received_at INTEGER NOT NULL,
    source TEXT NOT NULL,
    version TEXT,
    host TEXT,
    actor TEXT,
    client TEXT,
    trace_id TEXT,
    props TEXT NOT NULL,      -- JSON object
    problems TEXT,            -- JSON list of strings; NULL when the event matched its declaration
    PRIMARY KEY (source, id)
);
INSERT INTO events_v2 (rowid, id, name, ts, received_at, source, version, host, actor, client, trace_id, props, problems)
    SELECT rowid, id, name, ts, received_at, source, version, host, actor, client, trace_id, props, problems
    FROM events ORDER BY rowid;
DROP TABLE events;
ALTER TABLE events_v2 RENAME TO events;
CREATE INDEX events_ts ON events (ts);
CREATE INDEX events_name_ts ON events (name, ts);
CREATE INDEX events_trace ON events (trace_id) WHERE trace_id IS NOT NULL;

CREATE TABLE errors_v2 (
    id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    received_at INTEGER NOT NULL,
    source TEXT NOT NULL,
    version TEXT,
    host TEXT,
    severity TEXT NOT NULL,
    kind TEXT NOT NULL,
    message TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    stack TEXT,
    context TEXT NOT NULL,    -- JSON object
    trace_id TEXT,
    PRIMARY KEY (source, id)
);
INSERT INTO errors_v2 (rowid, id, ts, received_at, source, version, host, severity, kind, message, fingerprint, stack, context, trace_id)
    SELECT rowid, id, ts, received_at, source, version, host, severity, kind, message, fingerprint, stack, context, trace_id
    FROM errors ORDER BY rowid;
DROP TABLE errors;
ALTER TABLE errors_v2 RENAME TO errors;
CREATE INDEX errors_ts ON errors (ts);
CREATE INDEX errors_fingerprint_ts ON errors (fingerprint, ts);

CREATE TABLE logs_v2 (
    id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    received_at INTEGER NOT NULL,
    source TEXT NOT NULL,
    host TEXT,
    stream TEXT,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    fields TEXT NOT NULL,     -- JSON object
    PRIMARY KEY (source, id)
);
INSERT INTO logs_v2 (rowid, id, ts, received_at, source, host, stream, level, message, fields)
    SELECT rowid, id, ts, received_at, source, host, stream, level, message, fields
    FROM logs ORDER BY rowid;
DROP TABLE logs;
ALTER TABLE logs_v2 RENAME TO logs;
CREATE INDEX logs_ts ON logs (ts);
CREATE INDEX logs_source_ts ON logs (source, ts);
