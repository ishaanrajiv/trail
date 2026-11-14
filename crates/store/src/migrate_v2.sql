-- Version 1 to 2 (PRAGMA user_version). spec/storage.md describes every table; schema.sql is
-- version 1 and never changes. This runs in one transaction, after `previous/v1/` is in place.

-- Every event, error and log gets a seq: its position in arrival order, from one counter for the
-- whole server. Rows already here are numbered in (received_at, id) order across the three
-- tables; marks.seq holds the last one given.
CREATE TEMP TABLE v2_seq (
    t TEXT NOT NULL,
    r INTEGER NOT NULL,
    n INTEGER NOT NULL,
    PRIMARY KEY (t, r)
) WITHOUT ROWID;
INSERT INTO v2_seq (t, r, n)
    SELECT t, r, row_number() OVER (ORDER BY received_at, id, source, t) FROM (
        SELECT 'events' AS t, rowid AS r, received_at, id, source FROM events
        UNION ALL SELECT 'errors', rowid, received_at, id, source FROM errors
        UNION ALL SELECT 'logs', rowid, received_at, id, source FROM logs
    );
INSERT INTO marks (key, value) SELECT 'seq', coalesce(max(n), 0) FROM v2_seq WHERE true
    ON CONFLICT (key) DO UPDATE SET value = excluded.value;

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
    seq INTEGER,              -- position in arrival order; see marks.seq
    PRIMARY KEY (source, id)
);
INSERT INTO events_v2 (rowid, id, name, ts, received_at, source, version, host, actor, client, trace_id, props, problems, seq)
    SELECT rowid, id, name, ts, received_at, source, version, host, actor, client, trace_id, props, problems,
        (SELECT n FROM v2_seq WHERE t = 'events' AND r = events.rowid)
    FROM events ORDER BY rowid;
DROP TABLE events;
ALTER TABLE events_v2 RENAME TO events;
CREATE INDEX events_ts ON events (ts);
CREATE INDEX events_name_ts ON events (name, ts);
CREATE INDEX events_trace ON events (trace_id) WHERE trace_id IS NOT NULL;
CREATE UNIQUE INDEX events_seq ON events (seq);

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
    seq INTEGER,              -- position in arrival order; see marks.seq
    PRIMARY KEY (source, id)
);
INSERT INTO errors_v2 (rowid, id, ts, received_at, source, version, host, severity, kind, message, fingerprint, stack, context, trace_id, seq)
    SELECT rowid, id, ts, received_at, source, version, host, severity, kind, message, fingerprint, stack, context, trace_id,
        (SELECT n FROM v2_seq WHERE t = 'errors' AND r = errors.rowid)
    FROM errors ORDER BY rowid;
DROP TABLE errors;
ALTER TABLE errors_v2 RENAME TO errors;
CREATE INDEX errors_ts ON errors (ts);
CREATE INDEX errors_fingerprint_ts ON errors (fingerprint, ts);
CREATE UNIQUE INDEX errors_seq ON errors (seq);

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
    seq INTEGER,              -- position in arrival order; see marks.seq
    PRIMARY KEY (source, id)
);
INSERT INTO logs_v2 (rowid, id, ts, received_at, source, host, stream, level, message, fields, seq)
    SELECT rowid, id, ts, received_at, source, host, stream, level, message, fields,
        (SELECT n FROM v2_seq WHERE t = 'logs' AND r = logs.rowid)
    FROM logs ORDER BY rowid;
DROP TABLE logs;
ALTER TABLE logs_v2 RENAME TO logs;
CREATE INDEX logs_ts ON logs (ts);
CREATE INDEX logs_source_ts ON logs (source, ts);
CREATE UNIQUE INDEX logs_seq ON logs (seq);
DROP TABLE v2_seq;

-- The largest seq in each day file, so a read by seq opens only the files that can hold one it
-- wants. NULL for a file written before seq existed: its rows have none.
ALTER TABLE archive_days ADD COLUMN max_seq INTEGER;
