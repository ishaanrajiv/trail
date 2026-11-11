-- Trail's database, version 1 (PRAGMA user_version). spec/storage.md describes every table.
-- A later version adds a migration beside this file rather than editing it.

-- Records. Today and the last `hot_days` finished days live here; older days are in archive/.
CREATE TABLE events (
    id TEXT PRIMARY KEY,
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
    problems TEXT             -- JSON list of strings; NULL when the event matched its declaration
);
CREATE INDEX events_ts ON events (ts);
CREATE INDEX events_name_ts ON events (name, ts);
CREATE INDEX events_trace ON events (trace_id) WHERE trace_id IS NOT NULL;

CREATE TABLE errors (
    id TEXT PRIMARY KEY,
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
    trace_id TEXT
);
CREATE INDEX errors_ts ON errors (ts);
CREATE INDEX errors_fingerprint_ts ON errors (fingerprint, ts);

CREATE TABLE logs (
    id TEXT PRIMARY KEY,
    ts INTEGER NOT NULL,
    received_at INTEGER NOT NULL,
    source TEXT NOT NULL,
    host TEXT,
    stream TEXT,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    fields TEXT NOT NULL      -- JSON object
);
CREATE INDEX logs_ts ON logs (ts);
CREATE INDEX logs_source_ts ON logs (source, ts);

-- Metrics. last_raw and last_raw_ts hold a counter's previous reading, for the next rate.
CREATE TABLE series (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    labels TEXT NOT NULL,     -- JSON object, keys sorted
    source TEXT NOT NULL,
    unit TEXT,
    kind TEXT NOT NULL,       -- gauge or counter
    first_ts INTEGER NOT NULL,
    last_ts INTEGER NOT NULL,
    last_raw REAL,
    last_raw_ts INTEGER,
    UNIQUE (name, labels)
);

CREATE TABLE samples (
    series INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    value REAL NOT NULL,
    PRIMARY KEY (series, ts)
) WITHOUT ROWID;

CREATE TABLE samples_5m (
    series INTEGER NOT NULL,
    ts INTEGER NOT NULL,      -- bucket start
    avg REAL NOT NULL,
    min REAL NOT NULL,
    max REAL NOT NULL,
    n INTEGER NOT NULL,
    PRIMARY KEY (series, ts)
) WITHOUT ROWID;

CREATE TABLE samples_1h (
    series INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    avg REAL NOT NULL,
    min REAL NOT NULL,
    max REAL NOT NULL,
    n INTEGER NOT NULL,
    PRIMARY KEY (series, ts)
) WITHOUT ROWID;

-- Tier buckets that got a sample after they were built: recomputed at the next maintenance.
CREATE TABLE tier_dirty (
    tier TEXT NOT NULL,       -- 5m or 1h
    series INTEGER NOT NULL,
    ts INTEGER NOT NULL,      -- bucket start
    PRIMARY KEY (tier, series, ts)
) WITHOUT ROWID;

CREATE TABLE error_groups (
    fingerprint TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    kind TEXT NOT NULL,
    message TEXT NOT NULL,    -- the first occurrence's
    first_seen INTEGER NOT NULL,
    last_seen INTEGER NOT NULL,
    count INTEGER NOT NULL,
    status TEXT NOT NULL      -- open, resolved or ignored
);
CREATE INDEX error_groups_last_seen ON error_groups (last_seen);

CREATE TABLE problems (
    source TEXT NOT NULL,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    message TEXT NOT NULL,
    first_seen INTEGER NOT NULL,
    last_seen INTEGER NOT NULL,
    count INTEGER NOT NULL,
    PRIMARY KEY (source, kind, name, message)
);

CREATE TABLE registry (
    source TEXT PRIMARY KEY,
    declarations TEXT NOT NULL -- JSON, Declarations::to_json
);

-- Day files. class is the archive folder: events, events-30d, samples, errors, logs.
CREATE TABLE archive_days (
    store TEXT NOT NULL,
    class TEXT NOT NULL,
    day TEXT NOT NULL,        -- YYYY-MM-DD, local
    rows INTEGER NOT NULL,
    bytes INTEGER NOT NULL,
    PRIMARY KEY (store, class, day)
);

-- Days that got rows after their file was written: rewritten, merged, at the next archive.
CREATE TABLE archive_late (
    store TEXT NOT NULL,
    day TEXT NOT NULL,
    PRIMARY KEY (store, day)
) WITHOUT ROWID;

-- The store's own progress: how far the tiers are built, where SQLite's complete rows begin.
CREATE TABLE marks (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
) WITHOUT ROWID;

-- Free-form state for the alert evaluator and the aggregator.
CREATE TABLE state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;
