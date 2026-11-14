//! Every fixed SQL statement in the crate. `tests::every_statement_prepares` prepares each one
//! against a fresh database, so a typo in a table or column name fails a test; the behaviour
//! tests execute them. Filtered reads add a WHERE clause to the `*_SELECT` statements.

macro_rules! event_cols {
    () => {
        "id, name, ts, received_at, source, version, host, actor, client, trace_id, props, problems, seq"
    };
}
macro_rules! error_cols {
    () => {
        "id, ts, received_at, source, version, host, severity, kind, message, fingerprint, stack, context, trace_id, seq"
    };
}
macro_rules! log_cols {
    () => {
        "id, ts, received_at, source, host, stream, level, message, fields, seq"
    };
}
macro_rules! group_cols {
    () => {
        "fingerprint, source, kind, message, first_seen, last_seen, count, status"
    };
}
macro_rules! series_cols {
    () => {
        "id, name, labels, source, unit, kind, first_ts, last_ts"
    };
}

macro_rules! statements {
    ($($name:ident = $sql:expr;)*) => {
        $(pub const $name: &str = $sql;)*
        /// Every statement above, by name.
        #[cfg(test)]
        pub const ALL: &[(&str, &str)] = &[$((stringify!($name), $name)),*];
    };
}

statements! {
    // ---------- records ----------
    EVENT_INSERT = concat!("INSERT INTO events (", event_cols!(), ") VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13) ON CONFLICT (source, id) DO NOTHING");
    ERROR_INSERT = concat!("INSERT INTO errors (", error_cols!(), ") VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14) ON CONFLICT (source, id) DO NOTHING");
    LOG_INSERT = concat!("INSERT INTO logs (", log_cols!(), ") VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) ON CONFLICT (source, id) DO NOTHING");
    EVENTS_SELECT = concat!("SELECT ", event_cols!(), " FROM events");
    EVENTS_NEWEST = concat!("SELECT ", event_cols!(), " FROM events ORDER BY rowid DESC");
    ERRORS_SELECT = concat!("SELECT ", error_cols!(), " FROM errors");
    LOGS_SELECT = concat!("SELECT ", log_cols!(), " FROM logs");
    GROUPS_SELECT = concat!("SELECT ", group_cols!(), " FROM error_groups");
    EVENTS_BY_NAME = concat!("SELECT ", event_cols!(), " FROM events WHERE name = ?1 AND ts >= ?2 AND ts < ?3 ORDER BY ts");
    EVENT_KEYS_BY_NAME = "SELECT source, id FROM events WHERE name = ?1 AND ts >= ?2 AND ts < ?3";
    EVENTS_DAY = concat!("SELECT ", event_cols!(), " FROM events WHERE ts >= ?1 AND ts < ?2 ORDER BY name, ts, id, source");
    ERRORS_DAY = concat!("SELECT ", error_cols!(), " FROM errors WHERE ts >= ?1 AND ts < ?2 ORDER BY source, ts, id");
    LOGS_DAY = concat!("SELECT ", log_cols!(), " FROM logs WHERE ts >= ?1 AND ts < ?2 ORDER BY source, ts, id");
    EVENT_NAMES_RANGE = "SELECT DISTINCT name FROM events WHERE ts >= ?1 AND ts < ?2";
    EVENT_SEQS_DAY = "SELECT source, id, seq FROM events WHERE ts >= ?1 AND ts < ?2 AND seq IS NOT NULL";
    ERROR_SEQS_DAY = "SELECT source, id, seq FROM errors WHERE ts >= ?1 AND ts < ?2 AND seq IS NOT NULL";
    LOG_SEQS_DAY = "SELECT source, id, seq FROM logs WHERE ts >= ?1 AND ts < ?2 AND seq IS NOT NULL";

    // ---------- error groups and problems ----------
    ERROR_GROUP_UPSERT = "INSERT INTO error_groups (fingerprint, source, kind, message, first_seen, last_seen, count, status)
        VALUES (?1, ?2, ?3, ?4, ?5, ?5, 1, 'open')
        ON CONFLICT (fingerprint) DO UPDATE SET
            count = count + 1,
            first_seen = min(first_seen, excluded.first_seen),
            last_seen = max(last_seen, excluded.last_seen),
            status = CASE WHEN status = 'resolved' THEN 'open' ELSE status END";
    ERROR_GROUP_STATUS = "UPDATE error_groups SET status = ?1 WHERE fingerprint = ?2";
    ERROR_GROUP_ONE = concat!("SELECT ", group_cols!(), " FROM error_groups WHERE fingerprint = ?1");
    PROBLEM_UPSERT = "INSERT INTO problems (source, kind, name, message, first_seen, last_seen, count)
        VALUES (?1, ?2, ?3, ?4, ?5, ?5, 1)
        ON CONFLICT (source, kind, name, message) DO UPDATE SET
            count = count + 1,
            first_seen = min(first_seen, excluded.first_seen),
            last_seen = max(last_seen, excluded.last_seen)";
    PROBLEMS_LIST = "SELECT source, kind, name, message, first_seen, last_seen, count FROM problems
        WHERE ?1 IS NULL OR source = ?1 ORDER BY last_seen DESC, source, kind, name, message";
    PROBLEMS_CLEAR = "DELETE FROM problems WHERE source = ?1";
    PROBLEMS_TRIM = "DELETE FROM problems WHERE rowid IN
        (SELECT rowid FROM problems ORDER BY last_seen DESC, rowid DESC LIMIT -1 OFFSET ?1)";
    GROUPS_TRIM = "DELETE FROM error_groups WHERE rowid IN
        (SELECT rowid FROM error_groups ORDER BY last_seen DESC, rowid DESC LIMIT -1 OFFSET ?1)";

    // ---------- registry, state, marks ----------
    REGISTRY_ALL = "SELECT source, declarations FROM registry ORDER BY source";
    REGISTRY_PUT = "INSERT INTO registry (source, declarations) VALUES (?1, ?2)
        ON CONFLICT (source) DO UPDATE SET declarations = excluded.declarations";
    REGISTRY_DELETE = "DELETE FROM registry WHERE source = ?1";
    STATE_GET = "SELECT value FROM state WHERE key = ?1";
    STATE_SET = "INSERT INTO state (key, value) VALUES (?1, ?2) ON CONFLICT (key) DO UPDATE SET value = excluded.value";
    MARK_GET = "SELECT value FROM marks WHERE key = ?1";
    MARK_SET = "INSERT INTO marks (key, value) VALUES (?1, ?2) ON CONFLICT (key) DO UPDATE SET value = excluded.value";

    // ---------- series and samples ----------
    SERIES_STATE_ALL = "SELECT id, name, labels, kind, unit, first_ts, last_ts, last_raw, last_raw_ts FROM series";
    SERIES_INSERT = "INSERT INTO series (name, labels, source, unit, kind, first_ts, last_ts) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";
    SERIES_UPDATE = "UPDATE series SET unit = ?1, kind = ?2, first_ts = ?3, last_ts = ?4, last_raw = ?5, last_raw_ts = ?6 WHERE id = ?7";
    SERIES_BY_NAME = concat!("SELECT ", series_cols!(), " FROM series WHERE name = ?1 ORDER BY name, labels");
    SERIES_ALL = concat!("SELECT ", series_cols!(), " FROM series ORDER BY name, labels");
    SERIES_SPANS = "SELECT id, first_ts, last_ts FROM series ORDER BY id";
    SERIES_IDS = "SELECT id FROM series ORDER BY id";
    SERIES_NAMES = "SELECT id, name, labels FROM series ORDER BY id";
    SAMPLE_UPSERT = "INSERT OR REPLACE INTO samples (series, ts, value) VALUES (?1, ?2, ?3)";
    SAMPLES_RANGE = "SELECT ts, value FROM samples WHERE series = ?1 AND ts >= ?2 AND ts < ?3 ORDER BY ts";
    SAMPLE_LATEST = "SELECT ts, value FROM samples WHERE series = ?1 ORDER BY ts DESC LIMIT 1";
    SAMPLE_WINDOW_AVG = "SELECT avg(value) FROM samples WHERE series = ?1 AND ts >= ?2 AND ts < ?3";

    // ---------- tiers ----------
    TIER_5M_RANGE = "SELECT ts, avg, min, max, n FROM samples_5m WHERE series = ?1 AND ts >= ?2 AND ts < ?3";
    TIER_1H_RANGE = "SELECT ts, avg, min, max, n FROM samples_1h WHERE series = ?1 AND ts >= ?2 AND ts < ?3";
    TIER_5M_BUILD = "INSERT OR REPLACE INTO samples_5m (series, ts, avg, min, max, n)
        SELECT ?1, (ts / 300000) * 300000 AS b, avg(value), min(value), max(value), count(*)
        FROM samples WHERE series = ?1 AND ts >= ?2 AND ts < ?3 GROUP BY b";
    TIER_1H_BUILD = "INSERT OR REPLACE INTO samples_1h (series, ts, avg, min, max, n)
        SELECT ?1, ((ts + ?4) / 3600000) * 3600000 - ?4 AS b, sum(avg * n) / sum(n), min(min), max(max), sum(n)
        FROM samples_5m WHERE series = ?1 AND ts >= ?2 AND ts < ?3 GROUP BY b";
    TIER_5M_SUM = "SELECT sum(avg * n), sum(n), min(min), max(max) FROM samples_5m WHERE series = ?1 AND ts >= ?2 AND ts < ?3";
    TIER_5M_PUT = "INSERT OR REPLACE INTO samples_5m (series, ts, avg, min, max, n) VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
    TIER_1H_PUT = "INSERT OR REPLACE INTO samples_1h (series, ts, avg, min, max, n) VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
    TIER_5M_LATEST = "SELECT avg FROM samples_5m WHERE series = ?1 ORDER BY ts DESC LIMIT 1";
    TIER_1H_LATEST = "SELECT avg FROM samples_1h WHERE series = ?1 ORDER BY ts DESC LIMIT 1";
    TIER_5M_HAS = "SELECT 1 FROM samples_5m WHERE series = ?1 AND ts = ?2";
    TIER_1H_HAS = "SELECT 1 FROM samples_1h WHERE series = ?1 AND ts = ?2";
    TIER_5M_DELETE = "DELETE FROM samples_5m WHERE series = ?1 AND ts = ?2";
    TIER_1H_DELETE = "DELETE FROM samples_1h WHERE series = ?1 AND ts = ?2";
    TIER_5M_EXPIRE = "DELETE FROM samples_5m WHERE series = ?1 AND ts < ?2";
    TIER_1H_EXPIRE = "DELETE FROM samples_1h WHERE series = ?1 AND ts < ?2";
    DIRTY_MARK = "INSERT OR IGNORE INTO tier_dirty (tier, series, ts) VALUES (?1, ?2, ?3)";
    DIRTY_LIST = "SELECT series, ts FROM tier_dirty WHERE tier = ?1 ORDER BY series, ts";
    DIRTY_CLEAR = "DELETE FROM tier_dirty WHERE tier = ?1";

    // ---------- archive ----------
    ARCHIVE_PUT = "INSERT INTO archive_days (store, class, day, rows, bytes, max_seq) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT (store, class, day) DO UPDATE SET rows = excluded.rows, bytes = excluded.bytes, max_seq = excluded.max_seq";
    ARCHIVE_DELETE = "DELETE FROM archive_days WHERE store = ?1 AND class = ?2 AND day = ?3";
    ARCHIVE_ALL = "SELECT store, class, day, rows, bytes, max_seq FROM archive_days ORDER BY day, store, class";
    ARCHIVE_FILED_DAYS = "SELECT DISTINCT store, day FROM archive_days";
    ARCHIVE_STORE_DAYS = "SELECT DISTINCT day FROM archive_days WHERE store = ?1";
    ARCHIVE_DAY_CLASSES = "SELECT class FROM archive_days WHERE store = ?1 AND day = ?2";
    ARCHIVE_DAYS_RANGE = "SELECT class, day FROM archive_days WHERE store = ?1 AND day >= ?2 AND day <= ?3 ORDER BY day DESC, class";
    ARCHIVE_DAYS_AFTER_SEQ = "SELECT class, day FROM archive_days WHERE store = ?1 AND max_seq > ?2 AND day >= ?3 AND day <= ?4 ORDER BY day, class";
    ARCHIVE_TOTALS = "SELECT count(*), coalesce(sum(bytes), 0) FROM archive_days WHERE store = ?1";
    LATE_MARK = "INSERT INTO archive_late (store, day, claimed) VALUES (?1, ?2, 0)
        ON CONFLICT (store, day) DO UPDATE SET claimed = 0";
    LATE_LIST = "SELECT day FROM archive_late WHERE store = ?1";
    LATE_CLAIM = "UPDATE archive_late SET claimed = 1 WHERE store = ?1 AND day = ?2";
    LATE_DONE = "DELETE FROM archive_late WHERE store = ?1 AND day = ?2 AND claimed = 1";
    LATE_UNFILED = "DELETE FROM archive_late WHERE store = ?1 AND day = ?2
        AND NOT EXISTS (SELECT 1 FROM archive_days a WHERE a.store = ?1 AND a.day = ?2)";
    EVENTS_MIN_TS = "SELECT min(ts) FROM events WHERE ts >= ?1 AND ts < ?2";
    ERRORS_MIN_TS = "SELECT min(ts) FROM errors WHERE ts >= ?1 AND ts < ?2";
    LOGS_MIN_TS = "SELECT min(ts) FROM logs WHERE ts >= ?1 AND ts < ?2";
    SAMPLES_MIN_TS = "SELECT min(ts) FROM samples WHERE series = ?1 AND ts >= ?2 AND ts < ?3";
    EVENTS_DELETE_RANGE = "DELETE FROM events WHERE ts >= ?1 AND ts < ?2";
    ERRORS_DELETE_RANGE = "DELETE FROM errors WHERE ts >= ?1 AND ts < ?2";
    LOGS_DELETE_RANGE = "DELETE FROM logs WHERE ts >= ?1 AND ts < ?2";
    SAMPLES_DELETE_RANGE = "DELETE FROM samples WHERE series = ?1 AND ts >= ?2 AND ts < ?3";
    LOGS_EXPIRE = "DELETE FROM logs WHERE ts < ?1";
    ERRORS_EXPIRE = "DELETE FROM errors WHERE ts < ?1";

    // ---------- stats ----------
    COUNT_EVENTS = "SELECT count(*) FROM events";
    COUNT_SAMPLES = "SELECT count(*) FROM samples";
    COUNT_ERRORS = "SELECT count(*) FROM errors";
    COUNT_LOGS = "SELECT count(*) FROM logs";
    COUNT_SERIES = "SELECT count(*) FROM series";
}
