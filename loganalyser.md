### Structured Log Explorer
Extend the existing `loganalyser` project into a full log exploration tool.

**What loganalyser already has:**
- File watching with byte-offset cursor tracking (survives restarts)
- Two parser types: `Single` (point-in-time events) and `Span` (START/END pairs with duration)
- Rich filter DSL: `Cmp` operators (Eq, Gt, Lt, Gte, Lte, Like, In, Json), composable `And`/`Or` expressions, predicates on timestamp, duration, name, data fields, parent ID
- Storage abstraction with SQLite and MySQL backends
- Pending span persistence across restarts

**What it needs to be excellent — detailed TODO:**

*Storage*
- [ ] Add Postgres backend (consistent with preferred stack; replace MySQL)
- [ ] Retention policy: auto-delete events older than N days, configurable per parser
- [ ] Partitioning strategy for the events table (partition by month) so old data can be dropped cheaply

*Ingestion*
- [ ] Native JSON log parsing — detect JSON lines and index all fields without needing a regex config
- [ ] logfmt parser (`key=value key2="value 2"`) as a built-in parser type
- [ ] HTTP ingest endpoint — accept log lines pushed over HTTP so remote services don't need file access
- [ ] Syslog receiver (UDP/TCP) so network devices and systemd can ship logs directly *(Linux/network devices only — Windows doesn't use syslog; the rest of the ingest pipeline is cross-platform)*
- [ ] Log level as a first-class field — `level` extracted and stored as an indexed column, not just a JSON data field

*Querying*
- [ ] HTTP query API — expose `GET /events` with filter params so other tools can query the store
- [x] Full-text search across the raw log line, not just structured fields
- [ ] Aggregation queries: count events by name/level over a time window, average duration of spans
- [ ] Time-bucket grouping: "count errors per hour for the last 7 days" as a single query

*Web UI*
- [ ] Log viewer: paginated list of events, filterable by time range, level, name, data fields
- [ ] Real-time tail: websocket-backed live view of incoming events for a given filter
- [ ] Span waterfall view: given a parent event ID, show all child spans as a trace diagram
- [ ] Saved searches: store a filter with a name, recall it from the UI
- [ ] Charts: error rate over time, p50/p95/p99 span duration over time

*Alerting*
- [ ] Alert rules: "if more than N events matching filter X arrive in Y minutes, fire webhook"
- [ ] Alert history stored in Postgres: when it fired, how many events matched, whether it resolved
- [ ] Silence windows: suppress alerts during known maintenance

*Operational*
- [ ] Multi-file source config via glob patterns (already exists) + remote SSH log tailing
- [ ] CLI query tool: `loganalyser query --name "http_request" --level error --since 1h`
- [ ] Structured export: query results as JSON/CSV download from the UI

**Stack:** Rust + Postgres

---

### Plan: Replace SQLite State DB with Binary File Persistence

The `StateDb` in `file_watcher/src/state_db.rs` persists two things at restart boundaries:
- **File cursors**: `HashMap<String, u64>` — byte offsets per watched file
- **Pending spans**: per `(file_path, parser_name, span_ref)` — incomplete span state to survive restarts

The event storage DB (SQLite/Postgres in `event_storage/`) is **not in scope** — that's an append-only fact log queried via the Filter API.

**Crate**: `postcard` (serde-based binary serialization). Note: bincode is **unmaintained** as of 2025 with a RUSTSEC advisory — do not use it.

**Atomicity**: `tempfile::NamedTempFile` + `fsync` + `persist()` (atomic rename on POSIX).

#### Wire Format

```rust
#[derive(Serialize, Deserialize)]
struct StateSnapshot {
    version: u32,                        // bump when schema changes incompatibly
    file_cursors: HashMap<String, u64>,
    pending_spans: Vec<PendingSpanRecord>,
}
```

`PendingSpanRecord` already has all required fields — add `#[derive(Serialize, Deserialize)]` to it. `Uuid`, `NaiveDateTime`, and `HashMap<String, String>` all work via serde feature flags on those crates.

#### Write Path

```
change happens (cursor updated or pending span upserted)
  → serialize StateSnapshot via postcard::to_stdvec()
  → write to NamedTempFile in same dir as state file
  → file.sync_all()  (fsync before rename)
  → temp_file.persist(STATE_PATH)  (atomic rename)
```

State writes are infrequent (per parsed line section), so rewriting the whole file each time is fine.

#### Read Path

```
watcher startup
  → fs::read(STATE_PATH)  (missing file → start fresh, no error)
  → postcard::from_bytes::<StateSnapshot>(&bytes)
  → version mismatch or corrupt → log warning, start fresh
  → restore cursors + pending spans as today
```

Graceful degradation on corrupt/missing state is safe — worst case is re-reading some already-processed log lines.

#### Forward Compatibility

| Scenario | Strategy |
|---|---|
| New optional field added | Wrap in `Option<T>`; old files deserialize to `None` |
| Field removed or type changed | Bump `version`; discard old state on mismatch |

#### Tradeoffs vs SQLite

| | SQLite `StateDb` | Postcard file |
|---|---|---|
| Partial updates | Yes (upsert single row) | No (rewrite whole file) |
| Queryable | Yes (SQL) | No — but state is never queried |
| Dependencies | `sqlx` + `libsqlite3` | `postcard` + `tempfile` |
| Async required | Yes | **No — can be sync** |
| Schema evolution | `ALTER TABLE` | Manual version field |
| Corruption recovery | SQLite WAL | Start fresh (state is ephemeral) |

Biggest win: state operations become **synchronous** — no connection pool, no `await` chains, much simpler call sites in `file_watcher/src/lib.rs`.

#### Implementation Steps

1. Add `postcard = { version = "1", features = ["alloc"] }` and `tempfile = "3"` to `file_watcher/Cargo.toml`
2. Ensure `uuid` and `chrono` deps have `serde` features enabled
3. Derive `Serialize, Deserialize` on `PendingSpanRecord` and `StateSnapshot`
4. Replace `StateDb` with a `StateFile` struct holding the path + in-memory `StateSnapshot`
5. Implement `load()` → returns `StateSnapshot` (or default on missing/corrupt)
6. Implement `save()` → tempfile + fsync + persist pattern
7. Update `file_watcher/src/lib.rs` call sites — remove `.await` from state saves, drop sqlx pool init
8. Remove `sqlx` dep from `file_watcher/Cargo.toml` if no longer needed there

Result: `StateDb` shrinks from ~150 lines of async SQL to ~60 lines of sync file I/O with no connection management.

