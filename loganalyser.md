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
- [ ] Full-text search across the raw log line, not just structured fields
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

