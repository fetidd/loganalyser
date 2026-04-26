# FileWatcher internals

A guided tour for a new reader: from config file on disk to stored events, covering every
major design decision along the way.

---

## What it does

The file watcher is a daemon that polls one or more log files, runs user-defined regex
parsers over new lines, turns matches into structured events, and writes those events to a
database (SQLite or MySQL).  It handles:

- **Single-line events** — one log line → one event.
- **Span events** — a matching START/END pair → one event carrying the elapsed duration and
  both raw lines.
- **Nested events** — lines appearing between a span's START and END that belong to a child
  parser; emitted as individual events linked to the enclosing span via `parent_id`.
- **Crash recovery** — after an unclean shutdown, open spans and file-read positions are
  restored from a separate SQLite state database so no events are lost or double-counted.
- **Graceful shutdown and automatic restart** — SIGTERM / Ctrl-C finish the current cycle
  cleanly; a database failure triggers a full restart with a fresh connection.

---

## Config file

Everything is driven by a single TOML file passed as the first CLI argument.

```toml
state_db_path = "/var/lib/filewatcher/state.db"   # optional; see State DB section

[settings]
poll_interval_secs = 3        # default if omitted

[storage]
storage_type = "sqlite"       # "sqlite" | "mysql" | "memory"
connection_string = "/var/lib/filewatcher/events.db"

[defaults]
timestamp_format = "%Y-%m-%d %H:%M:%S"   # used by any parser that omits its own

[components]                  # reusable regex fragments, referenced as ${name}
timestamp = '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
ref       = '[A-Z]{3}'

[[parsers]]
type    = "single"
name    = "http_log"
glob    = "/var/log/nginx/*.log"
pattern = '(?P<timestamp>${timestamp}) (?P<status>\d{3}) (?P<path>\S+)'

[[parsers]]
type             = "span"
name             = "request"
glob             = "/var/log/app/app*.log"
start_pattern    = '(?P<timestamp>${timestamp}) (?P<ref>${ref}) START'
end_pattern      = '(?P<timestamp>${timestamp}) (?P<ref>${ref}) END'
reference_fields = ["ref"]

[[parsers.nested]]
type    = "single"
name    = "request_step"
pattern = '(?P<timestamp>${timestamp}) (?P<ref>${ref}) STEP (?P<msg>.*)'
```

Key rules enforced at build time (not runtime):

- Every top-level parser needs a `glob`.
- `timestamp_format` must come from either the parser itself or `[defaults]`.
- Every parser's pattern must contain a named capture group `(?P<timestamp>...)`.
- Every span parser's `start_pattern` and `end_pattern` must both be different and must
  both contain every field listed in `reference_fields`.
- Nested span parsers must declare their own `reference_fields` (cannot be empty).
- A nested span parser may not re-use a `reference_field` already declared by its parent.

---

## Startup: `FileWatcher::new`

`main.rs` reads the config file bytes and calls `FileWatcher::new(&config_file)`.
Everything that can fail during startup happens here so the run loop itself stays clean.

```
config bytes
  │
  ├─► toml::from_slice → Config { settings, storage, state_db_path }
  │
  ├─► make_storage(&config.storage) → EventStorage   (event DB)
  │
  ├─► State::new(&config) → Arc<State>               (crash-recovery DB)
  │
  ├─► Parser::from_config_file(config_bytes)
  │     → HashMap<PathBuf, Vec<Parser>>               (glob-expanded)
  │
  ├─► build_file_parser_map(resolved)
  │     → HashMap<PathBuf, ParserOffsets>             (cursors set to EOF)
  │
  └─► restore_pending_state(map, pending, saved_cursors)
        (rewinds cursors, reinstalls open spans)
```

### Storage setup — `make_storage`

`event_storage/src/config.rs` reads `[storage]` and connects:

| `storage_type` | backend | notes |
|---|---|---|
| `"sqlite"` | SQLite file | `create_if_missing(true)`; connection string is a file path |
| `"mysql"` | MySQL pool | connection string is a URL; SSL disabled by default |
| `"memory"` | in-memory SQLite | useful for development/tests; data lost on exit |

The `EventStorage` enum (`event_storage/src/lib.rs`) wraps all three backends behind
uniform `store()` and `load()` methods. The event table schema includes `id`, `event_type`
(`"single"` or `"span"`), `name`, `timestamp`, `duration_ms`, `parent_id`, `data` (JSON),
and `raw_line`.

Inserts use `INSERT OR IGNORE` (SQLite) / `INSERT IGNORE` (MySQL) so re-processing a line
after a crash produces no duplicates.

### State DB — `State::new`

The state database is **always SQLite**, independent of the event storage backend.  Its path
comes from `state_db_path` in the config, or from a platform default: on Linux it tries
`/var/lib/loganalyser/state.db` first (if writable), then
`~/.local/share/loganalyser/state.db`, then `/tmp/loganalyser/state.db` as a last
resort.  It holds two tables:

```sql
pending_spans (file_path, parser_name, span_ref, id, timestamp, data, parent_id, raw_line)
  PRIMARY KEY (file_path, parser_name, span_ref)

file_cursors  (file_path PRIMARY KEY, cursor INTEGER)
```

These are the two pieces of state that must survive a crash for recovery to work (see
[Crash recovery](#crash-recovery)).

### Parser building — `Parser::from_config_file`

`log_parser/src/parser/build.rs` walks the raw TOML config in three phases:

**1. Whitespace normalisation**

Before anything else, any literal whitespace in the raw pattern string is replaced with
`\s+`, so a space in the config matches one or more whitespace characters in the log line.

**2. Component expansion**

`${name}` placeholders are then replaced with the corresponding `[components]` value,
wrapped in a non-capturing group:

```
'(?P<timestamp>${timestamp}) (?P<ref>${ref}) START'
→  '(?P<timestamp>(?:\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}))\s+(?P<ref>(?:[A-Z]{3}))\s+START'
```

Components are pure regex fragments; named capture groups must still be declared
explicitly in the pattern with `(?P<name>...)`.

Both this expansion and the `${VAR}` expansion used in the storage `connection_string`
(which resolves *environment variables*) share the same underlying implementation:
`shared::env::expand_vars`, a generic function that takes a `${name}` string and any
lookup closure.  The two callers differ only in what the lookup returns — components wrap
values in `(?:...)` for regex safety; the connection-string path substitutes the env var
value directly.

**3. Glob expansion**

After building a `Parser` value, `glob::glob(pattern)` expands the parser's `glob` field
into concrete file paths.  The result is a `HashMap<PathBuf, Vec<Parser>>` — multiple
parsers can be registered for the same file.  Parsers are cloned, one copy per matched
path.

**`build_file_parser_map`** (`file_watcher/src/file_watcher.rs`)

Takes the glob-resolved map and produces `FileParserMapping` —
`HashMap<PathBuf, ParserOffsets>` where `ParserOffsets` holds:

- `parsers: Vec<Parser>` — the ordered list of parsers for this file
- `offset: u64` — the byte position to seek to before reading

The initial cursor for every file is set to the **current file length** (EOF).  This means
the watcher only processes lines written *after startup*, not historical content.

Each parser also receives its `file_seed` — the file path as a string — stored as
`Option<String>`.  The seed is used when deriving deterministic event IDs (see
[Idempotent events](#idempotent-events)).

---

## Crash recovery

Before entering the run loop, `restore_pending_state` reconciles the freshly-built map
against whatever the previous run saved to the state DB.

```rust
fn restore_pending_state(
    file_parser_map: &mut FileParserMapping,
    pending: Vec<PendingSpanRecord>,
    saved_cursors: &HashMap<String, u64>,
)
```

It runs in **two passes**:

**Pass 1 — rewind cursors**

For every entry in `saved_cursors`, if the file still exists in the map, the cursor is
set to `min(saved_cursor, current_cursor)`.  The `min` ensures the cursor can only be
moved backwards — toward the unsaved content — never forwards.

This pass applies to *all* files that have a saved cursor, even those with no pending
spans.  That covers files that only produced single events in the last un-saved cycle.

**Pass 2 — restore span state**

Each `PendingSpanRecord` is looked up by `(file_path, parser_name)` and installed
back into the matching `InternalSpanParser`'s pending map, complete with its original
UUID, timestamp, captured data, and raw start line.

After restoration the watcher will re-read from the rewound cursor, re-parse the lines
from the unsaved cycle, and regenerate events with the **same deterministic UUIDs** —
which the `INSERT OR IGNORE` / `INSERT IGNORE` constraint then deduplicates harmlessly.

---

## The run loop

`FileWatcher::run()` loops forever until told to stop.  Each iteration is one *poll cycle*.

```
┌───────────────────────────────────────────────────────────────┐
│  top of loop                                                  │
│    check shutdown signal (try_recv — non-blocking)            │
│    interval.tick().await  ← waits poll_interval_secs          │
│                                                               │
│  dirty_parsers = vec![]   ← fresh each cycle                  │
│                                                               │
│  for each file in file_parser_map:                            │
│    get_file_len()                                             │
│    if len < cursor  → file truncated:                         │
│                         reset cursor to 0, skip file          │
│                         (re-read from start next cycle)       │
│    if len > cursor  → new bytes available:                    │
│      open file, seek to cursor                                │
│      for each line:                                           │
│        for each parser in order:                              │
│          parser.parse(line)                                   │
│          → Some(event)  → push to file_events, break         │
│          → None, is_dirty → push to dirty_parsers, break     │
│          → None, not dirty → try next parser                  │
│      update cursor to new stream position                     │
│      push (path, new_cursor) to cursor_updates                │
│    events.extend(file_events)                                 │
│                                                               │
│  ── after all files ──────────────────────────────────────────│
│                                                               │
│  if events non-empty:                                         │
│    async_retry! storage.store(&events)                        │
│    on first failure: warn, continue 'main (events kept)       │
│    on second failure: return DatabaseFailure                  │
│    on success: events.clear()                                 │
│                                                               │
│  flush dirty_parsers → state.save_pending(...)  (async_retry) │
│  flush cursor_updates → state.save_cursor(...)  (async_retry) │
│  cursor_updates.clear()                                       │
│                                                               │
│  if cycle took > poll_interval_secs: warn overrun             │
└───────────────────────────────────────────────────────────────┘
```

`events` and `cursor_updates` are declared **outside** the loop and persist across poll
cycles — this is what allows events to be retried on storage failure.  `dirty_parsers` is
declared **inside** the loop and is recreated fresh every cycle.

### Shutdown check

At the very top of every loop iteration, before the tick:

```rust
if let Some(rx) = rx && rx.try_recv().is_ok() {
    return Ok(ExitReason::Interrupt);
}
```

`try_recv()` is non-blocking — it returns immediately if no message has arrived.
The shutdown signal is sent by the outer wrapper via a `tokio::sync::oneshot` channel.

### Tick

`tokio::time::interval` with `MissedTickBehavior::Delay`.  If a cycle takes longer than
`poll_interval_secs`, the *next* tick fires one full `poll_interval_secs` after the
overrunning cycle completes — no burst catch-up, no immediate re-fire.

### Parser cascade

For each new line, parsers are tried **in declaration order** and the loop breaks on the
first parser that does anything:

- **Event produced** (`parse()` returns `Some`) → event queued.  If the parser is also
  dirty (span completion: the removal from the pending map sets the flag), flush it to
  `dirty_parsers` immediately before breaking.
- **Parser dirty, no event** → the parser consumed a START line; flush it to
  `dirty_parsers` and break.  The actual state DB write happens after *all* files have been
  processed, not inline.
- **No match, not dirty** → try the next parser.

Span completion sets dirty *and* returns `Some(event)`, so both actions happen in the same
branch.  Flushing inline (rather than in a separate pass) closes the crash window: without
it, the stale pending-span record would survive in the state DB until the next
non-matching line happened to flush it.  In that window a crash would restore the stale
entry, silently suppress any future START with the same reference, and deduplicate the
eventual END into the already-stored event — permanently losing the new span.

The `events` and `cursor_updates` buffers are declared *outside* the loop and accumulate
across files within a single poll cycle.  If storage fails on the first attempt, `events`
is retained — not cleared — and the cycle retries automatically.

### Storage failure and restart

```
first failure  → storage_failures = 1, continue 'main (events kept)
second failure → return Ok(ExitReason::DatabaseFailure)
```

On `DatabaseFailure`, the outer wrapper in `main.rs` catches the result and re-enters its
own loop, re-reading the config file and constructing a fresh `FileWatcher` with a new
database connection.  Because the state DB was written before the failed storage call, the
new watcher will restore correctly and re-process the lost events.

State saves (`save_pending`, `save_cursor`) also go through `async_retry!` but their
errors are silently ignored (`let _ = ...`).  A state save failure means recovery after a
crash may re-process more lines than strictly necessary, but it cannot lose events —
`INSERT OR IGNORE` handles the duplicates.

---

## Parser internals

### Single parser — `InternalSingleParser`

```
line
  │
  ├─ pattern.captures(line)?  → no match → None
  │
  ├─ extract timestamp from captures["timestamp"]
  │    → fails to parse → warn + None
  │
  ├─ extract_data(capture_names, captures)
  │    → HashMap of all named capture groups
  │
  ├─ derive event ID:
  │    seed = if file_seed.is_some() { "path|line" } else { "line" }
  │    id   = UUIDv5(NAMESPACE, seed)
  │
  └─ Event::Single { id, name, timestamp, data, raw_line: line }
```

`file_seed` is `None` until `set_file_seed` is called by `build_file_parser_map`, which
sets it to the file path.  Without a seed, two identical lines in different files would
produce the same UUID; the seed scopes each UUID to its source file.

### Span parser — `InternalSpanParser`

The span parser maintains a `PendingSpans` map:
`HashMap<SpanReference, PendingSpan>`.

```
line matches start_pattern?
  │
  yes ──► extract data, build SpanReference from reference_fields
  │       compute id = UUIDv5("path|start_line")
  │       create PendingSpan { id, timestamp, data, raw_line: start_line }
  │       insert into pending map  (sets dirty = true)
  │       return None
  │
line matches end_pattern?
  │
  yes ──► extract SpanReference from end captures
  │       look up in pending map
  │         found:
  │           compute duration = end_timestamp - start_timestamp
  │           merge end data into start data
  │           emit Event::Span {
  │             id: pending_span.id,       ← UUID from START line
  │             raw_lines: (start, end),
  │             duration, data, ...
  │           }
  │           sets dirty = true
  │         not found: return None (orphan END, ignored)
  │
neither?
  └──► try each nested parser in order
         if nested parser emits an event:
           look for a pending span whose reference_fields match the event's data
           found  → emit event with parent_id = pending_span.id
           not found → suppress (no parent to link to)
```

**`SpanReference`** is a `Vec<String>` of the values of the `reference_fields` captures,
in declaration order.  It is the key that links a START line to its END line, and that
links a nested event to its enclosing span.

**Dirty tracking** — `PendingSpans` has a `dirty: bool` flag.  `add` always sets it to
true; `remove` sets it to true only on a successful removal (an orphan END does not dirty
the parser).  The run loop checks `is_dirty()` after each `parse()` returns `None`; if
true it clones the current pending map, calls `clean()` immediately to reset the flag, and
pushes the parser onto `dirty_parsers`.  After *all* files have been processed,
`save_pending(...)` is called for each entry in `dirty_parsers`, writing the snapshot to
the state DB.

---

## Idempotent events

Event IDs are **deterministic UUIDv5** values derived from the raw log content:

- **Single**: `UUIDv5(NAMESPACE, "file_path|raw_line")`
- **Span**: `UUIDv5(NAMESPACE, "file_path|start_raw_line")` — the ID is assigned when the
  START line is seen and carried through to the completed span.

The fixed `NAMESPACE` UUID is a project-specific constant defined in
`log_parser/src/pending_span.rs`.

This means if the watcher re-processes a line after a crash, it generates the same UUID
and the `INSERT OR IGNORE` / `INSERT IGNORE` constraint silently discards the duplicate.

---

## Outer wrapper: restart and graceful shutdown

`file_watcher/src/main.rs` is the thin shell around the watcher:

```rust
'main: loop {
    let config_file = fs::read(&config_path).await?;
    let (interrupt_tx, interrupt_rx) = oneshot::channel::<ExitReason>();
    let mut watcher = FileWatcher::new(&config_file).await?.with_receiver(interrupt_rx);
    let mut join_handle = tokio::spawn(async move { watcher.run().await });

    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = sigterm.recv()  => { interrupt_tx.send(ExitReason::Interrupt); }
        _ = ctrl_c()        => { interrupt_tx.send(ExitReason::Interrupt); }
        _ = &mut join_handle => {}
    };

    match join_handle.await {
        Ok(Ok(ExitReason::DatabaseFailure)) => continue 'main,
        _                                   => break 'main,
    }
}
```

**How it works:**

1. The watcher is spawned as a Tokio task and given a `oneshot::Receiver`.
2. `select!` borrows `join_handle` (does not consume it) and races three arms:
   - SIGTERM received → send `Interrupt` through the oneshot channel.
   - Ctrl-C received → same.
   - Watcher task completes on its own → fall through.
3. After `select!` returns, `join_handle.await` waits for the watcher to actually finish
   (important if a signal was received — the watcher needs time to finish its current
   cycle and write state before the process exits).
4. If the exit reason was `DatabaseFailure`, the outer loop continues, re-reading the
   config file and constructing a new watcher with a fresh database connection.
   Any other outcome (clean shutdown, error, SIGTERM/Ctrl-C) breaks the outer loop and
   the process exits.

The config file is re-read on every restart so a database connection fix that also
involves a config change (e.g. a new connection string) takes effect automatically.

---

## `async_retry!`

A macro defined in `shared/src/lib.rs`.  Default: **5 attempts**, starting at **100 ms**,
doubling on each failure (100 ms → 200 ms → 400 ms → 800 ms).

Used for:

| call site | failure mode |
|---|---|
| `storage.store(events)` | error logged; after all attempts → `DatabaseFailure` |
| `state.save_pending(...)` | error **silently ignored** (`let _ = ...`) |
| `state.save_cursor(...)` | error **silently ignored** (`let _ = ...`) |

---

## Data-flow summary

```
TOML config
    │
    ▼
Parser::from_config_file
    components expanded, whitespace normalised, globs resolved
    │
    ▼
build_file_parser_map
    cursors set to EOF, file_seed injected
    │
    ▼
restore_pending_state          ◄── state DB (pending_spans + file_cursors)
    cursors rewound
    open spans reinstalled
    │
    ▼
FileWatcher::run ─────────────────────────────────────────────┐
    tick every poll_interval_secs                             │
    for each file with new bytes:                             │
      for each line:                                          │
        try parsers in order                                  │
          single  → Event::Single (UUIDv5)                    │
          span    → PendingSpan (on START) /                  │
                    Event::Span (on END, UUIDv5 from START)   │
          nested  → Event::Single with parent_id              │
    events ──► INSERT OR IGNORE ──► event DB                  │
    dirty spans ──► state DB (pending_spans)                  │
    cursors ──► state DB (file_cursors)                       │
    DatabaseFailure? ─────────────────────────────────────────┘
        outer loop restarts with fresh connection
```
