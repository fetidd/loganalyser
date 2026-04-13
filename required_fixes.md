# Required Fixes

## Bugs

### ~~1. One pending span per SpanReference — silent data loss~~ ✅ Resolved
~~`PendingSpans` is `HashMap<SpanReference, PendingSpan>` where `SpanReference` is
`Vec<String>` (the values of the reference fields). If two spans with identical
reference field values start before either ends, the second silently overwrites the
first.~~

**Resolved:** Concurrent spans with the same reference value indicate a bug in the
instrumentation — the reference field is not actually unique. FIFO matching would
produce silently wrong durations. Instead, a duplicate START is now logged as a
warning and skipped: `self.pending.0.contains_key(&span_reference)` guards the
insert, preserving the first open span.

### ~~2. Cursor rewind duplicates completed events on restart~~ ✅ Resolved
~~On restart the cursor is rewound to `min(saved_cursor, current_cursor)` so lines
written during downtime are re-read. Any completed events that fall between the
rewind point and the current cursor get re-parsed and re-stored.~~

**Resolved:** The cursor was previously only written inside `save_pending`, which only
runs when a parser is dirty. Across multiple polls with the same open spans and no
new starts/ends, the cursor never updated — drifting further behind and causing a
large rewind overshoot on restart. Now `save_cursor` is called every poll right after
seeking, so the saved cursor always reflects the end of the last complete poll
regardless of pending span activity. The overshoot window is reduced to at most one
poll's worth of lines.

---

## Inefficiencies

### ~~3. Every parser scans every line — O(N×M) regex attempts~~ ✅ Resolved
~~For N parsers on the same file, each new line is matched against all N parsers'
regexes independently with no short-circuiting. At 10 parsers and 10,000 new lines
per poll that is 100,000 regex attempts.~~

**Resolved:** The inner parser loop in `file_watcher` now `break`s as soon as a parser
claims a line — either by emitting an event or by updating pending span state. Lines
are tried against parsers in order and the rest are skipped on first match.

### ~~4. Entire new file content read into one String per poll~~ ✅ Resolved
~~All bytes since the last cursor position are read into a single heap allocation
before parsing begins. On a busy log file this can be a large transient allocation
every poll cycle.~~

**Resolved:** `file_watcher` now uses `tokio::io::BufReader` with `AsyncBufReadExt::lines()`,
reading and allocating one line at a time. No single large allocation for the whole chunk.

### ~~5. `save_pending` spawned every poll regardless of change~~ ✅ Resolved
~~A storage write is spawned for every parser on every poll even if no pending spans
were opened or closed since the last poll.~~

**Resolved:** `PendingSpans` is now a tuple struct `(HashMap<...>, bool)` where the second
field is a dirty flag. `is_dirty()` and `clean()` are exposed on both `InternalSpanParser`
and `Parser`. `file_watcher` only calls `save_pending` when `p.is_dirty()` is true, and
clears the flag with `p.clean()` afterwards.

---

## Accidental Complexity

### ~~6. Lookup closure threading for nested event linking~~ ✅ Resolved
~~To link nested events to their parent span a closure is constructed in the span
parser and passed into `parse_line_with_context`. The closure captures a reference
into `self.pending`, forcing a borrow checker dance and making the call chain hard
to follow.~~

**Resolved:** Two-pass approach implemented. `parse_line_with_context` on both `InternalSingleParser`
and `InternalSpanParser` takes only `&str` — no lookup closure. In the nested branch of
`InternalSpanParser`, the line is parsed first, then `self.pending` is searched to find
a matching parent span by correlating `event.data()` against the reference fields of each
open span. Nested events with no matching parent are suppressed. The closure parameter
is gone from the entire call chain.

### ~~7. Components silently become named capture groups~~ ✅ Resolved
~~`update_component_patterns` wraps every component with `(?P<name>...)`, so every
`${timestamp}` substitution in a pattern produces a `timestamp` key in the event's
data HashMap. Timestamp is already a first-class field; this pollutes data with
fields the user did not ask for.~~

**Resolved:** `update_config_components` is removed entirely. The new `expand_components`
function always wraps substitutions in non-capturing `(?:...)` groups. Named fields
must be declared explicitly with `(?P<name>...)` in the pattern itself — components
are pure regex fragments.

### ~~8. Two-stage glob handling~~ ✅ Resolved
~~Globs are stored as strings in the `HashMap<glob, Vec<Parser>>` returned by the
parser builder, then expanded again in `FileWatcher::build_file_parser_map`. There
is no guarantee both stages use the same glob library or matching semantics, and the
split makes it hard to reason about which files a parser will actually watch.~~

**Resolved:** `Parser::from_config_file` now returns `HashMap<PathBuf, Vec<Parser>>`,
expanding globs once at config load time using the `glob` crate. `build_file_parser_map`
in `file_watcher` now takes a `HashMap<PathBuf, Vec<Parser>>` and only sets initial
cursors — no second glob expansion. There is exactly one glob expansion, in one place,
using one library.

### ~~9. Raw `toml::Value` for parser config~~ ✅ Resolved
~~`parsers: Vec<toml::Value>` loses type safety at the config boundary. Structural
errors are detected late, per-parser at build time, rather than up-front when the
config is first parsed.~~

**Resolved:** Parser config now uses typed serde structs (`RawSingleConfig`, `RawSpanConfig`)
with `#[serde(tag = "type", rename_all = "lowercase")]`. Structural errors (missing fields,
wrong types, invalid discriminant) are caught at deserialisation time by serde before
any build logic runs.
