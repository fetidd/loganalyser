# Next Steps

## Fix real gaps

- **Wire up filtering in `MemoryEventStore`** — `apply_filter()` always returns `true`; the
  `Filter`/`Expr` types are already expressive enough, it just needs an in-memory evaluator.
  (Both SQL backends already use the filter correctly via `build_where`.)

- **Persist `PendingSpans` across restarts** — span correlation state lives only in memory.
  If the watcher restarts between a span's START and END the pairing is silently lost.
  Serialising pending spans to SQLite would fix this.

## Query / observability tooling

- **Query CLI binary** (`logquery`) — a second binary that accepts filter arguments on the
  command line and prints matching events as a table or JSON. The storage and filter layer
  already supports everything needed.

- **Pattern test REPL** — a small tool where you paste a log line and a config fragment and
  it shows which capture groups matched and what the resulting event would look like.
  Invaluable when writing new parsers.

## More creative ideas

- **Alert rules in config** — extend the TOML config to define conditions (e.g. span duration
  > 5 s, data field `level = "ERROR"`) that write to a separate alerts store or call a webhook.
  The `Expr` type is already expressive enough to represent these conditions.

- **Metrics aggregation** — a post-processing layer that buckets events into time windows and
  computes count / min / max / avg duration per event name. Answers "how many times did this
  span fire per minute and how long did it take?" without querying individual events.

- **Flamegraph export** — events already have `parent_id` and durations. Walk the `EventNode`
  tree and emit a `.folded` file that tools like `inferno` can render as a proper flamegraph.
  Especially useful for nested span parsers.

- **Derived / synthetic events** — config rules that combine raw events into higher-level ones.
  For example: "if span A ends and span B starts within 100 ms with the same reference field,
  emit a `queue_wait` single event with the gap as its data". A small stream-processing DSL on
  top of the existing event model.
