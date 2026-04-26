#!/usr/bin/env python3
"""
FileWatcher load test harness.

Builds the watcher in release mode, generates a large config with many parsers
across many log files, then bombards those files at a configurable rate while
relaying all tracing output to stdout so you can see the watcher struggling.

Usage:
    python3 load_test.py [options]

    --files N           log files to create (default: 20)
    --parsers-per-file  single+span parser pairs per file (default: 2)
    --rate N            lines/sec per file (default: 50)
    --duration N        seconds to run (default: 60)
    --poll-interval N   watcher poll_interval_secs (default: 1)
    --debug             use debug build instead of release
    --rust-log LEVEL    RUST_LOG value (default: file_watcher=debug,warn)
    --no-build          skip cargo build (use existing binary)
"""

import argparse
import os
import re
import random
import signal
import sqlite3
import string
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

_START_RE = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) ([A-Z]{3}) START$')
_END_RE   = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) ([A-Z]{3}) END$')
_LOG_RE   = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} LOG ')
_NOTE_RE  = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [A-Z]{3} NOTE ')


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build(release: bool) -> Path:
    mode = "release" if release else "debug"
    print(f"[harness] building file_watcher ({mode})...")
    cmd = ["cargo", "build", "-p", "file_watcher"]
    if release:
        cmd.append("--release")
    result = subprocess.run(cmd, cwd=Path(__file__).parent)
    if result.returncode != 0:
        sys.exit(f"[harness] build failed (exit {result.returncode})")
    binary = Path(__file__).parent / "target" / mode / "file_watcher"
    print(f"[harness] built: {binary}")
    return binary


# ---------------------------------------------------------------------------
# Config generation
# ---------------------------------------------------------------------------

def generate_config(tmpdir: Path, log_paths: list[Path], db_path: Path,
                    state_path: Path, poll_interval: int,
                    mysql_url: str | None = None) -> Path:
    if mysql_url:
        storage_lines = ['storage_type = "mysql"', f'connection_string = "{mysql_url}"']
    else:
        storage_lines = ['storage_type = "sqlite"', f'connection_string = "{db_path}"']

    lines = [
        f'state_db_path = "{state_path}"',
        "",
        "[settings]",
        f"poll_interval_secs = {poll_interval}",
        "",
        "[storage]",
        *storage_lines,
        "",
        "[defaults]",
        'timestamp_format = "%Y-%m-%d %H:%M:%S"',
        "",
        "[components]",
        r"timestamp = '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'",
        r"ref       = '[A-Z]{3}'",
        "",
    ]

    for i, path in enumerate(log_paths):
        p = str(path)
        lines += [
            "[[parsers]]",
            f'name    = "single_{i}"',
            f'glob    = "{p}"',
            'type    = "single"',
            r"pattern = '(?P<timestamp>${timestamp}) LOG (?P<msg>.*)'",
            "",
            "[[parsers]]",
            f'name             = "span_{i}"',
            f'glob             = "{p}"',
            'type             = "span"',
            r"start_pattern    = '(?P<timestamp>${timestamp}) (?P<ref>${ref}) START'",
            r"end_pattern      = '(?P<timestamp>${timestamp}) (?P<ref>${ref}) END'",
            'reference_fields = ["ref"]',
            "",
            "[[parsers.nested]]",
            f'name    = "span_inner_{i}"',
            'type    = "single"',
            r"pattern = '(?P<timestamp>${timestamp}) (?P<ref>${ref}) NOTE (?P<msg>.*)'",
            "",
        ]

    config_path = tmpdir / "config.toml"
    config_path.write_text("\n".join(lines))
    return config_path


# ---------------------------------------------------------------------------
# Writer thread
# ---------------------------------------------------------------------------

_NOISE_TEMPLATES = [
    "TRACE entering function process_batch with args=()",
    "WARN slow query detected ({}ms)",
    "[INFO] cache miss for key user_{}",
    "DEBUG gc collected {} objects in {}ms",
    "at com.example.Worker.run(Worker.java:{})",
    "connecting to upstream host retry {}/3",
    "heartbeat ok latency={}ms",
    "config reload skipped — no changes detected",
]


class WriterThread(threading.Thread):
    def __init__(self, file_path: Path, file_index: int,
                 rate: float, stop: threading.Event, stats: dict,
                 noise_stats: dict, max_open_spans: int = 8,
                 noise_pct: float = 0.25):
        super().__init__(daemon=True)
        self.file_path = file_path
        self.file_index = file_index
        self.rate = rate
        self.stop = stop
        self.stats = stats
        self.noise_stats = noise_stats
        self.max_open_spans = max_open_spans
        self.noise_pct = noise_pct
        self._open_refs: list[str] = []
        self._ref_counter = 0
        self._note_counter = 0
        self.noise_written = 0

    def _next_ref(self) -> str:
        u = string.ascii_uppercase
        combo = (
            u[self._ref_counter // 676 % 26]
            + u[self._ref_counter // 26 % 26]
            + u[self._ref_counter % 26]
        )
        self._ref_counter += 1
        return combo

    def _noise_line(self, ts: str) -> str:
        template = random.choice(_NOISE_TEMPLATES)
        filled = template.format(*[random.randint(1, 999) for _ in range(template.count("{}"))])
        return f"{ts} {filled}"

    def _make_line(self) -> str:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        roll = random.random()

        if roll < 0.60:
            if random.random() < self.noise_pct:
                self.noise_written += 1
                return self._noise_line(ts)
            n = self.stats.get(self.file_index, 0)
            return f"{ts} LOG event #{n} from file {self.file_index}"

        at_cap = len(self._open_refs) >= self.max_open_spans
        if (roll < 0.80 or not self._open_refs) and not at_cap:
            ref = self._next_ref()
            self._open_refs.append(ref)
            return f"{ts} {ref} START"

        # END/NOTE branch — also reached when at cap instead of starting
        if not self._open_refs:
            n = self.stats.get(self.file_index, 0)
            return f"{ts} LOG event #{n} from file {self.file_index}"

        if random.random() < 0.3 and len(self._open_refs) > 1:
            ref = random.choice(self._open_refs)
            self._note_counter += 1
            return f"{ts} {ref} NOTE msg_{self._note_counter}"

        ref = self._open_refs.pop(random.randrange(len(self._open_refs)))
        return f"{ts} {ref} END"

    def run(self):
        interval = 1.0 / self.rate
        written = 0
        with self.file_path.open("a") as f:
            while not self.stop.is_set():
                t0 = time.monotonic()
                line = self._make_line()
                f.write(line + "\n")
                f.flush()
                written += 1
                self.stats[self.file_index] = written
                self.noise_stats[self.file_index] = self.noise_written
                elapsed = time.monotonic() - t0
                sleep_for = interval - elapsed
                if sleep_for > 0:
                    self.stop.wait(sleep_for)


# ---------------------------------------------------------------------------
# Stderr relay thread
# ---------------------------------------------------------------------------

class StderrRelay(threading.Thread):
    def __init__(self, proc: subprocess.Popen, counters: dict):
        super().__init__(daemon=True)
        self.proc = proc
        self.counters = counters   # {"overruns": 0, "errors": 0}

    def run(self):
        for raw in self.proc.stdout:
            line = raw.decode(errors="replace").rstrip()
            print(f"[watcher] {line}", flush=True)
            if "processing time exceeded" in line:
                self.counters["overruns"] += 1
            if "failed after" in line:
                self.counters["errors"] += 1
            if "Failed to store" in line:
                self.counters["errors"] += 1


# ---------------------------------------------------------------------------
# Metrics thread
# ---------------------------------------------------------------------------

class MetricsThread(threading.Thread):
    def __init__(self, db_path: Path | None, mysql_url: str | None,
                 writer_stats: dict, noise_stats: dict, watcher_counters: dict,
                 stop: threading.Event, pid: int | None):
        super().__init__(daemon=True)
        self.db_path = db_path
        self.mysql_url = mysql_url
        self.writer_stats = writer_stats
        self.noise_stats = noise_stats
        self.watcher_counters = watcher_counters
        self.stop = stop
        self.pid = pid
        self.samples: list[dict] = []

    def _query_stored(self) -> int:
        try:
            if self.mysql_url:
                return self._query_mysql()
            return self._query_sqlite()
        except Exception:
            return 0

    def _query_sqlite(self) -> int:
        con = sqlite3.connect(str(self.db_path), timeout=2)
        count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        con.close()
        return count

    def _query_line_equivalent(self) -> int:
        """Count stored events weighted by lines consumed: spans=2, singles=1."""
        sql = "SELECT SUM(CASE WHEN event_type = 'span' THEN 2 ELSE 1 END) FROM events"
        try:
            if self.mysql_url:
                con = self._mysql_con()
                with con.cursor() as cur:
                    cur.execute(sql)
                    result = cur.fetchone()[0]
                con.close()
            else:
                con = sqlite3.connect(str(self.db_path), timeout=2)
                result = con.execute(sql).fetchone()[0]
                con.close()
            return result or 0
        except Exception:
            return 0

    def _mysql_con(self):
        import pymysql, urllib.parse
        u = urllib.parse.urlparse(self.mysql_url)
        return pymysql.connect(
            host=u.hostname, port=u.port or 3306,
            user=u.username, password=u.password,
            database=u.path.lstrip("/"), connect_timeout=2,
        )

    def _query_mysql(self) -> int:
        con = self._mysql_con()
        with con.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM events")
            count = cur.fetchone()[0]
        con.close()
        return count

    def query_span_stats(self) -> dict | None:
        """Return avg/min/max span duration in seconds, or None if no spans stored."""
        sql = """
            SELECT COUNT(*), AVG(duration_ms), MIN(duration_ms), MAX(duration_ms)
            FROM events WHERE event_type = 'span'
        """
        try:
            if self.mysql_url:
                con = self._mysql_con()
                with con.cursor() as cur:
                    cur.execute(sql)
                    row = cur.fetchone()
                con.close()
            else:
                con = sqlite3.connect(str(self.db_path), timeout=2)
                row = con.execute(sql).fetchone()
                con.close()
            count, avg_ms, min_ms, max_ms = row
            if not count:
                return None
            return {
                "count": count,
                "avg_s": round(avg_ms / 1000, 2),
                "min_s": round(min_ms / 1000, 2),
                "max_s": round(max_ms / 1000, 2),
            }
        except Exception:
            return None

    def _rss_mb(self) -> float | None:
        if self.pid is None:
            return None
        try:
            status = Path(f"/proc/{self.pid}/status").read_text()
            for line in status.splitlines():
                if line.startswith("VmRSS:"):
                    kb = int(line.split()[1])
                    return round(kb / 1024, 1)
        except Exception:
            pass
        return None

    def run(self):
        interval = 2.0
        last_stored = 0
        t_start = time.monotonic()

        while not self.stop.wait(interval):
            t = round(time.monotonic() - t_start)
            stored = self._query_stored()
            written = sum(v for v in self.writer_stats.values() if isinstance(v, int))
            noise = sum(v for v in self.noise_stats.values() if isinstance(v, int))
            events_per_sec = round((stored - last_stored) / interval)
            pending = max(0, (written - noise) - stored)
            rss = self._rss_mb()
            overruns = self.watcher_counters["overruns"]
            errors = self.watcher_counters["errors"]

            rss_str = f"  rss={rss}MB" if rss is not None else ""
            print(
                f"[harness] t={t:>4}s  written={written:>7,}  stored={stored:>7,}"
                f"  events/sec={events_per_sec:>5}  pending≈{pending:>6,}"
                f"  overruns={overruns}  errors={errors}{rss_str}",
                flush=True,
            )

            self.samples.append({"t": t, "written": written, "stored": stored,
                                  "events_per_sec": events_per_sec})
            last_stored = stored


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _diff_logs(log_paths: list[Path], db_path: Path | None,
               mysql_url: str | None) -> list[str]:
    """Return raw_lines the watcher should have stored but didn't."""
    # Build expected set from log files
    expected: list[str] = []
    for log_path in log_paths:
        open_spans: dict[str, str] = {}  # ref -> start_line
        for raw in log_path.read_text().splitlines():
            line = raw.strip()
            if not line:
                continue
            m = _START_RE.match(line)
            if m:
                open_spans[m.group(2)] = line
                continue
            m = _END_RE.match(line)
            if m:
                ref = m.group(2)
                if ref in open_spans:
                    expected.append(open_spans.pop(ref) + "\n" + line)
                continue
            if _LOG_RE.match(line) or _NOTE_RE.match(line):
                expected.append(line)

    # Fetch stored raw_lines from DB as a Counter (multiset) — two events with
    # identical raw_line text (e.g. same ref at same second in different files)
    # both need to be matched against expected copies individually.
    stored: Counter[str] = Counter()
    try:
        if mysql_url:
            import pymysql, urllib.parse
            u = urllib.parse.urlparse(mysql_url)
            con = pymysql.connect(host=u.hostname, port=u.port or 3306,
                                  user=u.username, password=u.password,
                                  database=u.path.lstrip("/"), connect_timeout=5)
            with con.cursor() as cur:
                cur.execute("SELECT raw_line FROM events")
                for (raw_line,) in cur:
                    if raw_line:
                        stored[raw_line] += 1
            con.close()
        else:
            con = sqlite3.connect(str(db_path), timeout=5)
            for (raw_line,) in con.execute("SELECT raw_line FROM events"):
                if raw_line:
                    stored[raw_line] += 1
            con.close()
    except Exception as e:
        print(f"[harness] diff: DB query failed: {e}")
        return []

    # Subtract stored counts from expected counts; anything remaining is missed
    expected_counts: Counter[str] = Counter(expected)
    missed_counts = expected_counts - stored
    missed = []
    for line, count in missed_counts.items():
        missed.extend([line] * count)
    return missed


def _mysql_truncate(mysql_url: str) -> None:
    import pymysql, urllib.parse
    u = urllib.parse.urlparse(mysql_url)
    con = pymysql.connect(
        host=u.hostname, port=u.port or 3306,
        user=u.username, password=u.password,
        database=u.path.lstrip("/"), connect_timeout=5,
    )
    with con.cursor() as cur:
        cur.execute("TRUNCATE TABLE events")
    con.commit()
    con.close()
    print("[harness] truncated events table")


def run_test(args):
    with tempfile.TemporaryDirectory(prefix="fw_loadtest_") as tmp:
        tmpdir = Path(tmp)

        log_paths = [tmpdir / f"file_{i}.log" for i in range(args.files)]
        for p in log_paths:
            p.touch()

        mysql_url = args.mysql_url if args.mysql else None
        db_path = None if mysql_url else tmpdir / "events.db"
        state_path = tmpdir / "state.db"
        config_path = generate_config(
            tmpdir, log_paths, db_path, state_path, args.poll_interval, mysql_url,
        )

        if mysql_url:
            _mysql_truncate(mysql_url)

        storage_label = mysql_url or str(db_path)
        print(f"[harness] config:   {config_path}")
        print(f"[harness] storage:  {storage_label}")
        print(f"[harness] files:    {args.files}  parsers/file: {args.parsers_per_file * 2}"
              f"  rate: {args.rate} lines/sec/file  duration: {args.duration}s")
        print(f"[harness] total write rate: ~{args.rate * args.files} lines/sec")
        print()

        env = {**os.environ, "RUST_LOG": args.rust_log}
        proc = subprocess.Popen(
            [str(args.binary), str(config_path)],
            stdout=subprocess.PIPE,
            env=env,
        )
        print(f"[harness] watcher PID {proc.pid} started")

        watcher_counters = {"overruns": 0, "errors": 0}
        stop = threading.Event()
        writer_stats: dict[int, int] = {}
        noise_stats: dict[int, int] = {}

        relay = StderrRelay(proc, watcher_counters)
        relay.start()

        # Give the watcher a moment to open the files before writing starts
        time.sleep(1.5)

        writers = [
            WriterThread(log_paths[i], i, args.rate, stop, writer_stats,
                         noise_stats=noise_stats, max_open_spans=args.max_open_spans,
                         noise_pct=args.noise_pct)
            for i in range(args.files)
        ]
        metrics = MetricsThread(db_path, mysql_url, writer_stats, noise_stats, watcher_counters, stop, proc.pid)

        for w in writers:
            w.start()
        metrics.start()

        print(f"[harness] writing for {args.duration}s — watch for [watcher] lines below\n")
        try:
            time.sleep(args.duration)
        except KeyboardInterrupt:
            print("\n[harness] interrupted")

        print("\n[harness] stopping writers...")
        stop.set()
        for w in writers:
            w.join(timeout=3)
        metrics.join(timeout=3)  # stop periodic metrics prints before drain phase

        # Close any spans still open so the watcher can complete them during drain
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for w in writers:
            if w._open_refs:
                with w.file_path.open("a") as f:
                    for ref in w._open_refs:
                        f.write(f"{ts} {ref} END\n")
                        writer_stats[w.file_index] = writer_stats.get(w.file_index, 0) + 1
                w._open_refs.clear()

        final_written = sum(v for v in writer_stats.values() if isinstance(v, int))
        total_noise = sum(w.noise_written for w in writers)
        open_at_stop = 0  # all flushed
        print(f"[harness] all open spans flushed ({total_noise:,} noise lines written)")

        # Drain phase: poll until stored count plateaus (watcher has caught up)
        parseable_written = final_written - total_noise
        print(f"[harness] writers done ({final_written:,} lines, {total_noise:,} noise) — waiting for watcher to drain...\n")
        drain_timeout = max(30, args.poll_interval * 10)
        t_drain = time.monotonic()
        last_stored = -1
        stable_polls = 0
        final_stored = 0

        while time.monotonic() - t_drain < drain_timeout:
            line_equiv = metrics._query_line_equivalent()
            elapsed = round(time.monotonic() - t_drain)
            remaining = max(0, parseable_written - line_equiv)
            print(f"[harness] drain  t+{elapsed:>3}s  line_equiv={line_equiv:>7,} / {parseable_written:>7,}"
                  f"  remaining={remaining:>6,}",
                  flush=True)
            if line_equiv == last_stored:
                stable_polls += 1
                if remaining == 0 and stable_polls >= 2:
                    print("[harness] drain complete — watcher has caught up")
                    break
                if remaining > 0 and stable_polls >= 3:
                    print(f"[harness] drain stuck — watcher stopped making progress with {remaining:,} remaining")
                    break
            else:
                stable_polls = 0
            last_stored = line_equiv
            time.sleep(args.poll_interval + 0.5)
        else:
            print(f"[harness] drain timeout after {drain_timeout}s")

        print("\n[harness] sending SIGTERM to watcher...")
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            print("[harness] watcher did not exit cleanly, killing")
            proc.kill()

        relay.join(timeout=3)

        # Re-query after watcher has fully exited — it may have stored more during shutdown
        final_stored = metrics._query_line_equivalent()

        missed = _diff_logs(log_paths, db_path, mysql_url)
        if missed:
            print(f"[harness] diff: {len(missed)} line(s) not stored:")
            for line in missed[:20]:
                print(f"[harness]   {line!r}")
            if len(missed) > 20:
                print(f"[harness]   ... and {len(missed) - 20} more")
        else:
            print("[harness] diff: all parseable lines accounted for ✓")

        if args.save_logs:
            import shutil
            save_dir = Path(args.save_logs)
            save_dir.mkdir(parents=True, exist_ok=True)
            for p in log_paths:
                shutil.copy(p, save_dir / p.name)
            if db_path:
                shutil.copy(db_path, save_dir / "events.db")
            if missed:
                (save_dir / "missed.txt").write_text("\n".join(missed))
                print(f"[harness] missed lines saved to {save_dir / 'missed.txt'}")
            print(f"[harness] logs saved to {save_dir}")

        print()
        print("[harness] ═══ LOAD TEST COMPLETE ═══")
        print(f"[harness] duration:         {args.duration}s")
        print(f"[harness] files:            {args.files}  ({args.parsers_per_file * 2} parsers each)")
        unaccounted = parseable_written - final_stored
        print(f"[harness] lines written:    {final_written:,}  ({final_written // args.duration:,}/sec avg)")
        print(f"[harness] noise lines:      {total_noise:,}  ({100 * total_noise // max(1, final_written)}% of written)")
        print(f"[harness] line-equiv stored:{final_stored:>7,}")
        print(f"[harness] unaccounted:      {'0 ✓' if unaccounted == 0 else f'{unaccounted:,} ✗'}")
        print(f"[harness] overrun warnings: {watcher_counters['overruns']}")
        print(f"[harness] storage errors:   {watcher_counters['errors']}")
        if metrics.samples:
            peak_eps = max(s["events_per_sec"] for s in metrics.samples)
            print(f"[harness] peak events/sec:  {peak_eps:,}")
        span_stats = metrics.query_span_stats()
        if span_stats:
            print(f"[harness] span lifetime:    avg={span_stats['avg_s']}s  "
                  f"min={span_stats['min_s']}s  max={span_stats['max_s']}s  "
                  f"({span_stats['count']:,} completed spans)")


def parse_args():
    ap = argparse.ArgumentParser(description="FileWatcher load test")
    ap.add_argument("--files", type=int, default=20)
    ap.add_argument("--parsers-per-file", type=int, default=2, dest="parsers_per_file")
    ap.add_argument("--rate", type=float, default=50,
                    help="lines/sec per file (default 50 → 1000 total for 20 files)")
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--poll-interval", type=int, default=3, dest="poll_interval")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--rust-log", default="file_watcher=warn,error", dest="rust_log")
    ap.add_argument("--no-build", action="store_true", dest="no_build")
    ap.add_argument("--save-logs", metavar="DIR", dest="save_logs", default=None,
                    help="copy log files and events DB here before cleanup")
    ap.add_argument("--noise-pct", type=float, default=0.25, dest="noise_pct",
                    help="fraction of LOG-band lines emitted as unmatched noise (default 0.25)")
    ap.add_argument("--max-open-spans", type=int, default=8, dest="max_open_spans",
                    help="max concurrent open spans per file (default 8)")
    ap.add_argument("--mysql", action="store_true",
                    help="use MySQL instead of SQLite for event storage")
    ap.add_argument("--mysql-url", default="mysql://loganalyser:secret@localhost/loganalyser",
                    dest="mysql_url", metavar="URL")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if not args.no_build:
        args.binary = build(release=not args.debug)
    else:
        mode = "debug" if args.debug else "release"
        args.binary = Path(__file__).parent / "target" / mode / "file_watcher"
        if not args.binary.exists():
            sys.exit(f"[harness] binary not found: {args.binary} — remove --no-build")
    run_test(args)
