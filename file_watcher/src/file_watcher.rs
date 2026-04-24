use std::{
    collections::HashMap,
    io::SeekFrom,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use event_storage::{EventStorage, make_storage};
use log_parser::parser::Parser;
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncSeekExt, BufReader},
    sync::oneshot::Receiver,
    time::{Instant, MissedTickBehavior},
};
use tracing::warn;

use crate::{
    Settings,
    config::Config,
    state::{PendingSpanRecord, State},
};

pub struct FileWatcher {
    file_parser_map: FileParserMapping,
    storage: Arc<EventStorage>,
    state: Arc<State>,
    settings: Settings,
    rx: Option<Receiver<bool>>,
}

type FileParserMapping = HashMap<PathBuf, ParserOffsets>;

#[derive(Clone, Debug, Default)]
struct ParserOffsets {
    parsers: Vec<Parser>,
    offset: u64,
}

impl FileWatcher {
    pub async fn new(config_file: Vec<u8>) -> anyhow::Result<Self> {
        let config: Config = toml::from_slice(&config_file)?;
        tracing::debug!("config created: {config:?}");
        let storage = make_storage(&config.storage).await?;
        tracing::debug!("storage created: {storage:?}");
        let state = State::new(&config).await?;
        let built_parsers = Parser::from_config_file(&config_file)?;
        let mut file_parser_map = build_file_parser_map(built_parsers).await?;
        let saved_cursors = state.load_cursors().await?;
        let pending = state.load_pending().await?;
        restore_pending_state(&mut file_parser_map, pending, &saved_cursors);
        Ok(Self {
            file_parser_map,
            storage: Arc::new(storage),
            state,
            settings: config.settings,
            rx: None,
        })
    }

    pub fn with_receiver(mut self, rx: Receiver<bool>) -> Self {
        self.rx = Some(rx);
        self
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let Self { file_parser_map, storage, state, settings, rx } = self;
        let mut interval = tokio::time::interval(Duration::from_secs(settings.poll_interval_secs));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        'main: loop {
            if let Some(rx) = rx
                && rx.try_recv().is_ok()
            {
                break 'main;
            }
            interval.tick().await;
            let before_parse = Instant::now();
            for (path, ParserOffsets { parsers, offset }) in file_parser_map.iter_mut() {
                let file_len = get_file_len(path).await?;
                if file_len < *offset {
                    // The file has been truncated for some reason, so we need to rewind to the start of it - assuming that it's because of log rotation or similar
                    *offset = 0;
                    continue;
                } else if file_len > *offset {
                    // The file has been written to since we last checked it, so we need to parse out the new logs
                    tracing::debug!("{path:?} has new log lines...");
                    let path_str = path.to_string_lossy().to_string();

                    // Read the file to the end and break up into lines
                    let mut file = BufReader::new(fs::File::open(&path).await?);
                    file.seek(SeekFrom::Start(*offset)).await?;
                    let mut lines = file.lines();
                    let mut file_events = vec![];
                    let mut dirty_parsers = vec![];
                    // Start parsing the lines
                    while let Some(line) = lines.next_line().await? {
                        for parser in parsers.iter_mut() {
                            // If the line is an event we want parsed, store it
                            if let Some(event) = parser.parse(&line) {
                                file_events.push(event);
                                break;
                            // Otherwise if the parser is now dirty and has pending spans then save them
                            } else if parser.is_dirty()
                                && let Some(pending) = parser.pending_spans().cloned()
                            {
                                parser.clean();
                                dirty_parsers.push((parser.name().to_string(), pending));
                                break;
                            }
                            // Otherwise try the next parser
                        }
                    }
                    tracing::debug!("found {file_events:?}");
                    // If we parsed any events, save them all now
                    if !file_events.is_empty() {
                        let storage = Arc::clone(storage);
                        shared::async_retry!(storage.store(&file_events)).await;
                    }
                    if !dirty_parsers.is_empty() {
                        for (parser_name, pending) in dirty_parsers {
                            let path_str = path_str.clone();
                            let state = Arc::clone(state);
                            shared::async_retry!(state.save_pending(&path_str, &parser_name, &pending)).await;
                        }
                    }
                    // Store the end of this file as its cursor so we can start from here after a restart
                    let current_file_path = path_str.clone();
                    let new_cursor = get_file_len(path).await?;
                    let state = Arc::clone(state);
                    shared::async_retry!(state.save_cursor(&current_file_path, new_cursor)).await;
                    *offset = new_cursor;
                }
            }
            if before_parse.elapsed() > Duration::from_secs(self.settings.poll_interval_secs) {
                warn!("processing time exceeded polling interval!");
            }
        }
        Ok(())
    }
}

async fn build_file_parser_map(resolved: HashMap<PathBuf, Vec<Parser>>) -> anyhow::Result<FileParserMapping> {
    let mut map = FileParserMapping::default();
    for (path, path_parsers) in resolved {
        let file_len = get_file_len(&path).await?;
        let ParserOffsets { parsers, offset } = map.entry(path).or_default();
        parsers.extend(path_parsers);
        *offset = file_len;
    }
    Ok(map)
}

/// Restores pending span state and rewinds file cursors after a watcher restart.
///
/// For each pending span record:
/// - If a saved cursor exists for the file, the cursor is rewound to
///   `min(saved, current)` so content written during downtime is re-read.
/// - The span is restored into the matching parser so it can be completed
///   when its END line is eventually encountered.
///
/// Records whose path or parser name are not present in the map are silently
/// ignored (they refer to config that has since been removed).
fn restore_pending_state(file_parser_map: &mut FileParserMapping, pending: Vec<PendingSpanRecord>, saved_cursors: &HashMap<String, u64>) {
    for record in pending {
        let path = PathBuf::from(&record.file_path);
        if let Some(ParserOffsets { parsers, offset: cursor_loc }) = file_parser_map.get_mut(&path) {
            if let Some(&saved) = saved_cursors.get(&record.file_path) {
                *cursor_loc = saved.min(*cursor_loc);
            }
            if let Some(p) = parsers.iter_mut().find(|p| p.name() == record.parser_name) {
                p.restore_pending(vec![(record.span_ref, record.id, record.timestamp, record.data, record.parent_id, record.raw_line)]);
            }
        }
    }
}

async fn get_file_len(file: impl AsRef<Path>) -> anyhow::Result<u64> {
    Ok(fs::metadata(file).await?.len())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use chrono::NaiveDateTime;
    use log_parser::parser::Parser;
    use uuid::Uuid;

    use crate::state::PendingSpanRecord;

    use super::{FileParserMapping, ParserOffsets, build_file_parser_map, restore_pending_state};

    fn path_map(path: &PathBuf, parsers: Vec<Parser>) -> HashMap<PathBuf, Vec<Parser>> {
        HashMap::from([(path.clone(), parsers)])
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn parser_from_config(config: &str) -> Parser {
        // from_config_file now expands globs immediately, so we need a real file.
        // Create a temporary file whose exact path we embed in the config.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().replace('\\', "/");
        let config = config.replace("__GLOB__", &path);
        Parser::from_config_file(config.as_bytes()).unwrap().into_values().flatten().next().unwrap()
    }

    fn single_parser(name: &str) -> Parser {
        parser_from_config(&format!(
            r#"
[defaults]
timestamp_format = "%Y-%m-%d %H:%M:%S"

[[parsers]]
name = "{name}"
glob = "__GLOB__"
type = "single"
pattern = '(?P<timestamp>\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}}) (?P<data>.*)'
"#
        ))
    }

    fn span_parser(name: &str) -> Parser {
        parser_from_config(&format!(
            r#"
[defaults]
timestamp_format = "%Y-%m-%d %H:%M:%S"

[[parsers]]
name = "{name}"
glob = "__GLOB__"
type = "span"
start_pattern = '(?P<timestamp>\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}}) (?P<ref>[A-Z]+) START'
end_pattern = '(?P<timestamp>\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}}) (?P<ref>[A-Z]+) END'
reference_fields = ["ref"]
"#
        ))
    }

    fn map_with_span_parser(path: &str, parser_name: &str, cursor: u64) -> FileParserMapping {
        let mut map = FileParserMapping::default();
        map.insert(
            PathBuf::from(path),
            ParserOffsets {
                parsers: vec![span_parser(parser_name)],
                offset: cursor,
            },
        );
        map
    }

    fn pending_record(file_path: &str, parser_name: &str, span_ref: &str) -> PendingSpanRecord {
        PendingSpanRecord {
            file_path: file_path.to_string(),
            parser_name: parser_name.to_string(),
            span_ref: vec![span_ref.to_string()],
            id: Uuid::new_v4(),
            timestamp: NaiveDateTime::parse_from_str("2026-01-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            data: HashMap::from([("ref".to_string(), span_ref.to_string())]),
            parent_id: None,
            raw_line: None,
        }
    }

    // -----------------------------------------------------------------------
    // build_file_parser_map
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn build_map_empty_input_gives_empty_map() {
        let map = build_file_parser_map(HashMap::new()).await.unwrap();
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn build_map_single_entry_sets_parser_and_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.log");
        std::fs::write(&path, "hello world\n").unwrap(); // 12 bytes

        let map = build_file_parser_map(path_map(&path, vec![single_parser("p1")])).await.unwrap();

        let ParserOffsets { parsers, offset: cursor } = &map[&path];
        assert_eq!(parsers.len(), 1);
        assert_eq!(parsers[0].name(), "p1");
        assert_eq!(*cursor, 12);
    }

    #[tokio::test]
    async fn build_map_cursor_set_to_current_file_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.log");
        std::fs::write(&path, "abc").unwrap(); // 3 bytes

        let map = build_file_parser_map(path_map(&path, vec![single_parser("p1")])).await.unwrap();

        assert_eq!(map[&path].offset, 3);
    }

    #[tokio::test]
    async fn build_map_separate_entries_for_different_paths() {
        let dir = tempfile::tempdir().unwrap();
        let path_a = dir.path().join("a.log");
        let path_b = dir.path().join("b.log");
        std::fs::write(&path_a, "aa").unwrap(); // 2 bytes
        std::fs::write(&path_b, "bbbb").unwrap(); // 4 bytes

        let resolved = HashMap::from([(path_a.clone(), vec![single_parser("p1")]), (path_b.clone(), vec![single_parser("p2")])]);

        let map = build_file_parser_map(resolved).await.unwrap();

        assert_eq!(map.len(), 2);
        assert_eq!(map[&path_a].parsers[0].name(), "p1");
        assert_eq!(map[&path_a].offset, 2);
        assert_eq!(map[&path_b].parsers[0].name(), "p2");
        assert_eq!(map[&path_b].offset, 4);
    }

    #[tokio::test]
    async fn build_map_pattern_with_no_matches_adds_nothing() {
        // Glob expansion is done by from_config_file; build_file_parser_map receives
        // only resolved paths, so an empty input produces an empty map.
        let map = build_file_parser_map(HashMap::new()).await.unwrap();
        assert!(map.is_empty());
    }

    // -----------------------------------------------------------------------
    // restore_pending_state
    // -----------------------------------------------------------------------

    #[test]
    fn restore_rewinds_cursor_to_saved_when_saved_is_less() {
        let path = "/logs/test.log";
        let mut map = map_with_span_parser(path, "sp", 100);
        let saved = HashMap::from([(path.to_string(), 30u64)]);

        restore_pending_state(&mut map, vec![pending_record(path, "sp", "ABC")], &saved);

        assert_eq!(map[&PathBuf::from(path)].offset, 30);
    }

    #[test]
    fn restore_cursor_stays_when_saved_is_greater_than_current() {
        // min(200, 50) = 50 — a saved cursor can't advance the position.
        let path = "/logs/test.log";
        let mut map = map_with_span_parser(path, "sp", 50);
        let saved = HashMap::from([(path.to_string(), 200u64)]);

        restore_pending_state(&mut map, vec![pending_record(path, "sp", "ABC")], &saved);

        assert_eq!(map[&PathBuf::from(path)].offset, 50);
    }

    #[test]
    fn restore_cursor_unchanged_when_no_saved_entry() {
        let path = "/logs/test.log";
        let mut map = map_with_span_parser(path, "sp", 75);

        restore_pending_state(&mut map, vec![pending_record(path, "sp", "ABC")], &HashMap::new());

        assert_eq!(map[&PathBuf::from(path)].offset, 75);
    }

    #[test]
    fn restore_installs_pending_span_into_matching_parser() {
        let path = "/logs/test.log";
        let mut map = map_with_span_parser(path, "sp", 0);

        restore_pending_state(&mut map, vec![pending_record(path, "sp", "ABC")], &HashMap::new());

        assert_eq!(map[&PathBuf::from(path)].parsers[0].pending_spans().map(|m| m.len()).unwrap_or(0), 1);
    }

    #[test]
    fn restore_ignores_record_with_unknown_path() {
        let path = "/logs/test.log";
        let mut map = map_with_span_parser(path, "sp", 50);

        restore_pending_state(&mut map, vec![pending_record("/logs/other.log", "sp", "ABC")], &HashMap::new());

        assert_eq!(map.len(), 1);
        assert_eq!(map[&PathBuf::from(path)].parsers[0].pending_spans().map(|m| m.len()).unwrap_or(0), 0);
        assert_eq!(map[&PathBuf::from(path)].offset, 50); // cursor untouched
    }

    #[test]
    fn restore_ignores_record_with_unknown_parser_name() {
        let path = "/logs/test.log";
        let mut map = map_with_span_parser(path, "sp", 0);

        restore_pending_state(&mut map, vec![pending_record(path, "does_not_exist", "ABC")], &HashMap::new());

        assert_eq!(map[&PathBuf::from(path)].parsers[0].pending_spans().map(|m| m.len()).unwrap_or(0), 0);
    }

    #[test]
    fn restore_multiple_records_restores_all_spans_and_rewinds_cursor_once() {
        let path = "/logs/test.log";
        let mut map = map_with_span_parser(path, "sp", 100);
        let records = vec![pending_record(path, "sp", "ABC"), pending_record(path, "sp", "DEF")];
        let saved = HashMap::from([(path.to_string(), 40u64)]);

        restore_pending_state(&mut map, records, &saved);

        let ParserOffsets { parsers, offset: cursor } = &map[&PathBuf::from(path)];
        assert_eq!(parsers[0].pending_spans().map(|m| m.len()).unwrap_or(0), 2);
        assert_eq!(*cursor, 40);
    }

    #[test]
    fn restore_only_rewinds_files_that_have_pending_records() {
        // path_b has a saved cursor but no pending records — its cursor must not change.
        let path_a = "/logs/a.log";
        let path_b = "/logs/b.log";
        let mut map = FileParserMapping::default();
        map.insert(PathBuf::from(path_a), ParserOffsets { parsers: vec![span_parser("sp")], offset: 100 });
        map.insert(PathBuf::from(path_b), ParserOffsets { parsers: vec![span_parser("sp")], offset: 200 });
        let saved = HashMap::from([(path_a.to_string(), 10u64), (path_b.to_string(), 20u64)]);

        restore_pending_state(&mut map, vec![pending_record(path_a, "sp", "ABC")], &saved);

        assert_eq!(map[&PathBuf::from(path_a)].offset, 10); // rewound
        assert_eq!(map[&PathBuf::from(path_b)].offset, 200); // untouched
    }
}
