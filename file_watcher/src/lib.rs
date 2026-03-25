use std::{
    collections::HashMap, io::SeekFrom, path::Path, path::PathBuf, sync::Arc, time::Duration,
};

use event_storage::{EventStorage, PendingSpanRecord, StorageConfig, make_storage};
use glob::glob;
use log_parser::parser::Parser;
use serde::Deserialize;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
    sync::oneshot::Receiver,
    time::{Instant, MissedTickBehavior},
};
use tracing::warn;

pub struct FileWatcher {
    file_parser_map: FileParserMapping,
    storage: Arc<dyn EventStorage>,
    settings: Settings,
    rx: Option<Receiver<bool>>,
}

type FileParserMapping = HashMap<PathBuf, (Vec<Parser>, u64)>;

impl FileWatcher {
    pub async fn new(config_file: Vec<u8>) -> anyhow::Result<Self> {
        let config: Config = toml::from_slice(&config_file)?;
        tracing::debug!("config created: {config:?}");
        let storage: Arc<dyn EventStorage> = make_storage(&config.storage).await?;
        tracing::debug!("storage created: {storage:?}");
        let built_parsers = Parser::from_config_file(&config_file)?;
        let mut file_parser_map: FileParserMapping = HashMap::new();
        for (pattern, parsers) in built_parsers.into_iter() {
            for entry in glob(&pattern)? {
                let path = entry?;
                let file_len = get_file_len(&path).await?;
                let (bound_parsers, cursor_loc) = file_parser_map.entry(path).or_default();
                bound_parsers.extend_from_slice(parsers.as_slice());
                *cursor_loc = file_len;
            }
        }
        // Restore any pending spans that were in-flight when the watcher last stopped.
        // Also restore the file cursor so content written during downtime is not skipped.
        let saved_cursors = storage.load_file_cursors().await?;
        for record in storage.load_pending().await? {
            let path = PathBuf::from(&record.file_path);
            if let Some((parsers, cursor_loc)) = file_parser_map.get_mut(&path) {
                // Rewind the cursor to the saved position so the watcher re-reads
                // any content that arrived while it was down, but never past the
                // current file length (in case the file was truncated/rotated).
                if let Some(&saved) = saved_cursors.get(&record.file_path) {
                    *cursor_loc = saved.min(*cursor_loc);
                }
                if let Some(p) = parsers.iter_mut().find(|p| p.name() == record.parser_name) {
                    p.restore_pending(vec![(
                        record.span_ref,
                        record.id,
                        record.timestamp,
                        record.data,
                        record.parent_id,
                    )]);
                }
            }
        }

        tracing::debug!("{file_parser_map:?}");
        Ok(Self {
            file_parser_map,
            storage,
            settings: config.settings,
            rx: None,
        })
    }

    pub fn with_receiver(mut self, rx: Receiver<bool>) -> Self {
        self.rx = Some(rx);
        self
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let Self {
            file_parser_map,
            storage,
            settings,
            rx,
        } = self;
        let mut interval = tokio::time::interval(Duration::from_secs(settings.poll_interval_secs));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        'main: loop {
            if let Some(rx) = rx
                && rx.try_recv().is_ok()
            {
                println!("exiting...");
                break 'main;
            }
            let t = interval.tick().await;
            let before_parse = Instant::now();
            for (path, (parsers, cursor_loc)) in file_parser_map.iter_mut() {
                let file_len = get_file_len(path).await?;
                if file_len < *cursor_loc {
                    *cursor_loc = 0;
                    continue;
                } else if file_len > *cursor_loc {
                    tracing::debug!("{path:?} has new log lines...");
                    let mut file = fs::File::open(&path).await?;
                    file.seek(SeekFrom::Start(*cursor_loc)).await?;
                    let mut log_data = vec![];
                    file.read_to_end(&mut log_data).await?;
                    let logs = String::from_utf8_lossy(&log_data).to_string();
                    let path_str = path.to_string_lossy().to_string();
                    let mut all_events = vec![];
                    let new_cursor = get_file_len(path).await?;
                    for p in parsers.iter_mut() {
                        all_events.extend(p.parse(&logs));
                        // Persist pending spans after each parse so they survive restarts.
                        let records: Vec<PendingSpanRecord> = p
                            .pending_spans()
                            .into_iter()
                            .map(
                                |(span_ref, id, timestamp, data, parent_id)| PendingSpanRecord {
                                    file_path: path_str.clone(),
                                    parser_name: p.name().to_string(),
                                    span_ref,
                                    id,
                                    timestamp,
                                    data,
                                    parent_id,
                                },
                            )
                            .collect();
                        let s = Arc::clone(storage);
                        let fp = path_str.clone();
                        let pn = p.name().to_string();
                        tokio::spawn(shared::async_retry!(
                            s.save_pending(&fp, &pn, &records, new_cursor)
                        ));
                    }
                    tracing::debug!("found {all_events:?}");
                    if !all_events.is_empty() {
                        let s = Arc::clone(storage);
                        tokio::spawn(shared::async_retry!(s.store(&all_events)));
                    }
                    *cursor_loc = get_file_len(path).await?;
                }
            }
            let d = t.duration_since(before_parse);
            if d > Duration::from_secs(self.settings.poll_interval_secs) {
                warn!("processing time exceeded polling interval!");
            }
        }
        Ok(())
    }
}

#[derive(Deserialize, Debug)]
struct Config {
    #[serde(default)]
    settings: Settings,
    #[serde(default)]
    storage: StorageConfig,
}

#[derive(Deserialize, Debug)]
#[serde(default)]
struct Settings {
    poll_interval_secs: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            poll_interval_secs: 3,
        }
    }
}

async fn get_file_len(file: impl AsRef<Path>) -> anyhow::Result<u64> {
    Ok(fs::metadata(file).await?.len())
}
