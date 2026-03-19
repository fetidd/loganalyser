use std::{collections::HashMap, io::SeekFrom, path::Path, sync::Arc, time::Duration};

use event_storage::{EventStorage, StorageConfig, make_storage};
use glob::glob;
use log_parser::parser::Parser;
use serde::Deserialize;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
};

pub struct FileWatcher {
    file_parser_map: FileParserMapping,
    storage: Arc<dyn EventStorage>,
    settings: Settings,
}

type FileParserMapping = HashMap<std::path::PathBuf, (Vec<Parser>, u64)>;

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
        tracing::debug!("{file_parser_map:?}");
        Ok(Self {
            file_parser_map,
            storage,
            settings: config.settings,
        })
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let mut interval =
            tokio::time::interval(Duration::from_secs(self.settings.poll_interval_secs));
        loop {
            let _ = interval.tick().await;
            for (path, (parsers, cursor_loc)) in self.file_parser_map.iter_mut() {
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
                    *cursor_loc = get_file_len(path).await?;
                    let logs = String::from_utf8_lossy(&log_data).to_string();
                    let mut all_events = vec![];
                    for p in parsers.iter_mut() {
                        all_events.extend(p.parse(&logs));
                    }
                    tracing::debug!("found {all_events:?}");
                    if !all_events.is_empty() {
                        let storage = Arc::clone(&self.storage);
                        tokio::spawn(store_with_retry(storage, all_events));
                    }
                }
            }
        }
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

async fn store_with_retry(storage: Arc<dyn EventStorage>, events: Vec<shared::event::Event>) {
    let mut delay = Duration::from_millis(100);
    for attempt in 1..=5_u32 {
        match storage.store(&events).await {
            Ok(()) => return,
            Err(e) if attempt < 5 => {
                tracing::warn!("store attempt {attempt}/5 failed: {e}, retrying in {delay:?}");
                tokio::time::sleep(delay).await;
                delay *= 2;
            }
            Err(e) => tracing::error!("failed to store events after 5 attempts: {e}"),
        }
    }
}

async fn get_file_len(file: impl AsRef<Path>) -> anyhow::Result<u64> {
    Ok(fs::metadata(file).await?.len())
}
