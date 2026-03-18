use std::{collections::HashMap, io::SeekFrom, path::Path, sync::Arc, time::Duration};

use anyhow::anyhow;
use event_storage::{EventStorage, StorageConfig, make_storage};
use glob::glob;
use log_parser::parser::Parser;
use serde::Deserialize;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
};

type FileParserMapping = HashMap<std::path::PathBuf, (Vec<Parser>, u64)>;

pub struct FileWatcher {
    file_parser_map: FileParserMapping,
    storage: Arc<dyn EventStorage>,
    settings: Settings,
}

impl FileWatcher {
    pub async fn new(config_path: &str) -> anyhow::Result<Self> {
        let config_file = fs::read(config_path).await?;
        let config: Config = toml::from_slice(&config_file)?;
        let storage: Arc<dyn EventStorage> = make_storage(&config.storage).await?;
        tracing::debug!("storage created");

        let mut file_parser_map: FileParserMapping = HashMap::new();
        for p_table in config.parsers.iter().map(|v| {
            v.as_table()
                .ok_or_else(|| anyhow!("parsers should be tables"))
        }) {
            let p_table = p_table?;
            let parser = Parser::build_from_toml(p_table)?;
            let pattern = get_str(p_table, "glob")?;
            for entry in glob(pattern)? {
                let path = entry?;
                let file_len = get_file_len(&path).await?;
                let (parsers, cursor_loc) = file_parser_map.entry(path).or_default();
                parsers.push(parser.clone());
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
                    let parsers = parsers.clone();
                    let storage = Arc::clone(&self.storage);
                    tokio::spawn(async move {
                        for mut p in parsers {
                            let events = p.parse(&logs);
                            tracing::debug!("found {events:?}");
                            if !events.is_empty() {
                                let res = storage.store(&events).await;
                                if res.is_err() {
                                    tracing::error!(
                                        "Failed to add event(s) to storage: {}",
                                        res.unwrap_err()
                                    );
                                }
                            }
                        }
                    });
                }
            }
        }
    }
}

#[derive(Deserialize)]
struct Config {
    #[serde(default)]
    settings: Settings,
    #[serde(default)]
    storage: StorageConfig,
    #[serde(default)]
    parsers: Vec<toml::Value>,
}

#[derive(Deserialize)]
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

fn get_str<'a>(table: &'a toml::Table, key: &str) -> anyhow::Result<&'a str> {
    table
        .get(key)
        .ok_or_else(|| anyhow!("missing '{key}'"))?
        .as_str()
        .ok_or_else(|| anyhow!("'{key}' should be a string"))
}
