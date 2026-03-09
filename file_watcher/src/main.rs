use std::{collections::HashMap, io::SeekFrom, path::Path, sync::Arc, time::Duration};

use anyhow::anyhow;
use event_storage::{EventStorage, MemoryEventStore};
use glob::glob;
use log_parser::parser::Parser;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
};

type FileParserMapping = HashMap<std::path::PathBuf, (Vec<Parser>, u64)>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // read required config
    let config_path = "../gateway_config.toml";
    let config_file = fs::read(config_path).await?;
    let config_toml: toml::Table = toml::from_slice(&config_file)?;
    let config = WatcherConfig::default();
    // create event storage
    let event_storage = Arc::new(MemoryEventStore::new());
    // create parsers and assign to matched files
    let mut file_parser_map: FileParserMapping = HashMap::new();
    for p_table in get_array(&config_toml, "parsers")?.iter().map(|v| {
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
    // loop and periodically tail files and parse the new lines, then store them
    let mut interval = tokio::time::interval(config.poll_interval);
    loop {
        let _ = interval.tick().await;
        for (path, (parsers, cursor_loc)) in file_parser_map.iter_mut() {
            let file_len = get_file_len(path).await?;
            if file_len < *cursor_loc {
                // truncated file
                *cursor_loc = 0;
                continue;
            } else if file_len > *cursor_loc {
                let mut file = fs::File::open(&path).await?;
                file.seek(SeekFrom::Start(*cursor_loc)).await?;
                let mut log_data = vec![];
                file.read_to_end(&mut log_data).await?;
                *cursor_loc = get_file_len(path).await?;
                let logs = String::from_utf8_lossy(&log_data).to_string();
                let parsers = parsers.clone();
                let storage = Arc::clone(&event_storage);
                tokio::spawn(async move {
                    for mut p in parsers {
                        let events = p.parse(&logs);
                        if !events.is_empty() {
                            let _res = storage.store(&events).await;
                        }
                    }
                });
            }
        }
    }
}

async fn get_file_len(file: impl AsRef<Path>) -> Result<u64, anyhow::Error> {
    let length = fs::metadata(file).await?.len();
    Ok(length)
}

struct WatcherConfig {
    poll_interval: Duration,
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(3),
        }
    }
}

fn get_str<'a>(table: &'a toml::Table, key: &str) -> anyhow::Result<&'a str> {
    table
        .get(key)
        .ok_or_else(|| anyhow!("missing '{key}'"))?
        .as_str()
        .ok_or_else(|| anyhow!("'{key}' should be a string"))
}

fn get_array<'a>(table: &'a toml::Table, key: &str) -> anyhow::Result<&'a Vec<toml::Value>> {
    table
        .get(key)
        .ok_or_else(|| anyhow!("missing '{key}'"))?
        .as_array()
        .ok_or_else(|| anyhow!("'{key}' should be an array"))
}
