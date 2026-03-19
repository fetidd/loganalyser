use anyhow::anyhow;
use serde::Deserialize;
use sqlx::{MySqlPool, SqlitePool, sqlite::SqliteConnectOptions};
use std::sync::Arc;

use crate::{EventStorage, MemoryEventStore, MySqlEventStore, SqliteEventStore};

#[derive(Deserialize, Debug)]
#[serde(default)]
pub struct StorageConfig {
    pub storage_type: StorageType,
    pub connection_string: Option<String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            storage_type: StorageType::Memory,
            connection_string: None,
        }
    }
}

#[derive(Deserialize, Default, Debug)]
#[serde(rename_all = "lowercase")]
pub enum StorageType {
    #[default]
    Memory,
    Mysql,
    Sqlite,
}

pub async fn make_storage(config: &StorageConfig) -> anyhow::Result<Arc<dyn EventStorage>> {
    let s: Arc<dyn EventStorage> = match config.storage_type {
        StorageType::Mysql => {
            let conn_str = config
                .connection_string
                .as_deref()
                .ok_or_else(|| anyhow!("connection_string required for MySQL storage"))?;
            Arc::new(MySqlEventStore::new(MySqlPool::connect(conn_str).await?))
        }
        StorageType::Sqlite => {
            let conn_str = config
                .connection_string
                .as_deref()
                .ok_or_else(|| anyhow!("connection_string required for SQLite storage"))?;
            let opts = SqliteConnectOptions::new()
                .filename(conn_str)
                .create_if_missing(true);
            Arc::new(SqliteEventStore::new(SqlitePool::connect_with(opts).await?))
        }
        StorageType::Memory => Arc::new(MemoryEventStore::new()),
    };
    Ok(s)
}
