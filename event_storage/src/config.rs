use anyhow::anyhow;
use serde::Deserialize;
use shared::env::expand_env_vars;
use sqlx::{MySqlPool, SqlitePool, sqlite::SqliteConnectOptions};
use std::path::PathBuf;
use std::sync::Arc;

use crate::{EventStorage, MySqlEventStore, SqliteEventStore};

#[derive(Deserialize, Debug)]
#[serde(default)]
pub struct StorageConfig {
    pub storage_type: StorageType,
    pub connection_string: Option<String>,
    /// Path for the SQLite state file used by the MySQL backend to store
    /// pending spans and file cursors. Supports `${ENV_VAR}` expansion.
    /// Defaults to a platform-appropriate location when not set.
    pub state_db_path: Option<String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            storage_type: StorageType::Memory,
            connection_string: None,
            state_db_path: None,
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
            let conn_str = expand_env_vars(conn_str)?;
            let mysql_pool = MySqlPool::connect(&conn_str).await?;

            let state_path: PathBuf = match &config.state_db_path {
                Some(p) => PathBuf::from(expand_env_vars(p)?),
                None => shared::env::default_state_db_path(),
            };
            if let Some(parent) = state_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    anyhow!("cannot create state dir {:?}: {e}", parent)
                })?;
            }
            let state_path_str = state_path
                .to_str()
                .ok_or_else(|| anyhow!("state_db_path contains non-UTF8 characters"))?;
            let sqlite_opts = SqliteConnectOptions::new()
                .filename(state_path_str)
                .create_if_missing(true);
            let sidecar =
                SqliteEventStore::from_pool(SqlitePool::connect_with(sqlite_opts).await?).await;

            Arc::new(MySqlEventStore::new(mysql_pool, sidecar))
        }
        StorageType::Sqlite => {
            let conn_str = config
                .connection_string
                .as_deref()
                .ok_or_else(|| anyhow!("connection_string required for SQLite storage"))?;
            let conn_str = expand_env_vars(conn_str)?;
            let opts = SqliteConnectOptions::new()
                .filename(&conn_str)
                .create_if_missing(true);
            Arc::new(SqliteEventStore::from_pool(SqlitePool::connect_with(opts).await?).await)
        }
        StorageType::Memory => Arc::new(SqliteEventStore::new_in_memory().await),
    };
    Ok(s)
}
