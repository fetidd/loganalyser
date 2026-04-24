use anyhow::anyhow;
use serde::Deserialize;
use shared::env::expand_env_vars;
use sqlx::{MySqlPool, SqlitePool, mysql::{MySqlConnectOptions, MySqlSslMode}, sqlite::SqliteConnectOptions};
use std::str::FromStr;
use crate::EventStorage;

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

pub async fn make_storage(config: &StorageConfig) -> anyhow::Result<EventStorage> {
    let s = match config.storage_type {
        StorageType::Mysql => {
            let conn_str = config.connection_string.as_deref().ok_or_else(|| anyhow!("connection_string required for MySQL storage"))?;
            let conn_str = expand_env_vars(conn_str)?;
            let opts = MySqlConnectOptions::from_str(&conn_str)?.ssl_mode(MySqlSslMode::Disabled);
            let mysql_pool = MySqlPool::connect_with(opts).await?;
            EventStorage::new_mysql(mysql_pool)
        }
        StorageType::Sqlite => {
            let conn_str = config.connection_string.as_deref().ok_or_else(|| anyhow!("connection_string required for SQLite storage"))?;
            let conn_str = expand_env_vars(conn_str)?;
            let opts = SqliteConnectOptions::new().filename(&conn_str).create_if_missing(true);
            EventStorage::new_sqlite(SqlitePool::connect_with(opts).await?).await
        }
        StorageType::Memory => EventStorage::new_in_memory().await,
    };
    Ok(s)
}
