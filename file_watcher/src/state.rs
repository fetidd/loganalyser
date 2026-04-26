use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use chrono::NaiveDateTime;
use log_parser::pending_span::{PendingSpan, SpanReference};
use sqlx::{Row, SqlitePool, sqlite::SqliteConnectOptions};
use uuid::Uuid;

use crate::config::Config;

pub struct PendingSpanRecord {
    pub file_path: String,
    pub parser_name: String,
    pub span_ref: Vec<String>,
    pub id: Uuid,
    pub timestamp: NaiveDateTime,
    pub data: HashMap<String, String>,
    pub parent_id: Option<Uuid>,
    pub raw_line: Option<String>,
}

#[derive(Debug)]
pub struct State {
    pool: SqlitePool,
}

impl State {
    pub async fn new(config: &Config) -> anyhow::Result<Arc<State>> {
        let path = match &config.state_db_path {
            Some(p) => p.clone(),
            None => shared::env::default_state_db_path().to_string_lossy().into_owned(),
        };
        if let Some(parent) = std::path::Path::new(&path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(Arc::new(State::open(&path).await?))
    }

    async fn open(path: &str) -> Result<Self> {
        let opts = SqliteConnectOptions::new().filename(path).create_if_missing(true);
        let pool = SqlitePool::connect_with(opts).await?;
        Self::init(pool).await
    }

    async fn init(pool: SqlitePool) -> Result<Self> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS pending_spans (\
                file_path   TEXT NOT NULL,\
                parser_name TEXT NOT NULL,\
                span_ref    TEXT NOT NULL,\
                id          TEXT NOT NULL,\
                timestamp   TEXT NOT NULL,\
                data        TEXT NOT NULL,\
                parent_id   TEXT,\
                raw_line    TEXT,\
                PRIMARY KEY (file_path, parser_name, span_ref)\
            );",
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS file_cursors (\
                file_path TEXT NOT NULL PRIMARY KEY,\
                cursor    INTEGER NOT NULL\
            );",
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }

    pub async fn save_pending(&self, file_path: &str, parser_name: &str, pending: &HashMap<SpanReference, PendingSpan>) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM pending_spans WHERE file_path = ? AND parser_name = ?")
            .bind(file_path)
            .bind(parser_name)
            .execute(&mut *tx)
            .await?;
        for (span_ref, span) in pending {
            sqlx::query(
                "INSERT INTO pending_spans \
                    (file_path, parser_name, span_ref, id, timestamp, data, parent_id, raw_line) \
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(file_path)
            .bind(parser_name)
            .bind(serde_json::to_string(&span_ref)?)
            .bind(span.id.to_string())
            .bind(span.timestamp.format("%Y-%m-%d %H:%M:%S%.f").to_string())
            .bind(serde_json::to_string(&span.data)?)
            .bind(span.parent_id.map(|u| u.to_string()))
            .bind(&span.raw_line)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn save_cursor(&self, file_path: &str, cursor: u64) -> Result<()> {
        sqlx::query(
            "INSERT INTO file_cursors (file_path, cursor) VALUES (?, ?) \
             ON CONFLICT(file_path) DO UPDATE SET cursor = excluded.cursor",
        )
        .bind(file_path)
        .bind(cursor as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn load_cursors(&self) -> Result<HashMap<String, u64>> {
        let rows = sqlx::query("SELECT file_path, cursor FROM file_cursors").fetch_all(&self.pool).await?;
        let mut map = HashMap::with_capacity(rows.len());
        for row in rows {
            let path: String = row.try_get("file_path")?;
            let cursor: i64 = row.try_get("cursor")?;
            map.insert(path, cursor as u64);
        }
        Ok(map)
    }

    pub async fn load_pending(&self) -> Result<Vec<PendingSpanRecord>> {
        let rows = sqlx::query("SELECT file_path, parser_name, span_ref, id, timestamp, data, parent_id, raw_line FROM pending_spans")
            .fetch_all(&self.pool)
            .await?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let file_path: String = row.try_get("file_path")?;
            let parser_name: String = row.try_get("parser_name")?;
            let span_ref: Vec<String> = serde_json::from_str(&row.try_get::<String, _>("span_ref")?)?;
            let id = Uuid::parse_str(&row.try_get::<String, _>("id")?).map_err(|e| anyhow::anyhow!("invalid uuid: {e}"))?;
            let timestamp = NaiveDateTime::parse_from_str(&row.try_get::<String, _>("timestamp")?, "%Y-%m-%d %H:%M:%S%.f").map_err(|e| anyhow::anyhow!("invalid timestamp: {e}"))?;
            let data: HashMap<String, String> = serde_json::from_str(&row.try_get::<String, _>("data")?)?;
            let parent_id: Option<Uuid> = row
                .try_get::<Option<String>, _>("parent_id")?
                .map(|s| Uuid::parse_str(&s).map_err(|e| anyhow::anyhow!("invalid uuid: {e}")))
                .transpose()?;
            let raw_line = row.try_get::<Option<String>, _>("raw_line")?;
            records.push(PendingSpanRecord {
                file_path,
                parser_name,
                span_ref,
                id,
                timestamp,
                data,
                parent_id,
                raw_line,
            });
        }
        Ok(records)
    }
}
