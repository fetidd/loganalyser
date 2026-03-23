use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use sqlx::Row;
use uuid::Uuid;

use crate::Result;

#[derive(Debug, Clone)]
pub struct PendingSpanRecord {
    pub file_path: String,
    pub parser_name: String,
    pub span_ref: Vec<String>,
    pub id: Uuid,
    pub timestamp: NaiveDateTime,
    pub data: HashMap<String, String>,
    pub parent_id: Option<Uuid>,
}

#[async_trait]
pub trait PendingSpanStorage: Send + Sync + std::fmt::Debug {
    /// Replace all pending spans for the given (file_path, parser_name) with `records`.
    async fn save(
        &self,
        file_path: &str,
        parser_name: &str,
        records: &[PendingSpanRecord],
    ) -> Result<()>;
    /// Load all persisted pending spans.
    async fn load(&self) -> Result<Vec<PendingSpanRecord>>;
}

/// No-op implementation used when the storage backend is in-memory.
#[derive(Debug)]
pub struct MemoryPendingSpanStorage;

#[async_trait]
impl PendingSpanStorage for MemoryPendingSpanStorage {
    async fn save(&self, _: &str, _: &str, _: &[PendingSpanRecord]) -> Result<()> {
        Ok(())
    }
    async fn load(&self) -> Result<Vec<PendingSpanRecord>> {
        Ok(vec![])
    }
}

/// SQLite-backed pending span storage.
///
/// Expects the following table:
/// ```sql
/// CREATE TABLE IF NOT EXISTS pending_spans (
///     file_path   TEXT NOT NULL,
///     parser_name TEXT NOT NULL,
///     span_ref    TEXT NOT NULL,
///     id          TEXT NOT NULL,
///     timestamp   TEXT NOT NULL,
///     data        TEXT NOT NULL,
///     parent_id   TEXT,
///     PRIMARY KEY (file_path, parser_name, span_ref)
/// );
/// ```
#[derive(Debug)]
pub struct SqlitePendingSpanStorage {
    pool: sqlx::SqlitePool,
}

impl SqlitePendingSpanStorage {
    pub fn new(pool: sqlx::SqlitePool) -> Self {
        let p = pool.clone();
        tokio::spawn(async move {
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS pending_spans (\
                    file_path   TEXT NOT NULL,\
                    parser_name TEXT NOT NULL,\
                    span_ref    TEXT NOT NULL,\
                    id          TEXT NOT NULL,\
                    timestamp   TEXT NOT NULL,\
                    data        TEXT NOT NULL,\
                    parent_id   TEXT,\
                    PRIMARY KEY (file_path, parser_name, span_ref)\
                );",
            )
            .execute(&p)
            .await
            .expect("failed to create pending_spans table");
        });
        Self { pool }
    }
}

#[async_trait]
impl PendingSpanStorage for SqlitePendingSpanStorage {
    async fn save(
        &self,
        file_path: &str,
        parser_name: &str,
        records: &[PendingSpanRecord],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM pending_spans WHERE file_path = ? AND parser_name = ?")
            .bind(file_path)
            .bind(parser_name)
            .execute(&mut *tx)
            .await?;
        for r in records {
            sqlx::query(
                "INSERT INTO pending_spans \
                    (file_path, parser_name, span_ref, id, timestamp, data, parent_id) \
                    VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&r.file_path)
            .bind(&r.parser_name)
            .bind(serde_json::to_string(&r.span_ref)?)
            .bind(r.id.to_string())
            .bind(r.timestamp.format("%Y-%m-%d %H:%M:%S%.f").to_string())
            .bind(serde_json::to_string(&r.data)?)
            .bind(r.parent_id.map(|u| u.to_string()))
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn load(&self) -> Result<Vec<PendingSpanRecord>> {
        let rows = sqlx::query(
            "SELECT file_path, parser_name, span_ref, id, timestamp, data, parent_id \
             FROM pending_spans",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let file_path: String = row.try_get("file_path")?;
            let parser_name: String = row.try_get("parser_name")?;
            let span_ref: Vec<String> =
                serde_json::from_str(&row.try_get::<String, _>("span_ref")?)?;
            let id = Uuid::parse_str(&row.try_get::<String, _>("id")?)
                .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
            let timestamp =
                NaiveDateTime::parse_from_str(
                    &row.try_get::<String, _>("timestamp")?,
                    "%Y-%m-%d %H:%M:%S%.f",
                )
                .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
            let data: HashMap<String, String> =
                serde_json::from_str(&row.try_get::<String, _>("data")?)?;
            let parent_id: Option<Uuid> = row
                .try_get::<Option<String>, _>("parent_id")?
                .map(|s| Uuid::parse_str(&s))
                .transpose()
                .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
            records.push(PendingSpanRecord {
                file_path,
                parser_name,
                span_ref,
                id,
                timestamp,
                data,
                parent_id,
            });
        }
        Ok(records)
    }
}
