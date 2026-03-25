use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use shared::event::Event;
use sqlx::Row;
use uuid::Uuid;

use crate::pending::PendingSpanRecord;
use crate::sql::{Dialect, EventForInsert, ParamValue, Params, build_event, build_where};
use crate::{EventStorage, Filter, Result};

/// SQLite-backed event store.
///
/// Expects the following tables:
/// ```sql
/// CREATE TABLE events (
///     id          TEXT    NOT NULL PRIMARY KEY,
///     event_type  TEXT    NOT NULL,
///     name        TEXT    NOT NULL,
///     timestamp   TEXT    NOT NULL,
///     duration_ms INTEGER     NULL,
///     parent_id   TEXT        NULL,
///     data        TEXT    NOT NULL
/// );
/// CREATE TABLE pending_spans (
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
pub struct SqliteEventStore {
    pool: sqlx::SqlitePool,
}

impl SqliteEventStore {
    pub async fn from_pool(pool: sqlx::SqlitePool) -> Self {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS events (\
                id TEXT NOT NULL PRIMARY KEY, \
                event_type TEXT NOT NULL, \
                name TEXT NOT NULL, \
                timestamp TEXT NOT NULL, \
                duration_ms INTEGER NULL, \
                parent_id TEXT NULL, \
                data TEXT NOT NULL\
            );",
        )
        .execute(&pool)
        .await
        .expect("failed to create events table");

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
        .execute(&pool)
        .await
        .expect("failed to create pending_spans table");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS file_cursors (\
                file_path TEXT NOT NULL PRIMARY KEY,\
                cursor    INTEGER NOT NULL\
            );",
        )
        .execute(&pool)
        .await
        .expect("failed to create file_cursors table");

        Self { pool }
    }
}

struct SqliteDialect;

impl Dialect for SqliteDialect {
    fn placeholder(&mut self) -> String {
        "?".into()
    }

    fn json_condition(&mut self, field: &str, op: &str, val: String) -> (String, Vec<ParamValue>) {
        (
            format!("json_extract(data, '$.{field}') {op} ?"),
            vec![val.into()],
        )
    }
}

impl SqliteEventStore {
    fn get_where_sql(filter: &Filter) -> Params {
        build_where(filter, &mut SqliteDialect)
    }
}

#[async_trait]
impl EventStorage for SqliteEventStore {
    async fn store(&self, events: &[Event]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for event in events {
            let e = EventForInsert::from_event(event)?;
            sqlx::query(EventForInsert::insert_sql())
                .bind(e.id)
                .bind(e.event_type)
                .bind(e.name)
                .bind(e.timestamp)
                .bind(e.duration_ms)
                .bind(e.parent_id)
                .bind(e.data_json)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn load(&self, filter: Filter) -> Result<Vec<Event>> {
        let Params(where_sql, bindings) = Self::get_where_sql(&filter);
        let query = format!(
            "SELECT id, event_type, name, timestamp, duration_ms, parent_id, data FROM events{where_sql}",
        );
        let mut query = sqlx::query(&query);
        for b in bindings {
            query = match b {
                ParamValue::String(s) => query.bind(s),
                ParamValue::SignedNumber(n) => query.bind(n),
            };
        }
        let rows = query.fetch_all(&self.pool).await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let id: Uuid = row
                .try_get::<String, _>("id")
                .and_then(|s| Uuid::parse_str(&s).map_err(|e| sqlx::Error::Decode(Box::new(e))))?;
            let event_type: String = row.try_get("event_type")?;
            let name: String = row.try_get("name")?;
            let timestamp = row.try_get::<String, _>("timestamp").and_then(|s| {
                chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f")
                    .map_err(|e| sqlx::Error::Decode(Box::new(e)))
            })?;
            let data_json: String = row.try_get("data")?;
            let parent_id: Option<Uuid> = row
                .try_get::<Option<String>, _>("parent_id")?
                .map(|s| Uuid::parse_str(&s))
                .transpose()?;
            let duration_ms: Option<i64> = row.try_get("duration_ms")?;
            events.push(build_event(
                id,
                event_type,
                name,
                timestamp,
                data_json,
                parent_id,
                duration_ms,
            )?);
        }
        Ok(events)
    }

    async fn save_pending(
        &self,
        file_path: &str,
        parser_name: &str,
        records: &[PendingSpanRecord],
        cursor: u64,
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
        // Upsert cursor. If no pending spans remain, remove the cursor entry so
        // the next startup does not try to rewind a file that is fully caught up.
        if records.is_empty() {
            sqlx::query("DELETE FROM file_cursors WHERE file_path = ?")
                .bind(file_path)
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query(
                "INSERT INTO file_cursors (file_path, cursor) VALUES (?, ?) \
                 ON CONFLICT(file_path) DO UPDATE SET cursor = excluded.cursor",
            )
            .bind(file_path)
            .bind(cursor as i64)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn load_file_cursors(&self) -> Result<HashMap<String, u64>> {
        let rows = sqlx::query("SELECT file_path, cursor FROM file_cursors")
            .fetch_all(&self.pool)
            .await?;
        let mut map = HashMap::with_capacity(rows.len());
        for row in rows {
            let path: String = row.try_get("file_path")?;
            let cursor: i64 = row.try_get("cursor")?;
            map.insert(path, cursor as u64);
        }
        Ok(map)
    }

    async fn load_pending(&self) -> Result<Vec<PendingSpanRecord>> {
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
            let timestamp = NaiveDateTime::parse_from_str(
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
