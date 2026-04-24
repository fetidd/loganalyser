use shared::event::Event;
use sqlx::Row;
use uuid::Uuid;

use crate::sql::{Dialect, EventForInsert, ParamValue, Params, build_event, build_where};
use crate::{Filter, Result};

/// SQLite-backed event store.
///
/// Expects the following table:
/// ```sql
/// CREATE TABLE events (
///     id          TEXT    NOT NULL PRIMARY KEY,
///     event_type  TEXT    NOT NULL,
///     name        TEXT    NOT NULL,
///     timestamp   TEXT    NOT NULL,
///     duration_ms INTEGER     NULL,
///     parent_id   TEXT        NULL,
///     raw_line    TEXT,
///     data        TEXT    NOT NULL
/// );
/// ```
#[derive(Debug)]
pub struct SqliteEventStore {
    pub(crate) pool: sqlx::SqlitePool,
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
                data TEXT NOT NULL,\
                raw_line TEXT NULL \
            );",
        )
        .execute(&pool)
        .await
        .expect("failed to create events table");

        Self { pool }
    }

}

struct SqliteDialect;

impl Dialect for SqliteDialect {
    fn placeholder(&mut self) -> String {
        "?".into()
    }

    fn json_condition(&mut self, field: &str, op: &str, val: String) -> (String, Vec<ParamValue>) {
        (format!("json_extract(data, '$.{field}') {op} ?"), vec![val.into()])
    }

    fn json_in_condition(&mut self, field: &str, vals: &[String]) -> (String, Vec<ParamValue>) {
        let placeholders = vals.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let binds = vals.iter().map(|v| v.clone().into()).collect();
        (format!("json_extract(data, '$.{field}') IN ({placeholders})"), binds)
    }
}

impl SqliteEventStore {
    fn get_where_sql(filter: &Filter) -> Params {
        build_where(filter, &mut SqliteDialect)
    }

    pub async fn store(&self, events: &[Event]) -> Result<()> {
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
                .bind(e.raw_line)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn load(&self, filter: Filter) -> Result<Vec<Event>> {
        let Params(where_sql, bindings) = Self::get_where_sql(&filter);
        let query = format!("SELECT id, event_type, name, timestamp, duration_ms, parent_id, data, raw_line FROM events{where_sql}",);
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
            let id: Uuid = row.try_get::<String, _>("id").and_then(|s| Uuid::parse_str(&s).map_err(|e| sqlx::Error::Decode(Box::new(e))))?;
            let event_type: String = row.try_get("event_type")?;
            let name: String = row.try_get("name")?;
            let timestamp = row
                .try_get::<String, _>("timestamp")
                .and_then(|s| chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f").map_err(|e| sqlx::Error::Decode(Box::new(e))))?;
            let data_json: String = row.try_get("data")?;
            let parent_id: Option<Uuid> = row.try_get::<Option<String>, _>("parent_id")?.map(|s| Uuid::parse_str(&s)).transpose()?;
            let duration_ms: Option<i64> = row.try_get("duration_ms")?;
            let raw_line: Option<String> = row.try_get("raw_line")?;
            events.push(build_event(id, event_type, name, timestamp, data_json, parent_id, duration_ms, raw_line)?);
        }
        Ok(events)
    }
}
