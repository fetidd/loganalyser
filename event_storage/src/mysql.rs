use std::collections::HashMap;

use chrono::NaiveDateTime;
use shared::event::Event;
use sqlx::Row;
use uuid::Uuid;

use crate::pending::PendingSpanRecord;
use crate::sql::{Dialect, EventForInsert, ParamValue, Params, build_event, build_where};
use crate::{Filter, Result, SqliteEventStore};

#[derive(Debug)]
pub struct MySqlEventStore {
    pub(crate) pool: sqlx::MySqlPool,
    /// Local SQLite database used to persist pending spans and file cursors.
    pub(crate) sidecar: SqliteEventStore,
}

impl MySqlEventStore {
    pub fn new(pool: sqlx::MySqlPool, sidecar: SqliteEventStore) -> Self {
        Self { pool, sidecar }
    }
}

struct MySqlDialect;

impl Dialect for MySqlDialect {
    fn placeholder(&mut self) -> String {
        "?".into()
    }

    fn json_condition(&mut self, field: &str, op: &str, val: String) -> (String, Vec<ParamValue>) {
        (format!("data->>? {op} ?"), vec![format!("$.{field}").into(), val.into()])
    }

    fn json_in_condition(&mut self, field: &str, vals: &[String]) -> (String, Vec<ParamValue>) {
        let placeholders = vals.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let mut binds: Vec<ParamValue> = vec![format!("$.{field}").into()];
        binds.extend(vals.iter().map(|v| v.clone().into()));
        (format!("data->>? IN ({placeholders})"), binds)
    }
}

impl MySqlEventStore {
    fn get_where_sql(filter: &Filter) -> Params {
        build_where(filter, &mut MySqlDialect)
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
            let timestamp: NaiveDateTime = row.try_get("timestamp")?;
            let data_json: String = row.try_get("data")?;
            let parent_id: Option<Uuid> = row.try_get::<Option<String>, _>("parent_id")?.map(|s| Uuid::parse_str(&s)).transpose()?;
            let duration_ms: Option<i64> = row.try_get("duration_ms")?;
            let raw_line = row.try_get::<Option<String>, _>("raw_line")?;
            events.push(build_event(id, event_type, name, timestamp, data_json, parent_id, duration_ms, raw_line)?);
        }
        Ok(events)
    }

    pub async fn save_pending(&self, file_path: &str, parser_name: &str, records: &[PendingSpanRecord]) -> Result<()> {
        self.sidecar.save_pending(file_path, parser_name, records).await
    }

    pub async fn save_cursor(&self, file_path: &str, cursor: u64) -> Result<()> {
        self.sidecar.save_cursor(file_path, cursor).await
    }

    pub async fn load_pending(&self) -> Result<Vec<PendingSpanRecord>> {
        self.sidecar.load_pending().await
    }

    pub async fn load_file_cursors(&self) -> Result<HashMap<String, u64>> {
        self.sidecar.load_file_cursors().await
    }
}

/// MySQL-backed event store.
///
/// Expects the following table (adjust column sizes as needed):
/// ```sql
/// CREATE TABLE events (
///     id          CHAR(36)     NOT NULL PRIMARY KEY,
///     event_type  VARCHAR(10)  NOT NULL,
///     name        VARCHAR(255) NOT NULL,
///     timestamp   DATETIME(6)  NOT NULL,
///     duration_ms BIGINT           NULL,
///     parent_id   CHAR(36)         NULL,
///     raw_line    TEXT             NULL,
///     data        TEXT         NOT NULL
/// );
/// ```
#[cfg(test)]
mod tests {
    use crate::event_filter::{Cmp::*, and, data, id, or, timestamp};
    use crate::sql::{ParamValue, Params};

    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(
        Filter::new(),
        ("".into(), vec![])
    )]
    #[case(
        and([data("field", Eq("123")), or([timestamp(Lt("2026-01-01 00:00:00")), timestamp(Gt("2028-01-01 00:00:00"))])]),
        (" WHERE data->>? = ? AND (timestamp < ? OR timestamp > ?)".into(), vec!["$.field".into(), "123".into(), "2026-01-01 00:00:00".into(), "2028-01-01 00:00:00".into()])
    )]
    #[case(
        or([data("field", Eq("123")), timestamp(Lt("2026-01-01 00:00:00"))]),
        (" WHERE data->>? = ? OR timestamp < ?".into(), vec!["$.field".into(), "123".into(), "2026-01-01 00:00:00".into()])
    )]
    #[case(
        id(Eq("4cde4c35-9492-4f01-bd84-7109431c27cd")),
        (" WHERE id = ?".into(), vec!["4cde4c35-9492-4f01-bd84-7109431c27cd".into()])
    )]
    #[case(
        Filter::new().with_parent_id(Eq("4cde4c35-9492-4f01-bd84-7109431c27cd")),
        (" WHERE parent_id = ?".into(), vec!["4cde4c35-9492-4f01-bd84-7109431c27cd".into()])
    )]
    #[case(
        Filter::new().with_id(In(vec!["4cde4c35-9492-4f01-bd84-7109431c27ce", "4cde4c35-9492-4f01-bd84-7109431c27cd"])),
        (" WHERE id IN (?, ?)".into(), vec!["4cde4c35-9492-4f01-bd84-7109431c27ce".into(), "4cde4c35-9492-4f01-bd84-7109431c27cd".into()])
    )]
    #[case(
        Filter::new().with_duration(Eq(2000)),
        (" WHERE duration_ms = ?".into(), vec![2000.into()])
    )]
    #[case(
        Filter::new().with_timestamp(Lte("2026-01-01 00:00:00")).with_timestamp(Gte("2025-01-01 00:00:00")),
        (" WHERE timestamp <= ? AND timestamp >= ?".into(), vec!["2026-01-01 00:00:00".into(), "2025-01-01 00:00:00".into()])
    )]
    #[case(
        Filter::new().with_data("field", Eq("value")),
        (" WHERE data->>? = ?".into(), vec!["$.field".into(), "value".into()])
    )]
    #[case(
        Filter::new().with_data("field", Eq("value")).with_data("abc", Like("%123%")),
        (" WHERE data->>? = ? AND data->>? LIKE ?".into(), vec!["$.field".into(), "value".into(), "$.abc".into(), "%123%".into()])
    )]
    #[case(
        Filter::new().with_duration(Gt(500)),
        (" WHERE duration_ms > ?".into(), vec![500.into()])
    )]
    #[case(
        Filter::new().with_duration(Lt(1000)),
        (" WHERE duration_ms < ?".into(), vec![1000.into()])
    )]
    #[case(
        Filter::new().with_duration(Gte(100)),
        (" WHERE duration_ms >= ?".into(), vec![100.into()])
    )]
    #[case(
        Filter::new().with_duration(Lte(9999)),
        (" WHERE duration_ms <= ?".into(), vec![9999.into()])
    )]
    #[case(
        Filter::new().with_duration(In(vec![100, 200, 300])),
        (" WHERE duration_ms IN (?, ?, ?)".into(), vec![100.into(), 200.into(), 300.into()])
    )]
    #[case(
        Filter::new().with_id(Eq("4cde4c35-9492-4f01-bd84-7109431c27cd")),
        (" WHERE id = ?".into(), vec!["4cde4c35-9492-4f01-bd84-7109431c27cd".into()])
    )]
    #[case(
        Filter::new().with_parent_id(In(vec!["4cde4c35-9492-4f01-bd84-7109431c27ce", "4cde4c35-9492-4f01-bd84-7109431c27cd"])),
        (" WHERE parent_id IN (?, ?)".into(), vec!["4cde4c35-9492-4f01-bd84-7109431c27ce".into(), "4cde4c35-9492-4f01-bd84-7109431c27cd".into()])
    )]
    #[case(
        Filter::new().with_timestamp(Eq("2026-01-01 00:00:00")),
        (" WHERE timestamp = ?".into(), vec!["2026-01-01 00:00:00".into()])
    )]
    #[case(
        Filter::new().with_timestamp(Lt("2026-01-01 00:00:00")),
        (" WHERE timestamp < ?".into(), vec!["2026-01-01 00:00:00".into()])
    )]
    #[case(
        Filter::new().with_timestamp(Gt("2026-01-01 00:00:00")),
        (" WHERE timestamp > ?".into(), vec!["2026-01-01 00:00:00".into()])
    )]
    #[case(
        Filter::new().with_timestamp(In(vec!["2025-06-01 00:00:00", "2026-01-01 00:00:00"])),
        (" WHERE timestamp IN (?, ?)".into(), vec!["2025-06-01 00:00:00".into(), "2026-01-01 00:00:00".into()])
    )]
    #[case(
        Filter::new().with_duration(Gt(0)).with_id(Eq("4cde4c35-9492-4f01-bd84-7109431c27cd")),
        (" WHERE duration_ms > ? AND id = ?".into(), vec![0.into(), "4cde4c35-9492-4f01-bd84-7109431c27cd".into()])
    )]
    #[case(
        Filter::new().with_data("env", Like("%prod%")).with_timestamp(Gte("2025-01-01 00:00:00")),
        (" WHERE data->>? LIKE ? AND timestamp >= ?".into(), vec!["$.env".into(), "%prod%".into(), "2025-01-01 00:00:00".into()])
    )]
    #[case(
        Filter::new().with_raw_line(Like("errormessage=FatalError")),
        (" WHERE raw_line LIKE ?".into(), vec!["%errormessage=FatalError%".into()])
    )]
    fn test_get_where_sql(#[case] filter: impl Into<Filter>, #[case] expected: (String, Vec<ParamValue>)) {
        let mut expected_params = Params::new();
        expected_params.add(&expected.0, &expected.1);
        assert_eq!(MySqlEventStore::get_where_sql(&filter.into()), expected_params);
    }
}
