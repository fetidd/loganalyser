use std::collections::HashMap;

use chrono::{Duration, NaiveDateTime};
use shared::event::Event;
use sqlx::Row;
use uuid::Uuid;

use crate::{Error, EventFilter, EventStorage, Result, SqlCmp};

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
///     data        TEXT         NOT NULL
/// );
/// ```
pub struct MySqlEventStore {
    pool: sqlx::MySqlPool,
}

impl MySqlEventStore {
    pub fn new(pool: sqlx::MySqlPool) -> Self {
        Self { pool }
    }
}

impl EventStorage for MySqlEventStore {
    fn store(&self, events: &[Event]) -> impl Future<Output = Result<()>> + Send {
        let pool = self.pool.clone();
        let events = events.to_vec();
        async move {
            let mut tx = pool.begin().await?;
            for event in &events {
                match event {
                    Event::Span {
                        id,
                        name,
                        timestamp,
                        data,
                        duration,
                        parent_id,
                    } => {
                        let data_json = serde_json::to_string(data)?;
                        sqlx::query(
                            "INSERT INTO events \
                             (id, event_type, name, timestamp, duration_ms, parent_id, data) \
                             VALUES (?, 'span', ?, ?, ?, ?, ?)",
                        )
                        .bind(id.to_string())
                        .bind(name)
                        .bind(timestamp)
                        .bind(duration.num_milliseconds())
                        .bind(parent_id.map(|p| p.to_string()))
                        .bind(data_json)
                        .execute(&mut *tx)
                        .await?;
                    }
                    Event::Single {
                        id,
                        name,
                        timestamp,
                        data,
                        parent_id,
                    } => {
                        let data_json = serde_json::to_string(data)?;
                        sqlx::query(
                            "INSERT INTO events \
                             (id, event_type, name, timestamp, duration_ms, parent_id, data) \
                             VALUES (?, 'single', ?, ?, NULL, ?, ?)",
                        )
                        .bind(id.to_string())
                        .bind(name)
                        .bind(timestamp)
                        .bind(parent_id.map(|p| p.to_string()))
                        .bind(data_json)
                        .execute(&mut *tx)
                        .await?;
                    }
                }
            }
            tx.commit().await?;
            Ok(())
        }
    }

    fn load(&self, filter: EventFilter) -> impl Future<Output = Result<Vec<Event>>> + Send {
        let pool = self.pool.clone();
        let (where_sql, bindings) = Self::get_where(&filter);
        let query = format!(
            "SELECT id, event_type, name, timestamp, duration_ms, parent_id, data FROM events{where_sql}",
        );
        async move {
            let mut query = sqlx::query(&query);
            for b in bindings {
                query = match b {
                    MySqlParamValue::String(s) => query.bind(s),
                    MySqlParamValue::SignedNumber(n) => query.bind(n),
                    MySqlParamValue::UnsignedNumber(n) => query.bind(n),
                };
            }
            let rows = query.fetch_all(&pool).await?;

            let mut events = Vec::with_capacity(rows.len());
            for row in rows {
                let id: Uuid = row.try_get::<String, _>("id").and_then(|s| {
                    Uuid::parse_str(&s).map_err(|e| sqlx::Error::Decode(Box::new(e)))
                })?;
                let event_type: String = row.try_get("event_type")?;
                let name: String = row.try_get("name")?;
                let timestamp: NaiveDateTime = row.try_get("timestamp")?;
                let data_json: String = row.try_get("data")?;
                let data: HashMap<String, String> = serde_json::from_str(&data_json)?;
                let parent_id: Option<Uuid> = row
                    .try_get::<Option<String>, _>("parent_id")?
                    .map(|s| Uuid::parse_str(&s))
                    .transpose()?;

                let event = match event_type.as_str() {
                    "span" => {
                        let duration_ms: i64 = row.try_get("duration_ms")?;
                        Event::Span {
                            id,
                            name,
                            timestamp,
                            data,
                            duration: Duration::milliseconds(duration_ms),
                            parent_id,
                        }
                    }
                    "single" => Event::Single {
                        id,
                        name,
                        timestamp,
                        data,
                        parent_id,
                    },
                    other => return Err(Error::Storage(format!("unknown event_type: {other}"))),
                };
                events.push(event);
            }
            Ok(events)
        }
    }
}

type MySqlParams = Vec<(String, Vec<MySqlParamValue>)>;

#[derive(Debug, PartialEq)]
enum MySqlParamValue {
    String(String),
    SignedNumber(i64),
    UnsignedNumber(u64),
}

impl From<u64> for MySqlParamValue {
    fn from(n: u64) -> Self {
        MySqlParamValue::UnsignedNumber(n)
    }
}

impl From<i64> for MySqlParamValue {
    fn from(n: i64) -> Self {
        MySqlParamValue::SignedNumber(n)
    }
}

impl From<String> for MySqlParamValue {
    fn from(s: String) -> Self {
        MySqlParamValue::String(s)
    }
}

impl From<&str> for MySqlParamValue {
    fn from(s: &str) -> Self {
        MySqlParamValue::String(s.to_owned())
    }
}

impl MySqlEventStore {
    fn parse_data_filters(filter: &EventFilter, wheres: &mut MySqlParams) {
        if let Some(filters) = &filter.data {
            for filter in filters {
                match filter {
                    SqlCmp::Json(field, sql_cmp) => {
                        let (op, val) = match &**sql_cmp {
                            SqlCmp::Eq(s) => ("=", s),
                            SqlCmp::Like(s) => ("LIKE", s),
                            _ => panic!("only = or LIKE"),
                        };
                        wheres.push((
                            format!("data->>? {op} ?"),
                            vec![format!("$.{field}").into(), val.clone().into()],
                        ));
                    }
                    other => panic!("data can not be filtered by {other:?}"),
                }
            }
        }
    }

    fn parse_timestamp_filters(filter: &EventFilter, wheres: &mut MySqlParams) {
        if let Some(filters) = &filter.timestamp {
            for filter in filters {
                match filter {
                    SqlCmp::In(vals) => {
                        let placeholders = vec!["?"; vals.len()].join(", ");
                        wheres.push((
                            format!("timestamp IN ({placeholders})"),
                            vals.iter().map(|v| v.clone().into()).collect(),
                        ));
                    }
                    other => {
                        let (op, val) = match other {
                            SqlCmp::Eq(v) => ("=", v),
                            SqlCmp::Lt(v) => ("<", v),
                            SqlCmp::Gt(v) => (">", v),
                            SqlCmp::Lte(v) => ("<=", v),
                            SqlCmp::Gte(v) => (">=", v),
                            _ => panic!("timestamp can not be filtered by {other:?}"),
                        };
                        wheres.push((format!("timestamp {op} ?"), vec![val.clone().into()]));
                    }
                }
            }
        }
    }

    fn parse_duration_filters(filter: &EventFilter, wheres: &mut MySqlParams) {
        if let Some(filters) = &filter.duration {
            for filter in filters {
                match filter {
                    SqlCmp::In(vals) => {
                        let placeholders = vec!["?"; vals.len()].join(", ");
                        wheres.push((
                            format!("duration_ms IN ({placeholders})"),
                            vals.iter()
                                .map(|v| MySqlParamValue::UnsignedNumber(*v))
                                .collect(),
                        ));
                    }
                    other => {
                        let (op, val) = match other {
                            SqlCmp::Eq(v) => ("=", v),
                            SqlCmp::Lt(v) => ("<", v),
                            SqlCmp::Gt(v) => (">", v),
                            SqlCmp::Lte(v) => ("<=", v),
                            SqlCmp::Gte(v) => (">=", v),
                            _ => panic!("duration can not be filtered by {other:?}"),
                        };
                        wheres.push((
                            format!("duration_ms {op} ?"),
                            vec![MySqlParamValue::UnsignedNumber(*val)],
                        ));
                    }
                }
            }
        }
    }

    fn _parse_id_filter(filter: &SqlCmp<String>, wheres: &mut MySqlParams, field: &str) {
        match filter {
            SqlCmp::In(vals) => {
                let placeholders = vec!["?"; vals.len()].join(", ");
                wheres.push((
                    format!("{field} IN ({placeholders})"),
                    vals.iter().map(|v| v.clone().into()).collect(),
                ));
            }
            other => {
                let (op, val) = match other {
                    SqlCmp::Eq(v) => ("=", v),
                    SqlCmp::Lt(v) => ("<", v),
                    SqlCmp::Gt(v) => (">", v),
                    SqlCmp::Lte(v) => ("<=", v),
                    SqlCmp::Gte(v) => (">=", v),
                    _ => panic!("{field} can not be filtered by {other:?}"),
                };
                wheres.push((format!("{field} {op} ?"), vec![val.clone().into()]));
            }
        }
    }

    fn parse_id_filters(filter: &EventFilter, wheres: &mut MySqlParams) {
        if let Some(filters) = &filter.id {
            for filter in filters {
                Self::_parse_id_filter(filter, wheres, "id");
            }
        }
    }

    fn parse_parent_id_filters(filter: &EventFilter, wheres: &mut MySqlParams) {
        if let Some(filters) = &filter.parent_id {
            for filter in filters {
                Self::_parse_id_filter(filter, wheres, "parent_id");
            }
        }
    }

    fn get_where(filter: &EventFilter) -> (String, Vec<MySqlParamValue>) {
        let mut wheres: MySqlParams = vec![];
        for parser in [
            Self::parse_data_filters,
            Self::parse_timestamp_filters,
            Self::parse_duration_filters,
            Self::parse_id_filters,
            Self::parse_parent_id_filters,
        ] {
            parser(filter, &mut wheres);
        }
        if !wheres.is_empty() {
            wheres.into_iter().fold(
                (String::from(" WHERE "), vec![]),
                |(mut sql, mut binds), (clause, clause_binds)| {
                    if sql != " WHERE " {
                        sql.push_str(" AND ");
                    }
                    sql.push_str(&clause);
                    binds.extend(clause_binds);
                    (sql, binds)
                },
            )
        } else {
            ("".into(), vec![])
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::SqlCmp::*;

    use super::*;

    #[rstest]
    #[case(
        EventFilter::new(),
        ("".into(), vec![])
    )]
    #[case(
        EventFilter::new().with_parent_id(Eq("4cde4c35-9492-4f01-bd84-7109431c27cd")),
        (" WHERE parent_id = ?".into(), vec!["4cde4c35-9492-4f01-bd84-7109431c27cd".into()])
    )]
    #[case(
        EventFilter::new().with_id(In(vec!["4cde4c35-9492-4f01-bd84-7109431c27ce", "4cde4c35-9492-4f01-bd84-7109431c27cd"])),
        (" WHERE id IN (?, ?)".into(), vec!["4cde4c35-9492-4f01-bd84-7109431c27ce".into(), "4cde4c35-9492-4f01-bd84-7109431c27cd".into()])
    )]
    #[case(
        EventFilter::new().with_duration(Eq(2000_u64)),
        (" WHERE duration_ms = ?".into(), vec![2000_u64.into()])
    )]
    #[case(
        EventFilter::new().with_timestamp(Lte("2026-01-01 00:00:00")).with_timestamp(Gte("2025-01-01 00:00:00")),
        (" WHERE timestamp <= ? AND timestamp >= ?".into(), vec!["2026-01-01 00:00:00".into(), "2025-01-01 00:00:00".into()])
    )]
    #[case(
        EventFilter::new().with_data("field", Eq("value")),
        (" WHERE data->>? = ?".into(), vec!["$.field".into(), "value".into()])
    )]
    #[case(
        EventFilter::new().with_data("field", Eq("value")).with_data("abc", Like("%123%")),
        (" WHERE data->>? = ? AND data->>? LIKE ?".into(), vec!["$.field".into(), "value".into(), "$.abc".into(), "%123%".into()])
    )]
    // duration comparisons
    #[case(
        EventFilter::new().with_duration(Gt(500_u64)),
        (" WHERE duration_ms > ?".into(), vec![500_u64.into()])
    )]
    #[case(
        EventFilter::new().with_duration(Lt(1000_u64)),
        (" WHERE duration_ms < ?".into(), vec![1000_u64.into()])
    )]
    #[case(
        EventFilter::new().with_duration(Gte(100_u64)),
        (" WHERE duration_ms >= ?".into(), vec![100_u64.into()])
    )]
    #[case(
        EventFilter::new().with_duration(Lte(9999_u64)),
        (" WHERE duration_ms <= ?".into(), vec![9999_u64.into()])
    )]
    #[case(
        EventFilter::new().with_duration(In(vec![100_u64, 200_u64, 300_u64])),
        (" WHERE duration_ms IN (?, ?, ?)".into(), vec![100_u64.into(), 200_u64.into(), 300_u64.into()])
    )]
    // id comparisons
    #[case(
        EventFilter::new().with_id(Eq("4cde4c35-9492-4f01-bd84-7109431c27cd")),
        (" WHERE id = ?".into(), vec!["4cde4c35-9492-4f01-bd84-7109431c27cd".into()])
    )]
    #[case(
        EventFilter::new().with_id(Lt("4cde4c35-9492-4f01-bd84-7109431c27cd")),
        (" WHERE id < ?".into(), vec!["4cde4c35-9492-4f01-bd84-7109431c27cd".into()])
    )]
    #[case(
        EventFilter::new().with_id(Gt("4cde4c35-9492-4f01-bd84-7109431c27cd")),
        (" WHERE id > ?".into(), vec!["4cde4c35-9492-4f01-bd84-7109431c27cd".into()])
    )]
    // parent_id with In
    #[case(
        EventFilter::new().with_parent_id(In(vec!["4cde4c35-9492-4f01-bd84-7109431c27ce", "4cde4c35-9492-4f01-bd84-7109431c27cd"])),
        (" WHERE parent_id IN (?, ?)".into(), vec!["4cde4c35-9492-4f01-bd84-7109431c27ce".into(), "4cde4c35-9492-4f01-bd84-7109431c27cd".into()])
    )]
    // timestamp comparisons
    #[case(
        EventFilter::new().with_timestamp(Eq("2026-01-01 00:00:00")),
        (" WHERE timestamp = ?".into(), vec!["2026-01-01 00:00:00".into()])
    )]
    #[case(
        EventFilter::new().with_timestamp(Lt("2026-01-01 00:00:00")),
        (" WHERE timestamp < ?".into(), vec!["2026-01-01 00:00:00".into()])
    )]
    #[case(
        EventFilter::new().with_timestamp(Gt("2026-01-01 00:00:00")),
        (" WHERE timestamp > ?".into(), vec!["2026-01-01 00:00:00".into()])
    )]
    #[case(
        EventFilter::new().with_timestamp(In(vec!["2025-06-01 00:00:00", "2026-01-01 00:00:00"])),
        (" WHERE timestamp IN (?, ?)".into(), vec!["2025-06-01 00:00:00".into(), "2026-01-01 00:00:00".into()])
    )]
    // multi-field combinations
    #[case(
        EventFilter::new().with_id(Eq("4cde4c35-9492-4f01-bd84-7109431c27cd")).with_duration(Gt(0_u64)),
        (" WHERE duration_ms > ? AND id = ?".into(), vec![0_u64.into(), "4cde4c35-9492-4f01-bd84-7109431c27cd".into()])
    )]
    #[case(
        EventFilter::new().with_timestamp(Gte("2025-01-01 00:00:00")).with_data("env", Like("%prod%")),
        (" WHERE data->>? LIKE ? AND timestamp >= ?".into(), vec!["$.env".into(), "%prod%".into(), "2025-01-01 00:00:00".into()])
    )]
    fn test_get_where(
        #[case] filter: EventFilter,
        #[case] expected: (String, Vec<MySqlParamValue>),
    ) {
        assert_eq!(MySqlEventStore::get_where(&filter), expected);
    }
}
