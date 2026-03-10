use shared::event::Event;
use thiserror::Error;

pub mod memory;
pub mod mysql;

pub use memory::MemoryEventStore;
pub use mysql::MySqlEventStore;

#[derive(Debug, Error)]
pub enum Error {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("uuid error: {0}")]
    Uuid(#[from] uuid::Error),
    #[error("Storage error: {0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait EventStorage {
    fn store(&self, events: &[Event]) -> impl Future<Output = Result<()>> + Send;
    fn load(&self, filter: EventFilter) -> impl Future<Output = Result<Vec<Event>>> + Send;
}

#[derive(Clone, Debug)]
pub enum SqlCmp<T> {
    Eq(T),
    Gt(T),
    Lt(T),
    Gte(T),
    Lte(T),
    Like(T),
    In(Vec<T>),
    Json(String, Box<SqlCmp<T>>),
}

impl<T> SqlCmp<T> {
    pub fn map<U>(self, f: impl Fn(T) -> U) -> SqlCmp<U> {
        match self {
            SqlCmp::Eq(v)          => SqlCmp::Eq(f(v)),
            SqlCmp::Gt(v)          => SqlCmp::Gt(f(v)),
            SqlCmp::Lt(v)          => SqlCmp::Lt(f(v)),
            SqlCmp::Gte(v)         => SqlCmp::Gte(f(v)),
            SqlCmp::Lte(v)         => SqlCmp::Lte(f(v)),
            SqlCmp::Like(v)        => SqlCmp::Like(f(v)),
            SqlCmp::In(vals)       => SqlCmp::In(vals.into_iter().map(f).collect()),
            SqlCmp::Json(k, inner) => SqlCmp::Json(k, Box::new(inner.map(f))),
        }
    }
}

pub struct EventFilter {
    data: Option<Vec<SqlCmp<String>>>,
    timestamp: Option<Vec<SqlCmp<String>>>,
    id: Option<Vec<SqlCmp<String>>>,
    parent_id: Option<Vec<SqlCmp<String>>>,
    duration: Option<Vec<SqlCmp<u64>>>,
}

impl EventFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_data(mut self, field: &str, sql_cmp: SqlCmp<impl Into<String>>) -> Self {
        let s = SqlCmp::Json(field.into(), Box::new(sql_cmp.map(Into::into)));
        if let Some(current_data) = &mut self.data {
            current_data.push(s);
        } else {
            self.data = Some(vec![s]);
        }
        self
    }

    pub fn with_timestamp(mut self, sql_cmp: SqlCmp<impl Into<String>>) -> Self {
        let sql_cmp = sql_cmp.map(Into::into);
        if let Some(current_data) = &mut self.timestamp {
            current_data.push(sql_cmp);
        } else {
            self.timestamp = Some(vec![sql_cmp]);
        }
        self
    }

    pub fn with_id(mut self, sql_cmp: SqlCmp<impl Into<String>>) -> Self {
        let sql_cmp = sql_cmp.map(Into::into);
        if let Some(current_data) = &mut self.id {
            current_data.push(sql_cmp);
        } else {
            self.id = Some(vec![sql_cmp]);
        }
        self
    }

    pub fn with_parent_id(mut self, sql_cmp: SqlCmp<impl Into<String>>) -> Self {
        let sql_cmp = sql_cmp.map(Into::into);
        if let Some(current_data) = &mut self.parent_id {
            current_data.push(sql_cmp);
        } else {
            self.parent_id = Some(vec![sql_cmp]);
        }
        self
    }

    pub fn with_duration(mut self, sql_cmp: SqlCmp<impl Into<u64>>) -> Self {
        let sql_cmp = sql_cmp.map(Into::into);
        if let Some(current_data) = &mut self.duration {
            current_data.push(sql_cmp);
        } else {
            self.duration = Some(vec![sql_cmp]);
        }
        self
    }

    pub(crate) fn apply(&self, _event: &Event) -> bool {
        true
    }
}

impl Default for EventFilter {
    fn default() -> Self {
        Self {
            data: Default::default(),
            timestamp: Default::default(),
            id: Default::default(),
            parent_id: Default::default(),
            duration: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use shared::event::Event;

    use crate::{EventFilter, EventStorage, MemoryEventStore};

    #[tokio::test]
    async fn test_new_in_memory_storage() {
        let event = Event::new_single(
            "single1",
            shared::datetime_from("2026-01-01").unwrap(),
            HashMap::new(),
        );
        let store = MemoryEventStore::new();
        store
            .store(&[event.clone()])
            .await
            .expect("failed to store");
        let read = store
            .load(EventFilter::new())
            .await
            .expect("failed to load");
        assert_eq!(event, read[0]);
    }
}
