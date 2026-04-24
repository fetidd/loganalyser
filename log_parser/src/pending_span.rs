use std::collections::HashMap;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpanReference(pub Vec<String>);

#[derive(Debug, Clone)]
pub struct PendingSpan {
    pub id: Uuid,
    pub timestamp: NaiveDateTime,
    pub data: HashMap<String, String>,
    pub parent_id: Option<Uuid>,
    pub raw_line: Option<String>,
}

impl PendingSpan {
    pub fn new(timestamp: NaiveDateTime, data: HashMap<String, String>, parent_id: Option<Uuid>, raw_line: Option<String>) -> Self {
        Self {
            timestamp,
            data,
            id: Uuid::new_v4(),
            parent_id,
            raw_line,
        }
    }
}
