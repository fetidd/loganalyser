use std::collections::HashMap;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Fixed namespace for deterministic event ID derivation.
const NAMESPACE: Uuid = Uuid::from_bytes([
    0xf5, 0x4a, 0x3b, 0x2c, 0x1d, 0x8e, 0x4f, 0xa0,
    0xb9, 0x6c, 0x3e, 0x7d, 0x2f, 0x9a, 0x1b, 0x5e,
]);

pub fn id_from_line(line: &str) -> Uuid {
    Uuid::new_v5(&NAMESPACE, line.as_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpanReference(pub Vec<String>);

#[derive(Debug, Clone)]
pub struct PendingSpan {
    pub id: Uuid,
    pub timestamp: NaiveDateTime,
    pub data: HashMap<String, String>,
    pub parent_id: Option<Uuid>,
    pub raw_line: String,
}

impl PendingSpan {
    pub fn new(id: Uuid, timestamp: NaiveDateTime, data: HashMap<String, String>, parent_id: Option<Uuid>, raw_line: String) -> Self {
        Self { id, timestamp, data, parent_id, raw_line }
    }
}
