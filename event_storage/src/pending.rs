use std::collections::HashMap;

use chrono::NaiveDateTime;
use uuid::Uuid;

#[derive(Debug, Clone)]
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
