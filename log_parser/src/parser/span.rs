use std::{cell::RefCell, collections::HashMap};

use chrono::NaiveDateTime;
use regex::Regex;

use crate::event::Event;

use super::Parser;

#[derive(Debug, Clone)]
pub struct InternalSpanParser {
    pub name: String,
    pub timestamp_format: String,
    pub start_pattern: Regex,
    pub end_pattern: Regex,
    pub nested: Vec<Parser>,
    pub reference_fields: Vec<String>,
    pending: PendingSpans,
}

impl InternalSpanParser {
    pub(super) fn new(
        name: String,
        timestamp_format: String,
        start_pattern: Regex,
        end_pattern: Regex,
        nested: Vec<Parser>,
        reference_fields: Vec<String>,
    ) -> Self {
        Self {
            name,
            timestamp_format,
            start_pattern,
            end_pattern,
            nested,
            reference_fields,
            pending: PendingSpans::default(),
        }
    }

    pub(super) fn parse(&self, _input: &str) -> Vec<Event> {
        todo!()
    }
}

#[derive(Debug, Clone)]
struct SpanReference(Vec<String>);

#[derive(Debug, Clone)]
struct PendingSpan {
    timestamp: NaiveDateTime,
    data: HashMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct PendingSpans(RefCell<HashMap<SpanReference, PendingSpan>>);
