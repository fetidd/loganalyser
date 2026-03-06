use std::collections::HashMap;

use regex::Regex;

use crate::event::Event;

#[derive(Debug, Clone)]
pub struct InternalSingleParser {
    pub name: String,
    pub pattern: Regex,
    pub timestamp_format: String,
}

impl InternalSingleParser {
    pub(super) fn parse(&self, input: &str) -> Vec<Event> {
        let mut events = vec![];
        for line in input.lines() {
            if let Some(captures) = self.pattern.captures(line) {
                let Ok(timestamp) = chrono::NaiveDateTime::parse_from_str(
                    &captures["timestamp"],
                    &self.timestamp_format,
                ) else {
                    continue;
                };
                let mut data = HashMap::new();
                for field in self.pattern.capture_names() {
                    if let Some(field) = field
                        && let Some(value) = captures.name(field)
                    {
                        data.insert(field.to_owned(), value.as_str().to_owned());
                    }
                }
                events.push(Event::Single {
                    name: self.name.to_owned(),
                    timestamp,
                    data,
                });
            }
        }
        events
    }
}
