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
                    // TODO do we want to log here? Error?
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
                events.push(Event::new_single(&self.name, timestamp, data));
            }
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDateTime;
    use regex::Regex;

    use crate::event::Event;
    use crate::parser::tests::TEST_ID;

    use super::super::tests::common_test_data;
    use super::*;

    const TS_FMT: &str = "%Y-%m-%d %H:%M:%S";

    fn test_single(data: &[(&str, &str)], timestamp: &str) -> Event {
        let (ts, data_map) = common_test_data(data, timestamp);
        Event::Single {
            name: "test".into(),
            timestamp: NaiveDateTime::parse_from_str(&ts, TS_FMT).unwrap(),
            data: data_map,
            id: TEST_ID.to_string(),
        }
    }

    #[test]
    fn test_single_parse() {
        for (pattern, log, expected) in [
            (
                r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",
                "2026-01-01 12:34:56",
                vec![test_single(&[], "2026-01-01 12:34:56")],
            ),
            (
                r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (?P<level>\w+) (?P<message>.+)",
                "2026-03-05 08:00:00 INFO Server started",
                vec![test_single(
                    &[("level", "INFO"), ("message", "Server started")],
                    "2026-03-05 08:00:00",
                )],
            ),
            (
                r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (?P<user>\S+) (?P<action>\S+)",
                "not a log line\n2026-06-15 09:30:00 alice LOGIN\n2026-06-15 09:30:02 steve LOGIN\nskipped line",
                vec![
                    test_single(
                        &[("user", "alice"), ("action", "LOGIN")],
                        "2026-06-15 09:30:00",
                    ),
                    test_single(
                        &[("user", "steve"), ("action", "LOGIN")],
                        "2026-06-15 09:30:02",
                    ),
                ],
            ),
            (
                r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",
                "",
                vec![],
            ),
        ] {
            let parser = InternalSingleParser {
                name: "test".into(),
                pattern: Regex::new(pattern).unwrap(),
                timestamp_format: TS_FMT.into(),
            };
            let mut actual = parser.parse(log);
            actual.iter_mut().for_each(|f| f.set_id(TEST_ID));
            assert_eq!(actual, expected);
        }
    }
}
