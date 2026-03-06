mod single;
mod span;

use regex::Regex;

pub use single::InternalSingleParser;
pub use span::InternalSpanParser;

use crate::{
    error::{LogParserError, LogParserResult},
    event::Event,
};

#[derive(Debug, Clone)]
pub enum Parser {
    Single(InternalSingleParser),
    Span(InternalSpanParser),
}

impl Parser {
    pub fn name(&self) -> &str {
        match self {
            Parser::Single(p) => &p.name,
            Parser::Span(p) => &p.name,
        }
    }

    pub fn timestamp_format(&self) -> &str {
        match self {
            Parser::Single(p) => &p.timestamp_format,
            Parser::Span(p) => &p.timestamp_format,
        }
    }

    pub fn from_toml(t: &toml::Table) -> LogParserResult<Parser> {
        Self::from_toml_table_and_timestamp_format(
            t,
            Self::parse_and_validate_str("timestamp_format", t)?,
        )
    }

    fn from_toml_table_and_timestamp_format(
        t: &toml::Table,
        ts_fmt: &str,
    ) -> LogParserResult<Parser> {
        let config_type = t
            .get("type")
            .ok_or(error("missing type"))?
            .as_str()
            .ok_or(error("type was not a string"))?;
        let builder = match config_type {
            "span" => Self::build_span,
            "single" => Self::build_single,
            _ => todo!(),
        };
        builder(t, Self::parse_and_validate_str("name", t)?, ts_fmt)
    }

    fn parse_and_validate_str<'a>(field: &str, t: &'a toml::Table) -> LogParserResult<&'a str> {
        Ok(t.get(field)
            .ok_or(error(&format!("missing {field}")))?
            .as_str()
            .ok_or(error(&format!("{field} was not a string")))?)
    }

    fn build_span(t: &toml::Table, name: &str, timestamp_format: &str) -> LogParserResult<Parser> {
        let reference_fields = t
            .get("reference_fields")
            .ok_or(error("missing reference_fields"))?
            .as_array()
            .ok_or(error("reference_fields not an array"))?
            .into_iter()
            .map(|v| match v.as_str() {
                Some(s) => Ok(s.to_owned()),
                None => Err(error("reference_fields elements must be strings")),
            })
            .collect::<Result<Vec<String>, LogParserError>>()?;
        let mut nested_parsers = vec![];
        let nested = t.get("nested");
        if let Some(nested) = nested {
            if let Some(nested) = nested.as_array() {
                for value in nested {
                    let table = value
                        .as_table()
                        .ok_or(error("nested elements must be toml tables"))?;
                    let nested_ts = match table.get("timestamp_format") {
                        Some(found) => found
                            .as_str()
                            .ok_or(error("timestamp_format was not a string"))?,
                        None => timestamp_format,
                    };
                    let parser = Self::from_toml_table_and_timestamp_format(table, nested_ts)?;
                    nested_parsers.push(parser);
                }
            } else {
                return Err(error("nested should be an array"));
            }
        }
        let start_pattern =
            Regex::new(Self::parse_and_validate_str("start_pattern", t)?.into())?;
        let end_pattern =
            Regex::new(Self::parse_and_validate_str("end_pattern", t)?.into())?;
        Self::validate_required_pattern_fields(&start_pattern, &Self::REQUIRED_FIELDS)?;
        Self::validate_required_pattern_fields(&end_pattern, &Self::REQUIRED_FIELDS)?;
        Ok(Parser::Span(InternalSpanParser::new(
            name.into(),
            timestamp_format.into(),
            start_pattern,
            end_pattern,
            nested_parsers,
            reference_fields,
        )))
    }

    fn validate_required_pattern_fields(
        pattern: &Regex,
        fields: &[&str],
    ) -> LogParserResult<()> {
        let mut missing = vec![];
        let mut capture_names = pattern.capture_names().into_iter();
        for f in fields {
            if !capture_names.any(|c| c.is_some_and(|c| c == *f)) {
                missing.push(*f);
            }
        }
        if !missing.is_empty() {
            Err(error(&format!(
                "pattern missing fields: {}",
                missing.join(", ")
            )))
        } else {
            Ok(())
        }
    }

    const REQUIRED_FIELDS: [&str; 1] = ["timestamp"];

    fn build_single(
        t: &toml::Table,
        name: &str,
        timestamp_format: &str,
    ) -> LogParserResult<Parser> {
        let pattern = Regex::new(Self::parse_and_validate_str("pattern", t)?.into())?;
        Self::validate_required_pattern_fields(&pattern, &Self::REQUIRED_FIELDS)?;
        Ok(Parser::Single(InternalSingleParser {
            name: name.into(),
            pattern,
            timestamp_format: timestamp_format.into(),
        }))
    }

    pub fn parse(&self, input: &str) -> Vec<Event> {
        match self {
            Parser::Single(p) => p.parse(input),
            Parser::Span(p) => p.parse(input),
        }
    }
}

fn error(msg: &str) -> LogParserError {
    LogParserError::ConfigParseError(msg.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{Duration, NaiveDateTime};

    use super::*;

    const TS_FMT: &str = "%Y-%m-%d %H:%M:%S";

    fn test_span(data: &[(&str, &str)], timestamp: &str, duration: i64) -> Event {
        let (ts, data_map) = common_test_data(data, timestamp);
        Event::Span {
            source: "test".into(),
            timestamp: NaiveDateTime::parse_from_str(&ts, TS_FMT).unwrap(),
            data: data_map,
            duration: Duration::new(duration, 0).unwrap(),
        }
    }

    fn test_single(data: &[(&str, &str)], timestamp: &str) -> Event {
        let (ts, data_map) = common_test_data(data, timestamp);
        Event::Single {
            name: "test".into(),
            timestamp: NaiveDateTime::parse_from_str(&ts, TS_FMT).unwrap(),
            data: data_map,
        }
    }

    fn common_test_data(
        data: &[(&str, &str)],
        timestamp: &str,
    ) -> (String, HashMap<String, String>) {
        let mut data_map = HashMap::from([("timestamp".into(), timestamp.to_string())]);
        for (k, v) in data.iter() {
            data_map.insert(k.to_string(), v.to_string());
        }
        (timestamp.to_owned(), data_map)
    }

    #[test]
    fn test_span_parse() {
        for (start_pattern, end_pattern, log, expected) in [(
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s(?P<ref>[a-z0-9]{5})\s+START",
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s(?P<ref>[a-z0-9]{5})\s+END",
            "2026-01-01 00:00:00 abc01 START\n2026-01-01 00:00:05 abc01 END",
            vec![test_span(&[], "2026-01-01 00:00:00", 5)],
        )] {
            let parser = Parser::Span(InternalSpanParser::new(
                "test".into(),
                TS_FMT.into(),
                Regex::new(start_pattern).unwrap(),
                Regex::new(end_pattern).unwrap(),
                vec![],
                vec!["ref".into()],
            ));
            let actual = parser.parse(log);
            assert_eq!(actual, expected);
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
            let parser = Parser::Single(InternalSingleParser {
                name: "test".into(),
                pattern: Regex::new(pattern).unwrap(),
                timestamp_format: TS_FMT.into(),
            });
            let actual = parser.parse(log);
            assert_eq!(actual, expected);
        }
    }

    const GW_EXAMPLE: &str = include_str!("../../../gateway_example.log");
    const GW_CONFIG: &str = include_str!("../../../gateway_config.toml");

    fn create_gateway_parsers() -> Vec<Parser> {
        let table: toml::Table = toml::from_str(GW_CONFIG).expect("failed to read toml to str");
        let parsers = table.get("parsers").unwrap().as_array().unwrap();
        parsers
            .iter()
            .map(|p| Parser::from_toml(p.as_table().unwrap()).unwrap())
            .collect()
    }

    #[test]
    fn parse_gateway_log() {
        let parsers = create_gateway_parsers();
        // let events = parsers[0].parse(GW_EXAMPLE);
        // let expected = vec![];
        // assert_eq!(events, expected);
        let events = parsers[1].parse(GW_EXAMPLE);
        let expected = 50;
        assert_eq!(events.len(), expected);
    }

    #[test]
    fn test_create_gateway_parsers() {
        let parsers = create_gateway_parsers();
        let parser = parsers[0].clone();
        let Parser::Span(req) = &parser else {
            panic!("expected gateway_request to be a Span parser");
        };
        assert_eq!(req.name, "gateway_request");
        assert_eq!(req.timestamp_format, "%Y-%m-%d %H:%M:%S");
        assert_eq!(
            req.start_pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}\d{2})\s+(?P<requestreference>\S+)\sInHeads:(?P<headers>\{[^\}]*\})\s+Apache:(?P<apachereference>\S+)"
        );
        assert_eq!(
            req.end_pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}\d{2})\s+(?P<requestreference>\S+)\sReturn (?P<response_bytes>\d+)bytes to client\s(?P<username>\S+)\s+(?P<time_taken>[0-9\.]+)s"
        );
        assert_eq!(req.reference_fields, &["requestreference"]);
        assert_eq!(req.nested.len(), 1);

        let Parser::Span(txn) = &req.nested[0] else {
            panic!("expected gateway_transaction to be a Span parser");
        };
        assert_eq!(txn.name, "gateway_transaction");
        assert_eq!(txn.timestamp_format, "%Y-%m-%d %H:%M:%S");
        assert_eq!(
            txn.start_pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<requestreference>\S+):(?P<transactionreference>\S+)\s+Bgn:\s+(?P<interface>\S+)\s+(?P<requesttypedescription>\S+)\s+(?P<accounttypedescription>\S+)\s+(?P<sitereference>\S+)\s+(?P<paymenttypedescription>\S+)\s+(?P<currencyiso3a>\S+)\s+(?P<mainamount>\S+)\s+Status:(?P<status>\S+)"
        );
        assert_eq!(
            txn.end_pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<requestreference>\S+):(?P<transactionreference>\S+)\s+End:\s+(?P<interface>\S+)\s+(?P<requesttypedescription>\S+)\s+(?P<accounttypedescription>\S+)\s+(?P<sitereference>\S+)\s+(?P<paymenttypedescription>\S+)\s+(?P<currencyiso3a>\S+)\s+(?P<mainamount>\S+)\s+Status:(?P<status>\S+)\s+E:(?P<errorcode>\S+)"
        );
        assert_eq!(txn.reference_fields, &["requestreference", "transactionreference"]);
        assert_eq!(txn.nested.len(), 2);

        let Parser::Single(txn_req) = &txn.nested[0] else {
            panic!("expected gateway_transaction_request to be a Single parser");
        };
        assert_eq!(txn_req.name, "gateway_transaction_request");
        assert_eq!(txn_req.timestamp_format, "%Y-%m-%d %H:%M:%S");
        assert_eq!(
            txn_req.pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<requestreference>\S+):(?P<transactionreference>\S+)\s+REQ:(?P<encrypted_request>\S+)"
        );

        let Parser::Single(txn_res) = &txn.nested[1] else {
            panic!("expected gateway_transaction_response to be a Single parser");
        };
        assert_eq!(txn_res.name, "gateway_transaction_response");
        assert_eq!(txn_res.timestamp_format, "%Y-%m-%d %H:%M:%S");
        assert_eq!(
            txn_res.pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<requestreference>\S+):(?P<transactionreference>\S+)\s+RES:(?P<encrypted_response>\S+)"
        );
    }
}
