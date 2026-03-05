use std::collections::HashMap;

use regex::Regex;

use crate::{
    error::{LogParserError, LogParserResult},
    event::Event,
};

#[derive(Debug, Clone)]
pub enum Parser {
    Single {
        name: String,
        pattern: Regex,
        timestamp_format: String,
    },
    Span {
        name: String,
        timestamp_format: String,
        start_pattern: Regex,
        end_pattern: Regex,
        nested: Vec<Parser>,
        reference_fields: Vec<String>,
        // pending: HashMap<SpanReference, PendingSpan>
    },
}

impl Parser {
    pub fn name(&self) -> &str {
        match self {
            Parser::Single { name, .. } | Parser::Span { name, .. } => name,
        }
    }

    pub fn timestamp_format(&self) -> &str {
        match self {
            Parser::Single {
                timestamp_format, ..
            }
            | Parser::Span {
                timestamp_format, ..
            } => timestamp_format,
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
            regex::Regex::new(Self::parse_and_validate_str("start_pattern", t)?.into())?;
        let end_pattern =
            regex::Regex::new(Self::parse_and_validate_str("end_pattern", t)?.into())?;
        Self::validate_required_pattern_fields(&start_pattern, &Self::REQUIRED_FIELDS)?;
        Self::validate_required_pattern_fields(&end_pattern, &Self::REQUIRED_FIELDS)?;
        Ok(Parser::Span {
            name: name.into(),
            start_pattern,
            end_pattern,
            timestamp_format: timestamp_format.into(),
            reference_fields,
            nested: nested_parsers,
        })
    }

    fn validate_required_pattern_fields(
        pattern: &regex::Regex,
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
        Ok(Parser::Single {
            name: name.into(),
            pattern,
            timestamp_format: timestamp_format.into(),
        })
    }

    pub fn parse(&self, input: &str) -> Vec<Event> {
        match self {
            Parser::Single {
                name,
                pattern,
                timestamp_format,
            } => {
                let mut events = vec![];
                for line in input.lines() {
                    if let Some(captures) = pattern.captures(line) {
                        let Ok(timestamp) = chrono::NaiveDateTime::parse_from_str(
                            &captures["timestamp"],
                            &timestamp_format,
                        ) else {
                            continue;
                        };
                        let mut data = HashMap::new();
                        for field in pattern.capture_names() {
                            if let Some(field) = field
                                && let Some(value) = captures.name(field)
                            {
                                data.insert(field.to_owned(), value.as_str().to_owned());
                            }
                        }
                        events.push(Event::Single {
                            name: name.to_owned(),
                            timestamp,
                            data,
                        });
                    }
                }
                events
            }
            Parser::Span {
                name: _,
                timestamp_format: _,
                start_pattern: _,
                end_pattern: _,
                nested: _,
                reference_fields: _,
            } => {
                todo!()
            }
        }
    }
}

fn error(msg: &str) -> LogParserError {
    LogParserError::ConfigParseError(msg.to_string())
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDateTime;

    use super::*;

    const TS_FMT: &str = "%Y-%m-%d %H:%M:%S";

    #[test]
    fn test_single_parse() {
        for (pattern, log, expected) in [
            (
                r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",
                "2026-01-01 12:34:56",
                vec![Event::Single {
                    name: "test".into(),
                    timestamp: NaiveDateTime::parse_from_str("2026-01-01 12:34:56", TS_FMT)
                        .unwrap(),
                    data: HashMap::from([("timestamp".into(), "2026-01-01 12:34:56".into())]),
                }],
            ),
            (
                r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (?P<level>\w+) (?P<message>.+)",
                "2026-03-05 08:00:00 INFO Server started",
                vec![Event::Single {
                    name: "test".into(),
                    timestamp: NaiveDateTime::parse_from_str("2026-03-05 08:00:00", TS_FMT)
                        .unwrap(),
                    data: HashMap::from([
                        ("timestamp".into(), "2026-03-05 08:00:00".into()),
                        ("level".into(), "INFO".into()),
                        ("message".into(), "Server started".into()),
                    ]),
                }],
            ),
            (
                r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (?P<user>\S+) (?P<action>\S+)",
                "not a log line\n2026-06-15 09:30:00 alice LOGIN\n2026-06-15 09:30:02 steve LOGIN\nskipped line",
                vec![
                    Event::Single {
                        name: "test".into(),
                        timestamp: NaiveDateTime::parse_from_str("2026-06-15 09:30:00", TS_FMT)
                            .unwrap(),
                        data: HashMap::from([
                            ("timestamp".into(), "2026-06-15 09:30:00".into()),
                            ("user".into(), "alice".into()),
                            ("action".into(), "LOGIN".into()),
                        ]),
                    },
                    Event::Single {
                        name: "test".into(),
                        timestamp: NaiveDateTime::parse_from_str("2026-06-15 09:30:02", TS_FMT)
                            .unwrap(),
                        data: HashMap::from([
                            ("timestamp".into(), "2026-06-15 09:30:02".into()),
                            ("user".into(), "steve".into()),
                            ("action".into(), "LOGIN".into()),
                        ]),
                    },
                ],
            ),
            (
                r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",
                "",
                vec![],
            ),
        ] {
            let parser = Parser::Single {
                name: "test".into(),
                pattern: Regex::new(pattern).unwrap(),
                timestamp_format: TS_FMT.into(),
            };
            let actual = parser.parse(log);
            assert_eq!(actual, expected);
        }
    }

    const GW_EXAMPLE: &str = include_str!("../../gateway_example.log");
    const GW_CONFIG: &str = include_str!("../../gateway_config.toml");

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
        let Parser::Span {
            name,
            timestamp_format,
            start_pattern,
            end_pattern,
            nested,
            reference_fields,
        } = &parser
        else {
            panic!("expected gateway_request to be a Span parser");
        };
        assert_eq!(name, "gateway_request");
        assert_eq!(timestamp_format, "%Y-%m-%d %H:%M:%S");
        assert_eq!(
            start_pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}\d{2})\s+(?P<requestreference>\S+)\sInHeads:(?P<headers>\{[^\}]*\})\s+Apache:(?P<apachereference>\S+)"
        );
        assert_eq!(
            end_pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}\d{2})\s+(?P<requestreference>\S+)\sReturn (?P<response_bytes>\d+)bytes to client\s(?P<username>\S+)\s+(?P<time_taken>[0-9\.]+)s"
        );
        assert_eq!(reference_fields, &["requestreference"]);
        assert_eq!(nested.len(), 1);

        let Parser::Span {
            name,
            timestamp_format,
            start_pattern,
            end_pattern,
            nested,
            reference_fields,
        } = &nested[0]
        else {
            panic!("expected gateway_transaction to be a Span parser");
        };
        assert_eq!(name, "gateway_transaction");
        assert_eq!(timestamp_format, "%Y-%m-%d %H:%M:%S");
        assert_eq!(
            start_pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<requestreference>\S+):(?P<transactionreference>\S+)\s+Bgn:\s+(?P<interface>\S+)\s+(?P<requesttypedescription>\S+)\s+(?P<accounttypedescription>\S+)\s+(?P<sitereference>\S+)\s+(?P<paymenttypedescription>\S+)\s+(?P<currencyiso3a>\S+)\s+(?P<mainamount>\S+)\s+Status:(?P<status>\S+)"
        );
        assert_eq!(
            end_pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<requestreference>\S+):(?P<transactionreference>\S+)\s+End:\s+(?P<interface>\S+)\s+(?P<requesttypedescription>\S+)\s+(?P<accounttypedescription>\S+)\s+(?P<sitereference>\S+)\s+(?P<paymenttypedescription>\S+)\s+(?P<currencyiso3a>\S+)\s+(?P<mainamount>\S+)\s+Status:(?P<status>\S+)\s+E:(?P<errorcode>\S+)"
        );
        assert_eq!(
            reference_fields,
            &["requestreference", "transactionreference"]
        );
        assert_eq!(nested.len(), 2);

        let Parser::Single {
            name,
            timestamp_format,
            pattern,
        } = &nested[0]
        else {
            panic!("expected gateway_transaction_request to be a Single parser");
        };
        assert_eq!(name, "gateway_transaction_request");
        assert_eq!(timestamp_format, "%Y-%m-%d %H:%M:%S");
        assert_eq!(
            pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<requestreference>\S+):(?P<transactionreference>\S+)\s+REQ:(?P<encrypted_request>\S+)"
        );

        let Parser::Single {
            name,
            timestamp_format,
            pattern,
        } = &nested[1]
        else {
            panic!("expected gateway_transaction_response to be a Single parser");
        };
        assert_eq!(name, "gateway_transaction_response");
        assert_eq!(timestamp_format, "%Y-%m-%d %H:%M:%S");
        assert_eq!(
            pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<requestreference>\S+):(?P<transactionreference>\S+)\s+RES:(?P<encrypted_response>\S+)"
        );
    }
}
