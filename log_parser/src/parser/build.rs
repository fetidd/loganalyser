use std::collections::HashMap;

use regex::Regex;
use serde::Deserialize;

use crate::error::{Error, Result};

use super::{InternalSingleParser, InternalSpanParser, Parser};

struct ParserBuildContext<'a> {
    timestamp_format: &'a str,
    reference_fields: Option<&'a [String]>,
    parser_type: &'a str,
    config: &'a Config,
}

fn get_str<'a>(table: &'a toml::Table, key: &str) -> Result<&'a str> {
    table
        .get(key)
        .ok_or_else(|| error("missing '{key}'"))?
        .as_str()
        .ok_or_else(|| error("'{key}' should be a string"))
}

#[derive(Debug, Deserialize, Default)]
struct Config {
    #[serde(default)]
    parsers: Vec<toml::Value>,
    #[serde(default)]
    components: HashMap<String, String>,
    #[serde(default)]
    defaults: ParserDefaults,
}

#[derive(Debug, Deserialize, Default)]
struct ParserDefaults {
    timestamp_format: Option<String>,
}

impl Parser {
    pub fn from_config_file(config_file: &Vec<u8>) -> Result<HashMap<String, Vec<Parser>>> {
        let config: Config = toml::from_slice(config_file)?;
        tracing::debug!("created parser config: {config:?}");
        let mut parsers: HashMap<String, Vec<Parser>> = HashMap::new();
        for p_table in config.parsers.iter().map(|v| {
            v.as_table()
                .ok_or_else(|| error("parsers should be tables"))
        }) {
            let p_table = p_table?;
            let parser = Parser::build_from_toml_and_config(p_table, &config)?;
            let pattern = get_str(p_table, "glob")?;
            parsers.entry(pattern.to_string()).or_default().push(parser);
        }
        Ok(parsers)
    }

    fn extract_config_type(t: &toml::Table) -> Result<&str> {
        t.get("type")
            .ok_or(error("missing type"))?
            .as_str()
            .ok_or(error("type was not a string"))
    }

    fn build_from_toml_and_config(t: &toml::Table, config: &Config) -> Result<Parser> {
        let config_type = Self::extract_config_type(t)?;
        let reference_fields: Option<Vec<String>> =
            if let Some(reference_fields) = t.get("reference_fields") {
                reference_fields
                    .as_array()
                    .ok_or(error("reference_fields not an array"))?
                    .into_iter()
                    .map(|v| match v.as_str() {
                        Some(s) => Ok(s.to_owned()),
                        None => Err(error("reference_fields elements must be strings")),
                    })
                    .collect::<Result<Vec<String>>>()?
                    .into()
            } else if config_type == "span" {
                return Err(error("reference_fields must be provided for span parsers"));
            } else {
                None
            };
        let timestamp_format = if let Some(default_tsf) = &config.defaults.timestamp_format {
            default_tsf
        } else {
            Self::parse_and_validate_str("timestamp_format", t)?
        };
        let ctx = ParserBuildContext {
            timestamp_format,
            reference_fields: reference_fields.as_deref(),
            parser_type: config_type,
            config,
        };
        Self::from_toml_table_and_context(t, &ctx)
    }

    fn from_toml_table_and_context(
        t: &toml::Table,
        ctx: &ParserBuildContext<'_>,
    ) -> Result<Parser> {
        let name = Self::parse_and_validate_str("name", t)?;
        match ctx.parser_type {
            "span" => Self::build_span(t, name, ctx),
            "single" => Self::build_single(t, name, ctx),
            _ => todo!(),
        }
    }

    fn parse_and_validate_str<'a>(field: &str, t: &'a toml::Table) -> Result<&'a str> {
        Ok(t.get(field)
            .ok_or(error(&format!("missing {field}")))?
            .as_str()
            .ok_or(error(&format!("{field} was not a string")))?)
    }

    const REQUIRED_FIELDS: [&str; 1] = ["timestamp"];

    fn build_single(t: &toml::Table, name: &str, ctx: &ParserBuildContext<'_>) -> Result<Parser> {
        let pattern = Self::parse_pattern("", &ctx.config.components, t)?;
        Self::validate_required_pattern_fields(&pattern, Self::REQUIRED_FIELDS)?;
        if let Some(reference_fields) = ctx.reference_fields {
            Self::validate_required_pattern_fields(&pattern, reference_fields)?;
        }
        Ok(Parser::Single(InternalSingleParser {
            name: name.into(),
            pattern,
            timestamp_format: ctx.timestamp_format.into(),
        }))
    }

    fn parse_pattern(
        prefix: &str,
        components: &HashMap<String, String>,
        t: &toml::Table,
    ) -> Result<Regex> {
        // TODO unittest!
        let field = if !prefix.is_empty() {
            format!("{prefix}_pattern")
        } else {
            "pattern".to_string()
        };
        let mut pattern = Self::parse_and_validate_str(&field, t)?.to_string();
        for (var, replacement) in components {
            let var = format!("{{{{{var}}}}}");
            let mut searched_to = 0_usize;
            while searched_to < pattern.len() {
                if let Some(mut i) = pattern[searched_to..].find(&var) {
                    i += searched_to;
                    pattern.replace_range(i..i + var.len(), replacement);
                    searched_to = i + replacement.len();
                } else {
                    break;
                }
            }
        }
        Ok(Regex::new(&pattern)?)
    }

    fn build_span(t: &toml::Table, name: &str, ctx: &ParserBuildContext<'_>) -> Result<Parser> {
        let nested_parsers = Self::parse_nested(t, ctx)?;
        let start_pattern = Self::parse_pattern("start", &ctx.config.components, t)?;
        let end_pattern = Self::parse_pattern("end", &ctx.config.components, t)?;
        if start_pattern.as_str() == end_pattern.as_str() {
            return Err(error("start_pattern and end_pattern must be different"));
        }
        for pattern in [&start_pattern, &end_pattern] {
            Self::validate_required_pattern_fields(pattern, Self::REQUIRED_FIELDS)?;
            if let Some(reference_fields) = ctx.reference_fields {
                Self::validate_required_pattern_fields(pattern, reference_fields)?;
            }
        }
        Ok(Parser::Span(InternalSpanParser::new(
            name.into(),
            ctx.timestamp_format.into(),
            start_pattern,
            end_pattern,
            nested_parsers,
            ctx.reference_fields.map(|r| r.to_vec()).unwrap_or_default(),
        )))
    }

    fn parse_nested(t: &toml::Table, ctx: &ParserBuildContext<'_>) -> Result<Vec<Parser>> {
        let Some(nested) = t.get("nested") else {
            return Ok(vec![]);
        };
        let nested = nested
            .as_array()
            .ok_or(error("nested should be an array"))?;
        nested
            .iter()
            .map(|value| {
                let table = value
                    .as_table()
                    .ok_or(error("nested elements must be toml tables"))?;
                let nested_ts = match table.get("timestamp_format") {
                    Some(found) => found
                        .as_str()
                        .ok_or(error("timestamp_format was not a string"))?,
                    None => ctx.timestamp_format,
                };
                let config_type = Self::extract_config_type(table)?;
                let mut reference_fields: Vec<String> =
                    ctx.reference_fields.map(|r| r.to_vec()).unwrap_or_default();
                if let Some(nested_ref_fields) = table.get("reference_fields") {
                    let own_fields: Vec<String> = nested_ref_fields
                        .as_array()
                        .ok_or(error("reference_fields must be an array"))?
                        .iter()
                        .map(|v| {
                            v.as_str()
                                .ok_or(error("reference_fields must be strings"))
                                .map(|s| s.to_owned())
                        })
                        .collect::<Result<_>>()?;
                    if config_type == "span" {
                        for field in &own_fields {
                            if reference_fields.contains(field) {
                                return Err(error(&format!(
                                    "nested span reference field '{field}' duplicates an inherited field"
                                )));
                            }
                        }
                    }
                    reference_fields.extend(own_fields);
                } else if config_type == "span" {
                    return Err(error(
                        "nested span parsers must provide reference_fields to disambiguate from parent",
                    ));
                };
                let nested_ctx = ParserBuildContext {
                    timestamp_format: nested_ts,
                    reference_fields: Some(&reference_fields),
                    parser_type: config_type,
                    config: ctx.config
                };
                Self::from_toml_table_and_context(table, &nested_ctx)
            })
            .collect()
    }

    fn validate_required_pattern_fields(
        pattern: &Regex,
        fields: impl IntoIterator<Item: AsRef<str>>,
    ) -> Result<()> {
        let mut missing = vec![];
        for f in fields {
            if !pattern
                .capture_names()
                .any(|c| c.is_some_and(|c| c == f.as_ref()))
            {
                missing.push(f.as_ref().to_owned());
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
}

fn error(msg: &str) -> Error {
    Error::ConfigParse(msg.to_string())
}

#[cfg(test)]
mod tests {
    use regex::Regex;
    use rstest::rstest;

    use super::*;

    fn build(toml_str: &str) -> Result<Parser> {
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        Parser::build_from_toml_and_config(&t, &Config::default())
    }

    const SINGLE_VALID: &str = r#"
type = "single"
name = "my_parser"
timestamp_format = "%Y-%m-%d %H:%M:%S"
pattern = '(?P<timestamp>\d+) (?P<level>\w+)'
"#;

    const SPAN_VALID: &str = r#"
type = "span"
name = "my_span"
timestamp_format = "%Y-%m-%d %H:%M:%S"
start_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) END'
reference_fields = ["ref"]
"#;

    const SPAN_EMPTY_NESTED: &str = r#"
type = "span"
name = "my_span"
timestamp_format = "%Y-%m-%d %H:%M:%S"
start_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = []
"#;

    const SPAN_ONE_NESTED: &str = r#"
type = "span"
name = "my_span"
timestamp_format = "%Y-%m-%d %H:%M:%S"
start_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "single", name = "n", pattern = '(?P<timestamp>.+) (?P<ref>\S+)' }]
"#;

    const SPAN_TWO_NESTED: &str = r#"
type = "span"
name = "my_span"
timestamp_format = "%Y-%m-%d %H:%M:%S"
start_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [
  { type = "single", name = "n1", pattern = '(?P<timestamp>.+) (?P<ref>\S+)' },
  { type = "single", name = "n2", pattern = '(?P<timestamp>.+) (?P<ref>\S+)' }
]
"#;

    const SPAN_NESTED_INHERITS_TS_FMT: &str = r#"
type = "span"
name = "my_span"
timestamp_format = "%Y-%m-%d %H:%M:%S"
start_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "single", name = "n", pattern = '(?P<timestamp>.+) (?P<ref>\S+)' }]
"#;

    const SPAN_NESTED_OVERRIDES_TS_FMT: &str = r#"
type = "span"
name = "my_span"
timestamp_format = "%Y-%m-%d %H:%M:%S"
start_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "single", name = "n", pattern = '(?P<timestamp>.+) (?P<ref>\S+)', timestamp_format = "%Y" }]
"#;

    #[rstest]
    #[case(r#"name = "foo""#, "name", "foo")]
    #[case(r#"name = """#, "name", "")]
    fn test_parse_and_validate_str_ok(
        #[case] toml_str: &str,
        #[case] field: &str,
        #[case] expected: &str,
    ) {
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        assert_eq!(Parser::parse_and_validate_str(field, &t).unwrap(), expected);
    }

    #[rstest]
    #[case(r#"other = "x""#, "name", "missing name")]
    #[case(r#"name = 42"#, "name", "name was not a string")]
    #[case(r#"name = ["a", "b"]"#, "name", "name was not a string")]
    #[case(r#"name = true"#, "name", "name was not a string")]
    fn test_parse_and_validate_str_err(
        #[case] toml_str: &str,
        #[case] field: &str,
        #[case] expected_err: &str,
    ) {
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let err = Parser::parse_and_validate_str(field, &t).unwrap_err();
        assert!(
            err.to_string().contains(expected_err),
            "expected error containing {expected_err:?}, got: {}",
            err.to_string()
        );
    }

    #[rstest]
    #[case(r#"type = "span""#, "span")]
    #[case(r#"type = "single""#, "single")]
    fn test_extract_config_type_ok(#[case] toml_str: &str, #[case] expected: &str) {
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        assert_eq!(Parser::extract_config_type(&t).unwrap(), expected);
    }

    #[rstest]
    #[case(r#"other = "x""#, "missing type")]
    #[case(r#"type = 42"#, "type was not a string")]
    #[case(r#"type = true"#, "type was not a string")]
    fn test_extract_config_type_err(#[case] toml_str: &str, #[case] expected_err: &str) {
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let err = Parser::extract_config_type(&t).unwrap_err();
        assert!(
            err.to_string().contains(expected_err),
            "expected error containing {expected_err:?}, got: {}",
            err.to_string()
        );
    }

    #[rstest]
    #[case(r"(?P<timestamp>\d+) (?P<level>\w+)", vec!["timestamp", "level"])]
    #[case(r"(?P<timestamp>\d+)", vec!["timestamp"])]
    #[case(r"(?P<timestamp>\d+)", vec![])]
    #[case(r"(?P<timestamp>\d+) (?P<extra>.+)", vec!["timestamp"])] // extra fields in pattern are fine
    fn test_validate_required_pattern_fields_ok(#[case] pattern: &str, #[case] fields: Vec<&str>) {
        let re = Regex::new(pattern).unwrap();
        assert!(Parser::validate_required_pattern_fields(&re, fields).is_ok());
    }

    #[rstest]
    #[case(r"(?P<level>\w+)", vec!["timestamp"], "timestamp")]
    #[case(r"(?P<other>\w+)", vec!["timestamp", "level"], "timestamp")] // both missing, error mentions first
    #[case(r"(\d+)(\w+)", vec!["timestamp"], "timestamp")] // only unnamed groups
    fn test_validate_required_pattern_fields_err(
        #[case] pattern: &str,
        #[case] fields: Vec<&str>,
        #[case] expected_in_err: &str,
    ) {
        let re = Regex::new(pattern).unwrap();
        let err = Parser::validate_required_pattern_fields(&re, fields).unwrap_err();
        assert!(
            err.to_string().contains(expected_in_err),
            "expected error containing {expected_in_err:?}, got: {}",
            err.to_string()
        );
    }

    #[test]
    fn test_from_toml_single_valid() {
        let parser = build(SINGLE_VALID).unwrap();
        let Parser::Single(p) = parser else {
            panic!("expected Single")
        };
        assert_eq!(p.name, "my_parser");
        assert_eq!(p.timestamp_format, "%Y-%m-%d %H:%M:%S");
        assert!(p.pattern.capture_names().any(|c| c == Some("timestamp")));
        assert!(p.pattern.capture_names().any(|c| c == Some("level")));
    }

    #[rstest]
    #[case(
        r#"name = "t"
timestamp_format = "%Y"
pattern = '(?P<timestamp>.+)'"#,
        "missing type"
    )]
    #[case(
        r#"type = "single"
timestamp_format = "%Y"
pattern = '(?P<timestamp>.+)'"#,
        "missing name"
    )]
    #[case(
        r#"type = "single"
name = "t"
pattern = '(?P<timestamp>.+)'"#,
        "missing timestamp_format"
    )]
    #[case(
        r#"type = "single"
name = "t"
timestamp_format = "%Y""#,
        "missing pattern"
    )]
    #[case(
        r#"type = "single"
name = "t"
timestamp_format = "%Y"
pattern = '[invalid'"#,
        "bad regex"
    )]
    #[case(
        r#"type = "single"
name = "t"
timestamp_format = "%Y"
pattern = '(?P<level>\w+)'"#,
        "timestamp"
    )] // pattern missing timestamp capture group
    fn test_from_toml_single_err(#[case] toml_str: &str, #[case] expected_err: &str) {
        let err = build(toml_str).unwrap_err();
        assert!(
            err.to_string().contains(expected_err),
            "expected error containing {expected_err:?}, got: {}",
            err.to_string()
        );
    }

    #[test]
    fn test_from_toml_span_valid() {
        let parser = build(SPAN_VALID).unwrap();
        let Parser::Span(p) = parser else {
            panic!("expected Span")
        };
        assert_eq!(p.name, "my_span");
        assert_eq!(p.timestamp_format, "%Y-%m-%d %H:%M:%S");
        assert_eq!(p.reference_fields, &["ref"]);
        assert_eq!(p.nested.len(), 0);
    }

    #[rstest]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'"#,
        "reference_fields must be provided for span parsers"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = "ref""#,
        "reference_fields not an array"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = [1, 2]"#,
        "reference_fields elements must be strings"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = ["ref"]"#,
        "missing start_pattern"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
reference_fields = ["ref"]"#,
        "missing end_pattern"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '[invalid'
end_pattern = '(?P<timestamp>.+) END'
reference_fields = []"#,
        "bad regex"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<ref>\S+) START'
end_pattern = '(?P<ref>\S+) END'
reference_fields = ["ref"]"#,
        "timestamp"
    )] // patterns missing timestamp group
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) START'
end_pattern = '(?P<timestamp>.+) END'
reference_fields = ["ref"]"#,
        "ref"
    )] // reference field not in patterns
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) SAME'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) SAME'
reference_fields = ["ref"]"#,
        "start_pattern and end_pattern must be different"
    )] // identical start and end patterns are ambiguous
    fn test_from_toml_span_err(#[case] toml_str: &str, #[case] expected_err: &str) {
        let err = build(toml_str).unwrap_err();
        assert!(
            err.to_string().contains(expected_err),
            "expected error containing {expected_err:?}, got: {}",
            err.to_string()
        );
    }

    #[rstest]
    #[case(SPAN_VALID, 0)] // no nested key
    #[case(SPAN_EMPTY_NESTED, 0)] // nested = []
    #[case(SPAN_ONE_NESTED, 1)] // one nested single
    #[case(SPAN_TWO_NESTED, 2)] // two nested singles
    fn test_parse_nested_count(#[case] toml_str: &str, #[case] expected_count: usize) {
        let Parser::Span(p) = build(toml_str).unwrap() else {
            panic!("expected Span");
        };
        assert_eq!(p.nested.len(), expected_count);
    }

    #[rstest]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) START'
end_pattern = '(?P<timestamp>.+) END'
reference_fields = []
nested = "not an array""#,
        "nested should be an array"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) START'
end_pattern = '(?P<timestamp>.+) END'
reference_fields = []
nested = ["not a table"]"#,
        "nested elements must be toml tables"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) START'
end_pattern = '(?P<timestamp>.+) END'
reference_fields = []
nested = [{ name = "n", pattern = '(?P<timestamp>.+)' }]"#,
        "missing type"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) START'
end_pattern = '(?P<timestamp>.+) END'
reference_fields = []
nested = [{ type = "single", name = "n", pattern = '(?P<timestamp>.+)', timestamp_format = 42 }]"#,
        "timestamp_format was not a string"
    )]
    fn test_parse_nested_err(#[case] toml_str: &str, #[case] expected_err: &str) {
        let err = build(toml_str).unwrap_err();
        assert!(
            err.to_string().contains(expected_err),
            "expected error containing {expected_err:?}, got: {}",
            err.to_string()
        );
    }

    #[test]
    fn test_nested_inherits_parent_timestamp_format() {
        let Parser::Span(p) = build(SPAN_NESTED_INHERITS_TS_FMT).unwrap() else {
            panic!()
        };
        let Parser::Single(nested) = &p.nested[0] else {
            panic!()
        };
        assert_eq!(nested.timestamp_format, "%Y-%m-%d %H:%M:%S");
    }

    #[test]
    fn test_nested_overrides_timestamp_format() {
        let Parser::Span(p) = build(SPAN_NESTED_OVERRIDES_TS_FMT).unwrap() else {
            panic!()
        };
        let Parser::Single(nested) = &p.nested[0] else {
            panic!()
        };
        assert_eq!(nested.timestamp_format, "%Y");
    }

    #[test]
    fn test_top_level_span_requires_reference_fields() {
        let toml_str = r#"
type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) START'
end_pattern = '(?P<timestamp>.+) END'
"#;
        let err = build(toml_str).unwrap_err();
        assert!(
            err.to_string()
                .contains("reference_fields must be provided for span parsers"),
            "got: {err}"
        );
    }

    #[test]
    fn test_nested_single_inherits_parent_reference_fields() {
        // parent has reference_fields = ["ref"]; nested single pattern includes (?P<ref>...)
        let toml_str = r#"
type = "span"
name = "outer"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "single", name = "inner", pattern = '(?P<timestamp>.+) (?P<ref>\S+)' }]
"#;
        assert!(build(toml_str).is_ok());
    }

    #[test]
    fn test_nested_single_err_if_pattern_missing_parent_reference_field() {
        // parent has reference_fields = ["ref"]; nested single pattern has no (?P<ref>...)
        let toml_str = r#"
type = "span"
name = "outer"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "single", name = "inner", pattern = '(?P<timestamp>.+)' }]
"#;
        let err = build(toml_str).unwrap_err();
        assert!(
            err.to_string().contains("ref"),
            "expected error mentioning 'ref', got: {err}"
        );
    }

    #[test]
    fn test_nested_span_inherits_parent_and_adds_own_reference_fields() {
        // inner span patterns contain both the inherited "ref" and its own "sub"
        let toml_str = r#"
type = "span"
name = "outer"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "span", name = "inner", start_pattern = '(?P<timestamp>.+) (?P<ref>\S+):(?P<sub>\S+) START', end_pattern = '(?P<timestamp>.+) (?P<ref>\S+):(?P<sub>\S+) END', reference_fields = ["sub"] }]
"#;
        let Parser::Span(outer) = build(toml_str).unwrap() else {
            panic!("expected Span")
        };
        let Parser::Span(inner) = &outer.nested[0] else {
            panic!("expected nested Span")
        };
        // combined: parent's ["ref"] prepended to own ["sub"]
        assert_eq!(inner.reference_fields, &["ref", "sub"]);
    }

    #[test]
    fn test_nested_span_err_if_no_own_reference_fields() {
        // nested span omits reference_fields entirely — must provide at least one to disambiguate
        let toml_str = r#"
type = "span"
name = "outer"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "span", name = "inner", start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START', end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END' }]
"#;
        let err = build(toml_str).unwrap_err();
        assert!(
            err.to_string().contains(
                "nested span parsers must provide reference_fields to disambiguate from parent"
            ),
            "got: {err}"
        );
    }

    #[test]
    fn test_nested_span_err_if_own_reference_field_duplicates_inherited() {
        // nested span lists "ref" in its own reference_fields, but "ref" is already inherited
        let toml_str = r#"
type = "span"
name = "outer"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "span", name = "inner", start_pattern = '(?P<timestamp>.+) (?P<ref>\S+):(?P<sub>\S+) START', end_pattern = '(?P<timestamp>.+) (?P<ref>\S+):(?P<sub>\S+) END', reference_fields = ["ref", "sub"] }]
"#;
        let err = build(toml_str).unwrap_err();
        assert!(
            err.to_string().contains("duplicates an inherited field"),
            "got: {err}"
        );
    }

    #[test]
    fn test_nested_span_err_if_patterns_missing_inherited_reference_field() {
        // inner span patterns contain its own "sub" but NOT the inherited "ref"
        let toml_str = r#"
type = "span"
name = "outer"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "span", name = "inner", start_pattern = '(?P<timestamp>.+) (?P<sub>\S+) START', end_pattern = '(?P<timestamp>.+) (?P<sub>\S+) END', reference_fields = ["sub"] }]
"#;
        let err = build(toml_str).unwrap_err();
        assert!(
            err.to_string().contains("ref"),
            "expected error mentioning 'ref', got: {err}"
        );
    }

    #[rstest]
    #[case(SINGLE_VALID, "my_parser", "%Y-%m-%d %H:%M:%S")]
    #[case(SPAN_VALID, "my_span", "%Y-%m-%d %H:%M:%S")]
    fn test_parser_name_and_timestamp_format(
        #[case] toml_str: &str,
        #[case] expected_name: &str,
        #[case] expected_ts_fmt: &str,
    ) {
        let parser = build(toml_str).unwrap();
        assert_eq!(parser.name(), expected_name);
        assert_eq!(parser.timestamp_format(), expected_ts_fmt);
    }
}
