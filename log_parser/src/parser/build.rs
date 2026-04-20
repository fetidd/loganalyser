use std::{cell::LazyCell, collections::HashMap, path::PathBuf};

use regex::Regex;
use serde::Deserialize;

use crate::error::{Error, Result};

use super::{InternalSingleParser, InternalSpanParser, Parser};

#[derive(Debug, Deserialize, Default)]
struct RawConfig {
    #[serde(default)]
    parsers: Vec<RawParserConfig>,
    #[serde(default)]
    components: HashMap<String, String>,
    #[serde(default)]
    defaults: ParserDefaults,
}

#[derive(Debug, Deserialize, Default)]
struct ParserDefaults {
    timestamp_format: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum RawParserConfig {
    Single(RawSingleConfig),
    Span(RawSpanConfig),
}

#[derive(Debug, Deserialize)]
struct RawSingleConfig {
    pattern: String,
    #[serde(flatten)]
    common: CommonConfig,
}

#[derive(Debug, Deserialize)]
struct RawSpanConfig {
    start_pattern: String,
    end_pattern: String,
    reference_fields: Vec<String>,
    #[serde(default)]
    nested: Vec<RawParserConfig>,
    #[serde(flatten)]
    common: CommonConfig,
}

#[derive(Debug, Deserialize)]
struct CommonConfig {
    name: String,
    glob: Option<String>,
    timestamp_format: Option<String>,
    #[serde(default = "default_include_raw")]
    include_raw: bool,
}

fn default_include_raw() -> bool {
    true
}

impl RawParserConfig {
    fn glob(&self) -> Option<&str> {
        match self {
            RawParserConfig::Single(c) => c.common.glob.as_deref(),
            RawParserConfig::Span(c) => c.common.glob.as_deref(),
        }
    }

    fn timestamp_format(&self) -> Option<&str> {
        match self {
            RawParserConfig::Single(c) => c.common.timestamp_format.as_deref(),
            RawParserConfig::Span(c) => c.common.timestamp_format.as_deref(),
        }
    }
}

struct BuildCtx<'a> {
    components: &'a HashMap<String, String>,
    timestamp_format: &'a str,
    inherited_ref_fields: &'a [String],
}

impl Parser {
    pub fn from_config_file(config_file: &[u8]) -> Result<HashMap<PathBuf, Vec<Parser>>> {
        let config: RawConfig = toml::from_slice(config_file)?;
        tracing::debug!("created parser config: {config:?}");

        let mut parsers: HashMap<PathBuf, Vec<Parser>> = HashMap::new();
        for raw in &config.parsers {
            let glob_pattern = raw
                .glob()
                .ok_or_else(|| error("top-level parser missing 'glob'"))?;
            let ts_fmt = raw
                .timestamp_format()
                .or(config.defaults.timestamp_format.as_deref())
                .ok_or_else(|| error("missing timestamp_format"))?;
            let ctx = BuildCtx {
                components: &config.components,
                timestamp_format: ts_fmt,
                inherited_ref_fields: &[],
            };
            let parser = build_parser(raw, &ctx)?;
            for entry in glob::glob(glob_pattern).map_err(|e| error(&e.to_string()))? {
                let path = entry.map_err(|e| error(&e.to_string()))?;
                parsers.entry(path).or_default().push(parser.clone());
            }
        }
        Ok(parsers)
    }
}

fn build_parser(raw: &RawParserConfig, ctx: &BuildCtx<'_>) -> Result<Parser> {
    match raw {
        RawParserConfig::Single(c) => build_single(c, ctx),
        RawParserConfig::Span(c) => build_span(c, ctx),
    }
}

fn build_single(c: &RawSingleConfig, ctx: &BuildCtx<'_>) -> Result<Parser> {
    let pattern = compile_pattern(&c.pattern, ctx.components)?;
    validate_required_fields(&pattern, ["timestamp"])?;
    validate_required_fields(&pattern, ctx.inherited_ref_fields)?;
    Ok(Parser::Single(InternalSingleParser {
        name: c.common.name.clone(),
        pattern,
        timestamp_format: ctx.timestamp_format.to_string(),
        include_raw: c.common.include_raw,
    }))
}

fn build_span(c: &RawSpanConfig, ctx: &BuildCtx<'_>) -> Result<Parser> {
    let mut ref_fields: Vec<String> = ctx.inherited_ref_fields.to_vec();
    for field in &c.reference_fields {
        if ref_fields.contains(field) {
            return Err(error(&format!(
                "reference field '{field}' duplicates an inherited field"
            )));
        }
        ref_fields.push(field.clone());
    }
    let start = compile_pattern(&c.start_pattern, ctx.components)?;
    let end = compile_pattern(&c.end_pattern, ctx.components)?;
    if start.as_str() == end.as_str() {
        return Err(error("start_pattern and end_pattern must be different"));
    }
    for pattern in [&start, &end] {
        validate_required_fields(pattern, ["timestamp"])?;
        validate_required_fields(pattern, &ref_fields)?;
    }
    let nested = c
        .nested
        .iter()
        .map(|n| {
            if let RawParserConfig::Span(ns) = n {
                if ns.reference_fields.is_empty() {
                    return Err(error(
                        "nested span parsers must provide reference_fields to disambiguate from parent",
                    ));
                }
            }
            let ts_fmt = n.timestamp_format().unwrap_or(ctx.timestamp_format);
            let nested_ctx = BuildCtx {
                components: ctx.components,
                timestamp_format: ts_fmt,
                inherited_ref_fields: &ref_fields,
            };
            build_parser(n, &nested_ctx)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Parser::Span(InternalSpanParser::new(
        c.common.name.clone(),
        ctx.timestamp_format.to_string(),
        start,
        end,
        nested,
        ref_fields,
        c.common.include_raw,
    )))
}

fn compile_pattern(pattern: &str, components: &HashMap<String, String>) -> Result<Regex> {
    let mut raw = pattern.to_string();
    replace_whitespace_with_regex(&mut raw);
    let expanded = expand_components(&raw, components)?;
    Ok(Regex::new(&expanded)?)
}

/// Expand `${name}` placeholders, wrapping each substitution in `(?:...)`.
/// Components are pure pattern fragments; named fields must be declared
/// explicitly with `(?P<name>...)` in the pattern itself.
fn expand_components(pattern: &str, components: &HashMap<String, String>) -> Result<String> {
    let mut result = String::with_capacity(pattern.len() * 2);
    let bytes = pattern.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    while i < len {
        if bytes[i] == b'\\' {
            result.push(bytes[i] as char);
            i += 1;
            if i < len {
                result.push(bytes[i] as char);
                i += 1;
            }
            continue;
        }
        if bytes[i] == b'$' && i + 1 < len && bytes[i + 1] == b'{' {
            let name_start = i + 2;
            let close = pattern[name_start..]
                .find('}')
                .ok_or_else(|| error(&format!("unclosed '${{' at position {i}")))?;
            let name = &pattern[name_start..name_start + close];
            let value = components
                .get(name)
                .ok_or_else(|| error(&format!("missing component: {name}")))?;
            result.push_str(&format!("(?:{value})"));
            i = name_start + close + 1;
            continue;
        }
        result.push(bytes[i] as char);
        i += 1;
    }
    Ok(result)
}

fn validate_required_fields(
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

const WHITESPACE: LazyCell<Regex> = std::cell::LazyCell::new(|| Regex::new(r"\s+").unwrap());
fn replace_whitespace_with_regex(s: &mut String) {
    *s = WHITESPACE.replace_all(s, r"\s+").to_string();
}

fn error(msg: &str) -> Error {
    Error::ConfigParse(msg.to_string())
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use regex::Regex;
    use rstest::rstest;

    use super::*;

    // Helper: parse a single parser TOML fragment and build it.
    fn build(toml_str: &str) -> Result<Parser> {
        build_with_components(toml_str, HashMap::new())
    }

    fn build_with_components(
        toml_str: &str,
        components: HashMap<String, String>,
    ) -> Result<Parser> {
        let raw: RawParserConfig = toml::from_str(toml_str).map_err(|e| error(&e.to_string()))?;
        let ts_fmt = raw
            .timestamp_format()
            .ok_or_else(|| error("missing timestamp_format"))?;
        let ctx = BuildCtx {
            components: &components,
            timestamp_format: ts_fmt,
            inherited_ref_fields: &[],
        };
        build_parser(&raw, &ctx)
    }

    // ── serde structural validation ───────────────────────────────────────────

    #[rstest]
    #[case(r#"type = "single""#)] // missing name, pattern
    #[case(r#"type = "span""#)] // missing name, patterns, reference_fields
    #[case(r#"name = "x" pattern = "y""#)] // missing type
    #[case(r#"type = "unknown" name = "x""#)] // invalid type tag
    fn test_structural_errors_fail_deserialization(#[case] toml_str: &str) {
        assert!(toml::from_str::<RawParserConfig>(toml_str).is_err());
    }

    #[rstest]
    #[case(SINGLE_VALID)]
    #[case(SPAN_VALID)]
    #[case(SPAN_EMPTY_NESTED)]
    fn test_structural_valid_deserializes(#[case] toml_str: &str) {
        assert!(toml::from_str::<RawParserConfig>(toml_str).is_ok());
    }

    // ── component substitution ────────────────────────────────────────────────

    #[rstest]
    #[case(
        r#"type = "single"
name = "t"
timestamp_format = "%Y"
pattern = '(?P<timestamp>${ts}) (?P<level>${level})'"#,
        HashMap::from([("ts".to_string(), r"\d+".to_string()), ("level".to_string(), r"\w+".to_string())]),
        vec!["timestamp", "level"]
    )]
    #[case(
        r#"type = "single"
name = "t"
timestamp_format = "%Y"
pattern = '(?P<timestamp>${ts})'"#,
        HashMap::from([("ts".to_string(), r"\d+".to_string()), ("unused".to_string(), "x".to_string())]),
        vec!["timestamp"]
    )]
    fn test_parse_pattern_substitution(
        #[case] toml_str: &str,
        #[case] components: HashMap<String, String>,
        #[case] expected_captures: Vec<&str>,
    ) {
        let parser = build_with_components(toml_str, components).unwrap();
        let Parser::Single(p) = parser else {
            panic!("expected Single")
        };
        for cap in expected_captures {
            assert!(
                p.pattern.capture_names().any(|c| c == Some(cap)),
                "expected capture group '{cap}' in pattern {}",
                p.pattern
            );
        }
    }

    #[rstest]
    #[case(
        r#"type = "single"
name = "t"
timestamp_format = "%Y"
pattern = '(?P<timestamp>${missing})'"#,
        HashMap::new(),
        "missing"
    )]
    #[case(
        r#"type = "single"
name = "t"
timestamp_format = "%Y"
pattern = '(?P<timestamp>${unclosed)'"#,
        HashMap::new(),
        "unclosed"
    )]
    fn test_parse_pattern_expansion_err(
        #[case] toml_str: &str,
        #[case] components: HashMap<String, String>,
        #[case] expected_in_err: &str,
    ) {
        let err = build_with_components(toml_str, components).unwrap_err();
        assert!(
            err.to_string().contains(expected_in_err),
            "expected error containing {expected_in_err:?}, got: {err}"
        );
    }

    // ── validate_required_fields ──────────────────────────────────────────────

    #[rstest]
    #[case(r"(?P<timestamp>\d+) (?P<level>\w+)", vec!["timestamp", "level"])]
    #[case(r"(?P<timestamp>\d+)", vec!["timestamp"])]
    #[case(r"(?P<timestamp>\d+)", vec![])]
    #[case(r"(?P<timestamp>\d+) (?P<extra>.+)", vec!["timestamp"])]
    fn test_validate_required_fields_ok(#[case] pattern: &str, #[case] fields: Vec<&str>) {
        let re = Regex::new(pattern).unwrap();
        assert!(validate_required_fields(&re, fields).is_ok());
    }

    #[rstest]
    #[case(r"(?P<level>\w+)", vec!["timestamp"], "timestamp")]
    #[case(r"(?P<other>\w+)", vec!["timestamp", "level"], "timestamp")]
    #[case(r"(\d+)(\w+)", vec!["timestamp"], "timestamp")]
    fn test_validate_required_fields_err(
        #[case] pattern: &str,
        #[case] fields: Vec<&str>,
        #[case] expected_in_err: &str,
    ) {
        let re = Regex::new(pattern).unwrap();
        let err = validate_required_fields(&re, fields).unwrap_err();
        assert!(
            err.to_string().contains(expected_in_err),
            "expected error containing {expected_in_err:?}, got: {err}"
        );
    }

    // ── build outcomes ────────────────────────────────────────────────────────

    #[rstest]
    #[case(SINGLE_VALID)]
    #[case(SPAN_VALID)]
    #[case(SPAN_EMPTY_NESTED)]
    #[case(SPAN_ONE_NESTED)]
    #[case(SPAN_TWO_NESTED)]
    #[case(SPAN_NESTED_INHERITS_TS_FMT)]
    #[case(SPAN_NESTED_OVERRIDES_TS_FMT)]
    fn test_valid_builds_succeed(#[case] toml_str: &str) {
        assert!(build(toml_str).is_ok(), "{}", build(toml_str).unwrap_err());
    }

    #[rstest]
    #[case(SINGLE_MISSING_TIMESTAMP_CAPTURE, "timestamp")]
    #[case(SPAN_SAME_START_END, "different")]
    #[case(SPAN_MISSING_REF_IN_START, "ref")]
    #[case(NESTED_SPAN_MISSING_REF_FIELDS, "reference_fields")]
    #[case(NESTED_SPAN_DUPLICATE_REF_FIELD, "duplicates")]
    fn test_semantic_errors(#[case] toml_str: &str, #[case] expected_in_err: &str) {
        let err = build(toml_str).unwrap_err();
        assert!(
            err.to_string().contains(expected_in_err),
            "expected error containing {expected_in_err:?}, got: {err}"
        );
    }

    #[test]
    fn test_nested_single_inherits_timestamp_format() {
        let parser = build(SPAN_NESTED_INHERITS_TS_FMT).unwrap();
        let Parser::Span(span) = parser else { panic!() };
        let Parser::Single(nested) = &span.nested[0] else {
            panic!()
        };
        assert_eq!(nested.timestamp_format, "%Y-%m-%d %H:%M:%S");
    }

    #[test]
    fn test_nested_single_overrides_timestamp_format() {
        let parser = build(SPAN_NESTED_OVERRIDES_TS_FMT).unwrap();
        let Parser::Span(span) = parser else { panic!() };
        let Parser::Single(nested) = &span.nested[0] else {
            panic!()
        };
        assert_eq!(nested.timestamp_format, "%Y");
    }

    // ── test fixtures ─────────────────────────────────────────────────────────

    const SINGLE_VALID: &str = r#"
type = "single"
name = "my_parser"
timestamp_format = "%Y-%m-%d %H:%M:%S"
pattern = '(?P<timestamp>\d+) (?P<level>\w+)'
include_raw = false
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

    const SINGLE_MISSING_TIMESTAMP_CAPTURE: &str = r#"
type = "single"
name = "my_parser"
timestamp_format = "%Y-%m-%d %H:%M:%S"
pattern = '(?P<level>\w+)'
"#;

    const SPAN_SAME_START_END: &str = r#"
type = "span"
name = "my_span"
timestamp_format = "%Y-%m-%d %H:%M:%S"
start_pattern = '(?P<timestamp>\d+) (?P<ref>\S+)'
end_pattern = '(?P<timestamp>\d+) (?P<ref>\S+)'
reference_fields = ["ref"]
"#;

    const SPAN_MISSING_REF_IN_START: &str = r#"
type = "span"
name = "my_span"
timestamp_format = "%Y-%m-%d %H:%M:%S"
start_pattern = '(?P<timestamp>\d+) START'
end_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) END'
reference_fields = ["ref"]
"#;

    const NESTED_SPAN_MISSING_REF_FIELDS: &str = r#"
type = "span"
name = "outer"
timestamp_format = "%Y-%m-%d %H:%M:%S"
start_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "span", name = "inner", start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) S', end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) E', reference_fields = [] }]
"#;

    const NESTED_SPAN_DUPLICATE_REF_FIELD: &str = r#"
type = "span"
name = "outer"
timestamp_format = "%Y-%m-%d %H:%M:%S"
start_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "span", name = "inner", start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) S', end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) E', reference_fields = ["ref"] }]
"#;
}
