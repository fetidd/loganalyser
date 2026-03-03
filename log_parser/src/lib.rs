use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum SourceConfig {
    Single {
        name: String,
        #[serde(default)]
        glob: Option<String>,
        pattern: String,
        timestamp_format: String,
    },
    Span {
        name: String,
        #[serde(default)]
        glob: Option<String>,
        start_pattern: String,
        end_pattern: String,
        #[serde(default)]
        nested: Vec<SourceConfig>,
        timestamp_format: String,
        #[serde(default)]
        span_identifier: Vec<String>,
    },
}

impl SourceConfig {
    pub fn name(&self) -> String {
        match self {
            SourceConfig::Single { name, .. } | SourceConfig::Span { name, .. } => name.to_owned(),
        }
    }

    pub fn glob(&self) -> Option<String> {
        match self {
            SourceConfig::Single { glob, .. } | SourceConfig::Span { glob, .. } => glob.clone(),
        }
    }

    pub fn timestamp_format(&self) -> String {
        match self {
            SourceConfig::Single {
                timestamp_format, ..
            }
            | SourceConfig::Span {
                timestamp_format, ..
            } => timestamp_format.to_owned(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub enum LogParserError {
    ConfigParseError(String),
}

impl std::error::Error for LogParserError {}
impl std::fmt::Display for LogParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogParserError::ConfigParseError(e) => write!(f, "Failed to parse config file: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GW_EXAMPLE: &str = include_str!("../../gateway_example.log");
    const GW_CONFIG: &str = include_str!("../../gateway_config.toml");

    #[test]
    fn it_works() {
        let table: toml::Table = toml::from_str(GW_CONFIG).expect("failed to read toml to str");
        dbg!(&table);
        // let sources = SourceConfig::all_from_table(&table).expect("failed to parse config");
        // dbg!(sources);
        panic!();
    }
}
