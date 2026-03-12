use std::{collections::HashMap, env, fs};

use anyhow::{Context, bail};
use log_parser::parser::Parser;
use serde::Serialize;
use shared::event::Event;
use shared::tree::{EventNode, build_tree};

#[derive(Serialize)]
struct JsonEvent {
    id: String,
    name: String,
    #[serde(rename = "type")]
    kind: &'static str,
    timestamp: String,
    data: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_ms: Option<i64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    children: Vec<JsonEvent>,
}

impl JsonEvent {
    fn from_node(node: EventNode) -> Self {
        let children = node.children.into_iter().map(JsonEvent::from_node).collect();
        match node.event {
            Event::Span { id, name, timestamp, data, duration, .. } => JsonEvent {
                id: id.to_string(),
                name,
                kind: "span",
                timestamp: timestamp.to_string(),
                data,
                duration_ms: Some(duration.num_milliseconds()),
                children,
            },
            Event::Single { id, name, timestamp, data, .. } => JsonEvent {
                id: id.to_string(),
                name,
                kind: "single",
                timestamp: timestamp.to_string(),
                data,
                duration_ms: None,
                children,
            },
        }
    }
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        bail!("Usage: {} <log_file> <config_file> <output_file>", args[0]);
    }

    let log_path = &args[1];
    let config_path = &args[2];
    let output_path = &args[3];

    let log_content = fs::read_to_string(log_path)
        .with_context(|| format!("Failed to read log file '{log_path}'"))?;

    let config_content = fs::read_to_string(config_path)
        .with_context(|| format!("Failed to read config file '{config_path}'"))?;

    let config: toml::Table = config_content
        .parse()
        .with_context(|| format!("Failed to parse config '{config_path}'"))?;

    let parsers_array = config
        .get("parsers")
        .and_then(|v| v.as_array())
        .context("Config missing [[parsers]] array")?;

    let mut parsers: Vec<Parser> = parsers_array
        .iter()
        .filter_map(|v| v.as_table())
        .map(|table| Parser::build_from_toml(table).context("Failed to build parser"))
        .collect::<anyhow::Result<_>>()?;

    let all_events: Vec<Event> = parsers
        .iter_mut()
        .flat_map(|p| p.parse(&log_content))
        .collect();

    let tree: Vec<JsonEvent> = build_tree(all_events)
        .into_iter()
        .map(JsonEvent::from_node)
        .collect();

    let json = serde_json::to_string_pretty(&tree).context("Failed to serialize events")?;

    fs::write(output_path, &json)
        .with_context(|| format!("Failed to write output '{output_path}'"))?;

    println!("Wrote {} top-level events to {output_path}", tree.len());
    Ok(())
}

