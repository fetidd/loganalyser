use std::{collections::HashMap, env, fs};

use anyhow::{Context, bail};
use log_parser::parser::Parser;
use serde::Serialize;
use shared::event::Event;

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
    fn from_event(event: &Event) -> Self {
        match event {
            Event::Span {
                id,
                name,
                timestamp,
                data,
                duration,
                ..
            } => JsonEvent {
                id: id.to_string(),
                name: name.clone(),
                kind: "span",
                timestamp: timestamp.to_string(),
                data: data.clone(),
                duration_ms: Some(duration.num_milliseconds()),
                children: vec![],
            },
            Event::Single {
                id,
                name,
                timestamp,
                data,
                ..
            } => JsonEvent {
                id: id.to_string(),
                name: name.clone(),
                kind: "single",
                timestamp: timestamp.to_string(),
                data: data.clone(),
                duration_ms: None,
                children: vec![],
            },
        }
    }

    fn parent_id_str(event: &Event) -> Option<String> {
        match event {
            Event::Span { parent_id, .. } | Event::Single { parent_id, .. } => {
                parent_id.map(|u| u.to_string())
            }
        }
    }
}

fn build_tree(events: Vec<Event>) -> Vec<JsonEvent> {
    let mut json_map: HashMap<String, JsonEvent> = events
        .iter()
        .map(|e| (e.id().to_string(), JsonEvent::from_event(e)))
        .collect();

    let mut child_map: HashMap<String, Vec<String>> = HashMap::new();
    let mut roots: Vec<String> = vec![];

    for event in &events {
        let id = event.id().to_string();
        match JsonEvent::parent_id_str(event) {
            Some(pid) => child_map.entry(pid).or_default().push(id),
            None => roots.push(id),
        }
    }

    roots
        .iter()
        .map(|id| attach_children(id, &mut json_map, &child_map))
        .collect()
}

fn attach_children(
    id: &str,
    map: &mut HashMap<String, JsonEvent>,
    child_map: &HashMap<String, Vec<String>>,
) -> JsonEvent {
    let mut event = map.remove(id).expect("event id missing from map");
    if let Some(children) = child_map.get(id) {
        event.children = children
            .iter()
            .map(|cid| attach_children(cid, map, child_map))
            .collect();
    }
    event
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

    let tree = build_tree(all_events);

    let json = serde_json::to_string_pretty(&tree).context("Failed to serialize events")?;

    fs::write(output_path, &json)
        .with_context(|| format!("Failed to write output '{output_path}'"))?;

    println!("Wrote {} top-level events to {output_path}", tree.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared::event::Event;
    use std::collections::HashMap;

    fn ts() -> chrono::NaiveDateTime {
        chrono::NaiveDateTime::parse_from_str("2026-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
    }

    #[test]
    fn test_build_tree_nests_children_under_parent() {
        let parent = Event::new_span("outer", ts(), HashMap::new(), chrono::Duration::seconds(5));
        let parent_id = parent.id();
        let child1 = Event::new_single("inner_a", ts(), HashMap::new()).with_parent(parent_id);
        let child2 = Event::new_single("inner_b", ts(), HashMap::new()).with_parent(parent_id);

        let tree = build_tree(vec![child1, child2, parent]);

        assert_eq!(tree.len(), 1, "one root");
        assert_eq!(tree[0].name, "outer");
        assert_eq!(tree[0].children.len(), 2);
        let names: Vec<&str> = tree[0].children.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"inner_a"));
        assert!(names.contains(&"inner_b"));
    }

    #[test]
    fn test_build_tree_flat_when_no_parents() {
        let a = Event::new_single("a", ts(), HashMap::new());
        let b = Event::new_single("b", ts(), HashMap::new());
        let tree = build_tree(vec![a, b]);
        assert_eq!(tree.len(), 2);
        assert!(tree.iter().all(|e| e.children.is_empty()));
    }
}
