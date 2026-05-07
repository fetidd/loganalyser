use axum::extract::{Query, State};
use axum::response::Html;
use event_storage::Filter;
use minijinja::context;
use serde::{Deserialize, Serialize};
use shared::event::Event;
use shared::tree::{EventNode, build_tree};

use crate::{
    AppState,
    routes::{AppError, HtmlResult},
};

fn storage_err(e: event_storage::Error) -> AppError {
    AppError(minijinja::Error::new(minijinja::ErrorKind::InvalidOperation, e.to_string()))
}

fn fmt_duration(ms: i64) -> String {
    if ms < 1000 { format!("{ms}ms") } else { format!("{:.1}s", ms as f64 / 1000.0) }
}

#[derive(Serialize)]
struct RootSpan {
    id: String,
    name: String,
    timestamp: String,
    duration_label: String,
}

pub async fn handler(State(state): State<AppState>) -> HtmlResult {
    let events = state.store.load(&Filter::new()).await.map_err(storage_err)?;
    let mut root_spans: Vec<RootSpan> = events
        .iter()
        .filter_map(|e| match e {
            Event::Span { id, name, timestamp, duration, parent_id: None, .. } => Some(RootSpan {
                id: id.to_string(),
                name: name.clone(),
                timestamp: timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
                duration_label: fmt_duration(duration.num_milliseconds()),
            }),
            _ => None,
        })
        .collect();
    root_spans.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    root_spans.truncate(30);

    let tmpl = state.env.get_template("spans.html")?;
    let html = tmpl.render(context! { page => "spans", root_spans })?;
    Ok(Html(html))
}

#[derive(Deserialize)]
pub struct WaterfallQuery {
    pub id: String,
}

#[derive(Serialize)]
struct FieldEntry {
    key: String,
    val: String,
}

#[derive(Serialize)]
struct Tick {
    pct: u8,
    label: String,
}

#[derive(Serialize)]
struct SpanRow {
    id: String,
    name: String,
    kind: &'static str,
    depth: usize,
    timestamp: String,
    duration_ms: Option<i64>,
    duration_label: String,
    left_pct: String,
    width_pct: String,
    hue: u64,
    fields: Vec<FieldEntry>,
    parent_id: Option<String>,
    child_count: usize,
}

fn name_hue(name: &str) -> u64 {
    name.bytes().fold(2166136261u64, |a, b| a.wrapping_mul(16777619).wrapping_add(b as u64)) % 360
}

fn event_ts_ms(e: &Event) -> i64 {
    match e {
        Event::Span { timestamp, .. } | Event::Single { timestamp, .. } => timestamp.and_utc().timestamp_millis(),
    }
}

fn event_end_ms(e: &Event) -> i64 {
    match e {
        Event::Span { timestamp, duration, .. } => timestamp.and_utc().timestamp_millis() + duration.num_milliseconds(),
        Event::Single { timestamp, .. } => timestamp.and_utc().timestamp_millis(),
    }
}

fn subtree_extent(node: &EventNode) -> (i64, i64) {
    let start = event_ts_ms(&node.event);
    let end = event_end_ms(&node.event).max(start);
    let (mut lo, mut hi) = (start, end);
    for child in &node.children {
        let (cs, ce) = subtree_extent(child);
        lo = lo.min(cs);
        hi = hi.max(ce);
    }
    (lo, hi)
}

fn count_descendants(node: &EventNode) -> usize {
    node.children.iter().map(|c| 1 + count_descendants(c)).sum()
}

fn flatten(node: &EventNode, depth: usize, root_start_ms: i64, total_ms: i64, rows: &mut Vec<SpanRow>) {
    let ts_ms = event_ts_ms(&node.event);
    let offset_ms = (ts_ms - root_start_ms).max(0);
    let left_pct = offset_ms as f64 / total_ms as f64 * 100.0;

    let (kind, duration_ms, width_pct) = match &node.event {
        Event::Span { duration, .. } => {
            let dur_ms = duration.num_milliseconds();
            let w = (dur_ms as f64 / total_ms as f64 * 100.0).max(0.3);
            ("span", Some(dur_ms), w)
        }
        Event::Single { .. } => ("single", None, 0.0_f64),
    };

    let mut fields: Vec<FieldEntry> = node.event.data().iter().map(|(k, v)| FieldEntry { key: k.clone(), val: v.clone() }).collect();
    fields.sort_by(|a, b| a.key.cmp(&b.key));

    let timestamp = match &node.event {
        Event::Span { timestamp, .. } | Event::Single { timestamp, .. } => timestamp.format("%H:%M:%S%.3f").to_string(),
    };

    let parent_id = node.event.parent_id().map(|p| p.to_string());

    rows.push(SpanRow {
        id: node.event.id().to_string(),
        name: node.event.name().to_string(),
        kind,
        depth,
        timestamp,
        duration_label: duration_ms.map(fmt_duration).unwrap_or_default(),
        duration_ms,
        left_pct: format!("{left_pct:.2}"),
        width_pct: format!("{width_pct:.2}"),
        hue: name_hue(node.event.name()),
        fields,
        parent_id,
        child_count: count_descendants(node),
    });

    let mut children: Vec<&EventNode> = node.children.iter().collect();
    children.sort_by_key(|n| event_ts_ms(&n.event));
    for child in children {
        flatten(child, depth + 1, root_start_ms, total_ms, rows);
    }
}

fn find_node<'a>(nodes: &'a [EventNode], id: &str) -> Option<&'a EventNode> {
    for node in nodes {
        if node.event.id().to_string() == id {
            return Some(node);
        }
        if let Some(found) = find_node(&node.children, id) {
            return Some(found);
        }
    }
    None
}

pub async fn waterfall(State(state): State<AppState>, Query(query): Query<WaterfallQuery>) -> HtmlResult {
    let events = state.store.load(&Filter::new()).await.map_err(storage_err)?;
    let tree = build_tree(events);
    let target_id = query.id.trim();

    let tmpl = state.env.get_template("spans_waterfall.html")?;

    let Some(root_node) = find_node(&tree, target_id) else {
        let html = tmpl.render(context! {
            error => format!("No event found with ID: {target_id}"),
            rows => Vec::<SpanRow>::new(),
            ticks => Vec::<Tick>::new(),
            total_ms => 0i64,
        })?;
        return Ok(Html(html));
    };

    let (root_start_ms, root_end_ms) = subtree_extent(root_node);
    let total_ms = (root_end_ms - root_start_ms).max(1);

    let ticks: Vec<Tick> = [0u8, 25, 50, 75, 100].iter().map(|&pct| Tick { pct, label: fmt_duration(total_ms * pct as i64 / 100) }).collect();

    let mut rows = vec![];
    flatten(root_node, 0, root_start_ms, total_ms, &mut rows);

    let html = tmpl.render(context! {
        rows,
        ticks,
        total_ms,
        error => "",
    })?;
    Ok(Html(html))
}
