use axum::{
    extract::{Path, Query, State},
    response::Html,
};
use event_storage::{Filter, event_filter::Cmp};
use minijinja::context;
use serde::{Deserialize, Serialize};
use shared::event::Event;

use crate::{
    AppState,
    routes::{AppError, HtmlResult},
};

const PAGE_SIZE: usize = 50;

#[derive(Deserialize, Default)]
pub struct EventsQuery {
    pub name: Option<String>,
    pub kind: Option<String>,
    pub page: Option<usize>,
}

#[derive(Serialize)]
struct EventRow {
    id: String,
    kind: &'static str,
    name: String,
    timestamp: String,
    duration_ms: Option<i64>,
}

impl EventRow {
    fn from_event(e: &Event) -> Self {
        match e {
            Event::Span {
                id,
                name,
                timestamp,
                duration,
                ..
            } => EventRow {
                id: id.to_string(),
                kind: "span",
                name: name.clone(),
                timestamp: timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
                duration_ms: Some(duration.num_milliseconds()),
            },
            Event::Single {
                id,
                name,
                timestamp,
                ..
            } => EventRow {
                id: id.to_string(),
                kind: "single",
                name: name.clone(),
                timestamp: timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
                duration_ms: None,
            },
        }
    }
}

#[derive(Serialize)]
struct FieldEntry {
    key: String,
    val: String,
}

fn storage_err(e: event_storage::Error) -> AppError {
    AppError(minijinja::Error::new(
        minijinja::ErrorKind::InvalidOperation,
        e.to_string(),
    ))
}

async fn load_page(
    state: &AppState,
    query: &EventsQuery,
    page: usize,
) -> Result<(Vec<EventRow>, bool), AppError> {
    let mut filter = Filter::new();
    if let Some(name) = &query.name {
        let name = name.trim();
        if !name.is_empty() {
            filter = filter.with_name(Cmp::Like(format!("%{name}%")));
        }
    }

    let mut events = state.store.load(filter).await.map_err(storage_err)?;

    if let Some(kind) = &query.kind {
        events.retain(|e| match kind.as_str() {
            "span" => matches!(e, Event::Span { .. }),
            "single" => matches!(e, Event::Single { .. }),
            _ => true,
        });
    }

    events.sort_by(|a, b| {
        let ts = |e: &Event| match e {
            Event::Span { timestamp, .. } | Event::Single { timestamp, .. } => *timestamp,
        };
        ts(b).cmp(&ts(a))
    });

    let offset = (page - 1) * PAGE_SIZE;
    let has_more = offset + PAGE_SIZE < events.len();
    let rows = events
        .iter()
        .skip(offset)
        .take(PAGE_SIZE)
        .map(EventRow::from_event)
        .collect();
    Ok((rows, has_more))
}

// GET /events
pub async fn handler(
    State(state): State<AppState>,
    Query(query): Query<EventsQuery>,
) -> HtmlResult {
    let (rows, has_more) = load_page(&state, &query, 1).await?;
    let tmpl = state.env.get_template("events.html")?;
    let html = tmpl.render(context! {
        page_name => "events",
        rows,
        has_more,
        next_page => 2,
        name => query.name.as_deref().unwrap_or(""),
        kind => query.kind.as_deref().unwrap_or(""),
    })?;
    Ok(Html(html))
}

// GET /events/results — page 1 fragment; used by filter changes
pub async fn results(
    State(state): State<AppState>,
    Query(query): Query<EventsQuery>,
) -> HtmlResult {
    let (rows, has_more) = load_page(&state, &query, 1).await?;
    let tmpl = state.env.get_template("events_results.html")?;
    let html = tmpl.render(context! {
        rows,
        has_more,
        next_page => 2,
        name => query.name.as_deref().unwrap_or(""),
        kind => query.kind.as_deref().unwrap_or(""),
    })?;
    Ok(Html(html))
}

// GET /events/results/more — subsequent pages; sentinel swaps itself with these rows
pub async fn more(State(state): State<AppState>, Query(query): Query<EventsQuery>) -> HtmlResult {
    let page = query.page.unwrap_or(2).max(2);
    let (rows, has_more) = load_page(&state, &query, page).await?;
    let tmpl = state.env.get_template("events_more.html")?;
    let html = tmpl.render(context! {
        rows,
        has_more,
        next_page => page + 1,
        name => query.name.as_deref().unwrap_or(""),
        kind => query.kind.as_deref().unwrap_or(""),
    })?;
    Ok(Html(html))
}

// GET /events/{id}/detail
pub async fn detail(State(state): State<AppState>, Path(id): Path<String>) -> HtmlResult {
    let filter = Filter::new().with_id(Cmp::Eq(id));
    let events = state.store.load(filter).await.map_err(storage_err)?;

    let Some(event) = events.into_iter().next() else {
        return Ok(Html(r#"<tr class="detail-row"></tr>"#.into()));
    };

    let (eid, parent_id, mut fields, raw_line) = match &event {
        Event::Span {
            id,
            parent_id,
            data,
            raw_lines,
            ..
        } => (
            id.to_string(),
            parent_id.map(|p| p.to_string()),
            data.iter()
                .map(|(k, v)| FieldEntry {
                    key: k.clone(),
                    val: v.clone(),
                })
                .collect::<Vec<_>>(),
            raw_lines.as_ref().map(|(s1, s2)| format!("{s1} - {s2}")),
        ),
        Event::Single {
            id,
            parent_id,
            data,
            raw_line,
            ..
        } => (
            id.to_string(),
            parent_id.map(|p| p.to_string()),
            data.iter()
                .map(|(k, v)| FieldEntry {
                    key: k.clone(),
                    val: v.clone(),
                })
                .collect::<Vec<_>>(),
            raw_line.as_ref().map(|s| s.to_string()),
        ),
    };
    fields.sort_by(|a, b| a.key.cmp(&b.key));

    let tmpl = state.env.get_template("events_detail.html")?;
    let html = tmpl.render(context! { id => eid, parent_id, fields, raw_line })?;
    Ok(Html(html))
}
