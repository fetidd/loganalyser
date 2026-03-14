use std::{fmt, sync::Arc};

use anyhow::Result;
use event_storage::{
    EventStorage, Filter,
    event_filter::{self, Cmp, Expr},
};
use inquire::{Select, Text};
use shared::event::Event;

#[derive(Clone)]
enum FilterSpec {
    Sql(Expr),
    EventType(String),
}

#[derive(Clone)]
enum Selection {
    Event(Event),
    AddFilter,
    ClearFilters(usize),
    Refresh,
    Quit,
}

impl fmt::Display for Selection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Selection::Event(e) => write!(f, "{}", format_event_row(e)),
            Selection::AddFilter => write!(f, "[ Add Filter ]"),
            Selection::ClearFilters(n) => write!(f, "[ Clear Filters ({n} active) ]"),
            Selection::Refresh => write!(f, "[ Refresh ]"),
            Selection::Quit => write!(f, "[ Quit ]"),
        }
    }
}

fn format_event_row(event: &Event) -> String {
    let ts = match event {
        Event::Span { timestamp, .. } | Event::Single { timestamp, .. } => {
            timestamp.format("%Y-%m-%d %H:%M:%S").to_string()
        }
    };
    let name = event.name();
    match event {
        Event::Span { duration, .. } => {
            let ms = duration.num_milliseconds();
            format!("{ts}  {name:<24} [span  {ms:>6}ms]")
        }
        Event::Single { .. } => {
            format!("{ts}  {name:<24} [single      ]")
        }
    }
}

pub async fn run(storage: Arc<dyn EventStorage>) -> Result<()> {
    let mut active_filters: Vec<(String, FilterSpec)> = vec![];

    loop {
        let sql_exprs: Vec<Expr> = active_filters
            .iter()
            .filter_map(|(_, spec)| match spec {
                FilterSpec::Sql(expr) => Some(expr.clone()),
                FilterSpec::EventType(_) => None,
            })
            .collect();

        let filter = if sql_exprs.is_empty() {
            Filter::new()
        } else if sql_exprs.len() == 1 {
            Filter::from(sql_exprs.into_iter().next().unwrap())
        } else {
            Filter::from(event_filter::and(sql_exprs))
        };

        let mut events: Vec<Event> = storage
            .load(filter)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        for (_, spec) in &active_filters {
            if let FilterSpec::EventType(et) = spec {
                let et = et.clone();
                events.retain(|e| match (e, et.as_str()) {
                    (Event::Span { .. }, "span") | (Event::Single { .. }, "single") => true,
                    _ => false,
                });
            }
        }

        let event_count = events.len();
        let mut items: Vec<Selection> = events.into_iter().map(Selection::Event).collect();
        items.push(Selection::Refresh);
        items.push(Selection::AddFilter);
        if !active_filters.is_empty() {
            items.push(Selection::ClearFilters(active_filters.len()));
        }
        items.push(Selection::Quit);

        let title = if active_filters.is_empty() {
            format!("Events ({event_count})")
        } else {
            let labels: Vec<&str> = active_filters.iter().map(|(l, _)| l.as_str()).collect();
            format!("Events ({event_count}) — filters: {}", labels.join(", "))
        };

        let choice = match Select::new(&title, items).prompt() {
            Ok(c) => c,
            Err(_) => break,
        };

        match choice {
            Selection::Event(event) => {
                show_detail(&event)?;
            }
            Selection::AddFilter => {
                if let Some(f) = build_filter()? {
                    active_filters.push(f);
                }
            }
            Selection::ClearFilters(_) => {
                active_filters.clear();
            }
            Selection::Refresh => {}
            Selection::Quit => break,
        }
    }

    Ok(())
}

fn show_detail(event: &Event) -> Result<()> {
    println!();
    println!("── Event Detail ──────────────────────────");
    match event {
        Event::Span { id, name, timestamp, data, duration, parent_id } => {
            println!("  ID:        {id}");
            println!("  Name:      {name}");
            println!("  Type:      Span");
            println!("  Timestamp: {}", timestamp.format("%Y-%m-%d %H:%M:%S"));
            println!("  Duration:  {}ms", duration.num_milliseconds());
            println!(
                "  Parent:    {}",
                parent_id
                    .map(|p| p.to_string())
                    .as_deref()
                    .unwrap_or("(none)")
            );
            println!();
            println!("  Data:");
            for (k, v) in data {
                println!("    {k}: {v}");
            }
        }
        Event::Single { id, name, timestamp, data, parent_id } => {
            println!("  ID:        {id}");
            println!("  Name:      {name}");
            println!("  Type:      Single");
            println!("  Timestamp: {}", timestamp.format("%Y-%m-%d %H:%M:%S"));
            println!(
                "  Parent:    {}",
                parent_id
                    .map(|p| p.to_string())
                    .as_deref()
                    .unwrap_or("(none)")
            );
            println!();
            println!("  Data:");
            for (k, v) in data {
                println!("    {k}: {v}");
            }
        }
    }
    println!("──────────────────────────────────────────");
    println!();

    let _ = Select::new("", vec!["← Back"]).prompt();
    Ok(())
}

fn build_filter() -> Result<Option<(String, FilterSpec)>> {
    let field_choices = vec![
        "Name contains",
        "Timestamp from (YYYY-MM-DD)",
        "Timestamp to (YYYY-MM-DD)",
        "Event type",
        "Data field equals",
        "Duration min (ms)",
        "Duration max (ms)",
        "Cancel",
    ];

    let choice = match Select::new("Filter by", field_choices).prompt() {
        Ok(c) => c,
        Err(_) => return Ok(None),
    };

    match choice {
        "Name contains" => {
            let val = match Text::new("Name contains:").prompt() {
                Ok(v) => v,
                Err(_) => return Ok(None),
            };
            if val.trim().is_empty() {
                return Ok(None);
            }
            let label = format!("name ~ \"{val}\"");
            let expr = event_filter::name(Cmp::Like(format!("%{val}%")));
            Ok(Some((label, FilterSpec::Sql(expr))))
        }
        "Timestamp from (YYYY-MM-DD)" => {
            let val = match Text::new("From date (YYYY-MM-DD):").prompt() {
                Ok(v) => v,
                Err(_) => return Ok(None),
            };
            if val.trim().is_empty() {
                return Ok(None);
            }
            let label = format!("timestamp >= \"{val}\"");
            let expr = event_filter::timestamp(Cmp::Gte(val));
            Ok(Some((label, FilterSpec::Sql(expr))))
        }
        "Timestamp to (YYYY-MM-DD)" => {
            let val = match Text::new("To date (YYYY-MM-DD):").prompt() {
                Ok(v) => v,
                Err(_) => return Ok(None),
            };
            if val.trim().is_empty() {
                return Ok(None);
            }
            let label = format!("timestamp <= \"{val}\"");
            let expr = event_filter::timestamp(Cmp::Lte(val));
            Ok(Some((label, FilterSpec::Sql(expr))))
        }
        "Event type" => {
            let type_choice =
                match Select::new("Event type", vec!["span", "single"]).prompt() {
                    Ok(c) => c,
                    Err(_) => return Ok(None),
                };
            let label = format!("type = {type_choice}");
            Ok(Some((label, FilterSpec::EventType(type_choice.to_string()))))
        }
        "Data field equals" => {
            let key = match Text::new("Field name:").prompt() {
                Ok(v) => v,
                Err(_) => return Ok(None),
            };
            if key.trim().is_empty() {
                return Ok(None);
            }
            let val = match Text::new("Field value:").prompt() {
                Ok(v) => v,
                Err(_) => return Ok(None),
            };
            if val.trim().is_empty() {
                return Ok(None);
            }
            let label = format!("data.{key} = \"{val}\"");
            let expr = event_filter::data(&key, Cmp::Eq(val));
            Ok(Some((label, FilterSpec::Sql(expr))))
        }
        "Duration min (ms)" => {
            let val = match Text::new("Min duration (ms):").prompt() {
                Ok(v) => v,
                Err(_) => return Ok(None),
            };
            let ms: i64 = match val.trim().parse() {
                Ok(n) => n,
                Err(_) => {
                    println!("Invalid number");
                    return Ok(None);
                }
            };
            let label = format!("duration >= {ms}ms");
            let expr = event_filter::duration(Cmp::Gte(ms));
            Ok(Some((label, FilterSpec::Sql(expr))))
        }
        "Duration max (ms)" => {
            let val = match Text::new("Max duration (ms):").prompt() {
                Ok(v) => v,
                Err(_) => return Ok(None),
            };
            let ms: i64 = match val.trim().parse() {
                Ok(n) => n,
                Err(_) => {
                    println!("Invalid number");
                    return Ok(None);
                }
            };
            let label = format!("duration <= {ms}ms");
            let expr = event_filter::duration(Cmp::Lte(ms));
            Ok(Some((label, FilterSpec::Sql(expr))))
        }
        _ => Ok(None), // Cancel
    }
}
