use std::{collections::HashMap, sync::Arc};

use chrono::Duration;
use event_storage::{EventStorage, MemoryEventStore};
use minijinja::Environment;
use shared::event::Event;
use tower_http::services::ServeDir;

mod routes;

#[derive(Clone)]
pub struct AppState {
    env: Arc<Environment<'static>>,
    store: Arc<dyn EventStorage>,
}

impl AppState {
    async fn new() -> anyhow::Result<Self> {
        let mut env = Environment::new();
        env.add_template("base.html",           include_str!("../templates/base.html"))?;
        env.add_template("index.html",          include_str!("../templates/index.html"))?;
        env.add_template("events.html",         include_str!("../templates/events.html"))?;
        env.add_template("events_results.html", include_str!("../templates/events_results.html"))?;
        env.add_template("events_detail.html",  include_str!("../templates/events_detail.html"))?;
        env.add_template("events_more.html",    include_str!("../templates/events_more.html"))?;
        env.add_template("tail.html",           include_str!("../templates/tail.html"))?;
        env.add_template("spans.html",          include_str!("../templates/spans.html"))?;
        env.add_template("charts.html",         include_str!("../templates/charts.html"))?;
        env.add_template("searches.html",       include_str!("../templates/searches.html"))?;

        let store = MemoryEventStore::new_in_memory().await;
        seed_store(&store).await?;

        Ok(Self {
            env: Arc::new(env),
            store: Arc::new(store),
        })
    }
}

async fn seed_store(store: &MemoryEventStore) -> anyhow::Result<()> {
    let now = chrono::Utc::now().naive_utc();
    let mut events: Vec<Event> = Vec::new();

    // http_request spans — mix of 200/404/500, GET/POST
    let paths = ["/api/users", "/api/orders", "/api/products", "/api/auth", "/api/search"];
    for i in 0i64..20 {
        let status = if i % 9 == 0 { "500" } else if i % 6 == 0 { "404" } else { "200" };
        let method = if i % 3 == 0 { "POST" } else { "GET" };
        events.push(Event::new_span(
            "http_request",
            now - Duration::minutes(i * 4 + 1),
            HashMap::from([
                ("status".into(), status.into()),
                ("method".into(), method.into()),
                ("path".into(), format!("{}/{}", paths[i as usize % paths.len()], i * 7)),
            ]),
            Duration::milliseconds(15 + (i * 43 % 480)),
        ));
    }

    // db_query spans
    let tables = ["users", "orders", "products"];
    for i in 0i64..12 {
        events.push(Event::new_span(
            "db_query",
            now - Duration::minutes(i * 7 + 3),
            HashMap::from([
                ("table".into(), tables[i as usize % tables.len()].into()),
                ("op".into(), if i % 2 == 0 { "SELECT" } else { "INSERT" }.into()),
                ("rows".into(), format!("{}", (i * 13 + 1) % 200)),
            ]),
            Duration::milliseconds(2 + (i * 19 % 150)),
        ));
    }

    // cache_lookup singles
    for i in 0i64..10 {
        events.push(Event::new_single(
            "cache_lookup",
            now - Duration::minutes(i * 5 + 2),
            HashMap::from([
                ("key".into(), format!("user:session:{}", i * 17)),
                ("hit".into(), if i % 3 == 0 { "false" } else { "true" }.into()),
            ]),
        ));
    }

    // auth_check singles
    for i in 0i64..8 {
        events.push(Event::new_single(
            "auth_check",
            now - Duration::minutes(i * 9 + 5),
            HashMap::from([
                ("user_id".into(), format!("{}", 1000 + i * 7)),
                ("result".into(), if i % 4 == 0 { "denied" } else { "ok" }.into()),
            ]),
        ));
    }

    // background_job spans — infrequent, long-running
    let jobs = ["email_digest", "report_gen", "data_cleanup", "metrics_rollup", "db_backup"];
    for (i, job) in jobs.iter().enumerate() {
        let i = i as i64;
        events.push(Event::new_span(
            "background_job",
            now - Duration::hours(i + 1),
            HashMap::from([
                ("job".into(), (*job).into()),
                ("status".into(), if i == 2 { "failed" } else { "ok" }.into()),
            ]),
            Duration::milliseconds(800 + i * 1100),
        ));
    }

    store.store(&events).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let state = AppState::new().await?;
    let app = routes::router(state).nest_service("/static", ServeDir::new("web/static"));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("listening on http://0.0.0.0:3000");
    axum::serve(listener, app).await?;
    Ok(())
}
