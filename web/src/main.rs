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
        env.add_template("base.html", include_str!("../templates/base.html"))?;
        env.add_template("index.html", include_str!("../templates/index.html"))?;
        env.add_template("events.html", include_str!("../templates/events.html"))?;
        env.add_template("events_results.html", include_str!("../templates/events_results.html"))?;
        env.add_template("events_detail.html", include_str!("../templates/events_detail.html"))?;
        env.add_template("events_more.html", include_str!("../templates/events_more.html"))?;
        env.add_template("tail.html", include_str!("../templates/tail.html"))?;
        env.add_template("spans.html", include_str!("../templates/spans.html"))?;
        env.add_template("spans_waterfall.html", include_str!("../templates/spans_waterfall.html"))?;
        env.add_template("charts.html", include_str!("../templates/charts.html"))?;
        env.add_template("searches.html", include_str!("../templates/searches.html"))?;

        let store = MemoryEventStore::new_in_memory().await;
        seed_store(&store).await?;

        Ok(Self { env: Arc::new(env), store: Arc::new(store) })
    }
}

async fn seed_store(store: &MemoryEventStore) -> anyhow::Result<()> {
    let now = chrono::Utc::now().naive_utc();
    let mut events: Vec<Event> = Vec::new();

    // http_request spans — mix of 200/404/500, GET/POST
    let paths = ["/api/users", "/api/orders", "/api/products", "/api/auth", "/api/search"];
    for i in 0i64..20 {
        let status = if i % 9 == 0 {
            "500"
        } else if i % 6 == 0 {
            "404"
        } else {
            "200"
        };
        let method = if i % 3 == 0 { "POST" } else { "GET" };
        events.push(Event::new_span(
            "http_request",
            now - Duration::minutes(i * 4 + 1),
            HashMap::from([("status".into(), status.into()), ("method".into(), method.into()), ("path".into(), format!("{}/{}", paths[i as usize % paths.len()], i * 7))]),
            Duration::milliseconds(15 + (i * 43 % 480)),
            None,
        ));
    }

    // db_query spans
    let tables = ["users", "orders", "products"];
    for i in 0i64..12 {
        events.push(Event::new_span(
            "db_query",
            now - Duration::minutes(i * 7 + 3),
            HashMap::from([("table".into(), tables[i as usize % tables.len()].into()), ("op".into(), if i % 2 == 0 { "SELECT" } else { "INSERT" }.into()), ("rows".into(), format!("{}", (i * 13 + 1) % 200))]),
            Duration::milliseconds(2 + (i * 19 % 150)),
            None,
        ));
    }

    // cache_lookup singles
    for i in 0i64..10 {
        events.push(Event::new_single(
            "cache_lookup",
            now - Duration::minutes(i * 5 + 2),
            HashMap::from([("key".into(), format!("user:session:{}", i * 17)), ("hit".into(), if i % 3 == 0 { "false" } else { "true" }.into())]),
            Some(String::from("2026-08-12 12:32:08 cache_lookup data=123 error=0 miss=false")),
        ));
    }

    // auth_check singles
    for i in 0i64..8 {
        events.push(Event::new_single(
            "auth_check",
            now - Duration::minutes(i * 9 + 5),
            HashMap::from([("user_id".into(), format!("{}", 1000 + i * 7)), ("result".into(), if i % 4 == 0 { "denied" } else { "ok" }.into())]),
            None,
        ));
    }

    // background_job spans — infrequent, long-running
    let jobs = ["email_digest", "report_gen", "data_cleanup", "metrics_rollup", "db_backup"];
    for (i, job) in jobs.iter().enumerate() {
        let i = i as i64;
        events.push(Event::new_span(
            "background_job",
            now - Duration::hours(i + 1),
            HashMap::from([("job".into(), (*job).into()), ("status".into(), if i == 2 { "failed" } else { "ok" }.into())]),
            Duration::milliseconds(800 + i * 1100),
            None,
        ));
    }

    store.store(&events).await?;

    // Traced request chains with parent/child relationships
    let mut traced: Vec<Event> = Vec::new();

    // Trace 1: /api/orders — happy path with db + cache
    let req1 = Event::new_span(
        "http_request",
        now - Duration::minutes(2),
        HashMap::from([("method".into(), "GET".into()), ("path".into(), "/api/orders/42".into()), ("status".into(), "200".into())]),
        Duration::milliseconds(312),
        None,
    );
    let req1_id = req1.id();
    traced.push(req1);

    traced.push(Event::new_single("auth_check", now - Duration::minutes(2) + Duration::milliseconds(5), HashMap::from([("user_id".into(), "1007".into()), ("result".into(), "ok".into())]), None).with_parent(req1_id));

    traced.push(Event::new_single("cache_lookup", now - Duration::minutes(2) + Duration::milliseconds(12), HashMap::from([("key".into(), "order:42".into()), ("hit".into(), "false".into())]), None).with_parent(req1_id));

    let db1 = Event::new_span(
        "db_query",
        now - Duration::minutes(2) + Duration::milliseconds(18),
        HashMap::from([("table".into(), "orders".into()), ("op".into(), "SELECT".into()), ("rows".into(), "1".into())]),
        Duration::milliseconds(74),
        None,
    );
    let db1_id = db1.id();
    traced.push(db1.with_parent(req1_id));

    traced.push(
        Event::new_span(
            "db_query",
            now - Duration::minutes(2) + Duration::milliseconds(95),
            HashMap::from([("table".into(), "order_items".into()), ("op".into(), "SELECT".into()), ("rows".into(), "5".into())]),
            Duration::milliseconds(38),
            None,
        )
        .with_parent(db1_id),
    );

    traced.push(Event::new_single("cache_lookup", now - Duration::minutes(2) + Duration::milliseconds(140), HashMap::from([("key".into(), "product:prices".into()), ("hit".into(), "true".into())]), None).with_parent(req1_id));

    // Trace 2: /api/auth — failed login
    let req2 = Event::new_span(
        "http_request",
        now - Duration::minutes(45),
        HashMap::from([("method".into(), "POST".into()), ("path".into(), "/api/auth/login".into()), ("status".into(), "401".into())]),
        Duration::milliseconds(88),
        None,
    );
    let req2_id = req2.id();
    traced.push(req2);

    traced.push(
        Event::new_span(
            "db_query",
            now - Duration::minutes(45) + Duration::milliseconds(8),
            HashMap::from([("table".into(), "users".into()), ("op".into(), "SELECT".into()), ("rows".into(), "1".into())]),
            Duration::milliseconds(22),
            None,
        )
        .with_parent(req2_id),
    );

    traced.push(
        Event::new_single(
            "auth_check",
            now - Duration::minutes(45) + Duration::milliseconds(35),
            HashMap::from([("user_id".into(), "2031".into()), ("result".into(), "denied".into()), ("reason".into(), "bad_password".into())]),
            None,
        )
        .with_parent(req2_id),
    );

    // Trace 3: background job with sub-queries
    let job = Event::new_span("background_job", now - Duration::hours(3), HashMap::from([("job".into(), "report_gen".into()), ("status".into(), "ok".into())]), Duration::milliseconds(4200), None);
    let job_id = job.id();
    traced.push(job);

    for (i, table) in ["users", "orders", "products"].iter().enumerate() {
        traced.push(
            Event::new_span(
                "db_query",
                now - Duration::hours(3) + Duration::milliseconds(200 + i as i64 * 1100),
                HashMap::from([("table".into(), (*table).into()), ("op".into(), "SELECT".into()), ("rows".into(), format!("{}", 50 + i * 120))]),
                Duration::milliseconds(800 + i as i64 * 200),
                None,
            )
            .with_parent(job_id),
        );
    }

    store.store(&traced).await?;
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
