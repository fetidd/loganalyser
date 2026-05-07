mod useful;
use event_storage::{Filter, event_filter::Cmp};
use shared::event::Event;
use std::time::Duration;
use useful::*;

pub const CONFIG: &str = r#"
state_db_path = "STATE_DB_PATH"

[settings]
poll_interval_secs = 1

[storage]
storage_type = "sqlite"
connection_string = "DB_PATH"

[defaults]
timestamp_format = "%Y-%m-%d %H:%M:%S"

[components]
timestamp = '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
ref       = '[A-Z]{3}'

[[parsers]]
name    = "single_event"
glob    = "LOG_PATH"
type    = "single"
pattern = '(?P<timestamp>${timestamp}) LOG (?P<data>.*)'

[[parsers]]
name             = "span_event"
glob             = "LOG_PATH"
type             = "span"
start_pattern    = '(?P<timestamp>${timestamp}) (?P<ref>${ref})   START'
end_pattern      = '(?P<timestamp>${timestamp}) (?P<ref>${ref})   END'
reference_fields = ["ref"]

[[parsers.nested]]
name    = "span_inner"
type    = "single"
pattern = '(?P<timestamp>${timestamp}) (?P<ref>${ref}) NOTE (?P<note>.*)'
"#;

#[tokio::test]
async fn test_one() {
    let mut env = setup(CONFIG).await;
    let timeout = Duration::from_secs(3);
    env.append_log(&["{{ts}} LOG hello world", "{{ts}} LOG hello world"]);
    assert!(env.wait_for(&Filter::new(), 2, timeout).await.is_ok(), "missing first two events");
    env.append_log(&["{{ts}} AAA START", "{{ts}} AAA NOTE this is a note", "{{ts}} AAA END"]);
    if let Ok(events) = env.wait_for(&Filter::data("ref", Cmp::Eq("AAA")).with_type(Cmp::Eq("span")), 1, timeout).await {
        let Event::Span { data, id, .. } = &events[0] else { panic!("expected a span!") };
        assert_eq!(data["ref"], "AAA");
        let nested_single = &env
            .storage
            .load(&Filter::event_type(Cmp::Eq("single")).with_parent_id(Cmp::Eq(id.to_string())))
            .await
            .expect("missing nested single")[0];
        assert_eq!(nested_single.data()["note"], "this is a note".to_string());
    } else {
        panic!("missing first span");
    }

    env.append_log(&["{{ts}} AAB START"]);
    env.kill();
    assert!(env.wait_for(&Filter::event_type(Cmp::Eq("span")).with_data("ref", Cmp::Eq("AAB")), 1, timeout).await.is_err());
    env.restart().await;
    env.append_log(&["{{ts}} AAB END"]);
    assert!(env.wait_for(&Filter::event_type(Cmp::Eq("span")).with_data("ref", Cmp::Eq("AAB")), 1, timeout).await.is_ok());
}

#[tokio::test]
async fn test_two() {
    let mut env = setup(CONFIG).await;
    let timeout = Duration::from_secs(3);
    env.append_log(&[
        "{{ts}} BBB START",
        "{{ts}} CCC NOTE note in CCC",
        "{{ts}} CCC START",
        "{{ts}} BBB END",
        "{{ts}} DDD START",
        "{{ts}} EEE START",
        "{{ts}} DDD END",
        "{{ts}} FFF START",
        "{{ts}} GGG START",
        "{{ts}} EEE NOTE note in EEE",
        "{{ts}} HHH START",
        "{{ts}} HHH END",
        "{{ts}} CCC END",
        "{{ts}} FFF END",
        "{{ts}} EEE NOTE note in EEE",
        "{{ts}} EEE END",
        "{{ts}} III START",
        "{{ts}} GGG END",
        "{{ts}} III END",
    ]);
    let span_refs = &["BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH", "III"];
    let events = env.wait_for(&Filter::data("ref", Cmp::In(span_refs.to_vec())), 8, timeout).await.expect("missing spans");
    for event in events {
        if let Event::Span { duration, data, .. } = event {
            let exp_duration = match data["ref"].as_str() {
                "BBB" => 3,
                "CCC" => 10,
                "DDD" => 2,
                "EEE" => 10,
                "FFF" => 6,
                "GGG" => 9,
                "HHH" => 1,
                "III" => 2,
                _ => unreachable!(),
            };
            let exp_duration = chrono::Duration::seconds(exp_duration);
            assert_eq!(exp_duration, duration, "{}", data["ref"]);
        } else if let Event::Single { data, .. } = event {
            match data["ref"].as_str() {
                r @ "EEE" | r @ "CCC" => assert!(&data["note"] == &format!("note in {r}")),
                _ => unreachable!(),
            }
        } else {
            unreachable!()
        }
    }
}
