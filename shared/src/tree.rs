use std::collections::HashMap;

use crate::event::Event;

pub struct EventNode {
    pub event: Event,
    pub children: Vec<EventNode>,
}

pub fn build_tree(events: Vec<Event>) -> Vec<EventNode> {
    let mut node_map: HashMap<String, EventNode> = events.iter().map(|e| (e.id().to_string(), EventNode { event: e.clone(), children: vec![] })).collect();

    let mut child_map: HashMap<String, Vec<String>> = HashMap::new();
    let mut roots: Vec<String> = vec![];

    for event in &events {
        let id = event.id().to_string();
        match event.parent_id() {
            Some(pid) => child_map.entry(pid.to_string()).or_default().push(id),
            None => roots.push(id),
        }
    }

    roots.iter().map(|id| attach_children(id, &mut node_map, &child_map)).collect()
}

fn attach_children(id: &str, map: &mut HashMap<String, EventNode>, child_map: &HashMap<String, Vec<String>>) -> EventNode {
    let mut node = map.remove(id).expect("event id missing from map");
    if let Some(children) = child_map.get(id) {
        node.children = children.iter().map(|cid| attach_children(cid, map, child_map)).collect();
    }
    node
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::Duration;

    use super::*;
    use crate::datetime_from;

    fn ts() -> chrono::NaiveDateTime {
        datetime_from("2026-01-01").unwrap()
    }

    #[test]
    fn test_build_tree_nests_children_under_parent() {
        let parent = Event::new_span("outer", ts(), HashMap::new(), Duration::seconds(5), (String::new(), String::new()));
        let parent_id = parent.id();
        let child1 = Event::new_single("inner_a", ts(), HashMap::new(), String::new()).with_parent(parent_id);
        let child2 = Event::new_single("inner_b", ts(), HashMap::new(), String::new()).with_parent(parent_id);

        let tree = build_tree(vec![child1, child2, parent]);

        assert_eq!(tree.len(), 1, "one root");
        assert_eq!(tree[0].event.name(), "outer");
        assert_eq!(tree[0].children.len(), 2);
        let names: Vec<&str> = tree[0].children.iter().map(|c| c.event.name()).collect();
        assert!(names.contains(&"inner_a"));
        assert!(names.contains(&"inner_b"));
    }

    #[test]
    fn test_build_tree_flat_when_no_parents() {
        let a = Event::new_single("a", ts(), HashMap::new(), String::new());
        let b = Event::new_single("b", ts(), HashMap::new(), String::new());
        let tree = build_tree(vec![a, b]);
        assert_eq!(tree.len(), 2);
        assert!(tree.iter().all(|n| n.children.is_empty()));
    }
}
