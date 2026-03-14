CREATE TABLE events (
    id          TEXT    NOT NULL PRIMARY KEY,
    event_type  TEXT    NOT NULL,
    name        TEXT    NOT NULL,
    timestamp   TEXT    NOT NULL,
    duration_ms INTEGER     NULL,
    parent_id   TEXT        NULL,
    data        TEXT    NOT NULL
);
