CREATE TABLE IF NOT EXISTS events (
    id          CHAR(36)     NOT NULL PRIMARY KEY,
    event_type  VARCHAR(10)  NOT NULL,
    name        VARCHAR(255) NOT NULL,
    timestamp   DATETIME(6)  NOT NULL,
    duration_ms BIGINT           NULL,
    parent_id   CHAR(36)         NULL,
    raw_line    TEXT             NULL,
    data        TEXT         NOT NULL
);
