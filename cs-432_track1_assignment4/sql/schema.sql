-- ============================================================
-- schema.sql - FixIIT local SQLite schema
-- ============================================================
-- The audit_context table stores the actor/source used by audit
-- triggers when the database is accessed directly. The Flask app
-- uses a connection-local TEMP table with the same name while
-- handling API requests.
-- ============================================================

CREATE TABLE users (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    email    TEXT NOT NULL UNIQUE,
    password TEXT NOT NULL,       -- LIMITATION: plain-text for this assignment
    role     TEXT NOT NULL
);

CREATE TABLE logs (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    actor      TEXT NOT NULL,
    source     TEXT NOT NULL DEFAULT 'API',
    table_name TEXT,
    action     TEXT NOT NULL,
    details    TEXT,
    timestamp  DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Persistent fallback audit_context (used by direct-DB sessions).
-- The Flask app creates a per-connection TEMP TABLE with the same
-- name, which shadows this table for the duration of that connection.
CREATE TABLE audit_context (
    id     INTEGER PRIMARY KEY CHECK (id = 1),
    actor  TEXT NOT NULL,
    source TEXT NOT NULL
);

INSERT INTO audit_context (id, actor, source)
VALUES (1, 'DIRECT_DB', 'DIRECT_DB');

-- Triggers read from "audit_context" which resolves to temp.audit_context
-- when called from the Flask app, and main.audit_context otherwise.
CREATE TRIGGER trg_users_insert
AFTER INSERT ON users
BEGIN
    INSERT INTO logs (actor, source, table_name, action, details)
    SELECT actor, source, 'users', 'INSERT',
           'user_id=' || NEW.id || ', username=' || NEW.username
    FROM audit_context WHERE id = 1;
END;

CREATE TRIGGER trg_users_update
AFTER UPDATE ON users
BEGIN
    INSERT INTO logs (actor, source, table_name, action, details)
    SELECT actor, source, 'users', 'UPDATE',
           'user_id=' || NEW.id || ', username=' || NEW.username
    FROM audit_context WHERE id = 1;
END;

CREATE TRIGGER trg_users_delete
AFTER DELETE ON users
BEGIN
    INSERT INTO logs (actor, source, table_name, action, details)
    SELECT actor, source, 'users', 'DELETE',
           'user_id=' || OLD.id || ', username=' || OLD.username
    FROM audit_context WHERE id = 1;
END;

-- Assignment 4 sharding tables for complaints.
-- The Flask app also creates and refreshes these tables at startup.
CREATE TABLE IF NOT EXISTS complaint_shard_map (
    complaint_id INTEGER PRIMARY KEY,
    member_id    INTEGER NOT NULL,
    shard_id     INTEGER NOT NULL CHECK (shard_id >= 0 AND shard_id < 3)
);

CREATE INDEX IF NOT EXISTS idx_complaint_shard_map_member_id
ON complaint_shard_map(member_id);

CREATE INDEX IF NOT EXISTS idx_complaint_shard_map_shard_id
ON complaint_shard_map(shard_id);

CREATE TABLE IF NOT EXISTS shard_0_complaint (
    complaint_id    INTEGER PRIMARY KEY,
    member_id       INT NOT NULL,
    issue_type_id   INT NOT NULL,
    priority_id     INT NOT NULL,
    status_id       INT NOT NULL,
    description     TEXT NOT NULL,
    created_at      DATETIME NOT NULL,
    closed_at       DATETIME,
    hostel_id       INT,
    hostel_room_no  VARCHAR(20),
    location_id     INT
);

CREATE TABLE IF NOT EXISTS shard_1_complaint (
    complaint_id    INTEGER PRIMARY KEY,
    member_id       INT NOT NULL,
    issue_type_id   INT NOT NULL,
    priority_id     INT NOT NULL,
    status_id       INT NOT NULL,
    description     TEXT NOT NULL,
    created_at      DATETIME NOT NULL,
    closed_at       DATETIME,
    hostel_id       INT,
    hostel_room_no  VARCHAR(20),
    location_id     INT
);

CREATE TABLE IF NOT EXISTS shard_2_complaint (
    complaint_id    INTEGER PRIMARY KEY,
    member_id       INT NOT NULL,
    issue_type_id   INT NOT NULL,
    priority_id     INT NOT NULL,
    status_id       INT NOT NULL,
    description     TEXT NOT NULL,
    created_at      DATETIME NOT NULL,
    closed_at       DATETIME,
    hostel_id       INT,
    hostel_room_no  VARCHAR(20),
    location_id     INT
);

CREATE INDEX IF NOT EXISTS idx_shard_0_complaint_member_id
ON shard_0_complaint(member_id);

CREATE INDEX IF NOT EXISTS idx_shard_1_complaint_member_id
ON shard_1_complaint(member_id);

CREATE INDEX IF NOT EXISTS idx_shard_2_complaint_member_id
ON shard_2_complaint(member_id);

CREATE INDEX IF NOT EXISTS idx_shard_0_complaint_created_at
ON shard_0_complaint(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_shard_1_complaint_created_at
ON shard_1_complaint(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_shard_2_complaint_created_at
ON shard_2_complaint(created_at DESC);
