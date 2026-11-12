"""Durable state in one SQLite file (WAL): event log, id mappings, outbox, calls.

The gateway is one process on one host, so SQLite is enough for v0; every
statement is short, so calling it from the event loop is fine.
"""
from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import time
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    source TEXT NOT NULL,
    subject TEXT,
    conv_seq INTEGER,
    time TEXT NOT NULL,
    ingesttime TEXT NOT NULL,
    data TEXT NOT NULL,
    traceparent TEXT
);
CREATE TABLE IF NOT EXISTS conversations (
    id TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    platform_key TEXT NOT NULL,
    kind TEXT NOT NULL,              -- direct | group
    last_seq INTEGER NOT NULL DEFAULT 0,
    UNIQUE (channel, platform_key)
);
CREATE TABLE IF NOT EXISTS aliases (    -- another address of a direct conversation's peer
    channel TEXT NOT NULL,
    address TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    PRIMARY KEY (channel, address)
);
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL,
    direction TEXT NOT NULL,         -- in | out
    platform_id TEXT,
    thread_platform_id TEXT,
    status TEXT NOT NULL,
    error TEXT,
    created REAL NOT NULL,
    consumer TEXT                    -- who sent it (out), or whom it was addressed to (in)
);
CREATE INDEX IF NOT EXISTS messages_platform ON messages (conversation_id, platform_id);
CREATE INDEX IF NOT EXISTS messages_platform_only ON messages (platform_id);
CREATE TABLE IF NOT EXISTS outbox (
    message_id TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    op TEXT NOT NULL,                -- send | edit | delete | react
    payload TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_at REAL NOT NULL,
    done INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS idempotency (
    consumer TEXT NOT NULL,
    key TEXT NOT NULL,
    response TEXT NOT NULL,
    created REAL NOT NULL,
    PRIMARY KEY (consumer, key)
);
CREATE TABLE IF NOT EXISTS calls (
    id TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    platform_id TEXT,
    direction TEXT NOT NULL,
    peer TEXT,
    person_id TEXT,
    route TEXT,
    state TEXT NOT NULL,
    reason TEXT,
    created REAL NOT NULL,
    answered REAL,
    ended REAL
);
CREATE TABLE IF NOT EXISTS raw_inbound (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel TEXT NOT NULL,
    body TEXT NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS held (
    id TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    sender TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    data TEXT NOT NULL,
    platform_time TEXT NOT NULL,
    created REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS approved (
    channel TEXT NOT NULL,
    sender TEXT NOT NULL,
    PRIMARY KEY (channel, sender)
);
CREATE TABLE IF NOT EXISTS optouts (
    channel TEXT NOT NULL,
    address TEXT NOT NULL,
    created REAL NOT NULL,
    PRIMARY KEY (channel, address)
);
CREATE TABLE IF NOT EXISTS attachments (
    id TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    content_type TEXT,
    filename TEXT,
    size INTEGER NOT NULL,
    path TEXT NOT NULL,
    created REAL NOT NULL
);
"""


def new_id(prefix: str) -> str:
    """Sortable-ish id: prefix + 6 bytes of time + 6 random bytes, hex."""
    return f"{prefix}_{int(time.time() * 1000):012x}{secrets.token_hex(6)}"


class Store:
    def __init__(self, path: Path):
        self.path = path
        self.db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(SCHEMA)
        if "consumer" not in {r["name"] for r in self.q("PRAGMA table_info(messages)")}:
            self.x("ALTER TABLE messages ADD COLUMN consumer TEXT")      # databases from before #1 item 3
        self.files = path.parent / "files"
        self.files.mkdir(parents=True, exist_ok=True)

    def q(self, sql: str, *args) -> list[sqlite3.Row]:
        return self.db.execute(sql, args).fetchall()

    def one(self, sql: str, *args) -> sqlite3.Row | None:
        return self.db.execute(sql, args).fetchone()

    def x(self, sql: str, *args) -> sqlite3.Cursor:
        return self.db.execute(sql, args)

    # ---------------- conversations ----------------
    def conversation(self, channel: str, platform_key: dict, kind: str) -> str:
        key = json.dumps(platform_key, sort_keys=True)
        row = self.one("SELECT id FROM conversations WHERE channel=? AND platform_key=?", channel, key)
        if row:
            return row["id"]
        cid = "conv_" + hashlib.sha256(f"{channel}|{key}".encode()).hexdigest()[:16]
        self.x("INSERT OR IGNORE INTO conversations (id, channel, platform_key, kind) VALUES (?,?,?,?)",
               cid, channel, key, kind)
        return cid

    def find_conversation(self, channel: str, platform_key: dict) -> str | None:
        row = self.one("SELECT id FROM conversations WHERE channel=? AND platform_key=?",
                       channel, json.dumps(platform_key, sort_keys=True))
        return row["id"] if row else None

    def alias(self, channel: str, address: str) -> str | None:
        row = self.one("SELECT conversation_id FROM aliases WHERE channel=? AND address=?", channel, address)
        return row["conversation_id"] if row else None

    def drop_alias(self, channel: str, address: str) -> None:
        self.x("DELETE FROM aliases WHERE channel=? AND address=?", channel, address)

    def rekey(self, cid: str, platform_key: dict) -> None:
        self.x("UPDATE conversations SET platform_key=? WHERE id=?", json.dumps(platform_key, sort_keys=True), cid)

    def aci_aliases(self, channel: str, cid: str) -> list[str]:
        """Account ids (not numbers) aliased to a conversation."""
        return [r["address"] for r in self.q("SELECT address FROM aliases WHERE channel=? AND conversation_id=?"
                                             " AND address NOT LIKE '+%'", channel, cid)]

    def set_alias(self, channel: str, address: str, cid: str) -> None:
        self.x("INSERT OR REPLACE INTO aliases VALUES (?,?,?)", channel, address, cid)

    def conversation_row(self, cid: str) -> sqlite3.Row | None:
        return self.one("SELECT * FROM conversations WHERE id=?", cid)

    def platform_key(self, cid: str) -> dict:
        row = self.conversation_row(cid)
        if not row:
            raise KeyError(cid)
        return json.loads(row["platform_key"])

    # ---------------- messages ----------------
    def add_message(self, conversation_id: str, direction: str, platform_id: str | None,
                    status: str, thread: str | None = None, mid: str | None = None,
                    consumer: str | None = None) -> str:
        mid = mid or new_id("msg")
        self.x("INSERT INTO messages (id, conversation_id, direction, platform_id, thread_platform_id,"
               " status, created, consumer) VALUES (?,?,?,?,?,?,?,?)",
               mid, conversation_id, direction, platform_id, thread, status, time.time(), consumer)
        return mid

    def message(self, mid: str) -> sqlite3.Row | None:
        return self.one("SELECT * FROM messages WHERE id=?", mid)

    def message_by_platform(self, conversation_id: str | None, platform_id: str) -> sqlite3.Row | None:
        if conversation_id:
            return self.one("SELECT * FROM messages WHERE conversation_id=? AND platform_id=?",
                            conversation_id, platform_id)
        return self.one("SELECT * FROM messages WHERE platform_id=?", platform_id)

    def set_message(self, mid: str, **fields) -> None:
        cols = ", ".join(f"{k}=?" for k in fields)
        self.x(f"UPDATE messages SET {cols} WHERE id=?", *fields.values(), mid)

    # ---------------- idempotency ----------------
    def idem_get(self, consumer: str, key: str, ttl: int) -> dict | None:
        row = self.one("SELECT response, created FROM idempotency WHERE consumer=? AND key=?", consumer, key)
        if row and time.time() - row["created"] < ttl:
            return json.loads(row["response"])
        return None

    def idem_put(self, consumer: str, key: str, response: dict) -> None:
        self.x("INSERT OR REPLACE INTO idempotency VALUES (?,?,?,?)",
               consumer, key, json.dumps(response), time.time())

    def idem_gc(self, ttl: int) -> None:
        self.x("DELETE FROM idempotency WHERE created < ?", time.time() - ttl)

    # ---------------- attachments ----------------
    def put_file(self, data: bytes, content_type: str | None, filename: str | None) -> dict:
        sha = hashlib.sha256(data).hexdigest()
        path = self.files / sha
        if not path.exists():
            path.write_bytes(data)
        aid = new_id("att")
        self.x("INSERT INTO attachments VALUES (?,?,?,?,?,?,?)",
               aid, sha, content_type, filename, len(data), str(path), time.time())
        return {"type": "file", "attachment_id": aid, "content_type": content_type,
                "filename": filename, "size": len(data), "sha256": sha}

    def file(self, aid: str) -> sqlite3.Row | None:
        return self.one("SELECT * FROM attachments WHERE id=?", aid)
