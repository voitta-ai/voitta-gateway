"""The event log: every event is persisted before anyone is told about it.

Envelope = CloudEvents 1.0 (JSON format): `time` is when it happened on the
platform; `ingesttime` (extension) is when the gateway got it; `seq`
(extension) counts events per conversation so consumers can spot gaps.
Ids are `evt_<global sequence>`, so `?after=<id>` is a plain cursor.
"""
from __future__ import annotations

import asyncio
import fnmatch
import json
from datetime import datetime, timezone

from . import EVENT_PREFIX
from .store import Store


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def ms_to_iso(ms: int | float) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat(
        timespec="milliseconds").replace("+00:00", "Z")


def event_seq(event_id: str | None) -> int:
    if not event_id:
        return 0
    try:
        return int(event_id.removeprefix("evt_"))
    except ValueError:
        return 0


def matches(etype: str, patterns: list[str]) -> bool:
    short = etype.removeprefix(EVENT_PREFIX)
    short = short.rsplit(".v", 1)[0] if short.rsplit(".v", 1)[-1].isdigit() else short
    return any(fnmatch.fnmatchcase(short, p) or fnmatch.fnmatchcase(etype, p) for p in patterns)


class EventLog:
    def __init__(self, store: Store):
        self.store = store
        self._changed = asyncio.Condition()

    def append(self, etype: str, source: str, data: dict, *, subject: str | None = None,
               time: str | None = None, traceparent: str | None = None) -> dict:
        """Persist one event and wake the streams. `etype` is short: 'message.received'."""
        full = f"{EVENT_PREFIX}{etype}.v1"
        conv_seq = None
        if subject and subject.startswith("conv_"):
            self.store.x("UPDATE conversations SET last_seq = last_seq + 1 WHERE id=?", subject)
            row = self.store.one("SELECT last_seq FROM conversations WHERE id=?", subject)
            conv_seq = row["last_seq"] if row else None
        ingest = now_iso()
        cur = self.store.x(
            "INSERT INTO events (type, source, subject, conv_seq, time, ingesttime, data, traceparent)"
            " VALUES (?,?,?,?,?,?,?,?)",
            full, source, subject, conv_seq, time or ingest, ingest, json.dumps(data), traceparent)
        event = self._envelope(cur.lastrowid, full, source, subject, conv_seq, time or ingest,
                               ingest, data, traceparent)
        asyncio.get_event_loop().create_task(self._notify())
        return event

    async def _notify(self) -> None:
        async with self._changed:
            self._changed.notify_all()

    @staticmethod
    def _envelope(seq, etype, source, subject, conv_seq, time, ingest, data, traceparent) -> dict:
        e = {"specversion": "1.0", "id": f"evt_{seq}", "source": source, "type": etype,
             "datacontenttype": "application/json", "time": time, "ingesttime": ingest}
        if subject:
            e["subject"] = subject
        if conv_seq is not None:
            e["seq"] = conv_seq
        if traceparent:
            e["traceparent"] = traceparent
        e["data"] = data
        return e

    def after(self, cursor: int, limit: int = 500) -> list[dict]:
        rows = self.store.q("SELECT * FROM events WHERE seq > ? ORDER BY seq LIMIT ?", cursor, limit)
        return [self._envelope(r["seq"], r["type"], r["source"], r["subject"], r["conv_seq"],
                               r["time"], r["ingesttime"], json.loads(r["data"]), r["traceparent"])
                for r in rows]

    def last_seq(self) -> int:
        row = self.store.one("SELECT MAX(seq) AS m FROM events")
        return row["m"] or 0

    async def wait(self, cursor: int, timeout: float) -> None:
        """Return when an event newer than `cursor` exists, or after `timeout`."""
        if self.last_seq() > cursor:
            return
        try:
            async with self._changed:
                await asyncio.wait_for(self._changed.wait_for(lambda: self.last_seq() > cursor), timeout)
        except asyncio.TimeoutError:
            pass
