"""Durable outbox: every outbound operation is written down first, then sent by a
worker per channel with per-conversation pacing and retries. Survives restarts."""
from __future__ import annotations

import asyncio
import json
import logging
import time

from .adapters.base import PermanentError
from .core import Gateway

log = logging.getLogger(__name__)


class Outbox:
    def __init__(self, gw: Gateway):
        self.gw = gw
        self.wake: dict[str, asyncio.Event] = {}
        self.last_sent: dict[str, float] = {}           # conversation_id -> monotonic time
        self.tasks: list[asyncio.Task] = []

    def enqueue(self, op_id: str, channel: str, cid: str, op: str, payload: dict) -> None:
        self.gw.store.x("INSERT INTO outbox (message_id, channel, conversation_id, op, payload, next_at)"
                        " VALUES (?,?,?,?,?,?)", op_id, channel, cid, op, json.dumps(payload), time.time())
        if channel in self.wake:
            self.wake[channel].set()

    def start(self) -> None:
        for name in self.gw.messaging:
            self.wake[name] = asyncio.Event()
            self.wake[name].set()                       # pick up whatever survived a restart
            self.tasks.append(asyncio.create_task(self._worker(name), name=f"outbox-{name}"))

    async def stop(self) -> None:
        for t in self.tasks:
            t.cancel()

    async def _worker(self, channel: str) -> None:
        adapter = self.gw.messaging[channel]
        ev = self.wake[channel]
        while True:
            row = self.gw.store.one(
                "SELECT * FROM outbox WHERE channel=? AND done=0 ORDER BY next_at, rowid LIMIT 1", channel)
            if row is None:
                ev.clear()
                await ev.wait()
                continue
            wait = row["next_at"] - time.time()
            if wait > 0:
                ev.clear()
                try:
                    await asyncio.wait_for(ev.wait(), wait)
                except asyncio.TimeoutError:
                    pass
                continue
            # per-conversation pacing (Slack: ~1 message per second per channel)
            since = time.monotonic() - self.last_sent.get(row["conversation_id"], 0)
            if since < adapter.min_interval_s:
                await asyncio.sleep(adapter.min_interval_s - since)
            await self._run(adapter, row)
            self.last_sent[row["conversation_id"]] = time.monotonic()

    async def _run(self, adapter, row) -> None:
        store, events = self.gw.store, self.gw.events
        op, payload, oid, cid = row["op"], json.loads(row["payload"]), row["message_id"], row["conversation_id"]
        src = self.gw.source(adapter.name)
        sent = store.message(oid) if op == "send" else None
        who = {"addressed_to": sent["consumer"] if sent else payload.get("consumer")}   # the sender only
        if sent:
            who["person_id"] = self.gw.peer_person(cid)
        try:
            if op == "send":
                pid, thread = await adapter.send(payload["key"], payload["parts"], payload.get("reply_to"))
                store.set_message(oid, platform_id=pid, thread_platform_id=thread, status="sent")
                events.append("message.status", src, {"conversation_id": cid, "message_id": oid,
                                                      "status": "sent", **who}, subject=cid)
            elif op == "edit":
                await adapter.edit(payload["key"], payload["target"]["platform_id"], payload["parts"])
                if payload.get("message_id"):                     # keep recorded history current
                    store.set_message(payload["message_id"], parts=json.dumps(payload["parts"]))
            elif op == "delete":
                await adapter.delete(payload["key"], payload["target"]["platform_id"])
                if payload.get("message_id"):
                    store.set_message(payload["message_id"], parts=None)
            elif op == "react":
                await adapter.react(payload["key"], payload["target"], payload["emoji"],
                                    payload.get("remove", False))
            store.x("UPDATE outbox SET done=1 WHERE message_id=?", oid)
            if op != "send":
                events.append("operation.done", src, {"op_id": oid, "op": op, "conversation_id": cid,
                                                       **who}, subject=cid)
        except Exception as e:
            attempts = row["attempts"] + 1
            permanent = isinstance(e, PermanentError) or attempts >= self.gw.cfg.max_send_attempts
            log.warning("%s %s %s attempt %d failed: %s", adapter.name, op, oid, attempts, e)
            if permanent:
                store.x("UPDATE outbox SET done=1, attempts=? WHERE message_id=?", attempts, oid)
                if op == "send":
                    store.set_message(oid, status="failed", error=str(e)[:500])
                    events.append("message.status", src, {"conversation_id": cid, "message_id": oid,
                                                          "status": "failed", "error": str(e)[:500], **who},
                                  subject=cid)
                else:
                    events.append("operation.failed", src, {"op_id": oid, "op": op, "conversation_id": cid,
                                                            "error": str(e)[:500], **who}, subject=cid)
            else:
                backoff = min(60, 2 ** attempts)
                store.x("UPDATE outbox SET attempts=?, next_at=? WHERE message_id=?",
                        attempts, time.time() + backoff, oid)
