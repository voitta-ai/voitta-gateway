"""voitta-gateway as a library: the same core, embedded in one long-running agent
that owns its identities, with no HTTP listener.

    from voitta_gateway import embed

    gw = embed.build("gateway.yaml", consumer="leads")

    async def on_event(event: dict) -> None:
        if event["type"].endswith("message.received.v1"):
            d = event["data"]
            gw.send({"to": {"conversation_id": d["conversation_id"]},
                     "parts": [{"type": "text", "text": "got it"}],
                     "reply_to": d["message_id"]})

    gw.on_event(on_event, types=["message.*"])
    await gw.start()
    ...
    await gw.stop()

Events are the same CloudEvents envelopes the service streams, from the same
persisted log, so `after` resumes exactly where a previous run stopped.
`send` takes the body of `POST /v1/messages` and returns its result.

One owner per identity holds here too: an embedded gateway owns its Signal
account's signal-cli and its Slack app's Socket Mode connection, so nothing else
may run against either. Adapters that need public HTTP (`adapters.phone`, Slack
calls) are service-only and refused at build time.
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Awaitable, Callable

from . import config as config_mod
from .build import build_gateway
from .config import Config
from .core import Gateway
from .events import event_seq, matches

log = logging.getLogger(__name__)

Callback = Callable[[dict], Awaitable[None]]


class Embedded:
    def __init__(self, gw: Gateway, consumer: str):
        self.gw = gw
        self.consumer = consumer
        c = gw.cfg.consumers.get(consumer)
        self._patterns = c.events if c else ["*"]
        self._subs: list[tuple[Callback, list[str] | None, str]] = []
        self._tasks: list[asyncio.Task] = []
        self._started = False

    def on_event(self, callback: Callback, *, types: list[str] | None = None, after: str = "now") -> None:
        """Call `callback(event)` for every new event, in order. `types` narrows by
        type pattern (e.g. "message.*"); `after` is an event id to resume after,
        or "now" for only what arrives from here on. A callback that raises is
        logged and the event is not retried."""
        self._subs.append((callback, types, after))
        if self._started:
            self._tasks.append(asyncio.create_task(self._pump(callback, types, self._cursor(after))))

    def _cursor(self, after: str) -> int:
        retval = self.gw.events.last_seq() if after == "now" else event_seq(after)
        return retval

    async def start(self) -> None:
        # Cursors are fixed before any adapter runs: adapters emit events as soon
        # as they start (channel.up, and Signal replays unprocessed envelopes), and
        # a pump that read "now" after that would skip them.
        cursors = [self._cursor(after) for _cb, _types, after in self._subs]
        for (cb, types, _after), cursor in zip(self._subs, cursors):
            self._tasks.append(asyncio.create_task(self._pump(cb, types, cursor)))
        self._started = True
        await self.gw.start()
        self.gw.outbox.start()

    def send(self, body: dict) -> dict:
        """Queue an outbound message: the `POST /v1/messages` body, the same result."""
        retval = self.gw.send_message(self.consumer, body)
        return retval

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        await self.gw.calls.stop()
        await self.gw.outbox.stop()
        for adapter in {*self.gw.messaging.values(), *self.gw.voice.values()}:
            try:
                await adapter.stop()
            except Exception:
                log.exception("stopping %s", adapter.name)

    async def _pump(self, callback: Callback, types: list[str] | None, cursor: int) -> None:
        events = self.gw.events
        while True:
            await events.wait(cursor, 30.0)
            batch = events.after(cursor)
            for e in batch:
                cursor = event_seq(e["id"])
                if not matches(e["type"], self._patterns) or (types and not matches(e["type"], types)):
                    continue
                try:
                    await callback(e)
                except Exception:
                    log.exception("on_event callback failed on %s", e["id"])


def build(cfg: Config | str | Path, *, consumer: str = "local", signal_port_factory=None) -> Embedded:
    """An embedded gateway from a Config or a gateway.yaml path. `consumer` names
    this agent for outbound attribution; if it is listed under `consumers`, its
    `events` patterns filter what `on_event` delivers."""
    if not isinstance(cfg, Config):
        cfg = config_mod.load(cfg)
    gw = build_gateway(cfg, signal_port_factory=signal_port_factory)
    retval = Embedded(gw, consumer)
    return retval
