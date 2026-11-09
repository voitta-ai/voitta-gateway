"""Call state machine and routing, shared by every voice adapter.

ringing -> (agent connected, answered) -> answered -> ended
initiated -> ringing -> answered -> ended          (outbound)

The agent's media socket is opened while the call still rings, so a dead
agent is found before the caller hears anything; `start` is sent only once
the platform reports the call answered and its audio is flowing.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

from .adapters.base import CallAdapter, Unsupported
from .config import Route
from .core import ApiError, Gateway
from .media import AudioPort, MediaSession
from .store import new_id

log = logging.getLogger(__name__)


@dataclass
class Call:
    id: str
    channel: str
    adapter: CallAdapter
    direction: str                     # inbound | outbound
    peer: str                          # the other party (E.164 or Signal ACI)
    local: str                         # our number / account
    person_id: str | None
    route_name: str | None
    route: Route | None
    platform_id: str | None = None
    state: str = "ringing"
    created: float = field(default_factory=time.time)
    answered_at: float | None = None
    media: MediaSession | None = None
    reason: str | None = None


class CallManager:
    def __init__(self, gw: Gateway):
        self.gw = gw
        self.calls: dict[str, Call] = {}
        self.tasks: set[asyncio.Task] = set()

    # ------------------------------------------------------------ helpers
    def _spawn(self, coro) -> None:
        t = asyncio.create_task(coro)
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)

    def by_platform(self, channel: str, platform_id: str) -> Call | None:
        for c in self.calls.values():
            if c.channel == channel and c.platform_id == platform_id:
                return c
        return None

    def get(self, call_id: str) -> Call:
        c = self.calls.get(call_id)
        if not c:
            row = self.gw.store.one("SELECT * FROM calls WHERE id=?", call_id)
            if row:
                raise ApiError(409, "call_ended", f"call {call_id} is {row['state']}")
            raise ApiError(404, "unknown_call", call_id)
        return c

    def _event(self, c: Call, etype: str, **data) -> None:
        self.gw.events.append(etype, self.gw.source(c.channel),
                              {"call_id": c.id, "channel": c.channel, "direction": c.direction,
                               "from": c.peer if c.direction == "inbound" else c.local,
                               "to": c.local if c.direction == "inbound" else c.peer,
                               "person_id": c.person_id, **data},
                              subject=c.id)

    def _save(self, c: Call) -> None:
        self.gw.store.x(
            "INSERT OR REPLACE INTO calls VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            c.id, c.channel, c.platform_id, c.direction, c.peer, c.person_id, c.route_name, c.state,
            c.reason, c.created, c.answered_at, time.time() if c.state == "ended" else None)

    def _route(self, name: str | None) -> tuple[Route | None, str | None]:
        if not name:
            return None, None
        r = self.gw.cfg.routes.get(name)
        if not r:
            raise ApiError(422, "unknown_route", name)
        return r, self.gw.cfg.secret(r.media_secret_secret)

    def _media(self, c: Call, secret: str) -> MediaSession:
        return MediaSession(c.id, c.route, secret,
                            on_hangup=lambda reason: self.hangup(c.id, reason),
                            on_lost=lambda: self._agent_lost(c.id))

    def describe(self, c: Call) -> dict:
        return {"call_id": c.id, "channel": c.channel, "direction": c.direction, "state": c.state,
                "peer": c.peer, "local": c.local, "person_id": c.person_id, "route": c.route_name,
                "reason": c.reason, "answered": c.answered_at is not None}

    # ------------------------------------------------------------ adapter callbacks
    def incoming(self, adapter: CallAdapter, platform_id: str, peer: str, local: str,
                 route_name: str | None, peer_alt: str | None = None) -> Call:
        """A call is ringing on a platform. Returns at once; the work runs in a task."""
        person = self.gw.identity.lookup(adapter.name, peer, peer_alt)
        route = self.gw.cfg.routes.get(route_name) if route_name else None
        c = Call(new_id("call"), adapter.name, adapter, "inbound", peer, local, person,
                 route_name, route, platform_id=platform_id)
        self.calls[c.id] = c
        self._save(c)
        self._spawn(self._incoming(c, peer_alt))
        return c

    async def _incoming(self, c: Call, peer_alt: str | None) -> None:
        pol = self.gw.cfg.policy
        allowed = (pol.calls_allow_unknown or c.person_id is not None
                   or self.gw.allowed(c.channel, c.peer, c.person_id)
                   or (peer_alt is not None and self.gw.allowed(c.channel, peer_alt, c.person_id)))
        if not allowed:
            await self._reject(c, "policy_rejected")
            return
        self._event(c, "call.ringing", route=c.route_name)
        if not c.route:
            await self._reject(c, "no_route")
            return
        c.media = self._media(c, self.gw.cfg.secret(c.route.media_secret_secret))
        if not await c.media.connect():
            await self._fallback(c, "agent_unreachable")
            return
        if c.state == "ended":
            await c.media.stop("ended")
            return
        if c.route.answer == "auto":
            await c.adapter.answer(c.id, c.platform_id)

    def outbound_ringing(self, call_id: str) -> None:
        c = self.calls.get(call_id)
        if c and c.state == "initiated":
            c.state = "ringing"
            self._save(c)
            self._event(c, "call.ringing", route=c.route_name)

    async def connected(self, call_id: str, port: AudioPort) -> None:
        """Answered on the platform and audio is flowing: hand it to the agent."""
        c = self.calls.get(call_id)
        if not c or c.state == "ended":
            await port.close()
            return
        if c.state == "answered":
            return
        c.state, c.answered_at = "answered", time.time()
        self._save(c)
        self._event(c, "call.answered")
        if c.media:
            caps = c.adapter.call_capabilities
            await c.media.start(port, {"channel": c.channel, "direction": c.direction,
                                       "from": c.peer if c.direction == "inbound" else c.local,
                                       "to": c.local if c.direction == "inbound" else c.peer,
                                       "person_id": c.person_id,
                                       "capabilities": {"dtmf": bool(caps.get("dtmf")),
                                                        "transfer": caps.get("transfer", [])}})

    def state_note(self, call_id: str, state: str) -> None:
        """Transient platform states (Signal RECONNECTING) worth telling observers about."""
        c = self.calls.get(call_id)
        if c:
            self._event(c, "call.state", state=state)

    async def dtmf(self, call_id: str, digit: str) -> None:
        c = self.calls.get(call_id)
        if not c:
            return
        self._event(c, "call.dtmf", digit=digit)
        if c.media:
            await c.media.dtmf(digit)

    async def ended(self, call_id: str, reason: str) -> None:
        c = self.calls.pop(call_id, None)
        if not c:
            return
        c.state, c.reason = "ended", reason
        self._save(c)
        if c.media:
            await c.media.stop(reason)
        duration = round(time.time() - c.answered_at, 1) if c.answered_at else 0
        self._event(c, "call.ended", reason=reason, duration_s=duration,
                    answered=c.answered_at is not None)

    # ------------------------------------------------------------ failure paths
    async def _reject(self, c: Call, reason: str) -> None:
        try:
            await c.adapter.reject(c.id, c.platform_id, reason)
        except Exception:
            log.exception("call %s: reject failed", c.id)
        await self.ended(c.id, reason)

    async def _fallback(self, c: Call, reason: str) -> None:
        fb = c.route.fallback if c.route else "reject"
        if fb.startswith("transfer:") and "blind" in c.adapter.call_capabilities.get("transfer", []):
            try:
                if c.state != "answered" and c.direction == "inbound":
                    await c.adapter.answer(c.id, c.platform_id)
                await c.adapter.transfer(c.id, c.platform_id, fb.split(":", 1)[1])
                self._event(c, "call.transferred", to=fb.split(":", 1)[1], reason=reason)
                return
            except Exception:
                log.exception("call %s: fallback transfer failed", c.id)
        if c.state == "answered" or c.direction == "outbound" or fb == "hangup":
            try:
                await c.adapter.hangup(c.id, c.platform_id)
            except Exception:
                log.exception("call %s: hangup failed", c.id)
            await self.ended(c.id, reason)
        else:
            await self._reject(c, reason)

    async def _agent_lost(self, call_id: str) -> None:
        c = self.calls.get(call_id)
        if c:
            log.warning("call %s: agent did not come back within %.1fs", call_id, c.route.grace_s)
            await self._fallback(c, "agent_lost")

    # ------------------------------------------------------------ API
    async def start_call(self, body: dict) -> dict:
        via = body.get("via")
        to = body.get("to")
        if isinstance(to, dict) and "person_id" in to:
            addrs = self.gw.identity.addresses(to["person_id"])
            via = via or next((ch for ch in ("phone", "signal", "slack") if ch in self.gw.voice and addrs.get(ch)),
                              None)
            if not via or not addrs.get(via):
                raise ApiError(422, "no_route", f"no voice channel for {to['person_id']}")
            address = addrs[via][0]
        elif isinstance(to, str):
            address = to
        else:
            raise ApiError(422, "bad_target", "to must be an address string or {person_id}")
        adapter = self.gw.voice.get(via or "")
        if not adapter:
            raise ApiError(422, "unknown_channel", f"no voice adapter {via!r}")
        if not adapter.call_capabilities.get("outbound"):
            raise ApiError(422, "unsupported", f"{via} cannot place calls")
        if via == "phone" and self.gw.emergency(address):
            raise ApiError(403, "emergency_number_blocked", "outbound calls to emergency numbers are blocked")
        if self.gw.opted_out(via, address.split("/")[-1] if via == "slack" else address):
            raise ApiError(409, "opted_out", "recipient opted out")
        route_name = body.get("route") or getattr(adapter, "route", None)
        route, secret = self._route(route_name)
        if not route:
            raise ApiError(422, "no_route", "a route (agent media URL) is required")
        person = self.gw.identity.lookup(via, address)
        c = Call(new_id("call"), via, adapter, "outbound", address, getattr(adapter, "account", ""),
                 person, route_name, route, state="initiated")
        self.calls[c.id] = c
        self._save(c)
        self._spawn(self._outbound(c, secret))
        return {"call_id": c.id, "state": "initiated", "channel": via}

    async def _outbound(self, c: Call, secret: str) -> None:
        c.media = self._media(c, secret)
        if not await c.media.connect():
            await self.ended(c.id, "agent_unreachable")
            return
        try:
            c.platform_id = await c.adapter.dial(c.id, c.peer)
            self._save(c)
        except Exception as e:
            log.exception("call %s: dial failed", c.id)
            await self.ended(c.id, f"dial_failed: {e}"[:200])

    async def answer(self, call_id: str) -> dict:
        c = self.get(call_id)
        if c.direction != "inbound" or c.state != "ringing":
            raise ApiError(409, "bad_state", f"call is {c.direction} / {c.state}")
        if not c.media or c.media.ws is None:
            raise ApiError(409, "agent_not_connected", "no agent media socket for this call")
        await c.adapter.answer(c.id, c.platform_id)
        return self.describe(c)

    async def reject(self, call_id: str) -> dict:
        c = self.get(call_id)
        if c.state != "ringing" or c.direction != "inbound":
            raise ApiError(409, "bad_state", f"call is {c.direction} / {c.state}")
        await self._reject(c, "rejected")
        return self.describe(c)

    async def hangup(self, call_id: str, reason: str = "hangup") -> dict:
        c = self.calls.get(call_id)
        if not c:
            return {"call_id": call_id, "state": "ended"}
        try:
            if c.state == "ringing" and c.direction == "inbound":
                await c.adapter.reject(c.id, c.platform_id, reason)
            elif c.platform_id:
                await c.adapter.hangup(c.id, c.platform_id)
        except Exception:
            log.exception("call %s: hangup failed", c.id)
        await self.ended(c.id, reason)
        return self.describe(c)

    async def send_dtmf(self, call_id: str, digits: str) -> dict:
        c = self.get(call_id)
        if not c.adapter.call_capabilities.get("dtmf"):
            raise ApiError(422, "unsupported", f"{c.channel} has no DTMF")
        await c.adapter.send_dtmf(c.id, c.platform_id, digits)
        return self.describe(c)

    async def transfer(self, call_id: str, to: str, mode: str) -> dict:
        c = self.get(call_id)
        if mode not in c.adapter.call_capabilities.get("transfer", []):
            raise ApiError(422, "unsupported", f"{c.channel} does not support {mode} transfer")
        if c.channel == "phone" and self.gw.emergency(to):
            raise ApiError(403, "emergency_number_blocked", "transfers to emergency numbers are blocked")
        try:
            await c.adapter.transfer(c.id, c.platform_id, to)
        except Unsupported as e:
            raise ApiError(422, "unsupported", str(e))
        self._event(c, "call.transferred", to=to)
        return self.describe(c)

    async def stop(self) -> None:
        for cid in list(self.calls):
            await self.hangup(cid, "gateway_shutdown")
