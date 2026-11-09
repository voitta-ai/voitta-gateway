"""voitta-media/1: the per-call WebSocket between the gateway (client) and an agent (server).

gateway -> agent: connected, start, media, mark(name, cleared), dtmf, error, stop
agent -> gateway: media, mark(name), clear, hangup

The gateway owns playback timing: agent audio of any length is queued and
played out in real time, one frame per `frame_ms`. A mark comes back when the
audio queued before it has been played (cleared=false) or dropped by a clear
(cleared=true), so the agent knows what the caller actually heard.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
from collections import deque
from typing import Awaitable, Callable, Protocol
from urllib.parse import urlencode

import websockets

from . import MEDIA_PROTOCOL
from .audio import Framer, Resampler, frame_bytes, silence
from .config import Route

log = logging.getLogger(__name__)


class AudioPort(Protocol):
    """The platform side of a call's audio, in mono pcm16le at `rate`."""
    rate: int
    continuous: bool          # wants silence while the agent is quiet (Pulse); Telnyx does not

    def frames(self): ...     # async iterator of inbound pcm16le chunks (any size)
    async def write(self, pcm: bytes) -> None: ...
    async def clear(self) -> None: ...
    async def close(self) -> None: ...


def sign(secret: str, call_id: str, ts: int) -> str:
    return hmac.new(secret.encode(), f"{call_id}.{ts}".encode(), hashlib.sha256).hexdigest()


def verify(secret: str, call_id: str, ts: str | int, sig: str, max_age_s: int = 60) -> bool:
    """For agents: check the query string the gateway connected with."""
    try:
        ts = int(ts)
    except (TypeError, ValueError):
        return False
    if abs(time.time() - ts) > max_age_s:
        return False
    return hmac.compare_digest(sign(secret, call_id, ts), sig or "")


def signed_url(base: str, secret: str, call_id: str) -> str:
    ts = int(time.time())
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}{urlencode({'call_id': call_id, 'ts': ts, 'sig': sign(secret, call_id, ts)})}"


class Player:
    """Agent audio queue, played out one frame per tick; marks ride in the queue."""

    def __init__(self, rate: int, frame_ms: int, max_buffer_s: float):
        self.rate, self.frame_ms = rate, frame_ms
        self.fbytes = frame_bytes(rate, frame_ms)
        self.max_bytes = int(max_buffer_s * rate) * 2
        self.items: deque[tuple[str, bytes | str]] = deque()
        self.queued = 0

    def push_audio(self, pcm: bytes) -> bool:
        if len(pcm) % 2:
            pcm = pcm[:-1]
        if self.queued + len(pcm) > self.max_bytes:
            return False
        if pcm:
            self.items.append(("audio", pcm))
            self.queued += len(pcm)
        return True

    def push_mark(self, name: str) -> None:
        self.items.append(("mark", name))

    def clear(self) -> list[str]:
        marks = [v for k, v in self.items if k == "mark"]
        self.items.clear()
        self.queued = 0
        return marks

    def next_frame(self) -> tuple[bytes | None, list[str]]:
        """One frame of audio (None if nothing queued) and the marks it completes."""
        done: list[str] = []
        out = bytearray()
        while self.items and len(out) < self.fbytes:
            kind, val = self.items[0]
            if kind == "mark":
                if out:                       # audio before this mark is still in this frame
                    break
                done.append(val)
                self.items.popleft()
                continue
            need = self.fbytes - len(out)
            out += val[:need]
            if len(val) > need:
                self.items[0] = ("audio", val[need:])
            else:
                self.items.popleft()
            self.queued -= min(need, len(val))
        # marks right after the audio just taken are complete once this frame plays
        while out and self.items and self.items[0][0] == "mark":
            done.append(self.items.popleft()[1])
        if not out:
            return None, done
        if len(out) < self.fbytes:
            out += bytes(self.fbytes - len(out))
        return bytes(out), done

    @property
    def buffered_ms(self) -> int:
        return self.queued * 1000 // (2 * self.rate)


class MediaSession:
    def __init__(self, call_id: str, route: Route, secret: str, *,
                 on_hangup: Callable[[str], Awaitable[None]],
                 on_lost: Callable[[], Awaitable[None]]):
        self.call_id, self.route, self.secret = call_id, route, secret
        self.on_hangup, self.on_lost = on_hangup, on_lost
        fmt = route.media_format
        self.agent_rate = fmt.sample_rate
        self.player = Player(self.agent_rate, route.frame_ms, route.max_buffer_s)
        self.ws: websockets.ClientConnection | None = None
        self.start_msg: dict | None = None
        self.port: AudioPort | None = None
        self.tasks: list[asyncio.Task] = []
        self.closed = False
        self.seq = 0
        self.sent_samples = 0
        self._lost = False

    # ------------------------------------------------------------ connection
    async def connect(self) -> bool:
        url = signed_url(self.route.media_url, self.secret, self.call_id)
        try:
            self.ws = await asyncio.wait_for(
                websockets.connect(url, max_size=2 ** 22, open_timeout=self.route.connect_timeout_s),
                self.route.connect_timeout_s)
        except Exception as e:
            log.warning("call %s: agent at %s unreachable: %s", self.call_id, self.route.media_url, e)
            return False
        await self._send({"event": "connected", "protocol": MEDIA_PROTOCOL, "call_id": self.call_id})
        self.tasks.append(asyncio.create_task(self._reader(), name=f"media-reader-{self.call_id}"))
        return True

    async def _send(self, msg: dict) -> None:
        ws = self.ws
        if ws is None:
            return
        try:
            await ws.send(json.dumps(msg, separators=(",", ":")))
        except websockets.ConnectionClosed:
            pass

    async def start(self, port: AudioPort, info: dict) -> None:
        """The call is answered and audio flows: tell the agent, start both pumps."""
        self.port = port
        self.start_msg = {"event": "start", "call_id": self.call_id, **info,
                          "media_format": self.route.media_format.model_dump(),
                          "frame_ms": self.route.frame_ms}
        await self._send(self.start_msg)
        self.tasks.append(asyncio.create_task(self._uplink(), name=f"media-up-{self.call_id}"))
        self.tasks.append(asyncio.create_task(self._playout(), name=f"media-out-{self.call_id}"))

    async def dtmf(self, digit: str) -> None:
        await self._send({"event": "dtmf", "digit": digit})

    async def play(self, pcm: bytes) -> None:
        """Gateway-originated audio at the agent rate (fallback messages)."""
        self.player.push_audio(pcm)

    async def drained(self, timeout: float) -> None:
        end = time.monotonic() + timeout
        while self.player.items and time.monotonic() < end:
            await asyncio.sleep(0.05)

    async def stop(self, reason: str) -> None:
        if self.closed:
            return
        self.closed = True
        await self._send({"event": "stop", "reason": reason})
        for t in self.tasks:
            if t is not asyncio.current_task():
                t.cancel()
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
        if self.port:
            await self.port.close()

    # ------------------------------------------------------------ agent -> gateway
    async def _reader(self) -> None:
        while not self.closed:
            ws = self.ws
            try:
                async for raw in ws:
                    await self._on_agent(raw)
            except websockets.ConnectionClosed:
                pass
            except Exception:
                log.exception("call %s: agent message handling failed", self.call_id)
            if self.closed:
                return
            if not await self._reconnect():
                self._lost = True
                await self.on_lost()
                return

    async def _reconnect(self) -> bool:
        """Agent socket dropped mid-call: try again until the grace period runs out."""
        self.ws = None
        deadline = time.monotonic() + self.route.grace_s
        log.warning("call %s: agent socket lost, retrying for %.1fs", self.call_id, self.route.grace_s)
        while time.monotonic() < deadline and not self.closed:
            url = signed_url(self.route.media_url, self.secret, self.call_id)
            try:
                left = max(0.2, deadline - time.monotonic())
                self.ws = await asyncio.wait_for(websockets.connect(url, max_size=2 ** 22), left)
                await self._send({"event": "connected", "protocol": MEDIA_PROTOCOL,
                                  "call_id": self.call_id, "resumed": True})
                if self.start_msg:
                    await self._send(self.start_msg)
                log.info("call %s: agent reconnected", self.call_id)
                return True
            except Exception:
                await asyncio.sleep(0.5)
        return False

    async def _on_agent(self, raw: str | bytes) -> None:
        if isinstance(raw, bytes):
            await self._send({"event": "error", "code": "binary_not_supported",
                              "message": "send JSON text messages"})
            return
        msg = json.loads(raw)
        ev = msg.get("event")
        if ev == "media":
            if self.start_msg is None:
                return                                      # nothing plays before start
            pcm = base64.b64decode(msg.get("payload") or "")
            if not self.player.push_audio(pcm):
                await self._send({"event": "error", "code": "buffer_full",
                                  "message": f"more than {self.route.max_buffer_s}s queued",
                                  "buffered_ms": self.player.buffered_ms})
        elif ev == "mark":
            self.player.push_mark(str(msg.get("name", "")))
        elif ev == "clear":
            for name in self.player.clear():
                await self._send({"event": "mark", "name": name, "cleared": True})
            if self.port:
                await self.port.clear()
        elif ev == "hangup":
            await self.on_hangup(str(msg.get("reason") or "agent_hangup"))
        else:
            await self._send({"event": "error", "code": "unknown_event", "message": str(ev)})

    # ------------------------------------------------------------ pumps
    async def _uplink(self) -> None:
        """Caller audio: platform rate -> agent rate -> fixed frames."""
        port = self.port
        rs = Resampler(port.rate, self.agent_rate)
        framer = Framer(frame_bytes(self.agent_rate, self.route.frame_ms))
        try:
            async for chunk in port.frames():
                for frame in framer.push(rs(chunk)):
                    self.seq += 1
                    ts_ms = self.sent_samples * 1000 // self.agent_rate
                    self.sent_samples += len(frame) // 2
                    if self.ws is not None:
                        await self._send({"event": "media", "seq": self.seq, "ts_ms": ts_ms,
                                          "payload": base64.b64encode(frame).decode()})
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("call %s: uplink failed", self.call_id)

    async def _playout(self) -> None:
        """Agent audio: one frame per tick, resampled to the platform rate."""
        port = self.port
        rs = Resampler(self.agent_rate, port.rate)
        out_frames = Framer(frame_bytes(port.rate, self.route.frame_ms))   # resampler output is uneven
        quiet = silence(port.rate, self.route.frame_ms)
        tick = self.route.frame_ms / 1000
        loop = asyncio.get_running_loop()
        next_at = loop.time()
        try:
            while not self.closed:
                frame, marks = self.player.next_frame()
                if frame is not None:
                    for f in out_frames.push(rs(frame)):
                        await port.write(f)
                elif (tail := out_frames.flush()) is not None:
                    await port.write(tail)
                elif port.continuous:
                    await port.write(quiet)
                for name in marks:
                    await self._send({"event": "mark", "name": name, "cleared": False})
                next_at += tick
                delay = next_at - loop.time()
                if delay < -0.2:                            # fell far behind: resync, don't burst
                    next_at = loop.time()
                    delay = 0
                await asyncio.sleep(max(0.0, delay))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("call %s: playout failed", self.call_id)
