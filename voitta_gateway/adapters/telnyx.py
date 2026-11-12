"""Phone calls via Telnyx Call Control + bidirectional media streaming.

No media server of our own: Telnyx sends signed webhooks to the public
ingress, we answer with a `stream_url`, and Telnyx opens a WebSocket to it
carrying L16 16 kHz (or PCMU 8 kHz) both ways. Only one bidirectional stream
is allowed per call.

L16 on the wire is big-endian per RFC 3551; the port checks the first voiced
audio and flips the byte order (with a warning) if the config is wrong.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import secrets
import time
import uuid
from collections import OrderedDict

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey

from ..audio import Resampler, looks_byteswapped, pcm_to_ulaw, swap16, ulaw_to_pcm
from ..config import PhoneConfig
from .base import CallAdapter, PermanentError

log = logging.getLogger(__name__)


def verify_telnyx(public_key_b64: str, signature_b64: str, timestamp: str, signed: bytes,
                  tolerance_s: int = 300) -> bool:
    try:
        if abs(time.time() - int(timestamp)) > tolerance_s:
            return False
        VerifyKey(base64.b64decode(public_key_b64)).verify(
            timestamp.encode() + b"|" + signed, base64.b64decode(signature_b64))
        return True
    except (BadSignatureError, ValueError, TypeError):
        return False


class TelnyxPort:
    """One call's media WebSocket from Telnyx, as pcm16le."""
    continuous = False

    def __init__(self, ws: WebSocket, encoding: str, rate: int, out_codec: str, byte_order: str):
        self.ws = ws
        self.encoding = encoding.upper()
        self.rate = rate
        self.out_codec = out_codec
        self.out_rate = 16000 if out_codec == "L16" else 8000
        self.big = byte_order == "big"
        self.queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=500)
        self.closed = False
        self._votes: list[bool] = []
        self._probe = bytearray()
        self._out_rs = Resampler(rate, self.out_rate)

    def feed(self, payload_b64: str) -> None:
        data = base64.b64decode(payload_b64)
        if self.encoding == "PCMU":
            pcm = ulaw_to_pcm(data)
        else:
            pcm = swap16(data) if self.big else data
            if len(self._votes) < 3:
                self._probe += pcm                            # judge 100 ms windows, not 20 ms chunks
                if len(self._probe) >= self.rate // 5:
                    verdict = looks_byteswapped(bytes(self._probe))
                    self._probe.clear()
                    if verdict is not None:
                        self._votes.append(verdict)
                        if len(self._votes) == 3 and sum(self._votes) >= 2:
                            self.big = not self.big
                            log.warning("Telnyx L16 audio looks byte-swapped; switching to %s-endian. "
                                        "Set adapters.phone.l16_byte_order: %s",
                                        "big" if self.big else "little", "big" if self.big else "little")
                            pcm = swap16(pcm)
        try:
            self.queue.put_nowait(pcm)
        except asyncio.QueueFull:
            pass                                              # agent side stalled: drop, stay real-time

    async def frames(self):
        while True:
            chunk = await self.queue.get()
            if chunk is None:
                return
            yield chunk

    async def write(self, pcm: bytes) -> None:
        if self.closed:
            return
        pcm = self._out_rs(pcm)
        if self.out_codec == "PCMU":
            wire = pcm_to_ulaw(pcm)
        else:
            wire = swap16(pcm) if self.big else pcm
        try:
            await self.ws.send_text(json.dumps({"event": "media",
                                                "media": {"payload": base64.b64encode(wire).decode()}}))
        except Exception:
            self.closed = True

    async def clear(self) -> None:
        if not self.closed:
            try:
                await self.ws.send_text('{"event":"clear"}')
            except Exception:
                self.closed = True

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            self.queue.put_nowait(None)
            try:
                await self.ws.close()
            except Exception:
                pass


class TelnyxAdapter(CallAdapter):
    name = "phone"

    def __init__(self, cfg: PhoneConfig, api_key: str, public_key: str | None,
                 http: httpx.AsyncClient | None = None):
        self.cfg = cfg
        self.account = cfg.numbers[0] if cfg.numbers else ""
        self.route = cfg.route
        self.public_key = public_key
        self.call_capabilities = {"inbound": True, "outbound": bool(cfg.connection_id and cfg.numbers),
                                  "dtmf": True, "transfer": ["blind"],
                                  "audio": "L16 16 kHz" if cfg.codec == "L16" else "PCMU 8 kHz"}
        self.http = http or httpx.AsyncClient(base_url=cfg.api_base, timeout=15,
                                              headers={"Authorization": f"Bearer {api_key}"})
        self.secret = secrets.token_bytes(32)
        self.call_of: dict[str, str] = {}                     # call_control_id -> our call id
        self.pid_of: dict[str, str] = {}                      # our call id -> call_control_id
        self.answered: set[str] = set()                       # our call ids
        self.ports: dict[str, TelnyxPort] = {}                # our call id -> port
        self.seen: OrderedDict[str, None] = OrderedDict()
        self.gw = None

    async def start(self, gw) -> None:
        self.gw = gw
        gw.channel_state(self.name, True)

    async def stop(self) -> None:
        await self.http.aclose()

    # ================================================================ ingress routes
    def media_url(self, call_id: str) -> str:
        base = self.cfg.public_base_url.rstrip("/").replace("https://", "wss://").replace("http://", "ws://")
        token = hmac.new(self.secret, call_id.encode(), hashlib.sha256).hexdigest()[:32]
        return f"{base}/telnyx/media/{call_id}?t={token}"

    def _token_ok(self, call_id: str, token: str) -> bool:
        want = hmac.new(self.secret, call_id.encode(), hashlib.sha256).hexdigest()[:32]
        return hmac.compare_digest(want, token or "")

    def mount(self, app: FastAPI) -> None:
        @app.post("/telnyx/webhook")
        async def webhook(request: Request):
            raw = await request.body()
            if self.cfg.verify_signatures:
                sig = request.headers.get("telnyx-signature-ed25519", "")
                ts = request.headers.get("telnyx-timestamp", "")
                if not self.public_key or not verify_telnyx(self.public_key, sig, ts, raw):
                    log.warning("telnyx webhook with a bad signature rejected")
                    return JSONResponse({"error": "bad signature"}, status_code=403)
            try:
                body = json.loads(raw)
            except json.JSONDecodeError:
                return JSONResponse({"error": "bad json"}, status_code=400)
            asyncio.create_task(self._webhook(body.get("data") or {}))
            return {"ok": True}

        @app.websocket("/telnyx/media/{call_id}")
        async def media(ws: WebSocket, call_id: str):
            if not self._token_ok(call_id, ws.query_params.get("t", "")):
                await ws.close(code=4403)
                return
            if self.cfg.verify_signatures and self.public_key:
                sig = ws.headers.get("telnyx-signature-ed25519")
                ts = ws.headers.get("telnyx-timestamp")
                if sig and ts and not verify_telnyx(self.public_key, sig, ts, self.media_url(call_id).encode()):
                    log.warning("call %s: Telnyx media signature did not verify (token did)", call_id)
            await ws.accept()
            await self._media(ws, call_id)

    # ================================================================ webhooks
    async def _webhook(self, data: dict) -> None:
        eid = data.get("id")
        if eid:
            if eid in self.seen:
                return
            self.seen[eid] = None
            if len(self.seen) > 5000:
                self.seen.popitem(last=False)
        etype, p = data.get("event_type"), data.get("payload") or {}
        pid = p.get("call_control_id")
        call_id = self.call_of.get(pid)
        if not call_id and p.get("client_state"):
            try:
                call_id = base64.b64decode(p["client_state"]).decode()
                if call_id in self.pid_of or call_id in self.gw.calls.calls:
                    self.call_of[pid] = call_id
                    self.pid_of[call_id] = pid
                else:
                    call_id = None
            except Exception:
                call_id = None
        try:
            if etype == "call.initiated":
                if p.get("direction") == "incoming" and not call_id:
                    if self.cfg.numbers and p.get("to") not in self.cfg.numbers:
                        log.warning("call to an unconfigured number ignored")
                        return
                    call = self.gw.calls.incoming(self, pid, p.get("from", "unknown"), p.get("to", ""),
                                                  self.route)
                    self.call_of[pid], self.pid_of[call.id] = call.id, pid
                elif call_id:
                    self.gw.calls.outbound_ringing(call_id)
            elif etype == "call.answered" and call_id:
                self.answered.add(call_id)
                await self._maybe_connected(call_id)
            elif etype == "call.hangup" and call_id:
                await self._cleanup(call_id)
                await self.gw.calls.ended(call_id, (p.get("hangup_cause") or "hangup").lower())
            elif etype == "call.dtmf.received" and call_id:
                await self.gw.calls.dtmf(call_id, str(p.get("digit", "")))
            elif etype == "streaming.failed" and call_id:
                log.error("call %s: Telnyx streaming failed: %s", call_id, p.get("failure_reason"))
                await self.gw.calls.hangup(call_id, "media_failed")
        except Exception:
            log.exception("telnyx %s failed", etype)

    async def _cleanup(self, call_id: str) -> None:
        port = self.ports.pop(call_id, None)
        if port:
            await port.close()
        self.answered.discard(call_id)
        pid = self.pid_of.pop(call_id, None)
        if pid:
            self.call_of.pop(pid, None)

    # ================================================================ media socket
    async def _media(self, ws: WebSocket, call_id: str) -> None:
        port = None
        try:
            while True:
                msg = json.loads(await ws.receive_text())
                ev = msg.get("event")
                if ev == "media" and port:
                    m = msg.get("media") or {}
                    if m.get("track", "inbound") == "inbound" and m.get("payload"):
                        port.feed(m["payload"])
                elif ev == "start":
                    fmt = (msg.get("start") or {}).get("media_format") or {}
                    enc = (fmt.get("encoding") or self.cfg.codec).upper()
                    if enc in ("AUDIO/X-MULAW", "MULAW", "G711_ULAW"):
                        enc = "PCMU"
                    rate = int(fmt.get("sample_rate") or (16000 if enc == "L16" else 8000))
                    if enc not in ("L16", "PCMU"):
                        log.error("call %s: unsupported Telnyx stream encoding %s", call_id, enc)
                        await self.gw.calls.hangup(call_id, "media_unsupported")
                        return
                    port = TelnyxPort(ws, enc, rate, self.cfg.codec, self.cfg.l16_byte_order)
                    self.ports[call_id] = port
                    await self._maybe_connected(call_id)
                elif ev == "stop":
                    break
                elif ev == "error":
                    log.error("call %s: Telnyx media error %s", call_id, msg.get("payload"))
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            if port:
                port.closed = True
                port.queue.put_nowait(None)

    async def _maybe_connected(self, call_id: str) -> None:
        port = self.ports.get(call_id)
        if port and call_id in self.answered:
            await self.gw.calls.connected(call_id, port)

    # ================================================================ commands
    def _stream_params(self, call_id: str) -> dict:
        return {"stream_url": self.media_url(call_id), "stream_track": "inbound_track",
                "stream_codec": self.cfg.codec, "stream_bidirectional_mode": "rtp",
                "stream_bidirectional_codec": self.cfg.codec,
                "client_state": base64.b64encode(call_id.encode()).decode()}

    async def _action(self, pid: str, action: str, body: dict | None = None, ok_if_ended: bool = False) -> dict:
        r = await self.http.post(f"/calls/{pid}/actions/{action}",
                                 json={"command_id": str(uuid.uuid4()), **(body or {})})
        if r.status_code == 422 and ok_if_ended and "90018" in r.text:
            return {}
        if r.status_code >= 500 or r.status_code == 429:
            raise ConnectionError(f"telnyx {action}: {r.status_code} {r.text[:300]}")
        if r.status_code >= 400:
            raise PermanentError(f"telnyx {action}: {r.status_code} {r.text[:300]}")
        return r.json() if r.content else {}

    async def answer(self, call_id: str, platform_id: str) -> None:
        await self._action(platform_id, "answer", self._stream_params(call_id))

    async def reject(self, call_id: str, platform_id: str, reason: str) -> None:
        await self._action(platform_id, "reject", {"cause": "CALL_REJECTED"}, ok_if_ended=True)
        await self._cleanup(call_id)

    async def hangup(self, call_id: str, platform_id: str) -> None:
        await self._action(platform_id, "hangup", {}, ok_if_ended=True)
        await self._cleanup(call_id)

    async def send_dtmf(self, call_id: str, platform_id: str, digits: str) -> None:
        await self._action(platform_id, "send_dtmf", {"digits": digits})

    async def transfer(self, call_id: str, platform_id: str, to: str) -> None:
        await self._action(platform_id, "transfer", {"to": to})

    async def dial(self, call_id: str, to: str) -> str:
        body = {"connection_id": self.cfg.connection_id, "to": to, "from": self.cfg.numbers[0],
                "timeout_secs": 45, "command_id": str(uuid.uuid4()), **self._stream_params(call_id)}
        r = await self.http.post("/calls", json=body)
        if r.status_code >= 400:
            raise PermanentError(f"telnyx dial: {r.status_code} {r.text[:300]}")
        pid = r.json()["data"]["call_control_id"]
        self.call_of[pid], self.pid_of[call_id] = call_id, pid
        return pid
