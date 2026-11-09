"""voitta-ask: POST a text and a recipient; get back what they said.

    calling -> ringing -> speaking -> listening -> transcribing -> done
    calling / ringing -> no_answer                 (ended before pickup)
    any -> failed                                  (gateway refused, engine error)

The service places the call through the gateway API, follows call.* events
to see it ring or end, and serves the agent side of voitta-media/1 for the
audio. Asks live in memory: a restart forgets them.
"""
from __future__ import annotations

import asyncio
import base64
import hmac
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import httpx
import yaml
from fastapi import Depends, FastAPI, Header, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from voitta_gateway.audio import silence, tone
from voitta_gateway.media import verify
from voitta_gateway.store import new_id

from .endpoint import Endpointer
from .engines import RATE, STT, TTS, build_stt, build_tts

log = logging.getLogger(__name__)
FINAL = {"done", "no_answer", "failed"}
MAX_TEXT = 800                       # ~50 s of speech; the gateway queues at most 60 s ahead


# ================================================================ config
class TTSConfig(BaseModel):
    engine: Literal["piper", "fake"] = "piper"
    model: str = "models/en_US-lessac-medium.onnx"
    length_scale: float | None = None          # >1 speaks slower


class STTConfig(BaseModel):
    engine: Literal["whisper", "fake"] = "whisper"
    model: str = "small"
    device: str = "auto"
    compute_type: str = "default"              # cuda: int8_float16
    download_root: str | None = None
    beam_size: int = 1


class ListenConfig(BaseModel):
    end_silence_s: float = 1.2
    no_speech_s: float = 8.0
    max_answer_s: float = 30.0


class GatewayConfig(BaseModel):
    url: str = "http://127.0.0.1:8700"
    token_secret: str
    route: str = "ask"                         # gateway route whose media_url points at us


class AskConfig(BaseModel):
    listen: str = "127.0.0.1:8790"
    env_file: str | None = None
    gateway: GatewayConfig
    media_secret_secret: str                   # same secret as the gateway route
    clients: dict[str, str] = {}               # client name -> token secret name
    tts: TTSConfig = TTSConfig()
    stt: STTConfig = STTConfig()
    answer: ListenConfig = ListenConfig()
    cue_tone: bool = True                      # short beep after the text: "speak now"
    closing: str = "Thank you. Goodbye."
    ring_timeout_s: float = 90.0               # not picked up by then: no_answer (Signal rings ~60 s)
    check_every_s: float = 5.0                 # also ask the gateway for the call state, in case an event was missed
    keep_s: int = 3600                         # forget finished asks after this

    def secret(self, name: str) -> str:
        if name in os.environ:
            return os.environ[name]
        if self.env_file and Path(self.env_file).exists():
            for line in Path(self.env_file).read_text().splitlines():
                k, _, v = line.partition("=")
                if k.strip() == name:
                    return v.strip().strip('"').strip("'")
        raise KeyError(f"secret {name} is not set (environment or env_file)")


def load(path: str) -> AskConfig:
    cfg = AskConfig.model_validate(yaml.safe_load(Path(path).read_text()) or {})
    base = Path(path).parent
    if cfg.env_file and not Path(cfg.env_file).is_absolute():
        cfg.env_file = str(base / cfg.env_file)
    if cfg.tts.engine == "piper" and not Path(cfg.tts.model).is_absolute():
        cfg.tts.model = str(base / cfg.tts.model)
    if cfg.stt.download_root and not Path(cfg.stt.download_root).is_absolute():
        cfg.stt.download_root = str(base / cfg.stt.download_root)
    return cfg


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


# ================================================================ state
@dataclass
class Ask:
    id: str
    client: str
    to: str
    via: str
    text: str
    language: str | None
    state: str = "calling"
    call_id: str | None = None
    prompt: bytes = b""
    transcript: str | None = None
    detected_language: str | None = None
    answer_s: float = 0.0
    end_reason: str | None = None              # end_of_speech | no_speech | max_duration | hung_up | <call reason>
    error: str | None = None
    created: float = field(default_factory=time.time)
    times: dict[str, float] = field(default_factory=dict)
    prompt_done: bool = False
    audio: bytearray = field(default_factory=bytearray)
    endpointer: Endpointer | None = None
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    finishing: bool = False

    def set(self, state: str, **kw) -> None:
        self.state = state
        self.times[state] = round(time.time() - self.created, 2)
        for k, v in kw.items():
            setattr(self, k, v)
        self.changed.set()
        self.changed = asyncio.Event()
        log.info("ask %s: %s %s", self.id, state, kw or "")

    def view(self) -> dict:
        return {"ask_id": self.id, "state": self.state, "to": self.to, "via": self.via, "call_id": self.call_id,
                "text": self.text, "transcript": self.transcript, "language": self.detected_language,
                "answer_s": round(self.answer_s, 2), "end_reason": self.end_reason, "error": self.error,
                "times": self.times}


class AskService:
    def __init__(self, cfg: AskConfig, tts: TTS | None = None, stt: STT | None = None,
                 http: httpx.AsyncClient | None = None):
        self.cfg = cfg
        self.tts = tts or build_tts(cfg.tts)
        self.stt = stt or build_stt(cfg.stt)
        self.http = http or httpx.AsyncClient(base_url=cfg.gateway.url, timeout=20, headers={
            "Authorization": f"Bearer {cfg.secret(cfg.gateway.token_secret)}"})
        self.media_secret = cfg.secret(cfg.media_secret_secret)
        self.asks: dict[str, Ask] = {}
        self.by_call: dict[str, Ask] = {}
        self.idem: dict[tuple[str, str], str] = {}
        self.call_waiters: dict[str, asyncio.Event] = {}
        self.closing_pcm = b""
        self.cue_pcm = (silence(RATE, 250) + tone(RATE, 180, hz=880, amp=5000)) if cfg.cue_tone else b""
        self.tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        if self.cfg.closing:
            self.closing_pcm = await asyncio.to_thread(self.tts.synthesize, self.cfg.closing)
        self.tasks.append(asyncio.create_task(self._follow_gateway(), name="ask-gateway-events"))
        self.tasks.append(asyncio.create_task(self._gc(), name="ask-gc"))
        self.tasks.append(asyncio.create_task(self._watchdog(), name="ask-watchdog"))

    async def stop(self) -> None:
        for t in self.tasks:
            t.cancel()
        await self.http.aclose()

    # ------------------------------------------------------------ API
    async def create(self, client: str, body: dict) -> Ask:
        text = str(body.get("text") or "").strip()
        to = body.get("to")
        if not text or not to:
            raise ApiError(422, "bad_request", "to and text are required")
        if len(text) > MAX_TEXT:
            raise ApiError(422, "text_too_long", f"text is limited to {MAX_TEXT} characters")
        key = body.get("idempotency_key")
        if key and (client, key) in self.idem and self.idem[(client, key)] in self.asks:
            return self.asks[self.idem[(client, key)]]
        ask = Ask(new_id("ask"), client, to if isinstance(to, str) else json.dumps(to),
                  body.get("via", "signal"), text, body.get("language", "en"))
        ask.prompt = await asyncio.to_thread(self.tts.synthesize, text)
        r = await self.http.post("/v1/calls", json={"to": to, "via": ask.via, "route": self.cfg.gateway.route})
        if r.status_code >= 400:
            err = (r.json().get("error") or {}) if r.headers.get("content-type", "").startswith("application/json") else {}
            raise ApiError(r.status_code if r.status_code < 500 else 502, err.get("code", "gateway_error"),
                           err.get("message", r.text[:300]))
        ask.call_id = r.json()["call_id"]
        self.asks[ask.id] = ask
        self.by_call[ask.call_id] = ask
        if key:
            self.idem[(client, key)] = ask.id
        ask.set("calling")
        if ev := self.call_waiters.pop(ask.call_id, None):
            ev.set()
        return ask

    async def wait(self, ask: Ask, timeout: float) -> Ask:
        end = time.monotonic() + timeout
        while ask.state not in FINAL and time.monotonic() < end:
            try:
                await asyncio.wait_for(ask.changed.wait(), end - time.monotonic())
            except asyncio.TimeoutError:
                break
        return ask

    # ------------------------------------------------------------ gateway events
    async def _follow_gateway(self) -> None:
        cursor = "now"
        while True:
            try:
                r = await self.http.get("/v1/events/poll", params={"after": cursor, "types": "call.*", "wait": 20},
                                        timeout=30)
                r.raise_for_status()
                body = r.json()
                if body.get("reset"):
                    log.warning("gateway event log was reset; following the new one from the start")
                cursor = body["next"]
                for e in body["events"]:
                    await self._call_event(e["type"].split(".")[3], e["data"])
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("following gateway events: %s", e)
                await asyncio.sleep(2)

    async def _call_event(self, kind: str, data: dict) -> None:
        ask = self.by_call.get(data.get("call_id"))
        if not ask or ask.state in FINAL:
            return
        if kind == "ringing" and ask.state == "calling":
            ask.set("ringing")
        elif kind == "ended":
            if not data.get("answered"):
                ask.set("no_answer", end_reason=data.get("reason"))
            else:
                await self._finish(ask, "hung_up")

    # ------------------------------------------------------------ media
    async def media(self, ws: WebSocket) -> None:
        q = ws.query_params
        call_id = q.get("call_id", "")
        if not verify(self.media_secret, call_id, q.get("ts", "0"), q.get("sig", "")):
            await ws.close(code=4403)
            return
        ask = self.by_call.get(call_id)
        if ask is None:                                   # the gateway may connect before POST /v1/calls returned
            ev = self.call_waiters.setdefault(call_id, asyncio.Event())
            try:
                await asyncio.wait_for(ev.wait(), 5)
            except asyncio.TimeoutError:
                pass
            ask = self.by_call.get(call_id)
        if ask is None:
            log.warning("media for unknown call %s", call_id)
            await ws.close(code=4404)
            return
        await ws.accept()
        try:
            while True:
                msg = json.loads(await ws.receive_text())
                await self._on_media(ws, ask, msg)
        except (WebSocketDisconnect, RuntimeError):
            pass
        if ask.state in ("speaking", "listening"):
            await self._finish(ask, "hung_up")

    async def _send(self, ws: WebSocket, msg: dict) -> None:
        try:
            await ws.send_text(json.dumps(msg))
        except Exception:
            pass

    async def _on_media(self, ws: WebSocket, ask: Ask, msg: dict) -> None:
        ev = msg.get("event")
        if ev == "start":
            if ask.prompt_done or ask.state in FINAL:     # gateway re-sent start after a reconnect
                return
            ask.set("speaking")
            audio = silence(RATE, 300) + ask.prompt + self.cue_pcm
            for i in range(0, len(audio), RATE * 2):      # 1 s per message
                await self._send(ws, {"event": "media", "payload": base64.b64encode(audio[i:i + RATE * 2]).decode()})
            await self._send(ws, {"event": "mark", "name": "prompt"})
        elif ev == "mark" and msg.get("name") == "prompt" and not ask.prompt_done:
            ask.prompt_done = True
            a = self.cfg.answer
            ask.endpointer = Endpointer(rate=RATE, end_silence_s=a.end_silence_s, no_speech_s=a.no_speech_s,
                                        max_answer_s=a.max_answer_s)
            ask.set("listening")
        elif ev == "media" and ask.state == "listening":
            frame = base64.b64decode(msg.get("payload") or "")
            ask.audio += frame
            if ask.endpointer.push(frame):
                await self._finish(ask, ask.endpointer.reason, ws)
        elif ev == "mark" and msg.get("name") == "bye":
            await self._send(ws, {"event": "hangup", "reason": "ask_done"})
        elif ev == "stop":
            if ask.state in ("speaking", "listening"):
                await self._finish(ask, "hung_up")

    async def _finish(self, ask: Ask, reason: str, ws: WebSocket | None = None) -> None:
        """Answer is over: say goodbye (if the line is still up), transcribe, done."""
        if ask.finishing or ask.state in FINAL:
            return
        ask.finishing = True
        heard = ask.state == "listening"
        ask.set("transcribing" if heard else "done", end_reason=reason,
                answer_s=len(ask.audio) / 2 / RATE, **({} if heard else {"transcript": ""}))
        if ws is not None:
            if self.closing_pcm:
                await self._send(ws, {"event": "media", "payload": base64.b64encode(self.closing_pcm).decode()})
                await self._send(ws, {"event": "mark", "name": "bye"})
            else:
                await self._send(ws, {"event": "hangup", "reason": "ask_done"})
        if not heard:
            return
        try:
            text, lang = await asyncio.to_thread(self.stt.transcribe, bytes(ask.audio), ask.language)
            ask.set("done", transcript=text, detected_language=lang)
        except Exception as e:
            log.exception("ask %s: transcription failed", ask.id)
            ask.set("failed", error=f"transcription failed: {e}")
        ask.audio = bytearray()

    async def _watchdog(self) -> None:
        """Events are the fast path; this is the safety net. While an ask waits for pickup, check
        the call itself, and give up after ring_timeout_s so an ask can never hang."""
        while True:
            await asyncio.sleep(self.cfg.check_every_s)
            for ask in list(self.asks.values()):
                if ask.state not in ("calling", "ringing"):
                    continue
                try:
                    await self._check_call(ask)
                except Exception as e:
                    log.warning("ask %s: checking call %s: %s", ask.id, ask.call_id, e)

    async def _check_call(self, ask: Ask) -> None:
        r = await self.http.get(f"/v1/calls/{ask.call_id}")
        if r.status_code == 200:
            call = r.json()
            if call.get("state") == "ended" and ask.state not in FINAL:
                log.warning("ask %s: missed call.ended, caught by the watchdog", ask.id)
                await self._call_event("ended", {"call_id": ask.call_id, "answered": call.get("answered"),
                                                 "reason": call.get("reason")})
                return
        elif r.status_code == 404:                       # the gateway lost the call (restarted)
            ask.set("failed", error="the gateway no longer knows this call")
            return
        if ask.state in ("calling", "ringing") and time.time() - ask.created > self.cfg.ring_timeout_s:
            await self.http.post(f"/v1/calls/{ask.call_id}/hangup")
            ask.set("no_answer", end_reason="ring_timeout")

    async def _gc(self) -> None:
        while True:
            await asyncio.sleep(60)
            cutoff = time.time() - self.cfg.keep_s
            for aid, a in list(self.asks.items()):
                if a.state in FINAL and a.created < cutoff:
                    self.asks.pop(aid, None)
                    self.by_call.pop(a.call_id, None)


# ================================================================ HTTP
def build_app(svc: AskService) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await svc.start()
        yield
        await svc.stop()

    app = FastAPI(title="voitta-ask", version="0.1.0", lifespan=lifespan)
    tokens = [(svc.cfg.secret(sec), name) for name, sec in svc.cfg.clients.items()]

    @app.exception_handler(ApiError)
    async def _err(_: Request, e: ApiError):
        return JSONResponse({"error": {"code": e.code, "message": e.message}}, status_code=e.status)

    @app.exception_handler(RequestValidationError)
    async def _bad_request(_: Request, e: RequestValidationError):
        detail = "; ".join(f"{'.'.join(str(p) for p in x['loc'])}: {x['msg']}" for x in e.errors())
        return JSONResponse({"error": {"code": "bad_request", "message": detail}}, status_code=422)

    def client(authorization: str = Header(default="")) -> str:
        token = authorization.removeprefix("Bearer ").strip()
        for t, name in tokens:
            if token and hmac.compare_digest(t, token):
                return name
        raise ApiError(401, "unauthorized", "missing or unknown bearer token")

    @app.get("/v1/health")
    async def health():
        return {"ok": True, "asks": len(svc.asks)}

    @app.post("/v1/asks", status_code=202)
    async def create(request: Request, who: str = Depends(client)):
        try:
            body = await request.json()
        except Exception:
            raise ApiError(400, "bad_json", "request body must be JSON")
        return (await svc.create(who, body)).view()

    @app.get("/v1/asks/{ask_id}")
    async def get(ask_id: str, wait: float = 0, who: str = Depends(client)):
        ask = svc.asks.get(ask_id)
        if not ask or ask.client != who:
            raise ApiError(404, "unknown_ask", ask_id)
        if wait:
            await svc.wait(ask, min(wait, 120))
        return ask.view()

    @app.websocket("/media")
    async def media(ws: WebSocket):
        await svc.media(ws)

    return app
