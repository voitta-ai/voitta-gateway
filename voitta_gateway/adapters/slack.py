"""Slack: messaging over Socket Mode, and outbound voice calls through a browser join page.

Messaging
    Socket Mode is an outbound WebSocket, so the messaging side needs no public
    ingress. Register the app as an internal app in each workspace: non-Marketplace
    commercial apps are limited to 1 request/minute on conversations.history.
    Conversation keys: DMs are {"user": U...} (the DM channel is opened on demand),
    everything else is {"channel": C...}. A reply goes into the thread of the message
    it answers. Lessons carried over from shmobster: a watchdog for Socket Mode
    sessions that reconnect forever without working, the bot token sent only to
    slack.com, and Slack's 200-with-sign-in-page answer treated as an error.

Voice
    Slack bots cannot join huddles, so a "Slack call" is ours: the gateway DMs the
    callee a card with a Join button; the link opens a page served on the gateway's
    ingress, which streams the microphone over a WebSocket as pcm16 and plays the
    agent's audio back. The page plugs into the same media protocol as Signal and
    phone calls, so agents (voitta-ask, Pipecat bots) work unchanged.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import secrets
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import httpx
try:                    # the service extra; Slack messaging works without it, calls do not
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse
except ImportError:
    FastAPI = WebSocket = WebSocketDisconnect = HTMLResponse = None
from slack_sdk.errors import SlackApiError
from slack_sdk.socket_mode.aiohttp import SocketModeClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.socket_mode.response import SocketModeResponse
from slack_sdk.web.async_client import AsyncWebClient

from ..audio import Resampler
from ..config import SlackConfig
from ..events import ms_to_iso
from .base import CallAdapter, Inbound, MessagingAdapter, PermanentError, Unsupported

log = logging.getLogger(__name__)

EMOJI = {"👍": "thumbsup", "👎": "thumbsdown", "❤️": "heart", "😂": "joy", "👀": "eyes", "✅": "white_check_mark",
         "🎉": "tada", "🙏": "pray", "🔥": "fire", "😮": "open_mouth", "😢": "cry", "👋": "wave", "🤔": "thinking_face"}
TRANSIENT = {"ratelimited", "internal_error", "fatal_error", "service_unavailable", "request_timeout"}
MARKDOWN_MAX = 12000
MAX_DOWNLOAD = 50 * 1024 * 1024
BROADCASTS = ("<!here", "<!channel", "<!everyone")
MESSAGE_SUBTYPES = (None, "file_share", "thread_broadcast", "bot_message")
CALL_PAGE = (Path(__file__).parent / "slack_call.html").read_text(encoding="utf-8")
REJOIN_GRACE_S = 10.0


def emoji_name(e: str) -> str:
    e = e.strip()
    if e in EMOJI:
        return EMOJI[e]
    if e.startswith(":") and e.endswith(":"):
        return e[1:-1]
    if e.isascii():
        return e
    raise PermanentError(f"no Slack name for emoji {e!r}; send a Slack emoji name like 'thumbsup'")


def slack_host(url: str) -> bool:
    """May this URL receive the bot token? Only https on slack.com and its subdomains."""
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    return parts.scheme == "https" and (host == "slack.com" or host.endswith(".slack.com"))


def ts_iso(ts: str | float | None) -> str:
    return ms_to_iso(float(ts or 0) * 1000)


def _fmt_duration(s: float) -> str:
    s = int(round(s))
    return f"{s // 60}:{s % 60:02d}"


class _Client(SocketModeClient):
    """Socket Mode client that remembers when each session started, for the watchdog."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.connects: deque[float] = deque(maxlen=64)

    async def connect(self):
        self.connects.append(time.monotonic())
        await super().connect()


# ================================================================ call audio from a browser
class BrowserPort:
    """A Slack call's audio, carried by the join page's WebSocket (binary pcm16le).

    One port per call; a page reload reattaches a new WebSocket to the same port, so the
    media session keeps running. The page may run at any rate; the port converts to its own."""
    continuous = False

    def __init__(self, rate: int = 48000):
        self.rate = rate
        self.queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=500)
        self.ws: WebSocket | None = None
        self.closed = False
        self._in = self._out = None
        self.stats: dict = {}

    def attach(self, ws: WebSocket, ws_rate: int) -> None:
        self.ws = ws
        self._in = Resampler(ws_rate, self.rate)
        self._out = Resampler(self.rate, ws_rate)

    def detach(self, ws: WebSocket) -> None:
        if self.ws is ws:
            self.ws = None

    def feed(self, pcm: bytes) -> None:
        if self._in is not None and pcm:
            try:
                self.queue.put_nowait(self._in(pcm[: len(pcm) // 2 * 2]))
            except asyncio.QueueFull:
                pass

    async def frames(self):
        while True:
            chunk = await self.queue.get()
            if chunk is None:
                return
            yield chunk

    async def write(self, pcm: bytes) -> None:
        ws = self.ws
        if ws is None or self.closed:
            return
        try:
            await ws.send_bytes(self._out(pcm))
        except Exception:
            self.detach(ws)

    async def clear(self) -> None:
        await self._text({"event": "clear"})

    async def _text(self, msg: dict) -> None:
        ws = self.ws
        if ws is not None:
            try:
                await ws.send_text(json.dumps(msg))
            except Exception:
                self.detach(ws)

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.queue.put_nowait(None)
        ws, self.ws = self.ws, None
        if ws is not None:
            try:
                await ws.send_text('{"event":"ended"}')
                await ws.close()
            except Exception:
                pass


@dataclass
class SlackCall:
    call_id: str
    user: str
    dm: str
    ts: str | None = None                  # the card message
    slack_call_id: str | None = None       # Calls API id, when the native card is used
    port: BrowserPort | None = None
    joined_at: float | None = None
    ended: bool = False
    tasks: list[asyncio.Task] = field(default_factory=list)


class SlackAdapter(MessagingAdapter, CallAdapter):
    name = "slack"
    capabilities = {"text": True, "attachments": True, "reactions": True, "edit": True, "delete": True,
                    "typing": False, "threads": True, "receipts": [], "markdown": True, "blocks": True,
                    "history": True, "interactions": True, "voice_clips": True, "max_attachment_mb": 1024}

    def __init__(self, cfg: SlackConfig, bot_token: str, app_token: str):
        self.cfg = cfg
        self.min_interval_s = cfg.min_interval_s
        self.web = AsyncWebClient(token=bot_token)
        self.bot_token = bot_token
        self.app_token = app_token
        self.client: _Client | None = None
        self.bot_user = None
        self.bot_id = None
        self.team = None
        self.account = ""
        self.dm_channel: dict[str, str] = {}
        self.seen: OrderedDict[str, None] = OrderedDict()
        self.gw = None
        self.watch: asyncio.Task | None = None
        self.route = cfg.calls.route if cfg.calls else None
        self.call_capabilities = {"inbound": False, "outbound": cfg.calls is not None, "dtmf": False,
                                  "transfer": [], "audio": "browser join page (WebSocket, pcm16 48 kHz)"}
        self.secret = secrets.token_bytes(32)
        self.calls: dict[str, SlackCall] = {}
        self.http_transport: httpx.AsyncBaseTransport | None = None   # tests: simulated Slack file servers

    # ================================================================ lifecycle
    async def start(self, gw) -> None:
        self.gw = gw
        auth = await self.web.auth_test()
        self.bot_user, self.team = auth["user_id"], auth["team_id"]
        self.bot_id = auth.get("bot_id")
        self.account = f"{auth.get('team')}/{auth['user_id']}"
        await self._connect()
        gw.channel_state(self.name, True)
        self.watch = asyncio.create_task(self._watchdog(), name="slack-watchdog")

    async def _connect(self) -> None:
        self.client = _Client(app_token=self.app_token, web_client=self.web, ping_interval=10)
        self.client.socket_mode_request_listeners.append(self._on_request)
        await self.client.connect()

    def healthy(self, now_wall: float | None = None, now_mono: float | None = None) -> tuple[bool, str]:
        """Two signals, as in shmobster: sessions must not keep dying young, and pongs must be fresh."""
        c = self.client
        now_wall = now_wall or time.time()
        now_mono = now_mono or time.monotonic()
        recent = [t for t in c.connects if now_mono - t < 300]
        if len(recent) >= 6:
            return False, f"{len(recent)} sessions in 5 minutes: reconnecting without working"
        last = c.last_ping_pong_time
        since_connect = now_mono - (c.connects[-1] if c.connects else now_mono)
        if last is None and since_connect > 60:
            return False, "no pong since the session started"
        if last is not None and now_wall - last > 60:
            return False, f"no pong for {int(now_wall - last)} s"
        return True, ""

    async def _watchdog(self) -> None:
        bad, backoff = 0, 5.0
        while True:
            await asyncio.sleep(10)
            try:
                ok, why = self.healthy()
                ok = ok and await self.client.is_connected()
            except Exception as e:
                ok, why = False, str(e)
            if ok:
                bad, backoff = 0, 5.0
                self.gw.channel_state(self.name, True)
                continue
            bad += 1
            if bad < 2:
                continue
            log.warning("slack socket mode unhealthy (%s); rebuilding the client", why or "disconnected")
            self.gw.channel_state(self.name, False, why or "socket mode disconnected")
            try:
                await self.client.close()
            except Exception:
                pass
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 300)
            try:
                await self._connect()
            except Exception:
                log.exception("slack reconnect failed")
            bad = 0

    async def stop(self) -> None:
        if self.watch:
            self.watch.cancel()
        for sc in list(self.calls.values()):
            for t in sc.tasks:
                t.cancel()
        if self.client:
            await self.client.close()

    # ================================================================ inbound
    def _dupe(self, key: str) -> bool:
        if key in self.seen:
            return True
        self.seen[key] = None
        if len(self.seen) > 2000:
            self.seen.popitem(last=False)
        return False

    async def _on_request(self, client: SocketModeClient, req: SocketModeRequest) -> None:
        await client.send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))
        payload = req.payload or {}
        try:
            if req.type == "events_api":
                if not self._dupe(payload.get("event_id") or req.envelope_id):
                    await self.handle_event(payload.get("event") or {})
            elif req.type == "interactive":
                if not self._dupe(req.envelope_id):
                    await self.handle_interaction(payload)
        except Exception:
            log.exception("slack %s request failed", req.type)

    def _key(self, channel: str, user: str | None, channel_type: str | None) -> tuple[dict, str]:
        if channel_type == "im" or (channel_type is None and channel.startswith("D")):
            if user:
                self.dm_channel[user] = channel
            return {"user": user}, "direct"
        return {"channel": channel}, "group"

    async def _fetch(self, http: httpx.AsyncClient, url: str) -> bytes:
        """Download a Slack-hosted file. Redirects are followed by hand so the token never leaves slack.com."""
        for _ in range(5):
            if not slack_host(url):
                raise ValueError(f"refusing to send the bot token to {urlsplit(url).hostname!r}")
            r = await http.get(url, headers={"Authorization": f"Bearer {self.bot_token}"}, follow_redirects=False)
            if r.is_redirect:
                url = urljoin(url, r.headers.get("location", ""))
                continue
            r.raise_for_status()
            if (r.headers.get("content-type") or "").lower().startswith("text/html"):
                raise ValueError("got Slack's sign-in page instead of the file (is files:read granted?)")
            if len(r.content) > MAX_DOWNLOAD:
                raise ValueError(f"larger than {MAX_DOWNLOAD} bytes")
            return r.content
        raise ValueError("too many redirects")

    async def _files(self, files: list[dict]) -> list[dict]:
        parts = []
        async with httpx.AsyncClient(timeout=60, transport=self.http_transport) as http:
            for f in files:
                url = f.get("url_private_download") or f.get("url_private")
                part = {"type": "file", "attachment_id": None, "content_type": f.get("mimetype"),
                        "filename": f.get("name"), "size": f.get("size"), "missing": True}
                if url and (f.get("size") or 0) <= MAX_DOWNLOAD:
                    try:
                        part = self.gw.store.put_file(await self._fetch(http, url), f.get("mimetype"), f.get("name"))
                    except Exception as e:
                        log.warning("slack file %s download failed: %s", f.get("id"), e)
                if f.get("subtype") == "slack_audio" or str(f.get("mimetype") or "").startswith("audio/"):
                    part["clip"] = f.get("subtype") == "slack_audio"
                    if f.get("duration_ms"):
                        part["duration_s"] = round(f["duration_ms"] / 1000, 2)
                    tr = f.get("transcription") or {}
                    preview = tr.get("preview") or {}
                    if tr.get("status") == "complete" and preview.get("content") and not preview.get("has_more"):
                        part.update(transcript=preview["content"].strip(), transcript_source="slack")
                parts.append(part)
        return parts

    def _sender(self, ev: dict) -> dict:
        if ev.get("bot_id") or ev.get("subtype") == "bot_message":
            name = (ev.get("bot_profile") or {}).get("name") or ev.get("username")
            return {"id": ev.get("user") or ev.get("bot_id"), "kind": "bot", "name": name,
                    "team": ev.get("team") or self.team}
        return {"id": ev.get("user"), "kind": "human", "team": ev.get("team") or self.team}

    def _ours(self, ev: dict) -> bool:
        return bool((self.bot_user and ev.get("user") == self.bot_user) or
                    (self.bot_id and ev.get("bot_id") == self.bot_id))

    async def handle_event(self, ev: dict) -> None:
        etype = ev.get("type")
        if etype == "message":
            await self._message(ev)
        elif etype in ("reaction_added", "reaction_removed"):
            item = ev.get("item") or {}
            if item.get("type") != "message" or ev.get("user") == self.bot_user:
                return
            key, kind = self._key(item["channel"], ev.get("user"), None)
            self.gw.inbound_reaction(self.name, key, kind, ev.get("user"), item.get("ts"),
                                     ev.get("reaction", ""), etype == "reaction_removed",
                                     int(float(ev.get("event_ts", 0)) * 1000))

    async def _message(self, ev: dict) -> None:
        sub = ev.get("subtype")
        if sub == "message_changed":
            m = ev.get("message") or {}
            if m.get("bot_id") or self._ours(m):
                return
            key, kind = self._key(ev["channel"], m.get("user"), ev.get("channel_type"))
            self.gw.inbound_edit(self.name, key, kind, m.get("user"), m.get("ts"),
                                 [{"type": "text", "text": m.get("text", "")}],
                                 int(float(ev.get("event_ts", m.get("ts", 0))) * 1000))
            return
        if sub == "message_deleted":
            prev = ev.get("previous_message") or {}
            if prev.get("bot_id") or self._ours(prev):
                return
            key, kind = self._key(ev["channel"], prev.get("user"), ev.get("channel_type"))
            self.gw.inbound_delete(self.name, key, kind, prev.get("user"), ev.get("deleted_ts"),
                                   int(float(ev.get("event_ts", 0)) * 1000))
            return
        if sub not in MESSAGE_SUBTYPES or self._ours(ev):
            return
        sender = self._sender(ev)
        if not sender["id"]:
            return
        text = ev.get("text") or ""
        mentioned = bool(self.bot_user) and f"<@{self.bot_user}>" in text
        if sender["kind"] == "bot" and (self.cfg.bots == "ignore" or not mentioned):
            return                                         # other agents only when they address us
        key, kind = self._key(ev["channel"], sender["id"] if sender["kind"] == "human" else None,
                              ev.get("channel_type"))
        if kind == "direct" and sender["kind"] == "bot":
            return
        if self.gw.closed(self.name, key, kind):
            return                                         # unlisted channel: no files fetched or stored
        parts = [{"type": "text", "text": text}] if text else []
        if ev.get("files"):
            parts += await self._files(ev["files"])
        if not parts:
            return
        ts, thread = ev["ts"], ev.get("thread_ts")
        broadcast = sender["kind"] == "human" and any(b in text for b in BROADCASTS)
        addressed = kind == "direct" or self.cfg.channels == "all" or mentioned or broadcast
        await self.gw.inbound_message(Inbound(
            channel=self.name, conversation_key=key, kind=kind, sender=sender, platform_id=ts,
            time_ms=int(float(ts) * 1000), parts=parts,
            reply_to_platform=thread if thread and thread != ts else None,
            thread_platform=thread or None, addressed=addressed,
            extra={"slack_channel": ev["channel"]}))

    async def handle_interaction(self, payload: dict) -> None:
        """block_actions: a button or menu in one of our messages. Our own call buttons are links: ignored."""
        if payload.get("type") != "block_actions":
            return
        user = (payload.get("user") or {}).get("id")
        channel = (payload.get("channel") or {}).get("id") or (payload.get("container") or {}).get("channel_id")
        if not user or not channel:
            return
        msg_ts = (payload.get("message") or {}).get("ts") or (payload.get("container") or {}).get("message_ts")
        key, kind = self._key(channel, user, "im" if channel.startswith("D") else "channel")
        for action in payload.get("actions") or []:
            if str(action.get("action_id", "")).startswith("voitta_call"):
                continue
            value = action.get("value")
            if value is None and action.get("selected_option"):
                value = action["selected_option"].get("value")
            self.gw.inbound_interaction(self.name, key, kind, user, msg_ts, {**action, "value": value},
                                        int(float(action.get("action_ts") or time.time()) * 1000))

    # ================================================================ outbound messages
    def direct_key(self, address: str) -> dict:
        return {"user": address.split("/")[-1]}

    async def _channel(self, key: dict) -> str:
        if "channel" in key:
            return key["channel"]
        user = key["user"]
        if user not in self.dm_channel:
            res = await self._call(self.web.conversations_open, users=user)
            self.dm_channel[user] = res["channel"]["id"]
        return self.dm_channel[user]

    @staticmethod
    async def _call(fn, **kw):
        try:
            return await fn(**kw)
        except SlackApiError as e:
            err = (e.response or {}).get("error") if e.response is not None else None
            if err in TRANSIENT or (e.response is not None and e.response.status_code >= 500):
                raise ConnectionError(f"slack {err}") from e
            raise PermanentError(f"slack {err}") from e

    @staticmethod
    def _compose(parts: list[dict]) -> dict:
        """text and blocks parts -> chat.postMessage / chat.update arguments."""
        text = "\n".join(p["text"] for p in parts if p.get("type") == "text")
        blocks = [b for p in parts if p.get("type") == "blocks" for b in p["blocks"]]
        fallback = next((p.get("text") for p in parts if p.get("type") == "blocks" and p.get("text")), None)
        if not text and not blocks:
            return {}
        if blocks:
            if text:
                blocks = [{"type": "markdown", "text": text[:MARKDOWN_MAX]}] + blocks
            return {"text": text or fallback or "(message)", "blocks": blocks}
        kw = {"text": text}
        if len(text) <= MARKDOWN_MAX:
            kw["blocks"] = [{"type": "markdown", "text": text}]
        return kw

    async def send(self, key, parts, reply_to):
        channel = await self._channel(key)
        thread = None
        if reply_to:
            thread = reply_to.get("thread") or reply_to.get("platform_id")
        ts = None
        kw = self._compose(parts)
        if kw:
            kw["channel"] = channel
            if thread:
                kw["thread_ts"] = thread
            res = await self._call(self.web.chat_postMessage, **kw)
            ts = res["ts"]
        for p in parts:
            if p.get("type") != "file":
                continue
            row = self.gw.store.file(p["attachment_id"])
            kw = {"channel": channel, "file": row["path"], "filename": row["filename"] or "file"}
            if thread:
                kw["thread_ts"] = thread
            res = await self._call(self.web.files_upload_v2, **kw)
            if ts is None:
                f = (res.get("files") or [{}])[0]
                ts = f"file:{f.get('id')}"
        return ts, thread

    async def edit(self, key, platform_id, parts):
        kw = self._compose(parts)
        if not kw:
            raise PermanentError("an edit needs text or blocks")
        await self._call(self.web.chat_update, channel=await self._channel(key), ts=platform_id, **kw)

    async def delete(self, key, platform_id):
        await self._call(self.web.chat_delete, channel=await self._channel(key), ts=platform_id)

    async def react(self, key, target, emoji, remove):
        fn = self.web.reactions_remove if remove else self.web.reactions_add
        await self._call(fn, channel=await self._channel(key), timestamp=target["platform_id"],
                         name=emoji_name(emoji))

    async def history(self, key, thread_platform_id, limit):
        channel = await self._channel(key)
        limit = max(1, min(int(limit), 200))
        if thread_platform_id:
            res = await self._call(self.web.conversations_replies, channel=channel, ts=thread_platform_id, limit=limit)
            msgs = res.get("messages") or []
        else:
            res = await self._call(self.web.conversations_history, channel=channel, limit=limit)
            msgs = list(reversed(res.get("messages") or []))
        out = []
        for m in msgs:
            sender = self._sender(m)
            if self._ours(m):
                sender["kind"] = "self"
            parts = [{"type": "text", "text": m["text"]}] if m.get("text") else []
            parts += [{"type": "file", "attachment_id": None, "content_type": f.get("mimetype"),
                       "filename": f.get("name"), "size": f.get("size"), "missing": True}
                      for f in m.get("files") or []]
            out.append({"platform_id": m.get("ts"), "thread": m.get("thread_ts"), "time": ts_iso(m.get("ts")),
                        "sender": sender, "parts": parts})
        return out

    # ================================================================ calls
    def _token(self, call_id: str) -> str:
        return hmac.new(self.secret, call_id.encode(), hashlib.sha256).hexdigest()[:32]

    def join_url(self, call_id: str) -> str:
        base = self.cfg.calls.public_base_url.rstrip("/")
        return f"{base}/slack/call/{call_id}?t={self._token(call_id)}"

    def _card(self, state: str, url: str | None = None, detail: str = "") -> tuple[str, list[dict]]:
        title = self.cfg.calls.title
        texts = {"ringing": f":telephone_receiver: *{title}* is calling you.",
                 "joined": f":telephone_receiver: On a call with *{title}*{detail}",
                 "missed": f":telephone_receiver: Missed call from *{title}*.",
                 "cancelled": f":telephone_receiver: *{title}* cancelled the call.",
                 "ended": f":telephone_receiver: Call with *{title}* ended{detail}."}
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": texts[state]}}]
        if url and state in ("ringing", "joined"):
            label = "Join call" if state == "ringing" else "Open call page"
            button = {"type": "button", "action_id": "voitta_call_join", "url": url,
                      "text": {"type": "plain_text", "text": label}}
            if state == "ringing":
                button["style"] = "primary"
            blocks.append({"type": "actions", "elements": [button]})
        return texts[state].replace("*", "").replace(":telephone_receiver: ", ""), blocks

    async def dial(self, call_id: str, to: str) -> str:
        if not self.cfg.calls:
            raise Unsupported("Slack calls are not enabled (adapters.slack.calls)")
        user = to.split("/")[-1]
        if not user[:1] in ("U", "W"):
            raise PermanentError(f"Slack calls go to a user id (U...), not {to!r}")
        sc = SlackCall(call_id, user, await self._channel({"user": user}))
        self.calls[call_id] = sc
        url = self.join_url(call_id)
        text, blocks = self._card("ringing", url)
        res = None
        if self.cfg.calls.card == "calls_api":
            try:
                add = await self._call(self.web.calls_add, external_unique_id=call_id, join_url=url,
                                       created_by=self.bot_user, title=self.cfg.calls.title,
                                       date_start=int(time.time()))
                sc.slack_call_id = add["call"]["id"]
                res = await self._call(self.web.chat_postMessage, channel=sc.dm, text=text,
                                       blocks=[{"type": "call", "call_id": sc.slack_call_id}])
            except PermanentError as e:
                log.warning("call %s: Slack refused the native call card (%s); using the Join button", call_id, e)
                if sc.slack_call_id:
                    await self._quiet(self.web.calls_end, id=sc.slack_call_id)
                sc.slack_call_id = None
        if res is None:
            res = await self._call(self.web.chat_postMessage, channel=sc.dm, text=f"{text} Join: {url}",
                                   blocks=blocks)
        sc.ts = res["ts"]
        sc.tasks.append(asyncio.create_task(self._ring_timeout(call_id), name=f"slack-ring-{call_id}"))
        self.gw.calls.outbound_ringing(call_id)
        return call_id

    async def _quiet(self, fn, **kw) -> None:
        try:
            await self._call(fn, **kw)
        except Exception as e:
            log.warning("slack %s failed: %s", getattr(fn, "__name__", fn), e)

    async def _update_card(self, sc: SlackCall, state: str, detail: str = "") -> None:
        if sc.slack_call_id:
            if state == "joined":
                await self._quiet(self.web.calls_participants_add, id=sc.slack_call_id, users=[{"slack_id": sc.user}])
            elif state in ("missed", "cancelled", "ended"):
                duration = int(time.time() - sc.joined_at) if sc.joined_at else None
                await self._quiet(self.web.calls_end, id=sc.slack_call_id, **({"duration": duration} if duration else {}))
            return
        if sc.ts:
            text, blocks = self._card(state, self.join_url(sc.call_id), detail)
            await self._quiet(self.web.chat_update, channel=sc.dm, ts=sc.ts, text=text, blocks=blocks)

    async def _ring_timeout(self, call_id: str) -> None:
        await asyncio.sleep(self.cfg.calls.ring_timeout_s)
        sc = self.calls.get(call_id)
        if sc and not sc.joined_at and not sc.ended:
            await self._finish(sc, "timeout")
            await self.gw.calls.ended(call_id, "timeout")

    async def _finish(self, sc: SlackCall, reason: str) -> None:
        """End our side of a call once: stop the page, update the card, forget the call."""
        if sc.ended:
            return
        sc.ended = True
        for t in sc.tasks:
            if t is not asyncio.current_task():
                t.cancel()
        if sc.port:
            await sc.port.close()
        if sc.joined_at:
            await self._update_card(sc, "ended", f" · {_fmt_duration(time.time() - sc.joined_at)}")
        else:
            await self._update_card(sc, "missed" if reason == "timeout" else "cancelled")
        self.calls.pop(sc.call_id, None)

    async def answer(self, call_id: str, platform_id: str) -> None:
        raise Unsupported("Slack calls are outbound only")

    async def reject(self, call_id: str, platform_id: str, reason: str) -> None:
        sc = self.calls.get(call_id)
        if sc:
            await self._finish(sc, reason)

    async def hangup(self, call_id: str, platform_id: str) -> None:
        sc = self.calls.get(call_id)
        if sc:
            await self._finish(sc, "hangup")

    # ---------------------------------------------------------------- the join page
    def mount(self, app: FastAPI) -> None:
        @app.get("/slack/call/{call_id}", response_class=HTMLResponse)
        async def page(call_id: str, t: str = ""):
            if not hmac.compare_digest(self._token(call_id), t or ""):
                return HTMLResponse("<h1>This call link is not valid.</h1>", status_code=403)
            state = "ended" if call_id not in self.calls else "ringing"
            html = (CALL_PAGE.replace("{{TITLE}}", self.cfg.calls.title)
                    .replace("{{STATE}}", state).replace("{{CALL_ID}}", call_id))
            return HTMLResponse(html, headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"})

        @app.websocket("/slack/call/{call_id}/audio")
        async def audio(ws: WebSocket, call_id: str):
            if not hmac.compare_digest(self._token(call_id), ws.query_params.get("t", "")):
                await ws.close(code=4403)
                return
            await ws.accept()
            await self._browser(ws, call_id)

    async def _browser(self, ws: WebSocket, call_id: str) -> None:
        sc = self.calls.get(call_id)
        if sc is None or sc.ended:
            await ws.send_text('{"event":"ended"}')
            await ws.close()
            return
        port = None
        hung_up = False
        try:
            while True:
                msg = await ws.receive()
                if msg.get("type") == "websocket.disconnect":
                    break
                if msg.get("bytes") is not None:
                    if port is not None:
                        port.feed(msg["bytes"])
                    continue
                data = json.loads(msg.get("text") or "{}")
                ev = data.get("event")
                if ev == "hello" and port is None:
                    rate = int(data.get("rate") or 48000)
                    if sc.port is None:
                        sc.port = BrowserPort(48000)
                    port = sc.port
                    port.attach(ws, rate)
                    for t in sc.tasks:                   # ring timeout, or a pending rejoin deadline
                        t.cancel()
                    sc.tasks.clear()
                    if sc.joined_at is None:
                        sc.joined_at = time.time()
                        await self._update_card(sc, "joined", ".")
                        await self.gw.calls.connected(call_id, port)
                    else:
                        log.info("call %s: page reconnected", call_id)
                elif ev == "hangup":
                    hung_up = True
                    break
                elif ev == "stats" and port is not None:
                    port.stats = {k: data.get(k) for k in ("played_ms", "underruns", "captured_ms", "buffered_ms")}
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            if port is not None:
                port.detach(ws)
        if sc.ended or port is None:
            return
        if hung_up:
            await self._end_from_page(sc, "remote_hangup")
            return
        sc.tasks.append(asyncio.create_task(self._rejoin_or_end(sc), name=f"slack-rejoin-{call_id}"))

    async def _rejoin_or_end(self, sc: SlackCall) -> None:
        """The page went away without saying goodbye (tab closed, network): wait for a reload, then end."""
        await asyncio.sleep(REJOIN_GRACE_S)
        if not sc.ended and (sc.port is None or sc.port.ws is None):
            await self._end_from_page(sc, "remote_hangup")

    async def _end_from_page(self, sc: SlackCall, reason: str) -> None:
        await self._finish(sc, reason)
        await self.gw.calls.ended(sc.call_id, reason)
