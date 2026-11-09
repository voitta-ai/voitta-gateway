"""Slack via Socket Mode: an outbound WebSocket only, so no public ingress.

Register the app as an internal app in each workspace: non-Marketplace
commercial apps are limited to 1 request/minute on conversations.history.
Bots cannot join huddles, so Slack is messaging only.

Conversation keys: DMs are {"user": U...} (the DM channel is opened on
demand), everything else is {"channel": C...}. A reply goes into the thread
of the message it answers.
"""
from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict

import httpx
from slack_sdk.errors import SlackApiError
from slack_sdk.socket_mode.aiohttp import SocketModeClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.socket_mode.response import SocketModeResponse
from slack_sdk.web.async_client import AsyncWebClient

from ..config import SlackConfig
from .base import Inbound, MessagingAdapter, PermanentError

log = logging.getLogger(__name__)

EMOJI = {"👍": "thumbsup", "👎": "thumbsdown", "❤️": "heart", "😂": "joy", "👀": "eyes", "✅": "white_check_mark",
         "🎉": "tada", "🙏": "pray", "🔥": "fire", "😮": "open_mouth", "😢": "cry", "👋": "wave", "🤔": "thinking_face"}
TRANSIENT = {"ratelimited", "internal_error", "fatal_error", "service_unavailable", "request_timeout"}
MARKDOWN_MAX = 12000
MAX_DOWNLOAD = 50 * 1024 * 1024


def emoji_name(e: str) -> str:
    e = e.strip()
    if e in EMOJI:
        return EMOJI[e]
    if e.startswith(":") and e.endswith(":"):
        return e[1:-1]
    if e.isascii():
        return e
    raise PermanentError(f"no Slack name for emoji {e!r}; send a Slack emoji name like 'thumbsup'")


class SlackAdapter(MessagingAdapter):
    name = "slack"
    capabilities = {"text": True, "attachments": True, "reactions": True, "edit": True, "delete": True,
                    "typing": False, "threads": True, "receipts": [], "markdown": True,
                    "max_attachment_mb": 1024}

    def __init__(self, cfg: SlackConfig, bot_token: str, app_token: str):
        self.cfg = cfg
        self.min_interval_s = cfg.min_interval_s
        self.web = AsyncWebClient(token=bot_token)
        self.bot_token = bot_token
        self.app_token = app_token
        self.client: SocketModeClient | None = None
        self.bot_user = None
        self.team = None
        self.account = ""
        self.dm_channel: dict[str, str] = {}
        self.seen: OrderedDict[str, None] = OrderedDict()
        self.gw = None
        self.watch: asyncio.Task | None = None

    # ================================================================ lifecycle
    async def start(self, gw) -> None:
        self.gw = gw
        auth = await self.web.auth_test()
        self.bot_user, self.team = auth["user_id"], auth["team_id"]
        self.account = f"{auth.get('team')}/{auth['user_id']}"
        self.client = SocketModeClient(app_token=self.app_token, web_client=self.web)
        self.client.socket_mode_request_listeners.append(self._on_request)
        await self.client.connect()
        gw.channel_state(self.name, True)
        self.watch = asyncio.create_task(self._watch(), name="slack-watch")

    async def _watch(self) -> None:
        while True:
            await asyncio.sleep(15)
            try:
                up = await self.client.is_connected()
            except Exception:
                up = False
            self.gw.channel_state(self.name, up, "" if up else "socket mode disconnected")

    async def stop(self) -> None:
        if self.watch:
            self.watch.cancel()
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
        if req.type != "events_api":
            return
        payload = req.payload or {}
        if self._dupe(payload.get("event_id") or req.envelope_id):
            return
        try:
            await self.handle_event(payload.get("event") or {})
        except Exception:
            log.exception("slack event failed: %s", (payload.get("event") or {}).get("type"))

    def _key(self, channel: str, user: str | None, channel_type: str | None) -> tuple[dict, str]:
        if channel_type == "im" or (channel_type is None and channel.startswith("D")):
            if user:
                self.dm_channel[user] = channel
            return {"user": user}, "direct"
        return {"channel": channel}, "group"

    async def _files(self, files: list[dict]) -> list[dict]:
        parts = []
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as http:
            for f in files:
                url = f.get("url_private_download") or f.get("url_private")
                part = {"type": "file", "attachment_id": None, "content_type": f.get("mimetype"),
                        "filename": f.get("name"), "size": f.get("size"), "missing": True}
                if url and (f.get("size") or 0) <= MAX_DOWNLOAD:
                    try:
                        r = await http.get(url, headers={"Authorization": f"Bearer {self.bot_token}"})
                        r.raise_for_status()
                        part = self.gw.store.put_file(r.content, f.get("mimetype"), f.get("name"))
                    except Exception as e:
                        log.warning("slack file %s download failed: %s", f.get("id"), e)
                parts.append(part)
        return parts

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
            if m.get("bot_id") or m.get("user") == self.bot_user:
                return
            key, kind = self._key(ev["channel"], m.get("user"), ev.get("channel_type"))
            self.gw.inbound_edit(self.name, key, kind, m.get("user"), m.get("ts"),
                                 [{"type": "text", "text": m.get("text", "")}],
                                 int(float(ev.get("event_ts", m.get("ts", 0))) * 1000))
            return
        if sub == "message_deleted":
            prev = ev.get("previous_message") or {}
            if prev.get("bot_id") or prev.get("user") == self.bot_user:
                return
            key, kind = self._key(ev["channel"], prev.get("user"), ev.get("channel_type"))
            self.gw.inbound_delete(self.name, key, kind, prev.get("user"), ev.get("deleted_ts"),
                                   int(float(ev.get("event_ts", 0)) * 1000))
            return
        if sub not in (None, "file_share", "thread_broadcast") or ev.get("bot_id") or ev.get("user") == self.bot_user:
            return
        user = ev.get("user")
        if not user:
            return
        key, kind = self._key(ev["channel"], user, ev.get("channel_type"))
        text = ev.get("text") or ""
        parts = [{"type": "text", "text": text}] if text else []
        if ev.get("files"):
            parts += await self._files(ev["files"])
        if not parts:
            return
        ts, thread = ev["ts"], ev.get("thread_ts")
        addressed = kind == "direct" or self.cfg.channels == "all" or f"<@{self.bot_user}>" in text
        self.gw.inbound_message(Inbound(
            channel=self.name, conversation_key=key, kind=kind,
            sender={"id": user, "team": ev.get("team") or self.team}, platform_id=ts,
            time_ms=int(float(ts) * 1000), parts=parts,
            reply_to_platform=thread if thread and thread != ts else None,
            thread_platform=thread or None, addressed=addressed,
            extra={"slack_channel": ev["channel"]}))

    # ================================================================ outbound
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

    async def send(self, key, parts, reply_to):
        channel = await self._channel(key)
        thread = None
        if reply_to:
            thread = reply_to.get("thread") or reply_to.get("platform_id")
        text = "\n".join(p["text"] for p in parts if p.get("type") == "text")
        ts = None
        if text:
            kw = {"channel": channel, "text": text}
            if len(text) <= MARKDOWN_MAX:
                kw["blocks"] = [{"type": "markdown", "text": text}]
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
        text = "\n".join(p["text"] for p in parts if p.get("type") == "text")
        kw = {"channel": await self._channel(key), "ts": platform_id, "text": text}
        if len(text) <= MARKDOWN_MAX:
            kw["blocks"] = [{"type": "markdown", "text": text}]
        await self._call(self.web.chat_update, **kw)

    async def delete(self, key, platform_id):
        await self._call(self.web.chat_delete, channel=await self._channel(key), ts=platform_id)

    async def react(self, key, target, emoji, remove):
        fn = self.web.reactions_remove if remove else self.web.reactions_add
        await self._call(fn, channel=await self._channel(key), timestamp=target["platform_id"],
                         name=emoji_name(emoji))
