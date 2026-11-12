"""The gateway core: identity, policy, inbound normalization into events, outbound commands.

Adapters call `inbound_*` / `receipt` / `channel_state`; the API calls
`send_message` & co. Conversation logic lives in the agents, not here; what
lives here is what must hold across every channel: who may reach the agent,
who opted out, and that every event is persisted before anyone sees it.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time

import httpx

from .adapters.base import CallAdapter, Inbound, MessagingAdapter, Unsupported
from .config import Config, is_account
from .events import EventLog, ms_to_iso
from .store import Store, new_id

log = logging.getLogger(__name__)


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


def digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


GROUP_KEY = {"signal": "group", "slack": "channel"}       # where a group's id sits in its platform key


class Identity:
    """person_id <-> platform addresses. A lookup only: sessions stay per conversation."""

    def __init__(self, cfg: Config):
        self.by_addr: dict[tuple[str, str], str] = {}
        self.persons = cfg.persons
        for pid, p in cfg.persons.items():
            for ch in ("signal", "slack", "phone"):
                for addr in getattr(p, ch):
                    self.by_addr[(ch, self._norm(ch, addr))] = pid
                    if ch == "slack" and "/" in addr:          # events carry the bare user id
                        self.by_addr[(ch, addr.split("/", 1)[1])] = pid

    @staticmethod
    def _norm(channel: str, addr: str) -> str:
        if channel == "phone" or (channel == "signal" and addr.startswith("+")):
            return "+" + digits(addr)
        return addr.lower() if channel == "signal" else addr

    def lookup(self, channel: str, *addrs: str | None) -> str | None:
        for a in addrs:
            if not a:
                continue
            candidates = [a]
            if channel == "slack" and "/" in a:
                candidates.append(a.split("/", 1)[1])
            for c in candidates:
                pid = self.by_addr.get((channel, self._norm(channel, c)))
                if pid:
                    return pid
        return None

    def addresses(self, person_id: str) -> dict[str, list[str]]:
        p = self.persons.get(person_id)
        if not p:
            raise ApiError(404, "unknown_person", person_id)
        return {"signal": p.signal, "slack": p.slack, "phone": p.phone}


class Gateway:
    def __init__(self, cfg: Config, store: Store | None = None):
        self.cfg = cfg
        self.store = store or Store(cfg.data_path / "gateway.db")
        self.events = EventLog(self.store)
        self.identity = Identity(cfg)
        self.messaging: dict[str, MessagingAdapter] = {}
        self.voice: dict[str, CallAdapter] = {}
        self.channel_up: dict[str, bool] = {}
        self.outbox = None        # set by app wiring (outbox.Outbox)
        self.calls = None         # set by app wiring (calls.CallManager)
        self._speech_http: httpx.AsyncClient | None = None
        self.start_tasks: list[asyncio.Task] = []

    def source(self, channel: str) -> str:
        return f"/gateway/{channel}"

    # ================================================================ policy
    def allowed(self, channel: str, sender_id: str, person_id: str | None) -> bool:
        al = self.cfg.policy.inbound_allowlist
        if al == "*" or (person_id and person_id in al):
            return True
        return self.store.one("SELECT 1 FROM approved WHERE channel=? AND sender=?",
                              channel, sender_id) is not None

    def listed(self, channel: str, key: dict) -> str | None:
        """The `conversations` label of a group conversation, or None if it is not
        listed. Signal keys a group as {"group": id}, Slack a channel as
        {"channel": id}."""
        value = key.get("group") or key.get("channel")
        for label, conv in self.cfg.conversations.items():
            if conv.channel == channel and conv.key == value:
                return label
        return None

    def closed(self, channel: str, key: dict, kind: str) -> bool:
        """A group that is not listed under `conversations`. Its traffic is dropped
        before anything is stored."""
        return kind == "group" and self.listed(channel, key) is None

    def opted_out(self, channel: str, address: str | None) -> bool:
        return bool(address) and self.store.one(
            "SELECT 1 FROM optouts WHERE channel=? AND address=?", channel, address) is not None

    def emergency(self, number: str) -> bool:
        d = digits(number)
        return self.cfg.policy.block_emergency_numbers and any(
            d == e or (len(d) <= 4 and d.endswith(e)) for e in self.cfg.policy.emergency_numbers)

    # ================================================================ addressing (#1 item 3)
    @property
    def shared(self) -> bool:
        """A conversations map or a direct_consumer is configured: consumers share the account."""
        a = self.cfg.adapters
        return bool(self.cfg.conversations) or any(x is not None and x.direct_consumer for x in (a.signal, a.slack))

    def _agent(self, name: str | None) -> bool:
        """A configured consumer without the admin scope: one that a conversation can be routed to.
        An operator's admin send does not take a conversation over."""
        c = self.cfg.consumers.get(name or "")
        return c is not None and "admin" not in c.scopes

    def serving(self, channel: str, key: dict, kind: str) -> list[str] | None:
        """Consumers that serve a conversation, default addressee first.

        None: every consumer (no map covers it, or its map entry names no one).
        []: none but admin tokens (a group missing from a non-empty map).
        A direct conversation is served by the consumers that have sent into it, latest first,
        then the adapter's direct_consumer: a reply goes on with whoever spoke last."""
        if kind == "direct":
            d = getattr(getattr(self.cfg.adapters, channel, None), "direct_consumer", None)
            if not d:
                return None
            cid = self.conv_id(channel, key, kind)
            sent = [r["consumer"] for r in self.store.q(
                "SELECT consumer FROM messages WHERE conversation_id=? AND direction='out'"
                " AND consumer IS NOT NULL GROUP BY consumer ORDER BY MAX(created) DESC", cid)]
            sent = [s for s in sent if self._agent(s)]
            return sent + ([d] if d not in sent else [])
        if not self.cfg.conversations:
            return None
        label = self.listed(channel, key)
        if label is None:
            return []
        return list(self.cfg.conversations[label].consumers) or None

    def addressing(self, channel: str, key: dict, kind: str, text: str = "", target=None,
                   about: bool = False) -> dict:
        """Who an inbound event is for, so a shared account's consumers each see only their own.

        In order: the consumer that sent the message being replied to, reacted to or edited (on
        Slack, its latest post in the thread; for a human's message, the consumer it went to), then
        the consumer whose name keyword comes first in the text, then the conversation's default. `addressed_to: null` means every consumer, as before the
        map existed; "" means none but admin tokens."""
        out = {"addressed_to": None}
        label = self.listed(channel, key) if kind == "group" else None
        if label:
            out["conversation"] = label
        serving = self.serving(channel, key, kind)
        if serving is None:
            return out
        if not serving:
            out["addressed_to"] = ""
            return out
        # Our message's sender, or the consumer a human's message went to (edits, deletes, reactions)
        # A reply or quote follows the agent that sent the quoted message. An edit, delete or
        # reaction (`about`) also follows a human's message to the consumer it went to; a new
        # message quoting a human does not, so the name said in it still decides.
        if (target is not None and target["consumer"] in serving
                and (target["direction"] == "out" or about)):
            out["addressed_to"] = target["consumer"]
            return out
        best = None
        for name in serving:
            for word in self.cfg.consumers[name].names:
                # a whole word, optionally @-prefixed; not inside an address or a dotted name
                m = re.search(rf"(?<![\w.@])@?{re.escape(word)}(?![\w@]|\.\w)", text or "", re.IGNORECASE)
                if m and (best is None or m.start() < best[0]):
                    best = (m.start(), name)               # the name said first is the one addressed
        out["addressed_to"] = best[1] if best else serving[0]
        return out

    def check_scope(self, consumer: str | None, channel: str, key: dict, kind: str, op: str = "act") -> None:
        """A consumer acts (sends, reacts, types, reads history) only in conversations that it serves.
        A direct conversation with no messages yet may be started by any consumer, which then serves
        it too. Admin tokens are exempt; with nothing shared configured, nothing is scoped."""
        c = self.cfg.consumers.get(consumer or "")
        if c is None or "admin" in c.scopes:
            return
        if self.closed(channel, key, kind):
            raise ApiError(403, "not_listed", "that group is not in the gateway's conversations map")
        serving = self.serving(channel, key, kind)
        if serving is None or consumer in serving:
            return
        if kind == "direct" and op == "send":
            cid = self.conv_id(channel, key, kind)
            if self.store.one("SELECT 1 FROM messages WHERE conversation_id=?", cid) is None:
                return
            raise ApiError(403, "forbidden_conversation", f"{consumer} does not serve that direct conversation")
        if not serving:
            raise ApiError(403, "not_listed", "that conversation is not in the gateway's conversations map")
        raise ApiError(403, "forbidden_conversation", f"{consumer} does not serve that conversation")

    # ================================================================ inbound
    def channel_state(self, channel: str, up: bool, detail: str = "") -> None:
        if self.channel_up.get(channel) == up:
            return
        self.channel_up[channel] = up
        self.events.append("channel.up" if up else "channel.down", self.source(channel),
                           {"channel": channel, "detail": detail})

    def _resolve_reply(self, cid: str, platform_id: str | None) -> str | None:
        if not platform_id:
            return None
        row = self.store.message_by_platform(cid, platform_id)
        return row["id"] if row else None

    # ================================================================ voice clips
    async def _transcribe(self, parts: list[dict]) -> None:
        """Add `transcript` to audio file parts through the voitta-speech API, if configured.
        Parts that already carry a transcript (Slack's own) are left alone."""
        sp = self.cfg.speech
        if not sp:
            return
        for p in parts:
            if (p.get("type") != "file" or not p.get("attachment_id") or p.get("transcript")
                    or not str(p.get("content_type") or "").startswith("audio/")):
                continue
            row = self.store.file(p["attachment_id"])
            if not row:
                continue
            if self._speech_http is None:
                self._speech_http = httpx.AsyncClient(base_url=sp.url, timeout=sp.timeout_s, headers={
                    "Authorization": f"Bearer {self.cfg.secret(sp.token_secret)}"})
            try:
                with open(row["path"], "rb") as f:
                    data = f.read()
                params = {"language": sp.language} if sp.language else {}
                r = await self._speech_http.post("/v1/speech/transcribe", content=data, params=params,
                                                 headers={"Content-Type": p["content_type"]})
                r.raise_for_status()
                out = r.json()
                p.update(transcript=out.get("text", ""), transcript_source="voitta-speech",
                         duration_s=out.get("duration_s"))
            except Exception as e:
                log.warning("transcribing %s failed: %s", p["attachment_id"], e)
                p["transcript_error"] = str(e)[:300]

    async def inbound_message(self, m: Inbound) -> dict | None:
        if self.closed(m.channel, m.conversation_key, m.kind):
            return None                                           # policy: unlisted group
        number = m.sender.get("number") if m.kind == "direct" else None
        peer = self._peer(m.conversation_key) or ""
        if number and not self.store.find_conversation(m.channel, m.conversation_key):
            # A DM we started under their number becomes theirs: re-keyed to their account, so
            # replies follow the person, not whoever holds the number later. Only while it is still
            # keyed by the number, so a recycled number never lands a new owner in someone's DM.
            started = self.store.find_conversation(m.channel, self.messaging[m.channel].direct_key(number))
            if (started and self.store.alias(m.channel, peer) in (None, started)
                    and not any(a != peer for a in self.store.aci_aliases(m.channel, started))):
                self.store.rekey(started, m.conversation_key)
                self.store.drop_alias(m.channel, peer)            # the key now says it
        cid = self.conv_id(m.channel, m.conversation_key, m.kind)
        if self.store.message_by_platform(cid, m.platform_id):
            return None                                           # platform redelivery
        m.sender.setdefault("kind", "human")
        person = self.identity.lookup(m.channel, m.sender.get("id"), m.sender.get("number"))
        text = " ".join(p.get("text", "") for p in m.parts if p.get("type") == "text").strip()

        # opt-out / opt-in keywords, 1:1 only
        if m.kind == "direct" and text:
            word = text.upper()
            if word in {k.upper() for k in self.cfg.policy.opt_out_keywords}:
                self.store.x("INSERT OR REPLACE INTO optouts VALUES (?,?,?)",
                             m.channel, m.sender["id"], time.time())
                self.store.add_message(cid, "in", m.platform_id, "received")
                return self.events.append("contact.opted_out", self.source(m.channel),
                                          {"channel": m.channel, "conversation_id": cid,
                                           "address": m.sender["id"], "person_id": person,
                                           **self.addressing(m.channel, m.conversation_key, m.kind)},
                                          subject=cid, time=ms_to_iso(m.time_ms))
            if word == "START" and self.opted_out(m.channel, m.sender["id"]):
                self.store.x("DELETE FROM optouts WHERE channel=? AND address=?", m.channel, m.sender["id"])
                self.events.append("contact.opted_in", self.source(m.channel),
                                   {"channel": m.channel, "conversation_id": cid,
                                    "address": m.sender["id"], "person_id": person,
                                    **self.addressing(m.channel, m.conversation_key, m.kind)},
                                   subject=cid, time=ms_to_iso(m.time_ms))

        if m.kind == "direct" and m.sender.get("number"):
            self.store.set_alias(m.channel, m.sender["number"], cid)   # a send to the number lands here too
        mid = self.store.add_message(cid, "in", m.platform_id, "received", thread=m.thread_platform)
        target = self.store.message_by_platform(cid, m.reply_to_platform) if m.reply_to_platform else None
        if m.thread_platform:                                     # Slack: the agent that posted last in the thread
            target = next((r for r in self.store.q(
                "SELECT * FROM messages WHERE conversation_id=? AND direction='out' AND consumer IS NOT NULL"
                " AND thread_platform_id=? ORDER BY created DESC", cid, m.thread_platform)
                if self._agent(r["consumer"])), target)
        data = {
            "message_id": mid, "conversation_id": cid, "channel": m.channel,
            "conversation_kind": m.kind, "sender": m.sender, "person_id": person,
            "parts": m.parts, "reply_to": self._resolve_reply(cid, m.reply_to_platform),
            "thread": m.thread_platform, "addressed": m.addressed,
            **self.addressing(m.channel, m.conversation_key, m.kind, text, target),
        }
        if m.extra:
            data.update(m.extra)
        if m.kind == "group" and not m.addressed:
            return None                                           # policy: groups need a mention
        if data["addressed_to"]:
            self.store.set_message(mid, consumer=data["addressed_to"])   # its edits and reactions follow it
        if not self.allowed(m.channel, m.sender["id"], person):
            hid = new_id("held")
            self.store.x("INSERT INTO held VALUES (?,?,?,?,?,?,?,?)", hid, m.channel, m.sender["id"],
                         cid, "message.received", json.dumps(data), ms_to_iso(m.time_ms), time.time())
            return self.events.append("message.held", self.source(m.channel),
                                      {"held_id": hid, "channel": m.channel, "conversation_id": cid,
                                       "sender": {"id": m.sender["id"]}, "addressed_to": data["addressed_to"]},
                                      subject=cid, time=ms_to_iso(m.time_ms))
        await self._transcribe(data["parts"])                     # only for senders we let through
        return self.events.append("message.received", self.source(m.channel), data,
                                  subject=cid, time=ms_to_iso(m.time_ms))

    def inbound_interaction(self, channel: str, key: dict, kind: str, sender_id: str,
                            target_platform: str | None, action: dict, time_ms: int) -> None:
        """A button or menu in one of our messages was used (Slack block_actions)."""
        k = self._known(channel, key, kind, sender_id)
        if not k:
            return
        cid, person = k
        row = self.store.message_by_platform(cid, target_platform) if target_platform else None
        self.events.append("interaction.received", self.source(channel),
                           {"conversation_id": cid, "message_id": row["id"] if row else None,
                            "platform_id": target_platform, "person_id": person,
                            "sender": {"id": sender_id, "kind": "human"},
                            "action_id": action.get("action_id"), "block_id": action.get("block_id"),
                            "action_type": action.get("type"), "value": action.get("value"),
                            **self._about(channel, key, kind, row)},
                           subject=cid, time=ms_to_iso(time_ms))

    def _known(self, channel: str, key: dict, kind: str, sender_id: str) -> tuple[str, str | None] | None:
        """Conversation id and person for follow-up events, or None if the sender is not let through."""
        if self.closed(channel, key, kind):
            return None
        cid = self.conv_id(channel, key, kind)
        person = self.identity.lookup(channel, sender_id)
        if not self.allowed(channel, sender_id, person):
            return None
        return cid, person

    def _about(self, channel: str, key: dict, kind: str, row, text: str = "") -> dict:
        """Addressing for an event about an earlier message: whoever sent that message, if we did."""
        return self.addressing(channel, key, kind, text, row, about=True)

    def inbound_edit(self, channel: str, key: dict, kind: str, sender_id: str,
                     target_platform: str, parts: list[dict], time_ms: int) -> None:
        k = self._known(channel, key, kind, sender_id)
        if not k:
            return
        cid, person = k
        row = self.store.message_by_platform(cid, target_platform)
        self.events.append("message.edited", self.source(channel),
                           {"conversation_id": cid, "message_id": row["id"] if row else None,
                            "platform_id": target_platform, "person_id": person,
                            "sender": {"id": sender_id}, "parts": parts,
                            **self._about(channel, key, kind, row, " ".join(
                                p.get("text", "") for p in parts if p.get("type") == "text"))},
                           subject=cid, time=ms_to_iso(time_ms))

    def inbound_delete(self, channel: str, key: dict, kind: str, sender_id: str,
                       target_platform: str, time_ms: int) -> None:
        k = self._known(channel, key, kind, sender_id)
        if not k:
            return
        cid, person = k
        row = self.store.message_by_platform(cid, target_platform)
        self.events.append("message.deleted", self.source(channel),
                           {"conversation_id": cid, "message_id": row["id"] if row else None,
                            "platform_id": target_platform, "person_id": person,
                            "sender": {"id": sender_id}, **self._about(channel, key, kind, row)},
                           subject=cid, time=ms_to_iso(time_ms))

    def inbound_reaction(self, channel: str, key: dict, kind: str, sender_id: str,
                         target_platform: str, emoji: str, removed: bool, time_ms: int) -> None:
        k = self._known(channel, key, kind, sender_id)
        if not k:
            return
        cid, person = k
        row = self.store.message_by_platform(cid, target_platform)
        self.events.append("reaction.removed" if removed else "reaction.added", self.source(channel),
                           {"conversation_id": cid, "message_id": row["id"] if row else None,
                            "emoji": emoji, "person_id": person, "sender": {"id": sender_id},
                            **self._about(channel, key, kind, row)},
                           subject=cid, time=ms_to_iso(time_ms))

    def receipt(self, channel: str, platform_ids: list[str], status: str, time_ms: int) -> None:
        """Delivery / read receipts for our own messages. Status only moves forward."""
        order = {"queued": 0, "sent": 1, "delivered": 2, "read": 3}
        for pid in platform_ids:
            row = self.store.one("SELECT m.* FROM messages m JOIN conversations c ON c.id = m.conversation_id"
                                 " WHERE m.platform_id=? AND m.direction='out' AND c.channel=?", pid, channel)
            if not row or order.get(row["status"], 9) >= order[status]:
                continue
            self.store.set_message(row["id"], status=status)
            self.events.append("message.status", self.source(channel),
                               {"conversation_id": row["conversation_id"], "message_id": row["id"],
                                "status": status, "addressed_to": row["consumer"]},
                               subject=row["conversation_id"], time=ms_to_iso(time_ms))

    # ================================================================ held queue
    def held(self) -> list[dict]:
        return [{"held_id": r["id"], "channel": r["channel"], "sender": r["sender"],
                 "conversation_id": r["conversation_id"], "time": r["platform_time"]}
                for r in self.store.q("SELECT * FROM held ORDER BY created")]

    def approve(self, held_id: str) -> int:
        row = self.store.one("SELECT * FROM held WHERE id=?", held_id)
        if not row:
            raise ApiError(404, "unknown_held", held_id)
        self.store.x("INSERT OR IGNORE INTO approved VALUES (?,?)", row["channel"], row["sender"])
        released = self.store.q("SELECT * FROM held WHERE channel=? AND sender=? ORDER BY created",
                                row["channel"], row["sender"])
        for r in released:
            data = json.loads(r["data"])
            self.events.append(r["event_type"], self.source(r["channel"]), data,
                               subject=r["conversation_id"], time=r["platform_time"])
            self.store.x("DELETE FROM held WHERE id=?", r["id"])
        return len(released)

    def discard(self, held_id: str) -> None:
        if not self.store.x("DELETE FROM held WHERE id=?", held_id).rowcount:
            raise ApiError(404, "unknown_held", held_id)

    # ================================================================ outbound
    def adapter(self, channel: str) -> MessagingAdapter:
        a = self.messaging.get(channel)
        if not a:
            raise ApiError(422, "unknown_channel", f"no messaging adapter {channel!r}")
        return a

    def resolve_target(self, to: dict) -> tuple[str, str, dict]:
        """`to` -> (channel, conversation_id, platform key). Replies use the conversation;
        a person_id is only for conversations the agent starts itself."""
        if "conversation_id" in to:
            row = self.store.conversation_row(to["conversation_id"])
            if not row:
                raise ApiError(404, "unknown_conversation", to["conversation_id"])
            return row["channel"], row["id"], json.loads(row["platform_key"])
        if "person_id" in to:
            addrs = self.identity.addresses(to["person_id"])
            prefer = [to["channel"]] if to.get("channel") else ["signal", "slack"]
            for ch in prefer:
                usable = [a for a in addrs.get(ch) or [] if not self.opted_out(ch, a)]
                if ch in self.messaging and usable:
                    return (ch, *self._direct(ch, usable))
            raise ApiError(422, "no_route", f"no reachable channel for {to['person_id']}")
        if "conversation" in to:
            c = self.cfg.conversations.get(to["conversation"])
            if not c:
                raise ApiError(404, "unknown_conversation", str(to["conversation"]))
            key = {GROUP_KEY.get(c.channel, "group"): c.key}
            self.adapter(c.channel)
            return c.channel, self.store.conversation(c.channel, key, "group"), key
        if "channel" in to and "address" in to:
            ch = to["channel"]
            self.adapter(ch)
            person = self.identity.lookup(ch, to["address"])
            others = self.identity.addresses(person).get(ch) or [] if person else []
            return (ch, *self._direct(ch, [to["address"], *others]))
        raise ApiError(422, "bad_target", "to needs conversation_id, conversation, person_id, or channel+address")

    def _direct(self, channel: str, addresses: list[str]) -> tuple[str, dict]:
        """The direct conversation with whoever owns these addresses: one that already exists under
        any of them, else a new one under the first. Signal keys an inbound DM by ACI while a person
        or caller may give a number, and two conversations for one chat would split its routing."""
        adapter = self.messaging[channel]
        # An account id before a number: a conversation keyed by the account follows the person
        addresses = sorted(addresses, key=lambda a: not is_account(a))
        keys = [adapter.direct_key(a) for a in addresses]
        for key in keys:
            cid = self.store.alias(channel, self._peer(key)) or self.store.find_conversation(channel, key)
            if cid:
                return cid, self.store.platform_key(cid)
        cid = self.store.conversation(channel, keys[0], "direct")
        for key in keys[1:]:
            self.store.set_alias(channel, self._peer(key), cid)      # their reply, by ACI, lands here
        return cid, keys[0]

    def conv_id(self, channel: str, key: dict, kind: str) -> str:
        """A conversation's id; a direct one known under another of the peer's addresses keeps it."""
        if kind == "direct":
            cid = self.store.alias(channel, self._peer(key) or "")
            if cid:
                return cid
        return self.store.conversation(channel, key, kind)

    def _peer(self, key: dict) -> str | None:
        return key.get("peer") or key.get("user")

    def _check_parts(self, channel: str, adapter: MessagingAdapter, parts: list[dict]) -> None:
        if not parts:
            raise ApiError(422, "empty", "parts is empty")
        for p in parts:
            if p.get("type") == "file" and not self.store.file(p.get("attachment_id") or ""):
                raise ApiError(422, "unknown_attachment", str(p.get("attachment_id")))
            if p.get("type") == "blocks":
                if not adapter.capabilities.get("blocks"):
                    raise ApiError(422, "unsupported", f"{channel} does not support blocks")
                if not isinstance(p.get("blocks"), list) or not p["blocks"]:
                    raise ApiError(422, "bad_blocks", "a blocks part needs a non-empty blocks list")

    async def history(self, cid: str, thread: str | None, limit: int, consumer: str | None = None) -> list[dict]:
        """Recent messages of a conversation (or one thread), straight from the platform."""
        row = self.store.conversation_row(cid)
        if not row:
            raise ApiError(404, "unknown_conversation", cid)
        self.check_scope(consumer, row["channel"], json.loads(row["platform_key"]), row["kind"])
        adapter = self.adapter(row["channel"])
        if not adapter.capabilities.get("history"):
            raise ApiError(422, "unsupported", f"{row['channel']} has no message history")
        thread_pid = None
        if thread:
            m = self.store.message(thread)
            if not m or m["conversation_id"] != cid:
                raise ApiError(422, "bad_thread", "thread must be a message in this conversation")
            thread_pid = m["thread_platform_id"] or m["platform_id"]
        try:
            items = await adapter.history(json.loads(row["platform_key"]), thread_pid, limit)
        except Unsupported as e:
            raise ApiError(422, "unsupported", str(e))
        for it in items:
            known = self.store.message_by_platform(cid, it["platform_id"])
            it["message_id"] = known["id"] if known else None
            it["person_id"] = self.identity.lookup(row["channel"], it["sender"].get("id"))
        return items

    def send_message(self, consumer: str, body: dict) -> dict:
        channel, cid, key = self.resolve_target(body["to"])
        adapter = self.adapter(channel)
        conv = self.store.conversation_row(cid)
        self.check_scope(consumer, channel, key, conv["kind"], "send")
        if conv["kind"] == "direct" and self.opted_out(channel, self._peer(key)):
            raise ApiError(409, "opted_out", "recipient opted out of messages")
        reply = None
        if body.get("reply_to"):
            r = self.store.message(body["reply_to"])
            if not r or r["conversation_id"] != cid:
                raise ApiError(422, "bad_reply_to", "reply_to must be a message in the same conversation")
            reply = {"platform_id": r["platform_id"], "thread": r["thread_platform_id"],
                     "direction": r["direction"]}
        parts = body.get("parts") or []
        self._check_parts(channel, adapter, parts)
        parts = self._labelled(consumer, parts)
        mid = self.store.add_message(cid, "out", None, "queued", consumer=consumer)
        self.outbox.enqueue(mid, channel, cid, "send", {"key": key, "parts": parts, "reply_to": reply})
        self.events.append("message.status", self.source(channel),
                           {"conversation_id": cid, "message_id": mid, "status": "queued",
                            "addressed_to": consumer}, subject=cid)
        return {"message_id": mid, "conversation_id": cid, "channel": channel, "status": "queued"}

    def _labelled(self, consumer: str | None, parts: list[dict]) -> list[dict]:
        """Prefix the consumer's label, so people in a shared conversation can tell agents apart."""
        c = self.cfg.consumers.get(consumer or "")
        if not c or not c.label:
            return parts
        parts = [dict(p) for p in parts]
        for p in parts:
            if p.get("type") == "text":
                p["text"] = f"{c.label}: {p.get('text', '')}"
                return parts
        return [{"type": "text", "text": f"{c.label}:"}] + parts

    def message_op(self, mid: str, op: str, payload: dict, consumer: str | None = None) -> dict:
        row = self.store.message(mid)
        if not row:
            raise ApiError(404, "unknown_message", mid)
        conv = self.store.conversation_row(row["conversation_id"])
        self.check_scope(consumer, conv["channel"], json.loads(conv["platform_key"]), conv["kind"])
        c = self.cfg.consumers.get(consumer or "")
        if (op in ("edit", "delete") and self.shared and c is not None and "admin" not in c.scopes
                and row["consumer"] != consumer):
            raise ApiError(403, "not_yours", "only the consumer that sent a message can edit or delete it")
        adapter = self.adapter(conv["channel"])
        cap = {"edit": "edit", "delete": "delete", "react": "reactions"}[op]
        if not adapter.capabilities.get(cap):
            raise ApiError(422, "unsupported", f"{conv['channel']} does not support {cap}")
        if op in ("edit", "delete") and row["direction"] != "out":
            raise ApiError(422, "not_ours", "only the gateway's own messages can be edited or deleted")
        if not row["platform_id"]:
            raise ApiError(409, "not_sent_yet", "message has no platform id yet")
        if op == "edit":
            self._check_parts(conv["channel"], adapter, payload.get("parts") or [])
            payload = {**payload, "parts": self._labelled(row["consumer"], payload.get("parts") or [])}
        payload = {**payload, "key": json.loads(conv["platform_key"]), "consumer": consumer,
                   "target": {"platform_id": row["platform_id"], "direction": row["direction"]}}
        opid = new_id("op")
        self.outbox.enqueue(opid, conv["channel"], conv["id"], op, payload)
        return {"op_id": opid, "message_id": mid, "status": "queued"}

    async def typing(self, cid: str, on: bool, consumer: str | None = None) -> None:
        row = self.store.conversation_row(cid)
        if not row:
            raise ApiError(404, "unknown_conversation", cid)
        self.check_scope(consumer, row["channel"], json.loads(row["platform_key"]), row["kind"])
        adapter = self.adapter(row["channel"])
        if not adapter.capabilities.get("typing"):
            raise ApiError(422, "unsupported", f"{row['channel']} has no typing indicator")
        try:
            await adapter.typing(json.loads(row["platform_key"]), on)
        except Unsupported as e:
            raise ApiError(422, "unsupported", str(e))

    def channels(self) -> list[dict]:
        out = []
        for name, a in self.messaging.items():
            out.append({"channel": name, "account": a.account, "messaging": a.capabilities,
                        "calls": self.voice[name].call_capabilities if name in self.voice else None,
                        "up": self.channel_up.get(name, False)})
        for name, v in self.voice.items():
            if name not in self.messaging:
                out.append({"channel": name, "account": getattr(v, "account", ""), "messaging": None,
                            "calls": v.call_capabilities, "up": self.channel_up.get(name, False)})
        return out

    async def start(self) -> None:
        # Kept, so an embedded gateway's stop() can wait out a startup still in
        # flight instead of stopping an adapter that then finishes starting.
        self.start_tasks = [asyncio.create_task(self._start_adapter(a), name=f"start-{a.name}")
                            for a in list(self.messaging.values())
                            + [v for k, v in self.voice.items() if k not in self.messaging]]

    async def _start_adapter(self, a) -> None:
        try:
            await a.start(self)
        except Exception as e:
            log.exception("adapter %s failed to start", a.name)
            self.channel_state(a.name, False, f"start failed: {e}")
