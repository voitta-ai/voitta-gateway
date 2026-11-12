"""Signal via the signal-cli JSON-RPC daemon (TCP socket, --receive-mode=manual).

Messages: subscribeReceive on our own connection, so nothing is received
before the gateway listens; every notification is written to raw_inbound
before it is processed.

Calls (signal-cli >= 0.14.2, experimental): subscribeCallEvents is required
or incoming calls are silently ignored. signal-cli spawns one
signal-call-tunnel per call; audio never passes through signal-cli: the
tunnel creates per-call PulseAudio devices and we write to
`sink_for_<inputDeviceName>` and read `<outputDeviceName>.monitor`
(48 kHz mono s16le).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import sys
from pathlib import Path

from .. import scrub
from ..config import SignalConfig, host_port, is_account
from .base import CallAdapter, Inbound, MessagingAdapter, PermanentError, Unsupported

log = logging.getLogger(__name__)

PULSE_RATE = 48000


def _die_with_parent() -> None:
    """Runs in the forked child: on Linux, SIGTERM signal-cli when the gateway dies, however it dies.
    Two daemons on one account fight over its messages and its database."""
    if sys.platform.startswith("linux"):
        import ctypes
        import signal
        ctypes.CDLL("libc.so.6", use_errno=True).prctl(1, signal.SIGTERM)    # PR_SET_PDEATHSIG


class RpcError(Exception):
    def __init__(self, method: str, error: dict):
        super().__init__(f"{method}: {scrub.ids(error.get('message'))} ({error.get('code')})")   # logged
        self.code = error.get("code")
        self.error = error


class JsonRpc:
    """Newline-delimited JSON-RPC 2.0 over a stream socket."""

    def __init__(self, on_notification):
        self.on_notification = on_notification
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.pending: dict[int, asyncio.Future] = {}
        self.next_id = 1
        self.closed = asyncio.Event()

    async def connect(self, host: str, port: int) -> None:
        self.reader, self.writer = await asyncio.open_connection(host, port, limit=2 ** 24)
        self.closed.clear()
        asyncio.create_task(self._read(), name="signal-rpc-reader")

    async def _read(self) -> None:
        try:
            while True:
                line = await self.reader.readline()
                if not line:
                    break
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    log.warning("signal-cli sent bad JSON: %s", scrub.ids(repr(line[:200])))
                    continue
                if "id" in msg and ("result" in msg or "error" in msg) and "method" not in msg:
                    fut = self.pending.pop(msg["id"], None)
                    if fut and not fut.done():
                        fut.set_result(msg)
                elif "method" in msg:
                    try:
                        await self.on_notification(msg["method"], msg.get("params") or {})
                    except Exception:
                        log.exception("handling %s failed", msg.get("method"))
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            for fut in self.pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("signal-cli connection closed"))
            self.pending.clear()
            self.closed.set()

    async def call(self, method: str, timeout: float = 60, **params):
        if self.writer is None or self.closed.is_set():
            raise ConnectionError("signal-cli is not connected")
        rid = self.next_id
        self.next_id += 1
        fut = asyncio.get_running_loop().create_future()
        self.pending[rid] = fut
        self.writer.write((json.dumps({"jsonrpc": "2.0", "id": rid, "method": method,
                                       "params": params}) + "\n").encode())
        await self.writer.drain()
        msg = await asyncio.wait_for(fut, timeout)
        if "error" in msg:
            raise RpcError(method, msg["error"])
        return msg.get("result")

    async def close(self) -> None:
        if self.writer:
            self.writer.close()


class PulsePort:
    """A Signal call's audio through the tunnel's per-call PulseAudio devices."""
    rate = PULSE_RATE
    continuous = True

    def __init__(self, input_device: str, output_device: str, latency_ms: int = 40):
        self.sink = f"sink_for_{input_device}"
        self.monitor = f"{output_device}.monitor"
        self.latency_ms = latency_ms
        self.rec: asyncio.subprocess.Process | None = None
        self.play: asyncio.subprocess.Process | None = None

    async def open(self) -> None:
        fmt = ["--format=s16le", f"--rate={PULSE_RATE}", "--channels=1", "--raw",
               f"--latency-msec={self.latency_ms}"]
        self.rec = await asyncio.create_subprocess_exec(
            "parec", f"--device={self.monitor}", *fmt, stdout=asyncio.subprocess.PIPE)
        self.play = await asyncio.create_subprocess_exec(
            "pacat", "--playback", f"--device={self.sink}", *fmt, stdin=asyncio.subprocess.PIPE)

    async def frames(self):
        chunk = PULSE_RATE * 2 // 50                         # 20 ms
        while self.rec and self.rec.stdout:
            data = await self.rec.stdout.read(chunk)
            if not data:
                return
            yield data

    async def write(self, pcm: bytes) -> None:
        if self.play and self.play.stdin and not self.play.stdin.is_closing():
            self.play.stdin.write(pcm)
            try:
                await self.play.stdin.drain()
            except ConnectionError:
                pass

    async def clear(self) -> None:
        pass                                                  # pacat holds only latency_ms

    async def close(self) -> None:
        for p in (self.rec, self.play):
            if p and p.returncode is None:
                p.terminate()
                try:
                    await asyncio.wait_for(p.wait(), 2)
                except asyncio.TimeoutError:
                    p.kill()


def _group_ids(node) -> list[str]:
    """Every `groupId` value in a signal-cli envelope, at any depth."""
    out = []
    if isinstance(node, dict):
        for k, v in node.items():
            if k == "groupId" and isinstance(v, str):
                out.append(v)
            else:
                out += _group_ids(v)
    elif isinstance(node, list):
        for v in node:
            out += _group_ids(v)
    return out


class SignalAdapter(MessagingAdapter, CallAdapter):
    name = "signal"
    capabilities = {"text": True, "attachments": True, "reactions": True, "edit": True,
                    "delete": True, "typing": True, "threads": False, "receipts": ["delivered", "read"],
                    "max_attachment_mb": 100, "max_text_bytes": 2000, "styles": True,
                    "mentions": True}
    min_interval_s = 0.3

    def __init__(self, cfg: SignalConfig, port_factory=None):
        self.cfg = cfg
        self.account = cfg.account
        self.aci = cfg.aci
        self.route = cfg.route
        self.call_capabilities = {"inbound": cfg.calls, "outbound": cfg.calls, "dtmf": False,
                                  "transfer": [], "audio": "48 kHz Opus via signal-call-tunnel"}
        self.rpc: JsonRpc | None = None
        self.proc: asyncio.subprocess.Process | None = None
        self.gw = None
        self.devices: dict[str, tuple[str, str]] = {}         # platform call id -> (input, output)
        self.call_of: dict[str, str] = {}                     # platform call id -> our call id
        self.early: dict[str, list[dict]] = {}                # call events seen before startCall returned
        self.ports: dict[str, PulsePort] = {}
        self.port_factory = port_factory or (lambda i, o: PulsePort(i, o, cfg.pulse_latency_ms))
        self.task: asyncio.Task | None = None
        att = cfg.attachments_dir or (Path(cfg.data_dir) / "attachments" if cfg.data_dir
                                      else Path.home() / ".local/share/signal-cli/attachments")
        self.attachments = Path(att)

    @property
    def me(self) -> str:
        return self.aci or self.account

    # ================================================================ lifecycle
    async def start(self, gw) -> None:
        self.gw = gw
        gw.channel_state(self.name, False, "connecting to signal-cli")
        await self._replay_raw()
        if self.cfg.spawn:
            await self._spawn()
        self.task = asyncio.create_task(self._connection_loop(), name="signal-connection")

    async def _spawn(self) -> None:
        binary = shutil.which(self.cfg.binary) or self.cfg.binary
        cmd = [binary]
        if self.cfg.data_dir:
            cmd += ["--config", self.cfg.data_dir]
        cmd += ["-a", self.account, "daemon", "--tcp", self.cfg.tcp, "--receive-mode=manual", "--ignore-stories",
                "--no-receive-stdout"]
        if self.cfg.send_read_receipts:
            cmd.append("--send-read-receipts")
        log.info("starting signal-cli daemon on %s", self.cfg.tcp)     # never the account: logs travel
        # Its console output names the account ("Starting daemon ... for +1555..."): scrubbed, then logged
        self.proc = await asyncio.create_subprocess_exec(*cmd, env={**os.environ}, preexec_fn=_die_with_parent,
                                                         stdout=asyncio.subprocess.PIPE,
                                                         stderr=asyncio.subprocess.STDOUT)
        asyncio.create_task(self._relay_output(self.proc), name="signal-cli-output")

    @staticmethod
    async def _relay_output(proc) -> None:
        # Never stop reading: an undrained pipe fills and blocks signal-cli, sends and receives
        # with it. A line past the reader's limit is skipped, not fatal.
        while True:
            try:
                line = await proc.stdout.readline()
            except ValueError:                            # one line longer than the stream limit
                await proc.stdout.read(2 ** 16)
                continue
            if not line:
                return
            log.info("signal-cli: %s", scrub.ids(line.decode(errors="replace").rstrip()))

    async def _connection_loop(self) -> None:
        host, port = host_port(self.cfg.tcp)
        delay = 0.5
        while True:
            self.rpc = JsonRpc(self._notification)
            try:
                await self.rpc.connect(host, port)
                await self.rpc.call("subscribeReceive")
                if self.cfg.calls:
                    await self.rpc.call("subscribeCallEvents")
                if not self.aci:
                    await self._learn_aci()
                self.gw.channel_state(self.name, True)
                delay = 0.5
                await self.rpc.closed.wait()
                self.gw.channel_state(self.name, False, "signal-cli connection closed")
            except (ConnectionError, OSError, asyncio.TimeoutError) as e:
                if self.proc and self.proc.returncode is not None:
                    self.gw.channel_state(self.name, False, f"signal-cli exited {self.proc.returncode}")
                    await self._spawn()
                log.debug("signal-cli not reachable yet: %s", e)
            except RpcError as e:
                log.error("signal-cli refused subscription: %s", e)
                self.gw.channel_state(self.name, False, str(e))
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10)

    async def _learn_aci(self) -> None:
        try:
            res = await self.rpc.call("getUserStatus", recipient=[self.account])
            for r in res or []:
                if r.get("uuid"):
                    self.aci = r["uuid"]
                    log.info("signal account's own ACI learned")
        except Exception as e:
            log.warning("could not look up our own ACI (set adapters.signal.aci): %s", scrub.ids(e))

    async def stop(self) -> None:
        if self.task:
            self.task.cancel()
        for p in list(self.ports.values()):
            await p.close()
        if self.rpc:
            await self.rpc.close()
        if self.proc and self.proc.returncode is None:
            self.proc.terminate()
            try:
                await asyncio.wait_for(self.proc.wait(), 15)
            except asyncio.TimeoutError:
                self.proc.kill()

    # ================================================================ inbound
    async def _notification(self, method: str, params: dict) -> None:
        result = params.get("result", params) if "subscription" in params else params
        if method == "receive":
            if self._closed_group(result):
                return                                    # unlisted group: not even the raw envelope is kept
            rid = self.gw.store.x("INSERT INTO raw_inbound (channel, body) VALUES (?,?)",
                                  self.name, json.dumps(result)).lastrowid
            await self._process(result)
            self.gw.store.x("DELETE FROM raw_inbound WHERE id=?", rid)   # kept only until processed
        elif method == "callEvent":
            await self._call_event(result)

    async def _replay_raw(self) -> None:
        for row in self.gw.store.q("SELECT * FROM raw_inbound WHERE channel=? AND processed=0 ORDER BY id",
                                   self.name):
            result = json.loads(row["body"])
            if self._closed_group(result):
                # Kept before the group was unlisted; not kept any longer.
                self.gw.store.x("DELETE FROM raw_inbound WHERE id=?", row["id"])
                continue
            try:
                await self._process(result)
            except Exception:
                log.exception("replaying raw inbound %s failed", row["id"])
            self.gw.store.x("DELETE FROM raw_inbound WHERE id=?", row["id"])
        # Rows from before raw bodies were dropped once processed: senders, numbers, held text
        self.gw.store.x("DELETE FROM raw_inbound WHERE channel=? AND processed=1", self.name)

    def _closed_group(self, result: dict) -> bool:
        """An envelope that touches a group not listed under `conversations`.
        Checked before the raw envelope is written and before attachments are
        stored, so an unlisted group leaves nothing behind (signal-cli's own
        attachment directory is outside the gateway's reach).

        Every `groupId` anywhere in the envelope counts, not only the shapes
        `_process` reads (dataMessage, edits), so sync copies from linked
        devices, typing indicators and shapes added later fail closed."""
        retval = any(self.gw.closed(self.name, {"group": g}, "group")
                     for g in _group_ids(result.get("envelope") or {}))
        return retval

    def _author(self, uuid: str | None, number: str | None) -> str:
        if (uuid and self.aci and uuid == self.aci) or (number and number == self.account):
            return self.me
        return uuid or number or "unknown"

    def _conv(self, env: dict, dm: dict) -> tuple[dict, str]:
        group = (dm.get("groupInfo") or {}).get("groupId")
        if group:
            return {"group": group}, "group"
        return {"peer": env.get("sourceUuid") or env.get("sourceNumber")}, "direct"

    def _parts(self, dm: dict) -> list[dict]:
        parts = []
        if dm.get("message"):
            parts.append({"type": "text", "text": dm["message"]})
        for a in dm.get("attachments") or []:
            path = self.attachments / str(a.get("id"))
            if path.exists():
                part = self.gw.store.put_file(path.read_bytes(), a.get("contentType"), a.get("filename"))
            else:
                part = {"type": "file", "attachment_id": None, "content_type": a.get("contentType"),
                        "filename": a.get("filename"), "size": a.get("size"), "missing": True}
            if a.get("caption"):
                part["caption"] = a["caption"]
            parts.append(part)
        if dm.get("sticker"):
            parts.append({"type": "sticker", "pack_id": dm["sticker"].get("packId"),
                          "sticker_id": dm["sticker"].get("stickerId")})
        return parts

    def _addressed(self, dm: dict, kind: str) -> bool:
        if kind == "direct" or self.cfg.groups == "all":
            return True
        for m in dm.get("mentions") or []:
            if (self.aci and m.get("uuid") == self.aci) or m.get("number") == self.account:
                return True
        q = dm.get("quote") or {}
        return self._author(q.get("authorUuid"), q.get("authorNumber")) == self.me and bool(q)

    async def _process(self, result: dict) -> None:
        env = result.get("envelope") or {}
        if not env or self._closed_group(result):
            return                                            # replayed rows from before a group was unlisted
        sender_id = env.get("sourceUuid") or env.get("sourceNumber")
        if not sender_id or self._author(env.get("sourceUuid"), env.get("sourceNumber")) == self.me:
            return                                            # our own (sync) messages
        sender = {"id": sender_id, "kind": "human", "number": env.get("sourceNumber"), "name": env.get("sourceName")}
        ts = env.get("timestamp") or 0

        if rm := env.get("receiptMessage"):
            status = "read" if rm.get("isRead") or rm.get("isViewed") else "delivered" if rm.get("isDelivery") else None
            if status:
                self.gw.receipt(self.name, [f"{self.me}:{t}" for t in rm.get("timestamps") or []],
                                status, rm.get("when") or ts)
            return

        if em := env.get("editMessage"):
            dm = em.get("dataMessage") or {}
            key, kind = self._conv(env, dm)
            self.gw.inbound_edit(self.name, key, kind, sender_id,
                                 f"{sender_id}:{em.get('targetSentTimestamp')}", self._parts(dm), ts)
            return

        dm = env.get("dataMessage")
        if not dm:
            return
        key, kind = self._conv(env, dm)
        if kind == "group" and self.cfg.groups == "ignore":
            return
        if r := dm.get("reaction"):
            target = f"{self._author(r.get('targetAuthorUuid'), r.get('targetAuthorNumber'))}:{r.get('targetSentTimestamp')}"
            self.gw.inbound_reaction(self.name, key, kind, sender_id, target, r.get("emoji", ""),
                                     bool(r.get("isRemove")), ts)
            return
        if rd := dm.get("remoteDelete"):
            self.gw.inbound_delete(self.name, key, kind, sender_id, f"{sender_id}:{rd.get('timestamp')}", ts)
            return
        parts = self._parts(dm)
        if not parts:
            return
        q = dm.get("quote") or {}
        reply = f"{self._author(q.get('authorUuid'), q.get('authorNumber'))}:{q.get('id')}" if q else None
        await self.gw.inbound_message(Inbound(
            channel=self.name, conversation_key=key, kind=kind, sender=sender,
            platform_id=f"{sender_id}:{dm.get('timestamp') or ts}", time_ms=ts, parts=parts,
            reply_to_platform=reply, addressed=self._addressed(dm, kind),
            extra={"timestamp": dm.get("timestamp") or ts, "mentions": self._mentions(dm)}))

    def _mentions(self, dm: dict) -> list[dict]:
        """Mentions as signal-cli reports them: offsets in UTF-16 code units into the text, where
        each mention is one U+FFFC. Passed through unchanged; `self` marks this account."""
        out = []
        for m in dm.get("mentions") or []:
            mid = m.get("uuid") or m.get("number")
            out.append({"id": mid, "start": m.get("start"), "length": m.get("length"),
                        "self": bool(mid) and self._author(m.get("uuid"), m.get("number")) == self.me})
        return out

    # ================================================================ outbound messages
    @staticmethod
    def _to(key: dict) -> dict:
        return {"groupId": key["group"]} if "group" in key else {"recipient": [key["peer"]]}

    def direct_key(self, address: str) -> dict:
        a = address.strip()
        if is_account(a):
            return {"peer": a.lower()}
        if re.fullmatch(r"\+?[\d\s\-().]{6,}", a):
            a = "+" + re.sub(r"\D", "", a)               # one spelling per number: "+1 555-..." is "+1555..."
        return {"peer": a}

    async def _rpc(self, method: str, **params):
        if not self.rpc:
            raise ConnectionError("signal-cli is not connected")
        try:
            return await self.rpc.call(method, **params)
        except RpcError as e:
            msg = str(e).lower()
            if any(w in msg for w in ("network", "timeout", "rate limit", "temporar")):
                raise
            raise PermanentError(str(e)) from e

    @staticmethod
    def _check_results(result: dict) -> None:
        results = (result or {}).get("results") or []
        failed = [r for r in results if r.get("type") != "SUCCESS"]
        if results and len(failed) == len(results):
            kinds = {r.get("type") for r in failed}
            if kinds & {"UNREGISTERED_FAILURE", "IDENTITY_FAILURE"}:
                raise PermanentError(f"signal send failed: {sorted(kinds)}")
            raise ConnectionError(f"signal send failed: {sorted(kinds)}")

    def _split_ts(self, platform_id: str) -> tuple[str, int]:
        author, _, ts = platform_id.rpartition(":")
        return author, int(ts)

    @staticmethod
    def _text(parts: list[dict]) -> tuple[str, list[str], list[str]]:
        """Text parts joined with newlines, and their style ranges and mentions as signal-cli takes
        them (start:length:STYLE and start:length:recipient, UTF-16 code units, shifted by where
        each part lands)."""
        texts, styles, mentions, at = [], [], [], 0
        for p in parts:
            if p.get("type") != "text":
                continue
            if texts:
                at += 1                                       # the joining newline
            texts.append(p["text"])
            styles += [f"{at + st['start']}:{st['length']}:{st['style']}" for st in p.get("styles") or []]
            mentions += [f"{at + m['start']}:{m['length']}:{m['id']}" for m in p.get("mentions") or []]
            at += len(p["text"].encode("utf-16-le")) // 2
        return "\n".join(texts), styles, mentions

    async def send(self, key, parts, reply_to):
        text, styles, mentions = self._text(parts)
        params = {**self._to(key), "message": text}
        if styles:
            params["textStyle"] = styles
        if mentions:
            params["mention"] = mentions
        files = []
        for p in parts:
            if p.get("type") == "file":
                row = self.gw.store.file(p["attachment_id"])
                files.append(row["path"])
        if files:
            params["attachments"] = files
        if reply_to and reply_to.get("platform_id"):
            author, ts = self._split_ts(reply_to["platform_id"])
            params.update(quoteTimestamp=ts, quoteAuthor=self.account if author == self.me else author)
        result = await self._rpc("send", **params)
        self._check_results(result)
        return f"{self.me}:{result['timestamp']}", None

    async def edit(self, key, platform_id, parts):
        _, ts = self._split_ts(platform_id)
        text, styles, mentions = self._text(parts)
        extra = {"textStyle": styles} if styles else {}
        if mentions:
            extra["mention"] = mentions
        self._check_results(await self._rpc("send", **self._to(key), message=text, editTimestamp=ts, **extra))

    async def delete(self, key, platform_id):
        _, ts = self._split_ts(platform_id)
        await self._rpc("remoteDelete", **self._to(key), targetTimestamp=ts)

    async def react(self, key, target, emoji, remove):
        author, ts = self._split_ts(target["platform_id"])
        await self._rpc("sendReaction", **self._to(key), emoji=emoji,
                        targetAuthor=self.account if author == self.me else author,
                        targetTimestamp=ts, remove=remove)

    async def typing(self, key, on):
        await self._rpc("sendTyping", **self._to(key), stop=not on)

    # ================================================================ calls
    async def _call_event(self, ev: dict) -> None:
        pid = str(ev.get("callId"))
        state = ev.get("state")
        if ev.get("inputDeviceName") and ev.get("outputDeviceName"):
            self.devices[pid] = (ev["inputDeviceName"], ev["outputDeviceName"])
        call_id = self.call_of.get(pid)
        if state == "RINGING_INCOMING" and not ev.get("isOutgoing"):
            if call_id is None:
                peer = ev.get("uuid") or ev.get("number") or "unknown"
                call = self.gw.calls.incoming(self, pid, peer, self.account, self.route,
                                              peer_alt=ev.get("number"))
                self.call_of[pid] = call.id
            return
        if call_id is None:
            self.early.setdefault(pid, []).append(ev)          # startCall has not returned yet
            return
        await self._apply(call_id, pid, ev)

    async def _apply(self, call_id: str, pid: str, ev: dict) -> None:
        state = ev.get("state")
        if state == "RINGING_OUTGOING":
            self.gw.calls.outbound_ringing(call_id)
        elif state == "CONNECTED":
            if pid in self.ports:
                self.gw.calls.state_note(call_id, "connected")  # back from RECONNECTING
                return
            dev = self.devices.get(pid)
            if not dev:
                log.error("call %s connected without audio device names", call_id)
                await self.hangup(call_id, pid)
                return
            port = self.port_factory(*dev)
            await port.open()
            self.ports[pid] = port
            await self.gw.calls.connected(call_id, port)
        elif state == "RECONNECTING":
            self.gw.calls.state_note(call_id, "reconnecting")
        elif state == "ENDED":
            port = self.ports.pop(pid, None)
            if port:
                await port.close()
            self.call_of.pop(pid, None)
            self.devices.pop(pid, None)
            await self.gw.calls.ended(call_id, (ev.get("reason") or "ended").lower())

    async def dial(self, call_id: str, to: str) -> str:
        res = await self._rpc("startCall", recipient=to)
        pid = str(res["callId"])
        if res.get("inputDeviceName"):
            self.devices[pid] = (res["inputDeviceName"], res["outputDeviceName"])
        self.call_of[pid] = call_id
        for ev in self.early.pop(pid, []):
            await self._apply(call_id, pid, ev)
        return pid

    async def answer(self, call_id: str, platform_id: str) -> None:
        res = await self._rpc("acceptCall", callId=int(platform_id))
        if isinstance(res, dict) and res.get("inputDeviceName"):
            self.devices[platform_id] = (res["inputDeviceName"], res["outputDeviceName"])

    async def reject(self, call_id: str, platform_id: str, reason: str) -> None:
        await self._rpc("rejectCall", callId=int(platform_id))

    async def hangup(self, call_id: str, platform_id: str) -> None:
        await self._rpc("hangupCall", callId=int(platform_id))

    async def send_dtmf(self, call_id, platform_id, digits):
        raise Unsupported("Signal calls have no DTMF")
