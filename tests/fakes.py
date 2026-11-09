"""Stand-ins for signal-cli, a call's Pulse audio, Telnyx's REST API, and an agent."""
from __future__ import annotations

import asyncio
import base64
import json
import socket
from urllib.parse import parse_qs, urlparse

import httpx
import websockets

from .spec_check import VIOLATIONS, streams


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class FakeSignalCli:
    """JSON-RPC over TCP, newline-delimited, like `signal-cli daemon --tcp`."""

    def __init__(self, aci: str = "aaaaaaaa-0000-0000-0000-000000000001"):
        self.aci = aci
        self.port = free_port()
        self.writers: list[asyncio.StreamWriter] = []
        self.calls: list[tuple[str, dict]] = []
        self.ts = 1_700_000_000_000
        self.server = None
        self.connected = asyncio.Event()
        self.next_call_devices = ("signal_input_1", "signal_output_1")

    async def start(self):
        self.server = await asyncio.start_server(self._client, "127.0.0.1", self.port)

    async def stop(self):
        for w in self.writers:
            w.close()
        self.server.close()

    async def _client(self, reader, writer):
        self.writers.append(writer)
        while line := await reader.readline():
            req = json.loads(line)
            method, params = req["method"], req.get("params") or {}
            self.calls.append((method, params))
            result = self._result(method, params)
            writer.write((json.dumps({"jsonrpc": "2.0", "id": req["id"], "result": result}) + "\n").encode())
            await writer.drain()
            if method == "subscribeReceive":
                self.connected.set()

    def _result(self, method, params):
        if method == "subscribeReceive":
            return 0
        if method == "subscribeCallEvents":
            return 1
        if method == "getUserStatus":
            return [{"recipient": params["recipient"][0], "number": params["recipient"][0],
                     "uuid": self.aci, "isRegistered": True}]
        if method == "send":
            self.ts += 1000
            return {"timestamp": self.ts, "results": [{"type": "SUCCESS"}]}
        if method in ("sendReaction", "remoteDelete"):
            self.ts += 1000
            return {"timestamp": self.ts}
        if method == "acceptCall":
            i, o = self.next_call_devices
            return {"callId": params["callId"], "state": "CONNECTING", "inputDeviceName": i, "outputDeviceName": o}
        if method == "startCall":
            i, o = self.next_call_devices
            return {"callId": 987654321, "state": "RINGING_OUTGOING", "inputDeviceName": i, "outputDeviceName": o}
        return {}

    def methods(self, name: str) -> list[dict]:
        return [p for m, p in self.calls if m == name]

    async def notify(self, method: str, result: dict, subscription: int = 0):
        msg = {"jsonrpc": "2.0", "method": method, "params": {"subscription": subscription, "result": result}}
        for w in self.writers:
            if not w.is_closing():
                w.write((json.dumps(msg) + "\n").encode())
                await w.drain()

    async def receive(self, envelope: dict, account: str = "+15550001111"):
        await self.notify("receive", {"account": account, "envelope": envelope})

    async def call_event(self, **ev):
        await self.notify("callEvent", ev, subscription=1)


class FakePort:
    """A Signal call's Pulse devices, in memory (48 kHz pcm16le)."""
    rate = 48000
    continuous = True

    def __init__(self, input_device: str, output_device: str):
        self.devices = (input_device, output_device)
        self.inbound: asyncio.Queue = asyncio.Queue()
        self.written: list[bytes] = []
        self.closed = False

    async def open(self):
        pass

    async def frames(self):
        while True:
            chunk = await self.inbound.get()
            if chunk is None:
                return
            yield chunk

    async def write(self, pcm: bytes):
        self.written.append(pcm)

    async def clear(self):
        pass

    async def close(self):
        self.closed = True
        self.inbound.put_nowait(None)


class FakeTelnyxApi:
    def __init__(self):
        self.requests: list[tuple[str, str, dict]] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content or b"{}")
        self.requests.append((request.method, request.url.path, body))
        if request.url.path.endswith("/calls") and request.method == "POST":
            return httpx.Response(200, json={"data": {"call_control_id": "v3:outbound-1"}})
        return httpx.Response(200, json={"data": {"result": "ok"}})

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handler), base_url="https://telnyx.test/v2")

    def actions(self, name: str) -> list[tuple[str, dict]]:
        return [(p, b) for m, p, b in self.requests if p.endswith(f"/actions/{name}")]


class Agent:
    """A voitta-media/1 agent: records everything, echoes audio, optional scripted behaviour."""

    def __init__(self, echo: bool = True, drop_after_start: int = 0):
        self.port = free_port()
        self.echo = echo
        self.drop_after_start = drop_after_start         # close the socket N times right after start
        self.messages: list[dict] = []
        self.connections = 0
        self.ws_list = []
        self.server = None
        self.started = asyncio.Event()
        self.stopped = asyncio.Event()

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/media"

    async def start(self):
        self.server = await websockets.serve(self._handle, "127.0.0.1", self.port, max_size=2 ** 22)

    async def stop(self):
        self.server.close()

    async def _handle(self, ws):
        self.connections += 1
        self.ws_list.append(ws)
        q = parse_qs(urlparse(ws.request.path).query)
        self.query = {k: v[0] for k, v in q.items()}
        try:
            async for raw in ws:
                msg = json.loads(raw)
                self.messages.append(msg)
                VIOLATIONS.add(streams().check_media(msg, "to_agent"), "media")
                ev = msg.get("event")
                if ev == "start":
                    self.started.set()
                    if self.drop_after_start > 0:
                        self.drop_after_start -= 1
                        await ws.close()
                        return
                elif ev == "media" and self.echo:
                    await ws.send(json.dumps({"event": "media", "payload": msg["payload"]}))
                elif ev == "stop":
                    self.stopped.set()
        except websockets.ConnectionClosed:
            pass

    async def send(self, msg: dict):
        VIOLATIONS.add(streams().check_media(msg, "to_gateway"))
        await self.ws_list[-1].send(json.dumps(msg))

    def of(self, event: str) -> list[dict]:
        return [m for m in self.messages if m.get("event") == event]

    def audio_bytes(self) -> bytes:
        return b"".join(base64.b64decode(m["payload"]) for m in self.of("media"))


# ================================================================ Slack
def slack_error(error: str, status: int = 200):
    from slack_sdk.errors import SlackApiError
    from slack_sdk.web.async_slack_response import AsyncSlackResponse
    resp = AsyncSlackResponse(client=None, http_verb="POST", api_url="x", req_args={},
                              data={"ok": False, "error": error}, headers={}, status_code=status)
    return SlackApiError(error, resp)


class FakeSlackWeb:
    """Slack Web API stand-in: records every call; `fail[method] = error` makes one fail."""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []
        self.n = 0
        self.fail: dict[str, str] = {}
        self.replies: dict[str, list[dict]] = {}        # thread ts -> messages
        self.history: dict[str, list[dict]] = {}        # channel -> messages, newest first

    def _rec(self, name: str, kw: dict) -> None:
        self.calls.append((name, kw))
        if name in self.fail:
            raise slack_error(self.fail[name])

    def of(self, name: str) -> list[dict]:
        return [kw for n, kw in self.calls if n == name]

    def _ts(self) -> str:
        self.n += 1
        return f"1700000100.{self.n:06d}"

    async def auth_test(self, **kw):
        return {"ok": True, "user_id": "UBOT", "team_id": "T1", "team": "Voitta", "bot_id": "BBOT"}

    async def chat_postMessage(self, **kw):
        self._rec("chat_postMessage", kw)
        if any(b.get("type") == "call" for b in kw.get("blocks") or []) and "post_call_block" in self.fail:
            raise slack_error(self.fail["post_call_block"])
        return {"ok": True, "ts": self._ts(), "channel": kw["channel"]}

    async def chat_update(self, **kw):
        self._rec("chat_update", kw)
        return {"ok": True}

    async def chat_delete(self, **kw):
        self._rec("chat_delete", kw)
        return {"ok": True}

    async def conversations_open(self, **kw):
        self._rec("conversations_open", kw)
        return {"ok": True, "channel": {"id": "D" + kw["users"]}}

    async def reactions_add(self, **kw):
        self._rec("reactions_add", kw)
        return {"ok": True}

    async def reactions_remove(self, **kw):
        self._rec("reactions_remove", kw)
        return {"ok": True}

    async def files_upload_v2(self, **kw):
        self._rec("files_upload_v2", kw)
        return {"ok": True, "files": [{"id": "F1"}]}

    async def conversations_replies(self, **kw):
        self._rec("conversations_replies", kw)
        return {"ok": True, "messages": self.replies.get(kw["ts"], [])}

    async def conversations_history(self, **kw):
        self._rec("conversations_history", kw)
        return {"ok": True, "messages": self.history.get(kw["channel"], [])}

    async def calls_add(self, **kw):
        self._rec("calls_add", kw)
        return {"ok": True, "call": {"id": f"R{self._ts().split('.')[1]}"}}

    async def calls_end(self, **kw):
        self._rec("calls_end", kw)
        return {"ok": True}

    async def calls_participants_add(self, **kw):
        self._rec("calls_participants_add", kw)
        return {"ok": True}

    def join_url(self) -> str:
        """The Join link of the last call card posted."""
        for name, kw in reversed(self.calls):
            if name == "chat_postMessage":
                for b in kw.get("blocks") or []:
                    for el in b.get("elements") or []:
                        if el.get("action_id") == "voitta_call_join":
                            return el["url"]
        for name, kw in reversed(self.calls):
            if name == "calls_add":
                return kw["join_url"]
        raise AssertionError("no call card posted")


class BrowserSim:
    """What the join page does, without a browser: hello, pcm16 frames up, plays what comes down."""

    def __init__(self, join_url: str):
        base, _, query = join_url.partition("?")
        self.url = base.replace("http://", "ws://").replace("https://", "wss://") + "/audio?" + query
        self.audio: list[bytes] = []
        self.events: list[dict] = []
        self.ws = None
        self.task = None

    async def connect(self, rate: int = 48000):
        self.ws = await websockets.connect(self.url, max_size=2 ** 22)
        self.task = asyncio.create_task(self._read())
        try:
            await self.ws.send(json.dumps({"event": "hello", "rate": rate}))
        except websockets.ConnectionClosed:
            pass                                   # an ended call: the server said so and closed

    async def _read(self):
        try:
            async for msg in self.ws:
                if isinstance(msg, bytes):
                    self.audio.append(msg)
                else:
                    self.events.append(json.loads(msg))
        except websockets.ConnectionClosed:
            pass

    def of(self, event: str) -> list[dict]:
        return [e for e in self.events if e.get("event") == event]

    async def send(self, pcm: bytes):
        await self.ws.send(pcm)

    async def text(self, msg: dict):
        await self.ws.send(json.dumps(msg))

    async def close(self):
        if self.ws:
            await self.ws.close()
