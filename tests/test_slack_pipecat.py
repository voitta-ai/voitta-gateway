"""Slack adapter against a stub Web API; Pipecat example bot as a real agent on a Signal call."""
import asyncio
import importlib.util
import sys
import time
from pathlib import Path

import numpy as np
import pytest
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_slack_response import AsyncSlackResponse

from voitta_gateway.adapters.slack import SlackAdapter
from voitta_gateway.audio import tone

from .conftest import serve
from .fakes import FakePort, FakeSignalCli
from .test_signal import ROMAN, config as signal_config, up, wait_for


class FakeWeb:
    def __init__(self):
        self.calls: list[tuple[str, dict, float]] = []
        self.n = 0

    def _rec(self, name, kw):
        self.calls.append((name, kw, time.monotonic()))

    def of(self, name):
        return [kw for n, kw, _ in self.calls if n == name]

    async def chat_postMessage(self, **kw):
        self._rec("chat_postMessage", kw)
        if kw["channel"] == "CGONE":
            resp = AsyncSlackResponse(client=None, http_verb="POST", api_url="x", req_args={},
                                      data={"ok": False, "error": "channel_not_found"}, headers={}, status_code=200)
            raise SlackApiError("channel_not_found", resp)
        self.n += 1
        return {"ok": True, "ts": f"1700000100.{self.n:06d}"}

    async def conversations_open(self, **kw):
        self._rec("conversations_open", kw)
        return {"ok": True, "channel": {"id": "DNEW"}}

    async def reactions_add(self, **kw):
        self._rec("reactions_add", kw)
        return {"ok": True}

    async def chat_update(self, **kw):
        self._rec("chat_update", kw)
        return {"ok": True}


@pytest.fixture
def fake_slack(monkeypatch):
    web = FakeWeb()

    async def start(self, gw):
        self.gw, self.web, self.bot_user, self.team, self.account = gw, web, "UBOT", "T1", "T1/UBOT"
        gw.channel_state(self.name, True)

    async def stop(self):
        pass

    monkeypatch.setattr(SlackAdapter, "start", start)
    monkeypatch.setattr(SlackAdapter, "stop", stop)
    import os
    os.environ["SLACK_BOT"], os.environ["SLACK_APP"] = "xoxb-test", "xapp-test"
    return web


async def test_slack_messages(gateway, fake_slack):
    web = fake_slack
    gw = await gateway({"adapters": {"slack": {"bot_token_secret": "SLACK_BOT", "app_token_secret": "SLACK_APP",
                                               "min_interval_s": 0.3}},
                        "persons": {"p_roman": {"slack": ["T1/U1"]}},
                        "policy": {"inbound_allowlist": ["p_roman"]}})
    await wait_for(lambda: gw.gw.channel_up.get("slack"))
    slack: SlackAdapter = gw.gw.messaging["slack"]

    # DM -> direct conversation keyed by user, person resolved
    await slack.handle_event({"type": "message", "channel": "D1", "channel_type": "im", "user": "U1",
                              "text": "hello", "ts": "1700000000.000100", "team": "T1"})
    ev = await gw.wait_event("message.received")
    d = ev["data"]
    assert d["conversation_kind"] == "direct" and d["person_id"] == "p_roman"

    # channel chatter without a mention is dropped; a mention gets through
    await slack.handle_event({"type": "message", "channel": "C1", "channel_type": "channel", "user": "U1",
                              "text": "lunch?", "ts": "1700000000.000200"})
    await slack.handle_event({"type": "message", "channel": "C1", "channel_type": "channel", "user": "U1",
                              "text": "<@UBOT> status?", "ts": "1700000000.000300"})
    ch = await gw.wait_event("message.received", pred=lambda e: e["data"]["conversation_kind"] == "group")
    assert ch["data"]["parts"][0]["text"] == "<@UBOT> status?"

    # replies: DM goes to the DM channel; channel reply goes into the thread; pacing per channel
    for body in ({"to": {"conversation_id": d["conversation_id"]}, "parts": [{"type": "text", "text": "hi"}]},
                 {"to": {"conversation_id": ch["data"]["conversation_id"]}, "reply_to": ch["data"]["message_id"],
                  "parts": [{"type": "text", "text": "all green"}]},
                 {"to": {"conversation_id": ch["data"]["conversation_id"]}, "parts": [{"type": "text", "text": "2"}]}):
        assert (await gw.client.post("/v1/messages", json=body)).status_code == 202
    await wait_for(lambda: len(web.of("chat_postMessage")) == 3)
    posts = web.of("chat_postMessage")
    assert posts[0]["channel"] == "D1" and "thread_ts" not in posts[0]
    assert posts[1]["channel"] == "C1" and posts[1]["thread_ts"] == "1700000000.000300"
    assert posts[1]["blocks"][0]["type"] == "markdown"
    t = [ts for n, kw, ts in web.calls if n == "chat_postMessage" and kw["channel"] == "C1"]
    assert t[1] - t[0] >= 0.29

    # reaction with a Unicode emoji -> Slack name
    r = await gw.client.post(f"/v1/messages/{d['message_id']}/reactions", json={"emoji": "👍"})
    assert r.status_code == 202
    await wait_for(lambda: web.of("reactions_add"))
    assert web.of("reactions_add")[0] == {"channel": "D1", "timestamp": "1700000000.000100", "name": "thumbsup"}

    # Slack has no typing indicator: refused up front
    r = await gw.client.post(f"/v1/conversations/{d['conversation_id']}/typing", json={"on": True})
    assert r.status_code == 422

    # edit by the user -> message.edited pointing at our id for it
    await slack.handle_event({"type": "message", "subtype": "message_changed", "channel": "D1",
                              "channel_type": "im", "message": {"user": "U1", "text": "hello!",
                                                                "ts": "1700000000.000100"},
                              "event_ts": "1700000001.0"})
    ed = await gw.wait_event("message.edited")
    assert ed["data"]["message_id"] == d["message_id"]

    # agent-initiated DM to a person reuses the DM conversation (and its known channel)
    r = await gw.client.post("/v1/messages", json={"to": {"person_id": "p_roman", "channel": "slack"},
                                                   "parts": [{"type": "text", "text": "ping"}]})
    assert r.status_code == 202 and r.json()["conversation_id"] == d["conversation_id"]
    await wait_for(lambda: any(p["text"] == "ping" and p["channel"] == "D1" for p in web.of("chat_postMessage")))
    assert not web.of("conversations_open")

    # a person we never heard from on Slack: the DM channel is opened on demand
    gw.gw.identity.by_addr[("slack", "U2")] = "p_new"
    gw.gw.identity.persons["p_new"] = type(gw.gw.identity.persons["p_roman"])(slack=["U2"])
    r = await gw.client.post("/v1/messages", json={"to": {"person_id": "p_new"},
                                                   "parts": [{"type": "text", "text": "welcome"}]})
    assert r.status_code == 202
    await wait_for(lambda: any(p["text"] == "welcome" and p["channel"] == "DNEW" for p in web.of("chat_postMessage")))
    assert web.of("conversations_open") == [{"users": "U2"}]

    # a permanent Slack error fails the message, visibly
    gone_cid = gw.gw.store.conversation("slack", {"channel": "CGONE"}, "group")
    r = await gw.client.post("/v1/messages", json={"to": {"conversation_id": gone_cid},
                                                   "parts": [{"type": "text", "text": "x"}]})
    failed = await gw.wait_event("message.status", pred=lambda e: e["data"]["status"] == "failed")
    assert "channel_not_found" in failed["data"]["error"]


async def test_pipecat_bot_echoes_a_signal_call(gateway):
    """examples/pipecat_bot.py (echo mode) as the agent: serializer + Pipecat transport end to end."""
    spec = importlib.util.spec_from_file_location("pipecat_bot", Path(__file__).parent.parent / "examples/pipecat_bot.py")
    bot = importlib.util.module_from_spec(spec)
    sys.modules["pipecat_bot"] = bot
    spec.loader.exec_module(bot)
    import os
    os.environ["VOITTA_MEDIA_SECRET"] = "media-secret"
    os.environ.pop("MODE", None)
    server, task, url = await serve(bot.app)

    fake = FakeSignalCli()
    await fake.start()
    ports = []

    def factory(i, o):
        ports.append(FakePort(i, o))
        return ports[-1]

    bot_url = url.replace("http://", "ws://") + "/media"

    class A:                                                    # route to the Pipecat bot
        url = bot_url
    gw = await gateway(signal_config(fake, A), signal_port_factory=factory)
    await up(gw, fake)
    await fake.call_event(callId=5, state="RINGING_INCOMING", uuid=ROMAN, isOutgoing=False,
                          inputDeviceName="i", outputDeviceName="o")
    await wait_for(lambda: fake.methods("acceptCall"))
    await fake.call_event(callId=5, state="CONNECTED", uuid=ROMAN, isOutgoing=False)
    await gw.wait_event("call.answered")
    await wait_for(lambda: ports)
    for _ in range(100):                                        # 2 s of tone from the caller
        ports[0].inbound.put_nowait(tone(48000, 20, hz=400, amp=8000))
        await asyncio.sleep(0.02)

    def loud():
        return [w for w in ports[0].written if np.abs(np.frombuffer(w, "<i2")).max() > 2000]
    await wait_for(lambda: len(loud()) > 20, timeout=8)

    await fake.call_event(callId=5, state="ENDED", uuid=ROMAN, isOutgoing=False, reason="REMOTE_HANGUP")
    await gw.wait_event("call.ended")
    server.should_exit = True
    await task
    await fake.stop()
