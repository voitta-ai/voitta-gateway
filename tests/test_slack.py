"""Slack end to end against a simulated Slack: messaging hardening, voice clips, interactions,
blocks, history, and outbound voice calls through the browser join page."""
import asyncio
import base64
import os

import httpx
import numpy as np
import pytest

import voitta_gateway.adapters.slack as slack_mod
from voitta_ask.service import AskConfig, AskService, build_app
from voitta_gateway.adapters.slack import SlackAdapter, slack_host
from voitta_gateway.audio import silence, tone
from voitta_speech.engines import FakeSTT, FakeTTS

from .conftest import serve
from .fakes import Agent, BrowserSim, FakeSlackWeb, free_port
from .test_signal import wait_for

ROMAN = "U1"


@pytest.fixture
def web(monkeypatch):
    fake = FakeSlackWeb()

    async def start(self, gw):
        self.gw, self.web = gw, fake
        auth = await fake.auth_test()
        self.bot_user, self.team, self.bot_id = auth["user_id"], auth["team_id"], auth["bot_id"]
        self.account = "Voitta/UBOT"
        gw.channel_state(self.name, True)

    monkeypatch.setattr(SlackAdapter, "start", start)
    os.environ["SLACK_BOT"], os.environ["SLACK_APP"] = "xoxb-test", "xapp-test"
    return fake


def config(agent_url: str | None = None, speech_url: str | None = None, **calls) -> dict:
    cfg = {"adapters": {"slack": {"bot_token_secret": "SLACK_BOT", "app_token_secret": "SLACK_APP",
                                  "min_interval_s": 0}},
           "persons": {"p_roman": {"slack": [f"T1/{ROMAN}"]}},
           "policy": {"inbound_allowlist": ["p_roman"]},
           "conversations": {c.lower(): {"channel": "slack", "key": c} for c in ("C1", "C2", "C3", "C4")}}
    if agent_url:
        cfg["adapters"]["slack"]["calls"] = {"public_base_url": "http://placeholder", "route": "main", **calls}
        cfg["routes"] = {"main": {"media_url": agent_url, "media_secret_secret": "TEST_MEDIA_SECRET",
                                  "connect_timeout_s": 1.0, "fallback": "hangup"}}
    if speech_url:
        cfg["speech"] = {"url": speech_url, "token_secret": "SPEECH_TOKEN", "timeout_s": 5}
    return cfg


async def started(gateway, web, cfg, **kw):
    gw = await gateway(cfg, **kw)
    await wait_for(lambda: gw.gw.channel_up.get("slack"))
    sl: SlackAdapter = gw.gw.messaging["slack"]
    if sl.cfg.calls:
        sl.cfg.calls.public_base_url = gw.ingress_url
    return gw, sl


def msg(channel, ts, text="", user=ROMAN, **kw):
    ev = {"type": "message", "channel": channel, "user": user, "text": text, "ts": ts, "team": "T1",
          "channel_type": "im" if channel.startswith("D") else "channel"}
    ev.update(kw)
    return ev


# ================================================================ messaging
def test_slack_host():
    assert slack_host("https://files.slack.com/x") and slack_host("https://slack.com/x")
    assert not slack_host("http://files.slack.com/x")              # never over plain http
    assert not slack_host("https://files.slack.com.evil.com/x")
    assert not slack_host("https://evil.com/files.slack.com")


async def test_bots_broadcasts_and_safe_downloads(gateway, web):
    gw, sl = await started(gateway, web, config())
    seen_hosts = []

    def files(request: httpx.Request) -> httpx.Response:
        seen_hosts.append((request.url.host, request.headers.get("authorization")))
        path = request.url.path
        if path == "/files-pri/a.png":
            return httpx.Response(302, headers={"location": "https://files-edge.slack.com/a.png"})
        if request.url.host == "files-edge.slack.com":
            return httpx.Response(200, content=b"PNGDATA", headers={"content-type": "image/png"})
        if path == "/files-pri/evil.png":
            return httpx.Response(302, headers={"location": "https://evil.example.com/steal"})
        if path == "/files-pri/login.png":
            return httpx.Response(200, content=b"<html>sign in</html>", headers={"content-type": "text/html"})
        return httpx.Response(404)

    sl.http_transport = httpx.MockTransport(files)

    # another bot: dropped unless it mentions us; then delivered as kind=bot (policy still applies: it is
    # not on the allowlist, so first it is held like any stranger)
    await sl.handle_event(msg("C1", "1.0", "<@UBOT> hello", user="UOTHER", bot_id="BOTHER",
                              bot_profile={"name": "Listener"}))
    await gw.wait_event("message.held")
    gw.gw.cfg.policy.inbound_allowlist = "*"
    await sl.handle_event(msg("C1", "1.1", "status report", user="UOTHER", bot_id="BOTHER",
                              bot_profile={"name": "Listener"}))
    await sl.handle_event(msg("C1", "1.2", "<@UBOT> transcript ready", user="UOTHER", bot_id="BOTHER",
                              bot_profile={"name": "Listener"}))
    await sl.handle_event(msg("C1", "1.3", "<@UBOT> echo", user="UBOT", bot_id="BBOT"))   # ourselves
    ev = await gw.wait_event("message.received")
    assert ev["data"]["sender"] == {"id": "UOTHER", "kind": "bot", "name": "Listener", "team": "T1"}
    assert [e["data"]["parts"][0]["text"] for e in await gw.events("message.received")] == ["<@UBOT> transcript ready"]

    # @here in a channel counts as addressed for people
    await sl.handle_event(msg("C1", "1.4", "<!here> anyone around?"))
    ev = await gw.wait_event("message.received")
    assert ev["data"]["parts"][0]["text"] == "<!here> anyone around?" and ev["data"]["sender"]["kind"] == "human"

    # files: a redirect inside slack.com is followed; one leaving it never gets the token; sign-in page refused
    await sl.handle_event(msg("D1", "1.5", "three files", subtype="file_share", files=[
        {"id": "F1", "name": "a.png", "mimetype": "image/png", "size": 7,
         "url_private_download": "https://files.slack.com/files-pri/a.png"},
        {"id": "F2", "name": "evil.png", "mimetype": "image/png", "size": 7,
         "url_private_download": "https://files.slack.com/files-pri/evil.png"},
        {"id": "F3", "name": "login.png", "mimetype": "image/png", "size": 7,
         "url_private_download": "https://files.slack.com/files-pri/login.png"}]))
    ev = await gw.wait_event("message.received")
    good, evil, login = ev["data"]["parts"][1:]
    assert good["attachment_id"] and good["size"] == 7
    assert evil["missing"] and login["missing"]
    assert "evil.example.com" not in [h for h, _ in seen_hosts]
    assert all(auth == "Bearer xoxb-test" for _, auth in seen_hosts)


async def test_voice_clips_are_transcribed(gateway, web):
    os.environ["SPEECH_TOKEN"] = os.environ["ASK_CLIENT_TOKEN"] = "speech-token"
    stt = FakeSTT("call me back after lunch")
    ask_port = free_port()
    ask_cfg = AskConfig.model_validate({
        "listen": f"127.0.0.1:{ask_port}", "gateway": {"url": "http://127.0.0.1:1", "token_secret": "TEST_TOKEN"},
        "media_secret_secret": "TEST_MEDIA_SECRET", "clients": {"gateway": "SPEECH_TOKEN"},
        "tts": {"engine": "fake"}, "stt": {"engine": "fake"}})
    server, task, speech_url = await serve(build_app(AskService(ask_cfg, tts=FakeTTS(), stt=stt)), ask_port)
    gw, sl = await started(gateway, web, config(speech_url=speech_url))
    sl.http_transport = httpx.MockTransport(
        lambda r: httpx.Response(200, content=b"AUDIO" * 100, headers={"content-type": "audio/mp4"}))

    def clip(fid, **extra):
        return {"id": fid, "name": "Audio clip", "mimetype": "audio/mp4", "subtype": "slack_audio", "size": 500,
                "duration_ms": 4200, "url_private_download": f"https://files.slack.com/files-pri/{fid}.m4a", **extra}

    # Slack already transcribed it: used as is, our model not called
    await sl.handle_event(msg("D1", "2.1", subtype="file_share", files=[clip("C1", transcription={
        "status": "complete", "preview": {"content": "running ten minutes late", "has_more": False}})]))
    ev = await gw.wait_event("message.received")
    part = ev["data"]["parts"][0]
    assert part["transcript"] == "running ten minutes late" and part["transcript_source"] == "slack"
    assert part["clip"] is True and part["duration_s"] == 4.2 and stt.files == []

    # no Slack transcript: voitta-speech transcribes it
    await sl.handle_event(msg("D1", "2.2", subtype="file_share", files=[clip("C2")]))
    ev = await gw.wait_event("message.received")
    part = ev["data"]["parts"][0]
    assert part["transcript"] == "call me back after lunch" and part["transcript_source"] == "voitta-speech"
    assert stt.files == [b"AUDIO" * 100]

    # a stranger's clip is held, and never sent to the model
    await sl.handle_event(msg("D9", "2.3", user="U9", subtype="file_share", files=[clip("C3")]))
    await gw.wait_event("message.held")
    assert len(stt.files) == 1

    # speech service down: the message still arrives, with the reason
    server.should_exit = True
    await task
    await sl.handle_event(msg("D1", "2.4", subtype="file_share", files=[clip("C4")]))
    ev = await gw.wait_event("message.received", timeout=10)
    assert "transcript" not in ev["data"]["parts"][0] and ev["data"]["parts"][0]["transcript_error"]


async def test_interactions_blocks_and_history(gateway, web):
    gw, sl = await started(gateway, web, config())
    await sl.handle_event(msg("C1", "3.1", "<@UBOT> approve the deploy?"))
    ev = await gw.wait_event("message.received")
    cid, asked = ev["data"]["conversation_id"], ev["data"]["message_id"]

    # a Block Kit card, in the thread of the question
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "Deploy *v2*?"}},
              {"type": "actions", "elements": [{"type": "button", "action_id": "approve", "value": "v2",
                                                "text": {"type": "plain_text", "text": "Approve"}}]}]
    r = await gw.client.post("/v1/messages", json={"to": {"conversation_id": cid}, "reply_to": asked,
                                                   "parts": [{"type": "blocks", "blocks": blocks,
                                                              "text": "Deploy v2?"}]})
    assert r.status_code == 202, r.text
    card = r.json()["message_id"]
    await gw.wait_event("message.status", pred=lambda e: e["data"]["status"] == "sent")
    post = web.of("chat_postMessage")[-1]
    assert post["blocks"] == blocks and post["text"] == "Deploy v2?" and post["thread_ts"] == "3.1"
    r = await gw.client.post("/v1/messages", json={"to": {"conversation_id": cid},
                                                   "parts": [{"type": "blocks", "blocks": []}]})
    assert r.status_code == 422 and r.json()["error"]["code"] == "bad_blocks"

    # the click comes back as interaction.received, tied to our card; our own Join buttons are not events
    card_ts = gw.gw.store.message(card)["platform_id"]
    await sl.handle_interaction({"type": "block_actions", "user": {"id": ROMAN}, "channel": {"id": "C1"},
                                 "message": {"ts": card_ts}, "actions": [
                                     {"action_id": "approve", "block_id": "b1", "type": "button", "value": "v2",
                                      "action_ts": "1700000200.1"},
                                     {"action_id": "voitta_call_join", "type": "button"}]})
    it = await gw.wait_event("interaction.received")
    assert it["data"]["message_id"] == card and it["data"]["value"] == "v2"
    assert it["data"]["person_id"] == "p_roman" and it["data"]["action_id"] == "approve"
    assert len(await gw.events("interaction.received")) == 1

    # edit the card into a verdict
    r = await gw.client.post(f"/v1/messages/{card}/edit", json={"parts": [
        {"type": "blocks", "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "Approved"}}],
         "text": "Approved"}]})
    assert r.status_code == 202
    await gw.wait_event("operation.done")
    assert web.of("chat_update")[-1]["text"] == "Approved"

    # thread history from the platform: who said what, with our ids where we know them
    web.replies["3.1"] = [
        {"ts": "3.1", "user": ROMAN, "text": "<@UBOT> approve the deploy?", "thread_ts": "3.1"},
        {"ts": "3.2", "user": "U7", "text": "+1 from me", "thread_ts": "3.1"},
        {"ts": card_ts, "user": "UBOT", "bot_id": "BBOT", "text": "Deploy v2?", "thread_ts": "3.1"}]
    r = await gw.client.get(f"/v1/conversations/{cid}/history", params={"thread": asked, "limit": 10})
    assert r.status_code == 200, r.text
    h = r.json()["messages"]
    assert [m["sender"]["kind"] for m in h] == ["human", "human", "self"]
    assert h[0]["message_id"] == asked and h[0]["person_id"] == "p_roman"
    assert h[1]["message_id"] is None and h[2]["message_id"] == card


# ================================================================ calls
async def test_slack_call_answered(gateway, web):
    agent = Agent()
    await agent.start()
    gw, sl = await started(gateway, web, config(agent.url))
    ch = {c["channel"]: c for c in (await gw.client.get("/v1/channels")).json()["channels"]}
    assert ch["slack"]["calls"]["outbound"] is True and ch["slack"]["messaging"]["blocks"] is True

    r = await gw.client.post("/v1/calls", json={"to": {"person_id": "p_roman"}, "via": "slack"})
    assert r.status_code == 202, r.text
    call_id = r.json()["call_id"]
    await gw.wait_event("call.ringing", pred=lambda e: e["data"]["call_id"] == call_id)
    card = web.of("chat_postMessage")[-1]
    assert card["channel"] == f"D{ROMAN}" and "is calling you" in card["blocks"][0]["text"]["text"]
    join = web.join_url()

    async with httpx.AsyncClient() as http:
        page = await http.get(join)
        assert page.status_code == 200 and "Join call" in page.text and "{{" not in page.text
        assert (await http.get(join.replace("t=", "t=x"))).status_code == 403

    browser = BrowserSim(join)
    await browser.connect(48000)
    await gw.wait_event("call.answered")
    await asyncio.wait_for(agent.started.wait(), 3)
    start = agent.of("start")[0]
    assert start["channel"] == "slack" and start["direction"] == "outbound"
    assert "On a call" in web.of("chat_update")[-1]["blocks"][0]["text"]["text"]

    # mic audio 48 kHz up -> agent at 16 kHz; agent's echo -> browser at 48 kHz in 20 ms frames
    for _ in range(25):
        await browser.send(tone(48000, 20, hz=400, amp=8000))
    await wait_for(lambda: len(agent.of("media")) >= 20)
    assert all(len(base64.b64decode(m["payload"])) == 640 for m in agent.of("media"))
    await wait_for(lambda: len(browser.audio) >= 15)
    loud = [a for a in browser.audio if np.abs(np.frombuffer(a, "<i2")).max() > 2000]
    assert loud and all(len(a) == 1920 for a in browser.audio)

    # barge-in reaches the page; stats from the page are kept
    await agent.send({"event": "clear"})
    await wait_for(lambda: browser.of("clear"))
    await browser.text({"event": "stats", "played_ms": 480, "underruns": 0, "captured_ms": 500})
    await wait_for(lambda: sl.calls[call_id].port.stats.get("played_ms") == 480)

    # the agent hangs up: the page is told, the card says how long it lasted
    await agent.send({"event": "hangup", "reason": "done"})
    end = await gw.wait_event("call.ended")
    assert end["data"]["answered"] and end["data"]["reason"] == "done"
    await wait_for(lambda: browser.of("ended"))
    assert "ended" in web.of("chat_update")[-1]["blocks"][0]["text"]["text"]
    await browser.close()
    await agent.stop()


async def test_slack_call_missed_and_page_hangup(gateway, web, monkeypatch):
    monkeypatch.setattr(slack_mod, "REJOIN_GRACE_S", 0.5)
    agent = Agent()
    await agent.start()
    gw, sl = await started(gateway, web, config(agent.url, ring_timeout_s=0.5))

    # nobody joins: missed
    r = await gw.client.post("/v1/calls", json={"to": ROMAN, "via": "slack"})
    missed_id = r.json()["call_id"]
    end = await gw.wait_event("call.ended", pred=lambda e: e["data"]["call_id"] == missed_id)
    assert end["data"]["reason"] == "timeout" and not end["data"]["answered"]
    assert "Missed call" in web.of("chat_update")[-1]["blocks"][0]["text"]["text"]
    late = BrowserSim(web.join_url())
    await late.connect()
    await wait_for(lambda: late.of("ended"))
    async with httpx.AsyncClient() as http:
        assert 'const STATE = "ended"' in (await http.get(web.join_url())).text

    # joined, page reloaded (socket dropped and back within the grace), then Hang up on the page
    sl.cfg.calls.ring_timeout_s = 30
    r = await gw.client.post("/v1/calls", json={"to": ROMAN, "via": "slack"})
    call_id = r.json()["call_id"]
    await gw.wait_event("call.ringing", pred=lambda e: e["data"]["call_id"] == call_id)
    first = BrowserSim(web.join_url())
    await first.connect()
    await gw.wait_event("call.answered", pred=lambda e: e["data"]["call_id"] == call_id)
    await first.close()
    second = BrowserSim(web.join_url())
    await second.connect()
    await asyncio.sleep(0.8)                                   # past the grace: still up, the reload won
    assert call_id in sl.calls
    await second.text({"event": "hangup"})
    end = await gw.wait_event("call.ended", pred=lambda e: e["data"]["call_id"] == call_id)
    assert end["data"]["reason"] == "remote_hangup" and end["data"]["answered"]

    # joined, then the tab is closed for good: ends after the grace
    r = await gw.client.post("/v1/calls", json={"to": ROMAN, "via": "slack"})
    gone_id = r.json()["call_id"]
    await gw.wait_event("call.ringing", pred=lambda e: e["data"]["call_id"] == gone_id)
    third = BrowserSim(web.join_url())
    await third.connect()
    await gw.wait_event("call.answered", pred=lambda e: e["data"]["call_id"] == gone_id)
    await third.close()
    end = await gw.wait_event("call.ended", pred=lambda e: e["data"]["call_id"] == gone_id)
    assert end["data"]["reason"] == "remote_hangup"
    await agent.stop()


async def test_slack_calls_api_card_and_fallback(gateway, web):
    agent = Agent()
    await agent.start()
    gw, sl = await started(gateway, web, config(agent.url, card="calls_api"))

    r = await gw.client.post("/v1/calls", json={"to": ROMAN, "via": "slack"})
    call_id = r.json()["call_id"]
    await gw.wait_event("call.ringing", pred=lambda e: e["data"]["call_id"] == call_id)
    add = web.of("calls_add")[-1]
    assert add["external_unique_id"] == call_id and add["created_by"] == "UBOT" and add["join_url"].startswith(gw.ingress_url)
    assert web.of("chat_postMessage")[-1]["blocks"][0]["type"] == "call"
    browser = BrowserSim(web.join_url())
    await browser.connect()
    await gw.wait_event("call.answered")
    await wait_for(lambda: web.of("calls_participants_add"))
    assert web.of("calls_participants_add")[-1]["users"] == [{"slack_id": ROMAN}]
    await gw.client.post(f"/v1/calls/{call_id}/hangup")
    await gw.wait_event("call.ended")
    await wait_for(lambda: web.of("calls_end"))

    # Slack refuses the native card in DMs: the call is ended there and the Join button is used instead
    web.fail["post_call_block"] = "not_allowed_token_type"
    r = await gw.client.post("/v1/calls", json={"to": ROMAN, "via": "slack"})
    second = r.json()["call_id"]
    await gw.wait_event("call.ringing", pred=lambda e: e["data"]["call_id"] == second)
    assert len(web.of("calls_end")) == 2
    assert web.of("chat_postMessage")[-1]["blocks"][1]["elements"][0]["action_id"] == "voitta_call_join"
    await gw.client.post(f"/v1/calls/{second}/hangup")
    await browser.close()
    await agent.stop()


async def test_voitta_ask_over_slack(gateway, web):
    """The same voitta-ask, unchanged, asking over a Slack call."""
    os.environ["ASK_CLIENT_TOKEN"] = "client-token"
    ask_port = free_port()
    gw, sl = await started(gateway, web, config(f"ws://127.0.0.1:{ask_port}/media"))
    ask_cfg = AskConfig.model_validate({
        "listen": f"127.0.0.1:{ask_port}", "gateway": {"url": gw.api_url, "token_secret": "TEST_TOKEN", "route": "main"},
        "media_secret_secret": "TEST_MEDIA_SECRET", "clients": {"tester": "ASK_CLIENT_TOKEN"},
        "tts": {"engine": "fake"}, "stt": {"engine": "fake"},
        "answer": {"end_silence_s": 1.0, "no_speech_s": 5.0, "max_answer_s": 15.0}})
    server, task, url = await serve(build_app(AskService(ask_cfg, tts=FakeTTS(), stt=FakeSTT("yes, at ten"))), ask_port)
    async with httpx.AsyncClient(base_url=url, headers={"Authorization": "Bearer client-token"}, timeout=30) as c:
        ask = (await c.post("/v1/asks", json={"to": ROMAN, "via": "slack", "text": "Review at ten?"})).json()
        assert ask["state"] == "calling", ask
        await wait_for(lambda: c and web.of("chat_postMessage"))
        browser = BrowserSim(web.join_url())
        await browser.connect()
        for _ in range(200):
            if (await c.get(f"/v1/asks/{ask['ask_id']}")).json()["state"] == "listening":
                break
            await asyncio.sleep(0.05)
        assert any(np.abs(np.frombuffer(a, "<i2")).max() > 2000 for a in browser.audio)   # heard the question
        for _ in range(50):
            await browser.send(tone(48000, 20, hz=220, amp=7000))
        for _ in range(75):
            await browser.send(silence(48000, 20))
        done = (await c.get(f"/v1/asks/{ask['ask_id']}", params={"wait": 15})).json()
        assert done["state"] == "done" and done["transcript"] == "yes, at ten", done
        assert done["via"] == "slack" and done["end_reason"] == "end_of_speech"
        await wait_for(lambda: browser.of("ended"), timeout=8)
        await browser.close()
    server.should_exit = True
    await task


def test_watchdog_signals():
    from collections import deque
    from types import SimpleNamespace

    from voitta_gateway.config import SlackConfig
    sl = SlackAdapter(SlackConfig(bot_token_secret="x", app_token_secret="y"), "xoxb", "xapp")
    wall, mono = 1_000_000.0, 5000.0
    sl.client = SimpleNamespace(connects=deque([mono - 120]), last_ping_pong_time=wall - 5)
    assert sl.healthy(wall, mono) == (True, "")
    sl.client.last_ping_pong_time = wall - 90                       # session up, but silent
    ok, why = sl.healthy(wall, mono)
    assert not ok and "no pong for 90 s" in why
    sl.client.last_ping_pong_time = None                            # never a pong since connecting
    ok, why = sl.healthy(wall, mono)
    assert not ok and "no pong" in why
    sl.client = SimpleNamespace(connects=deque(mono - 20 * i for i in range(6)), last_ping_pong_time=wall - 1)
    ok, why = sl.healthy(wall, mono)                                # pongs fresh, but sessions keep dying young
    assert not ok and "6 sessions in 5 minutes" in why
