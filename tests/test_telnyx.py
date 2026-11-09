"""Phone end to end: signed Telnyx webhooks + Telnyx's media WebSocket <-> gateway <-> agent."""
import asyncio
import base64
import json
import os
import time

import httpx
import numpy as np
import pytest
import websockets
from nacl.signing import SigningKey

from voitta_gateway.audio import swap16, tone

from .fakes import Agent, FakeTelnyxApi
from .test_signal import wait_for

OUR = "+15550002222"
ROMAN = "+15551112222"


@pytest.fixture
def key():
    k = SigningKey.generate()
    os.environ["TELNYX_API_KEY"] = "KEY_test"
    os.environ["TELNYX_PUBLIC_KEY"] = base64.b64encode(bytes(k.verify_key)).decode()
    return k


def signed(key, body: dict) -> tuple[bytes, dict]:
    raw = json.dumps(body).encode()
    ts = str(int(time.time()))
    sig = key.sign(ts.encode() + b"|" + raw).signature
    return raw, {"telnyx-signature-ed25519": base64.b64encode(sig).decode(), "telnyx-timestamp": ts,
                 "content-type": "application/json"}


def hook(event_type: str, eid: str, **payload) -> dict:
    return {"data": {"event_type": event_type, "id": eid, "occurred_at": "2026-11-08T12:00:00Z",
                     "record_type": "event", "payload": payload}}


async def start(gateway, agent, api: FakeTelnyxApi):
    return await gateway({
        "listen": {"ingress": "127.0.0.1:0"},
        "adapters": {"phone": {"api_key_secret": "TELNYX_API_KEY", "public_key_secret": "TELNYX_PUBLIC_KEY",
                               "connection_id": "conn-1", "numbers": [OUR], "route": "main",
                               "public_base_url": "http://placeholder"}},
        "routes": {"main": {"media_url": agent.url, "media_secret_secret": "TEST_MEDIA_SECRET",
                            "connect_timeout_s": 1.0}},
        "persons": {"p_roman": {"phone": [ROMAN]}},
    }, telnyx_http=api.client())


async def post_hook(gw, key, body):
    raw, headers = signed(key, body)
    async with httpx.AsyncClient() as c:
        return await c.post(f"{gw.ingress_url}/telnyx/webhook", content=raw, headers=headers)


async def test_inbound_phone_call(gateway, key):
    agent, api = Agent(), FakeTelnyxApi()
    await agent.start()
    gw = await start(gateway, agent, api)
    tel = gw.gw.voice["phone"]
    tel.cfg.public_base_url = gw.ingress_url                     # now that we know the port

    # forged webhook is refused
    async with httpx.AsyncClient() as c:
        bad = await c.post(f"{gw.ingress_url}/telnyx/webhook", json=hook("call.initiated", "x"),
                           headers={"telnyx-signature-ed25519": "AAAA", "telnyx-timestamp": str(int(time.time()))})
    assert bad.status_code == 403

    pid = "v3:inbound-1"
    r = await post_hook(gw, key, hook("call.initiated", "e1", call_control_id=pid, direction="incoming",
                                      **{"from": ROMAN, "to": OUR}, state="parked"))
    assert r.status_code == 200
    ring = await gw.wait_event("call.ringing")
    assert ring["data"]["person_id"] == "p_roman" and ring["data"]["from"] == ROMAN
    await wait_for(lambda: api.actions("answer"))
    path, body = api.actions("answer")[0]
    assert path == f"/v2/calls/{pid}/actions/answer"
    assert body["stream_bidirectional_mode"] == "rtp" and body["stream_bidirectional_codec"] == "L16"
    assert body["stream_codec"] == "L16" and body["stream_url"].startswith("ws://127.0.0.1")

    # play Telnyx: open the media socket, start, then the answered webhook
    async with websockets.connect(body["stream_url"]) as tws:
        await tws.send(json.dumps({"event": "connected", "version": "1.0.0"}))
        await tws.send(json.dumps({"event": "start", "sequence_number": "1", "stream_id": "s1",
                                   "start": {"call_control_id": pid,
                                             "media_format": {"encoding": "L16", "sample_rate": 16000,
                                                              "channels": 1}}}))
        await post_hook(gw, key, hook("call.answered", "e2", call_control_id=pid))
        await asyncio.wait_for(agent.started.wait(), 3)
        await gw.wait_event("call.answered")

        # caller audio: big-endian L16 on the wire -> little-endian pcm16 at the agent
        voice = tone(16000, 400, hz=300, amp=8000)
        wire = swap16(voice)
        for i in range(0, len(wire), 640):
            await tws.send(json.dumps({"event": "media", "media": {"track": "inbound", "payload":
                                       base64.b64encode(wire[i:i + 640]).decode()}}))
        await wait_for(lambda: len(agent.of("media")) >= 20)
        assert agent.audio_bytes()[:640 * 20] == voice[:640 * 20]

        # agent echo -> Telnyx gets big-endian frames of exactly 20 ms
        got = []
        while len(got) < 20:
            m = json.loads(await asyncio.wait_for(tws.recv(), 3))
            if m.get("event") == "media":
                got.append(base64.b64decode(m["media"]["payload"]))
        assert all(len(g) == 640 for g in got)
        back = swap16(b"".join(got))
        assert np.abs(np.frombuffer(back, "<i2")).max() > 4000     # it's the tone, not noise

        # DTMF from the webhook reaches the agent and the event stream
        await post_hook(gw, key, hook("call.dtmf.received", "e3", call_control_id=pid, digit="5"))
        await gw.wait_event("call.dtmf")
        await wait_for(lambda: agent.of("dtmf"))
        assert agent.of("dtmf")[0]["digit"] == "5"

        # barge-in from the agent clears Telnyx's queue too
        await agent.send({"event": "clear"})
        for _ in range(50):
            m = json.loads(await asyncio.wait_for(tws.recv(), 3))
            if m.get("event") == "clear":
                break
        else:
            raise AssertionError("no clear sent to Telnyx")

        # duplicate webhook delivery is ignored; hangup ends the call
        await post_hook(gw, key, hook("call.dtmf.received", "e3", call_control_id=pid, digit="5"))
        await post_hook(gw, key, hook("call.hangup", "e4", call_control_id=pid, hangup_cause="normal_clearing"))
        end = await gw.wait_event("call.ended")
        assert end["data"]["reason"] == "normal_clearing"
        assert len(await gw.events("call.dtmf")) == 1
        await asyncio.wait_for(agent.stopped.wait(), 3)
    await agent.stop()


async def test_byte_order_autodetect(gateway, key):
    """Config says big-endian, Telnyx actually sends little-endian: the port flips and warns."""
    agent, api = Agent(echo=False), FakeTelnyxApi()
    await agent.start()
    gw = await start(gateway, agent, api)
    tel = gw.gw.voice["phone"]
    tel.cfg.public_base_url = gw.ingress_url
    pid = "v3:inbound-2"
    await post_hook(gw, key, hook("call.initiated", "f1", call_control_id=pid, direction="incoming",
                                  **{"from": ROMAN, "to": OUR}))
    await wait_for(lambda: api.actions("answer"))
    url = api.actions("answer")[0][1]["stream_url"]
    async with websockets.connect(url) as tws:
        await tws.send(json.dumps({"event": "start", "start": {"media_format": {"encoding": "L16",
                                                                                 "sample_rate": 16000}}}))
        await post_hook(gw, key, hook("call.answered", "f2", call_control_id=pid))
        await asyncio.wait_for(agent.started.wait(), 3)
        voice = tone(16000, 1000, hz=300, amp=8000)
        for i in range(0, len(voice), 640):                     # little-endian on the wire
            await tws.send(json.dumps({"event": "media", "media": {"payload":
                                       base64.b64encode(voice[i:i + 640]).decode()}}))
        await wait_for(lambda: len(agent.of("media")) >= 45)
        tail = agent.audio_bytes()[-640 * 10:]
        assert tail in voice                                    # after the flip the agent hears it right
        assert tel.ports[next(iter(tel.ports))].big is False
    await agent.stop()


async def test_outbound_phone_call_and_emergency_block(gateway, key):
    agent, api = Agent(), FakeTelnyxApi()
    await agent.start()
    gw = await start(gateway, agent, api)
    gw.gw.voice["phone"].cfg.public_base_url = gw.ingress_url

    r = await gw.client.post("/v1/calls", json={"to": "911", "via": "phone"})
    assert r.status_code == 403 and r.json()["error"]["code"] == "emergency_number_blocked"

    r = await gw.client.post("/v1/calls", json={"to": {"person_id": "p_roman"}})
    assert r.status_code == 202, r.text
    call_id = r.json()["call_id"]
    await wait_for(lambda: any(p == "/v2/calls" for _, p, _ in api.requests))
    body = [b for m, p, b in api.requests if p == "/v2/calls"][0]
    assert body["to"] == ROMAN and body["from"] == OUR and body["connection_id"] == "conn-1"
    assert base64.b64decode(body["client_state"]).decode() == call_id

    await post_hook(gw, key, hook("call.initiated", "o1", call_control_id="v3:outbound-1", direction="outgoing",
                                  client_state=body["client_state"]))
    await gw.wait_event("call.ringing", pred=lambda e: e["data"]["call_id"] == call_id)

    r = await gw.client.post(f"/v1/calls/{call_id}/dtmf", json={"digits": "12#"})
    assert r.status_code == 200
    assert api.actions("send_dtmf")[0][1]["digits"] == "12#"

    r = await gw.client.post(f"/v1/calls/{call_id}/transfer", json={"to": "112"})
    assert r.status_code == 403

    r = await gw.client.post(f"/v1/calls/{call_id}/hangup")
    assert r.status_code == 200 and api.actions("hangup")
    await gw.wait_event("call.ended")
    r = await gw.client.post(f"/v1/calls/{call_id}/answer")
    assert r.status_code == 409
    await agent.stop()
