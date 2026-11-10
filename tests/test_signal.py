"""Signal end to end: fake signal-cli <-> gateway <-> API consumer (and agent for calls)."""
import asyncio
import base64

import numpy as np
import pytest

from voitta_gateway.audio import tone

from .fakes import Agent, FakePort, FakeSignalCli

ROMAN = "bbbbbbbb-0000-0000-0000-000000000002"
STRANGER = "cccccccc-0000-0000-0000-000000000003"
ACCOUNT = "+15550001111"


def envelope(src, ts, **kw):
    return {"source": src, "sourceUuid": src, "sourceNumber": None, "sourceName": "x",
            "sourceDevice": 1, "timestamp": ts, **kw}


def config(fake, agent=None, **over):
    cfg = {
        "adapters": {"signal": {"account": ACCOUNT, "spawn": False, "tcp": f"127.0.0.1:{fake.port}",
                                "calls": agent is not None, "route": "main" if agent else None}},
        "persons": {"p_roman": {"signal": [ROMAN]}},
        "policy": {"inbound_allowlist": ["p_roman"]},
        "conversations": {"group1": {"channel": "signal", "key": "GROUP1"}},
    }
    if agent:
        cfg["routes"] = {"main": {"media_url": agent.url, "media_secret_secret": "TEST_MEDIA_SECRET",
                                  "grace_s": 2.0, "connect_timeout_s": 1.0}}
    for k, v in over.items():
        cfg[k] = v
    return cfg


@pytest.fixture
async def fake():
    f = FakeSignalCli()
    await f.start()
    yield f
    await f.stop()


async def up(gw, fake):
    await asyncio.wait_for(fake.connected.wait(), 5)
    for _ in range(100):
        if gw.gw.channel_up.get("signal"):
            return
        await asyncio.sleep(0.05)
    raise AssertionError("signal channel never came up")


async def test_messages_receipts_policy(gateway, fake):
    gw = await gateway(config(fake))
    await up(gw, fake)
    assert ("subscribeReceive", {}) in fake.calls

    # inbound -> message.received, with person resolved
    await fake.receive(envelope(ROMAN, 1000, dataMessage={"timestamp": 1000, "message": "hello"}))
    ev = await gw.wait_event("message.received")
    d = ev["data"]
    assert d["person_id"] == "p_roman" and d["parts"] == [{"type": "text", "text": "hello"}]
    assert ev["time"].startswith("1970-01-01T00:00:01") and ev["seq"] == 1 and ev["subject"] == d["conversation_id"]

    # redelivery of the same message is ignored
    await fake.receive(envelope(ROMAN, 1000, dataMessage={"timestamp": 1000, "message": "hello"}))

    # reply: idempotency key derived from the event
    body = {"to": {"conversation_id": d["conversation_id"]}, "reply_to": d["message_id"],
            "parts": [{"type": "text", "text": "hi back"}], "idempotency_key": f"reply:{ev['id']}"}
    r1 = await gw.client.post("/v1/messages", json=body)
    r2 = await gw.client.post("/v1/messages", json=body)
    assert r1.status_code == 202 and r2.status_code == 200 and r2.json()["duplicate"]
    mid = r1.json()["message_id"]
    sent = await gw.wait_event("message.status", pred=lambda e: e["data"]["status"] == "sent")
    assert sent["data"]["message_id"] == mid
    sends = fake.methods("send")
    assert len(sends) == 1
    assert sends[0]["recipient"] == [ROMAN] and sends[0]["quoteTimestamp"] == 1000
    ts = fake.ts

    # receipts move the status forward, never back
    await fake.receive(envelope(ROMAN, 2000, receiptMessage={"when": 2000, "isRead": True, "timestamps": [ts]}))
    await gw.wait_event("message.status", pred=lambda e: e["data"]["status"] == "read")
    await fake.receive(envelope(ROMAN, 2100, receiptMessage={"when": 2100, "isDelivery": True, "timestamps": [ts]}))
    await asyncio.sleep(0.2)
    statuses = [e["data"]["status"] for e in await gw.events("message.status") if e["data"]["message_id"] == mid]
    assert statuses == ["queued", "sent", "read"]

    # reaction to our message
    await fake.receive(envelope(ROMAN, 3000, dataMessage={"timestamp": 3000, "reaction": {
        "emoji": "👍", "targetAuthorUuid": fake.aci, "targetSentTimestamp": ts, "isRemove": False}}))
    re = await gw.wait_event("reaction.added")
    assert re["data"]["message_id"] == mid and re["data"]["emoji"] == "👍"

    # react back through the API
    r = await gw.client.post(f"/v1/messages/{d['message_id']}/reactions", json={"emoji": "❤️"})
    assert r.status_code == 202
    await gw.wait_event("operation.done")
    assert fake.methods("sendReaction")[0]["targetAuthor"] == ROMAN

    # unknown sender is held without content, released on approval
    await fake.receive(envelope(STRANGER, 4000, dataMessage={"timestamp": 4000, "message": "secret"}))
    held = await gw.wait_event("message.held")
    assert "parts" not in held["data"]
    hid = held["data"]["held_id"]
    assert [h["held_id"] for h in (await gw.client.get("/v1/held")).json()["held"]] == [hid]
    assert (await gw.client.post(f"/v1/held/{hid}/approve")).json() == {"released": 1}
    rel = await gw.wait_event("message.received", pred=lambda e: e["data"]["sender"]["id"] == STRANGER)
    assert rel["data"]["parts"][0]["text"] == "secret"

    # group without a mention is dropped; with a mention of our ACI it is delivered
    g = {"groupId": "GROUP1", "type": "DELIVER"}
    await fake.receive(envelope(ROMAN, 5000, dataMessage={"timestamp": 5000, "message": "chatter", "groupInfo": g}))
    await fake.receive(envelope(ROMAN, 5001, dataMessage={"timestamp": 5001, "message": "@bot hi", "groupInfo": g,
                                                         "mentions": [{"uuid": fake.aci, "start": 0, "length": 1}]}))
    ge = await gw.wait_event("message.received", pred=lambda e: e["data"]["conversation_kind"] == "group")
    assert ge["data"]["parts"][0]["text"] == "@bot hi"
    texts = [e["data"]["parts"][0]["text"] for e in await gw.events("message.received")]
    assert "chatter" not in texts

    # STOP opts out; further sends to that conversation are refused
    await fake.receive(envelope(ROMAN, 6000, dataMessage={"timestamp": 6000, "message": "stop"}))
    await gw.wait_event("contact.opted_out")
    r = await gw.client.post("/v1/messages", json={"to": {"conversation_id": d["conversation_id"]},
                                                   "parts": [{"type": "text", "text": "x"}]})
    assert r.status_code == 409 and r.json()["error"]["code"] == "opted_out"

    # auth
    bad = await gw.client.get("/v1/channels", headers={"Authorization": "Bearer nope"})
    assert bad.status_code == 401
    ch = (await gw.client.get("/v1/channels")).json()["channels"]
    assert ch[0]["channel"] == "signal" and ch[0]["up"] is True


async def test_sse_stream_resumes(gateway, fake):
    gw = await gateway(config(fake))
    await up(gw, fake)
    await fake.receive(envelope(ROMAN, 1000, dataMessage={"timestamp": 1000, "message": "one"}))
    first = await gw.wait_event("message.received")
    got = []
    async with gw.client.stream("GET", "/v1/events", params={"types": "message.received"},
                                headers={"Last-Event-ID": first["id"]}) as r:
        await fake.receive(envelope(ROMAN, 2000, dataMessage={"timestamp": 2000, "message": "two"}))
        async for line in r.aiter_lines():
            if line.startswith("data: "):
                import json
                got.append(json.loads(line[6:]))
                break
    assert got[0]["data"]["parts"][0]["text"] == "two"


async def wait_for(cond, timeout=5.0):
    for _ in range(int(timeout / 0.02)):
        if cond():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition not met")


async def test_incoming_call_audio_both_ways(gateway, fake):
    agent = Agent()
    await agent.start()
    ports = []

    def factory(i, o):
        p = FakePort(i, o)
        ports.append(p)
        return p

    gw = await gateway(config(fake, agent), signal_port_factory=factory)
    await up(gw, fake)
    assert ("subscribeCallEvents", {}) in fake.calls
    call_id = -4611686018427387904                             # Java long, negative: must round-trip
    fake.next_call_devices = ("signal_input_9", "signal_output_9")   # acceptCall echoes the call's devices
    await fake.call_event(callId=call_id, state="RINGING_INCOMING", uuid=ROMAN, isOutgoing=False,
                          inputDeviceName="signal_input_9", outputDeviceName="signal_output_9")
    ring = await gw.wait_event("call.ringing")
    assert ring["data"]["person_id"] == "p_roman"
    await wait_for(lambda: fake.methods("acceptCall"))
    assert fake.methods("acceptCall")[0]["callId"] == call_id
    assert agent.of("connected") and not agent.of("start")    # agent connected, silent until answered
    assert agent.query["call_id"] == ring["data"]["call_id"]

    await fake.call_event(callId=call_id, state="CONNECTED", uuid=ROMAN, isOutgoing=False)
    await gw.wait_event("call.answered")
    await asyncio.wait_for(agent.started.wait(), 3)
    start = agent.of("start")[0]
    assert start["media_format"] == {"encoding": "pcm16le", "sample_rate": 16000, "channels": 1}
    assert ports[0].devices == ("signal_input_9", "signal_output_9")

    # caller audio 48k -> agent 16k frames of 20 ms; agent echoes -> back at 48k
    for _ in range(25):
        ports[0].inbound.put_nowait(tone(48000, 20, hz=400))
    await wait_for(lambda: len(agent.of("media")) >= 20)
    assert all(len(base64.b64decode(m["payload"])) == 640 for m in agent.of("media"))
    assert [m["seq"] for m in agent.of("media")[:3]] == [1, 2, 3]
    await agent.send({"event": "mark", "name": "after-echo"})
    await wait_for(lambda: any(m.get("name") == "after-echo" for m in agent.of("mark")))
    assert agent.of("mark")[-1]["cleared"] is False
    loud = [w for w in ports[0].written if np.abs(np.frombuffer(w, "<i2")).max() > 1000]
    assert loud and all(len(w) == 1920 for w in ports[0].written)

    # barge-in: queued audio is dropped and its mark comes back cleared
    await agent.send({"event": "media", "payload": base64.b64encode(bytes(64000)).decode()})
    await agent.send({"event": "mark", "name": "long"})
    await agent.send({"event": "clear"})
    await wait_for(lambda: any(m.get("name") == "long" for m in agent.of("mark")))
    assert [m for m in agent.of("mark") if m["name"] == "long"][0]["cleared"] is True

    await fake.call_event(callId=call_id, state="ENDED", uuid=ROMAN, isOutgoing=False, reason="REMOTE_HANGUP")
    end = await gw.wait_event("call.ended")
    assert end["data"]["reason"] == "remote_hangup" and end["data"]["answered"]
    await asyncio.wait_for(agent.stopped.wait(), 3)
    assert ports[0].closed
    await agent.stop()


async def test_call_rejected_when_agent_unreachable(gateway, fake):
    agent = Agent()                                           # never started: nothing listens
    gw = await gateway(config(fake, agent), signal_port_factory=FakePort)
    await up(gw, fake)
    await fake.call_event(callId=55, state="RINGING_INCOMING", uuid=ROMAN, isOutgoing=False,
                          inputDeviceName="i", outputDeviceName="o")
    end = await gw.wait_event("call.ended")
    assert end["data"]["reason"] == "agent_unreachable"
    assert fake.methods("rejectCall") == [{"callId": 55}] and not fake.methods("acceptCall")


async def test_unknown_caller_rejected_by_policy(gateway, fake):
    agent = Agent()
    await agent.start()
    gw = await gateway(config(fake, agent), signal_port_factory=FakePort)
    await up(gw, fake)
    await fake.call_event(callId=56, state="RINGING_INCOMING", uuid=STRANGER, isOutgoing=False,
                          inputDeviceName="i", outputDeviceName="o")
    end = await gw.wait_event("call.ended")
    assert end["data"]["reason"] == "policy_rejected" and agent.connections == 0
    await agent.stop()


async def test_agent_reconnects_within_grace(gateway, fake):
    agent = Agent(drop_after_start=1)
    await agent.start()
    gw = await gateway(config(fake, agent), signal_port_factory=FakePort)
    await up(gw, fake)
    await fake.call_event(callId=77, state="RINGING_INCOMING", uuid=ROMAN, isOutgoing=False,
                          inputDeviceName="i", outputDeviceName="o")
    await wait_for(lambda: fake.methods("acceptCall"))
    await fake.call_event(callId=77, state="CONNECTED", uuid=ROMAN, isOutgoing=False)
    await wait_for(lambda: agent.connections == 2 and len(agent.of("start")) == 2)
    assert any(m.get("resumed") for m in agent.of("connected"))
    # agent hangs up from the media socket
    await agent.send({"event": "hangup", "reason": "done"})
    end = await gw.wait_event("call.ended")
    assert end["data"]["reason"] == "done" and fake.methods("hangupCall") == [{"callId": 77}]
    await agent.stop()


async def test_agent_lost_hangs_up_after_grace(gateway, fake):
    agent = Agent(drop_after_start=99)
    await agent.start()
    gw = await gateway(config(fake, agent), signal_port_factory=FakePort)
    await up(gw, fake)
    await fake.call_event(callId=78, state="RINGING_INCOMING", uuid=ROMAN, isOutgoing=False,
                          inputDeviceName="i", outputDeviceName="o")
    await wait_for(lambda: fake.methods("acceptCall"))
    await agent.stop()                                       # agent goes away for good
    await fake.call_event(callId=78, state="CONNECTED", uuid=ROMAN, isOutgoing=False)
    end = await gw.wait_event("call.ended", timeout=8)
    assert end["data"]["reason"] == "agent_lost" and fake.methods("hangupCall") == [{"callId": 78}]


async def test_outbound_signal_call(gateway, fake):
    agent = Agent()
    await agent.start()
    gw = await gateway(config(fake, agent), signal_port_factory=FakePort)
    await up(gw, fake)
    r = await gw.client.post("/v1/calls", json={"to": {"person_id": "p_roman"}, "via": "signal",
                                                "idempotency_key": "k1"})
    assert r.status_code == 202, r.text
    call_id = r.json()["call_id"]
    await wait_for(lambda: fake.methods("startCall"))
    assert fake.methods("startCall")[0]["recipient"] == ROMAN
    await fake.call_event(callId=987654321, state="RINGING_OUTGOING", uuid=ROMAN, isOutgoing=True)
    await gw.wait_event("call.ringing", pred=lambda e: e["data"]["call_id"] == call_id)
    await fake.call_event(callId=987654321, state="CONNECTED", uuid=ROMAN, isOutgoing=True)
    await asyncio.wait_for(agent.started.wait(), 3)
    assert agent.of("start")[0]["direction"] == "outbound"
    r = await gw.client.post(f"/v1/calls/{call_id}/hangup")
    assert r.status_code == 200
    await gw.wait_event("call.ended")
    assert fake.methods("hangupCall") == [{"callId": 987654321}]
    await agent.stop()
