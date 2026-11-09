"""voitta-ask end to end: client -> ask service -> gateway -> fake signal-cli call -> back."""
import asyncio

import httpx
import numpy as np
import pytest

from voitta_speech.engines import FakeSTT, FakeTTS
from voitta_ask.endpoint import Endpointer
from voitta_ask.service import AskConfig, AskService, build_app
from voitta_gateway.audio import silence, tone

from .conftest import MEDIA_SECRET, TOKEN, serve
from .fakes import FakePort, FakeSignalCli, free_port
from .spec_check import ask_api, response_hook
from .test_signal import ROMAN, up, wait_for

CLIENT_TOKEN = "client-token"


def test_endpointer():
    e = Endpointer(end_silence_s=1.0, no_speech_s=3.0, max_answer_s=10.0)
    for _ in range(40):                                        # 0.8 s of speech
        e.push(tone(16000, 20, amp=6000))
    assert e.started and not e.done
    for _ in range(49):
        e.push(silence(16000, 20))
    assert not e.done
    e.push(silence(16000, 20))
    assert e.reason == "end_of_speech"
    quiet = Endpointer(no_speech_s=1.0)
    while not quiet.push(silence(16000, 20)):
        pass
    assert quiet.reason == "no_speech" and abs(quiet.elapsed - 1.0) < 0.05


@pytest.fixture
def engines():
    """(tts, stt) for the ask service; test_ask_real.py overrides this with the real models."""
    return FakeTTS(), FakeSTT("yes I will be there")


@pytest.fixture
async def setup(gateway, engines):
    import os
    os.environ["ASK_CLIENT_TOKEN"] = CLIENT_TOKEN
    fake = FakeSignalCli()
    await fake.start()
    ports: list[FakePort] = []

    def factory(i, o):
        ports.append(FakePort(i, o))
        return ports[-1]

    ask_port = free_port()
    gw = await gateway({
        "adapters": {"signal": {"account": "+15550001111", "spawn": False, "tcp": f"127.0.0.1:{fake.port}",
                                "calls": True}},
        "routes": {"ask": {"media_url": f"ws://127.0.0.1:{ask_port}/media",
                           "media_secret_secret": "TEST_MEDIA_SECRET", "fallback": "hangup"}},
        "persons": {"p_roman": {"signal": [ROMAN]}},
    }, signal_port_factory=factory)
    await up(gw, fake)

    cfg = AskConfig.model_validate({
        "listen": f"127.0.0.1:{ask_port}",
        "gateway": {"url": gw.api_url, "token_secret": "TEST_TOKEN", "route": "ask"},
        "media_secret_secret": "TEST_MEDIA_SECRET",
        "clients": {"tester": "ASK_CLIENT_TOKEN"},
        "tts": {"engine": "fake"}, "stt": {"engine": "fake"},
        "answer": {"end_silence_s": 1.0, "no_speech_s": 5.0, "max_answer_s": 15.0},
        "check_every_s": 0.3, "ring_timeout_s": 4.0,
    })
    tts, stt = engines
    svc = AskService(cfg, tts=tts, stt=stt)
    server, task, url = await serve(build_app(svc), ask_port)
    client = httpx.AsyncClient(base_url=url, headers={"Authorization": f"Bearer {CLIENT_TOKEN}"}, timeout=30,
                               event_hooks={"response": [response_hook(ask_api())]})
    yield fake, ports, client, stt, gw, svc
    await client.aclose()
    server.should_exit = True
    await task
    await fake.stop()


async def state_is(client, ask_id, state, timeout=10.0):
    for _ in range(int(timeout / 0.05)):
        r = (await client.get(f"/v1/asks/{ask_id}")).json()
        if r["state"] == state:
            return r
        await asyncio.sleep(0.05)
    raise AssertionError(f"ask never reached {state}: {r}")


async def speak(port: FakePort, speech_s: float, quiet_s: float):
    for _ in range(int(speech_s * 50)):
        port.inbound.put_nowait(tone(48000, 20, hz=220, amp=7000))
    for _ in range(int(quiet_s * 50)):
        port.inbound.put_nowait(silence(48000, 20))


async def test_ask_answered(setup):
    fake, ports, client, stt, gw, svc = setup
    r = await client.post("/v1/asks", json={"to": ROMAN, "via": "signal", "text": "Are you coming tomorrow?"})
    assert r.status_code == 202, r.text
    ask = r.json()
    assert ask["state"] == "calling" and ask["call_id"].startswith("call_")
    await wait_for(lambda: fake.methods("startCall"))
    await fake.call_event(callId=987654321, state="RINGING_OUTGOING", uuid=ROMAN, isOutgoing=True)
    await state_is(client, ask["ask_id"], "ringing")
    await fake.call_event(callId=987654321, state="CONNECTED", uuid=ROMAN, isOutgoing=True)
    await state_is(client, ask["ask_id"], "speaking")
    await state_is(client, ask["ask_id"], "listening")

    # the prompt was played into the call before listening started
    loud = [w for w in ports[0].written if np.abs(np.frombuffer(w, "<i2")).max() > 2000]
    assert len(loud) * 0.02 >= 1.0

    await speak(ports[0], 1.0, 1.5)
    done = (await client.get(f"/v1/asks/{ask['ask_id']}", params={"wait": 15})).json()
    assert done["state"] == "done", done
    assert done["transcript"] == "yes I will be there" and done["end_reason"] == "end_of_speech"
    assert 1.5 < done["answer_s"] < 2.6
    heard = np.frombuffer(stt.heard[0], "<i2")
    assert np.abs(heard).max() > 4000                         # STT got the caller's audio at 16 kHz
    await wait_for(lambda: fake.methods("hangupCall"), timeout=8)    # goodbye played, then hung up
    await gw.wait_event("call.ended", pred=lambda e: e["data"]["call_id"] == ask["call_id"], from_start=True)


async def test_ask_no_answer(setup):
    fake, ports, client, stt, gw, svc = setup
    ask = (await client.post("/v1/asks", json={"to": ROMAN, "text": "Hello?"})).json()
    await wait_for(lambda: fake.methods("startCall"))
    await fake.call_event(callId=987654321, state="RINGING_OUTGOING", uuid=ROMAN, isOutgoing=True)
    await fake.call_event(callId=987654321, state="ENDED", uuid=ROMAN, isOutgoing=True, reason="TIMEOUT")
    r = (await client.get(f"/v1/asks/{ask['ask_id']}", params={"wait": 10})).json()
    assert r["state"] == "no_answer" and r["end_reason"] == "timeout" and r["transcript"] is None


async def test_ask_hangup_while_answering(setup):
    fake, ports, client, stt, gw, svc = setup
    ask = (await client.post("/v1/asks", json={"to": ROMAN, "text": "Quick question"})).json()
    await wait_for(lambda: fake.methods("startCall"))
    await fake.call_event(callId=987654321, state="CONNECTED", uuid=ROMAN, isOutgoing=True)
    await state_is(client, ask["ask_id"], "listening")
    await speak(ports[0], 0.8, 0.0)
    await asyncio.sleep(0.3)
    await fake.call_event(callId=987654321, state="ENDED", uuid=ROMAN, isOutgoing=True, reason="REMOTE_HANGUP")
    r = (await client.get(f"/v1/asks/{ask['ask_id']}", params={"wait": 10})).json()
    assert r["state"] == "done" and r["end_reason"] == "hung_up" and r["transcript"] == "yes I will be there"


async def test_ask_refusals(setup):
    fake, ports, client, stt, gw, svc = setup
    r = await client.post("/v1/asks", json={"to": "+15551234567", "via": "phone", "text": "hi"})
    assert r.status_code == 422 and r.json()["error"]["code"] == "unknown_channel"
    r = await client.post("/v1/asks", json={"to": ROMAN, "text": ""})
    assert r.status_code == 422
    r = await client.post("/v1/asks", json={"to": ROMAN, "text": "x"}, headers={"Authorization": "Bearer nope"})
    assert r.status_code == 401
    r = await client.get("/v1/asks/ask_nope")
    assert r.status_code == 404


def follower(svc) -> asyncio.Task:
    return next(t for t in svc.tasks if t.get_name() == "ask-gateway-events")


async def test_missed_end_event_caught_by_watchdog(setup):
    """The Nadya bug: the call ends unanswered but the ask service never sees call.ended."""
    fake, ports, client, stt, gw, svc = setup
    ask = (await client.post("/v1/asks", json={"to": ROMAN, "text": "Hello?"})).json()
    await wait_for(lambda: fake.methods("startCall"))
    follower(svc).cancel()                                     # events stop reaching the ask service
    await fake.call_event(callId=987654321, state="RINGING_OUTGOING", uuid=ROMAN, isOutgoing=True)
    await fake.call_event(callId=987654321, state="ENDED", uuid=ROMAN, isOutgoing=True, reason="TIMEOUT")
    r = (await client.get(f"/v1/asks/{ask['ask_id']}", params={"wait": 3})).json()
    assert r["state"] == "no_answer" and r["end_reason"] == "timeout", r


async def test_ring_timeout(setup):
    """Nothing at all comes back from the platform: give up after ring_timeout_s and hang up."""
    fake, ports, client, stt, gw, svc = setup
    ask = (await client.post("/v1/asks", json={"to": ROMAN, "text": "Hello?"})).json()
    r = (await client.get(f"/v1/asks/{ask['ask_id']}", params={"wait": 8})).json()
    assert r["state"] == "no_answer" and r["end_reason"] == "ring_timeout", r
    assert fake.methods("hangupCall") == [{"callId": 987654321}]


async def test_poll_cursor_from_another_log(gateway, tmp_path):
    """A consumer holding evt_N from a previous gateway database gets this log replayed, flagged."""
    gw = await gateway({})
    gw.gw.channel_state("slack", True)                        # one real event in this log
    r = (await gw.client.get("/v1/events/poll", params={"after": "evt_50", "wait": 2})).json()
    assert r.get("reset") is True and [e["id"] for e in r["events"]] == ["evt_1"]
    r = (await gw.client.get("/v1/events/poll", params={"after": "evt_1"})).json()
    assert "reset" not in r and r["events"] == []
