"""voitta-ask with the real models (Piper + faster-whisper); the 'caller' is Piper too.

    VOITTA_ASK_REAL=run/ask.yaml pytest tests/test_ask_real.py -s

Skipped unless VOITTA_ASK_REAL points at an ask.yaml with model paths.
"""
import os

import numpy as np
import pytest

from voitta_ask.engines import PiperTTS, WhisperSTT
from voitta_ask.service import load
from voitta_gateway.audio import Resampler, silence

from .test_ask import setup, state_is  # noqa: F401  (setup uses our engines)
from .test_signal import ROMAN, wait_for

CFG = os.environ.get("VOITTA_ASK_REAL")
pytestmark = pytest.mark.skipif(not CFG, reason="set VOITTA_ASK_REAL=path/to/ask.yaml")


@pytest.fixture
def engines():
    cfg = load(CFG)
    return (PiperTTS(cfg.tts.model),
            WhisperSTT(cfg.stt.model, cfg.stt.device, cfg.stt.compute_type, cfg.stt.download_root))


async def test_real_models_round_trip(setup, engines):
    fake, ports, client, stt, gw, svc = setup
    tts = PiperTTS(load(CFG).tts.model)                          # the caller's voice
    ask = (await client.post("/v1/asks", json={"to": ROMAN, "text": "Will you join the review at ten tomorrow?"})).json()
    await wait_for(lambda: fake.methods("startCall"))
    await fake.call_event(callId=987654321, state="CONNECTED", uuid=ROMAN, isOutgoing=True)
    await state_is(client, ask["ask_id"], "listening", timeout=20)

    answer16 = tts.synthesize("Yes, I will be there at ten. Please send me the agenda.")
    answer48 = Resampler(16000, 48000)(answer16, last=True)
    step = 48000 * 2 // 50
    for i in range(0, len(answer48), step):
        chunk = answer48[i:i + step]
        ports[0].inbound.put_nowait(chunk + bytes(step - len(chunk)))
    for _ in range(100):
        ports[0].inbound.put_nowait(silence(48000, 20))

    done = (await client.get(f"/v1/asks/{ask['ask_id']}", params={"wait": 30})).json()
    print("\n", done)
    assert done["state"] == "done" and done["end_reason"] == "end_of_speech"
    words = done["transcript"].lower()
    assert "there" in words and ("ten" in words or "10" in words) and "agenda" in words
    prompt = [w for w in ports[0].written if np.abs(np.frombuffer(w, "<i2")).max() > 2000]
    assert len(prompt) * 0.02 > 2.0                               # the question was spoken into the call
