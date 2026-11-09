"""Voice clips with the real models: Piper speaks, the result is encoded like a Slack clip (AAC in
m4a) or a webm/Opus recording, and Whisper transcribes it, directly and over the speech API.

    VOITTA_ASK_REAL=run/ask.yaml pytest tests/test_speech_real.py -s
"""
import io
import os

import httpx
import numpy as np
import pytest

from voitta_ask.service import AskService, build_app, load
from voitta_speech.engines import RATE, PiperTTS, WhisperSTT

from .conftest import serve
from .fakes import free_port

CFG = os.environ.get("VOITTA_ASK_REAL")
pytestmark = pytest.mark.skipif(not CFG, reason="set VOITTA_ASK_REAL=path/to/ask.yaml")
SENTENCE = "Running ten minutes late, please start the review without me."


def encode(pcm: bytes, fmt: str, codec: str, rate: int) -> bytes:
    import av

    buf = io.BytesIO()
    out = av.open(buf, mode="w", format=fmt)
    st = out.add_stream(codec, rate=rate)
    st.layout = "mono"
    src = av.AudioFrame.from_ndarray(np.frombuffer(pcm, "<i2").reshape(1, -1), format="s16", layout="mono")
    src.sample_rate = RATE
    rs = av.AudioResampler(format=st.codec_context.format.name, layout="mono", rate=rate)
    fifo = av.AudioFifo()
    for f in rs.resample(src) + rs.resample(None):
        fifo.write(f)
    size = st.codec_context.frame_size or 1024
    while fifo.samples:
        frame = fifo.read(min(size, fifo.samples))
        for packet in st.encode(frame):
            out.mux(packet)
    for packet in st.encode(None):
        out.mux(packet)
    out.close()
    return buf.getvalue()


@pytest.fixture(scope="module")
def engines():
    cfg = load(CFG)
    return PiperTTS(cfg.tts.model), WhisperSTT(cfg.stt.model, cfg.stt.device, cfg.stt.compute_type,
                                               cfg.stt.download_root)


def words(text: str) -> set[str]:
    """Lower-case words, with Whisper's digits ("10") read back as the spoken word ("ten")."""
    w = set("".join(c if c.isalnum() or c == " " else " " for c in text.lower()).split())
    return w | {"ten"} if "10" in w else w


@pytest.mark.parametrize("fmt,codec,rate", [("mp4", "aac", 44100), ("webm", "libopus", 48000)])
def test_clip_formats(engines, fmt, codec, rate):
    tts, stt = engines
    data = encode(tts.synthesize(SENTENCE), fmt, codec, rate)
    text, lang, duration = stt.transcribe_file(data, None)
    print(f"\n{fmt}/{codec}: {len(data)} bytes, {duration:.1f} s, {lang}: {text!r}")
    assert {"ten", "minutes", "late", "review"} <= words(text) and lang == "en"


async def test_transcribe_over_http(engines):
    tts, stt = engines
    os.environ.setdefault("SPEECH_TEST_TOKEN", "speech-test")
    cfg = load(CFG)
    cfg.clients = {"test": "SPEECH_TEST_TOKEN"}
    port = free_port()
    server, task, url = await serve(build_app(AskService(cfg, tts=tts, stt=stt)), port)
    try:
        async with httpx.AsyncClient(base_url=url, headers={"Authorization": "Bearer speech-test"}, timeout=60) as c:
            clip = encode(tts.synthesize(SENTENCE), "mp4", "aac", 44100)
            r = await c.post("/v1/speech/transcribe", content=clip, headers={"Content-Type": "audio/mp4"})
            assert r.status_code == 200, r.text
            out = r.json()
            print("\nhttp:", out)
            assert {"ten", "minutes", "late"} <= words(out["text"]) and out["duration_s"] > 2
            r = await c.post("/v1/speech/transcribe", content=b"not audio at all")
            assert r.status_code == 422 and r.json()["error"]["code"] == "undecodable"
            r = await c.post("/v1/speech/synthesize", json={"text": "Hello from Voitta."})
            assert r.status_code == 200 and r.headers["content-type"] == "audio/wav" and len(r.content) > 20000
    finally:
        server.should_exit = True
        await task
