"""The real join page in a real browser: headless Chrome with a fake microphone.

Chrome plays a WAV file as its microphone; the page's JavaScript (audio worklets, WebSocket)
captures it, the gateway relays it to an echo agent, and the echo plays back in the page.
Skipped when no Chrome/Chromium is installed.
"""
import asyncio
import base64
import os
import shutil
import subprocess
import wave

import numpy as np
import pytest

from .fakes import Agent
from .test_slack import ROMAN, config, started, web  # noqa: F401  (web is a fixture)
from .test_signal import wait_for

CHROME = next((p for p in ("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                           shutil.which("google-chrome") or "", shutil.which("chromium") or "",
                           shutil.which("chromium-browser") or "") if p and os.path.exists(p)), None)
pytestmark = pytest.mark.skipif(not CHROME, reason="needs Chrome or Chromium")


def fake_mic(path, rate=48000, seconds=6):
    """Alternating 440 / 660 Hz bursts with gaps: speech-like enough that noise suppression keeps it."""
    t = np.arange(int(rate * 0.3)) / rate
    burst = lambda hz: (0.35 * np.sin(2 * np.pi * hz * t) * np.hanning(t.size)).astype(np.float32)
    gap = np.zeros(int(rate * 0.2), np.float32)
    one = np.concatenate([burst(440), gap, burst(660), gap])
    sig = np.tile(one, int(seconds / 1.0) + 1)
    with wave.open(str(path), "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes((sig * 32767).astype("<i2").tobytes())


def tone_share(pcm: bytes, rate=16000) -> float:
    """Share of the 100-4000 Hz energy that lies within 20 Hz of 440 or 660 Hz (incl. their 2nd harmonics,
    which Chrome's gain control adds when it clips)."""
    x = np.frombuffer(pcm, "<i2").astype(np.float64)
    power = np.abs(np.fft.rfft(x * np.hanning(x.size))) ** 2
    f = np.fft.rfftfreq(x.size, 1 / rate)
    speech = (f > 100) & (f < 4000)
    ours = np.zeros_like(f, bool)
    for hz in (440, 660, 880, 1320):
        ours |= np.abs(f - hz) < 20
    return float(power[speech & ours].sum() / power[speech].sum())


async def test_join_page_in_headless_chrome(gateway, web, tmp_path):  # noqa: F811
    agent = Agent()
    await agent.start()
    gw, sl = await started(gateway, web, config(agent.url, ring_timeout_s=30))
    r = await gw.client.post("/v1/calls", json={"to": ROMAN, "via": "slack"})
    call_id = r.json()["call_id"]
    await gw.wait_event("call.ringing")
    mic = tmp_path / "mic.wav"
    fake_mic(mic)
    chrome = subprocess.Popen(
        [CHROME, "--headless=new", "--no-first-run", "--no-default-browser-check", "--disable-gpu",
         f"--user-data-dir={tmp_path / 'profile'}", "--use-fake-ui-for-media-stream",
         "--use-fake-device-for-media-stream", f"--use-file-for-fake-audio-capture={mic}",
         # the sandboxed audio service cannot read the fake-mic file (the page then gets silence)
         "--disable-features=AudioServiceSandbox",
         "--autoplay-policy=no-user-gesture-required", web.join_url() + "&autojoin=1"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        await gw.wait_event("call.answered", timeout=30)
        await asyncio.wait_for(agent.started.wait(), 10)
        await wait_for(lambda: len(agent.of("media")) >= 150, timeout=20)       # 3 s of microphone
        heard = b"".join(base64.b64decode(m["payload"]) for m in agent.of("media")[50:150])
        x = np.frombuffer(heard, "<i2").astype(np.float64)
        rms = float(np.sqrt(np.mean(x ** 2)))
        assert rms > 300, f"the page sent silence (rms {rms:.1f})"
        share = tone_share(heard)
        assert share > 0.5, f"only {share:.0%} of the energy is in the 440/660 Hz bands"

        # the echo came back down and the page played it (its own report)
        port = sl.calls[call_id].port
        try:
            await wait_for(lambda: (port.stats.get("played_ms") or 0) > 1000, timeout=15)
        except AssertionError:
            raise AssertionError(f"the page did not play the echo back: {port.stats}")
        assert port.stats.get("captured_ms", 0) > 2000, port.stats

        await gw.client.post(f"/v1/calls/{call_id}/hangup")
        end = await gw.wait_event("call.ended")
        assert end["data"]["answered"] and end["data"]["duration_s"] > 2, end["data"]
    finally:
        chrome.terminate()
        try:
            chrome.wait(5)
        except subprocess.TimeoutExpired:
            chrome.kill()
        await agent.stop()
