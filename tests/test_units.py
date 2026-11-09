import numpy as np
import pytest

from voitta_gateway.audio import (Framer, Resampler, looks_byteswapped, pcm_to_ulaw, swap16, tone,
                                  ulaw_to_pcm)
from voitta_gateway.config import Config
from voitta_gateway.core import Gateway, Identity
from voitta_gateway.events import matches
from voitta_gateway.media import Player, sign, signed_url, verify


def test_ulaw_roundtrip_is_close():
    pcm = tone(8000, 100, amp=12000)
    back = np.frombuffer(ulaw_to_pcm(pcm_to_ulaw(pcm)), "<i2").astype(float)
    orig = np.frombuffer(pcm, "<i2").astype(float)
    snr = 10 * np.log10(np.sum(orig ** 2) / np.sum((orig - back) ** 2))
    assert len(back) == len(orig) and snr > 30


def test_resampler_lengths_and_passthrough():
    rs = Resampler(48000, 16000)
    out = b"".join(rs(tone(48000, 20)) for _ in range(50))   # 1 s in 20 ms chunks
    assert abs(len(out) // 2 - 16000) < 400
    same = Resampler(16000, 16000)
    assert same(b"\x01\x02") == b"\x01\x02"


def test_byteswap_detector():
    speech_like = tone(16000, 200, hz=300, amp=8000)
    assert looks_byteswapped(speech_like) is False
    assert looks_byteswapped(swap16(speech_like)) is True
    assert looks_byteswapped(bytes(6400)) is None              # silence: can't tell


def test_framer():
    f = Framer(4)
    assert f.push(b"abcdef") == [b"abcd"]
    assert f.push(b"gh") == [b"efgh"]


def test_player_marks_and_clear():
    p = Player(rate=16000, frame_ms=20, max_buffer_s=1)
    fb = p.fbytes
    assert p.push_audio(b"\x01\x00" * (fb // 2 + 10))         # 1 frame + a bit
    p.push_mark("a")
    p.push_audio(b"\x02\x00" * fb)
    p.push_mark("b")
    f1, m1 = p.next_frame()
    assert len(f1) == fb and m1 == []
    f2, m2 = p.next_frame()                                   # the 'bit', padded; mark a is done
    assert len(f2) == fb and m2 == ["a"]
    assert p.clear() == ["b"]                                 # b was never played
    assert p.next_frame() == (None, [])


def test_player_buffer_limit():
    p = Player(rate=16000, frame_ms=20, max_buffer_s=1)
    assert p.push_audio(bytes(32000))
    assert not p.push_audio(bytes(2))


def test_media_signature():
    import time
    ts = int(time.time())
    s = sign("k", "call_1", ts)
    assert verify("k", "call_1", ts, s)
    assert not verify("k", "call_2", ts, s)
    assert not verify("k", "call_1", ts - 3600, sign("k", "call_1", ts - 3600))
    assert "sig=" in signed_url("ws://a/media", "k", "call_1")


def test_event_type_matching():
    assert matches("ai.voitta.message.received.v1", ["message.*"])
    assert not matches("ai.voitta.call.ended.v1", ["message.*"])
    assert matches("ai.voitta.call.ended.v1", ["*"])


def test_identity_and_emergency(tmp_path):
    cfg = Config.model_validate({"data_dir": str(tmp_path), "persons": {
        "p_roman": {"signal": ["AAAA-uuid"], "slack": ["T1/U1"], "phone": ["+1 (555) 111-2222"]}}})
    ident = Identity(cfg)
    assert ident.lookup("phone", "+15551112222") == "p_roman"
    assert ident.lookup("signal", "aaaa-uuid") == "p_roman"
    assert ident.lookup("slack", "T1/U1") == "p_roman"
    gw = Gateway(cfg)
    assert gw.emergency("911") and gw.emergency("+112") and not gw.emergency("+15551112222")


def test_config_rejects_unknown_route(tmp_path):
    with pytest.raises(ValueError):
        Config.model_validate({"data_dir": str(tmp_path),
                               "adapters": {"signal": {"account": "+1", "route": "nope"}}})
