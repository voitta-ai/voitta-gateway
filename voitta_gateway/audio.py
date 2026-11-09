"""PCM plumbing: codecs, one resampler per direction, fixed-size framing.

Everything inside the gateway is mono pcm16le. Adapters convert their wire
format (μ-law 8 kHz, L16 big-endian 16 kHz, Pulse 48 kHz) to and from that,
and the media session resamples once between the platform rate and the rate
announced to the agent.
"""
from __future__ import annotations

import logging

import numpy as np
import soxr

log = logging.getLogger(__name__)

# ---------------------------------------------------------------- G.711 μ-law
_BIAS = 0x84
_CLIP = 32635


def _ulaw_decode_table() -> np.ndarray:
    u = ~np.arange(256, dtype=np.int32) & 0xFF
    sign = u & 0x80
    exponent = (u >> 4) & 0x07
    mantissa = u & 0x0F
    sample = (((mantissa << 3) + _BIAS) << exponent) - _BIAS
    return np.where(sign != 0, -sample, sample).astype(np.int16)


_ULAW_DEC = _ulaw_decode_table()


def ulaw_to_pcm(data: bytes) -> bytes:
    return _ULAW_DEC[np.frombuffer(data, dtype=np.uint8)].tobytes()


def pcm_to_ulaw(data: bytes) -> bytes:
    s = np.frombuffer(data, dtype="<i2").astype(np.int32)
    sign = np.where(s < 0, 0x80, 0)
    s = np.minimum(np.abs(s), _CLIP) + _BIAS
    exponent = np.floor(np.log2(s)).astype(np.int32) - 7
    exponent = np.clip(exponent, 0, 7)
    mantissa = (s >> (exponent + 3)) & 0x0F
    return (~(sign | (exponent << 4) | mantissa) & 0xFF).astype(np.uint8).tobytes()


# ---------------------------------------------------------------- byte order
def swap16(data: bytes) -> bytes:
    return np.frombuffer(data, dtype="<i2").byteswap().tobytes()


def looks_byteswapped(pcm_le: bytes) -> bool | None:
    """Heuristic: speech is low-pass, so consecutive samples are close.
    Read with the wrong byte order it turns into wideband noise. None = too quiet to tell."""
    s = np.frombuffer(pcm_le, dtype="<i2").astype(np.float64)
    if len(s) < 1600 or np.sqrt(np.mean(s ** 2)) < 200:
        return None
    w = np.frombuffer(pcm_le, dtype="<i2").byteswap().astype(np.float64)

    def roughness(x):
        return np.mean(np.diff(x) ** 2) / (np.mean(x ** 2) + 1e-9)

    return bool(roughness(s) > 3 * roughness(w))


# ---------------------------------------------------------------- resampling
class Resampler:
    """Streaming mono int16 resampler; passthrough when rates match."""

    def __init__(self, in_rate: int, out_rate: int):
        self.in_rate, self.out_rate = in_rate, out_rate
        self._rs = None if in_rate == out_rate else soxr.ResampleStream(
            in_rate, out_rate, 1, dtype="int16", quality="HQ")

    def __call__(self, pcm: bytes, last: bool = False) -> bytes:
        if self._rs is None or not pcm and not last:
            return pcm
        x = np.frombuffer(pcm, dtype="<i2")
        return self._rs.resample_chunk(x, last=last).astype("<i2").tobytes()


class Framer:
    """Cut a byte stream into fixed-size frames."""

    def __init__(self, frame_bytes: int):
        self.frame_bytes = frame_bytes
        self._buf = bytearray()

    def push(self, data: bytes) -> list[bytes]:
        self._buf += data
        n = len(self._buf) // self.frame_bytes
        out = [bytes(self._buf[i * self.frame_bytes:(i + 1) * self.frame_bytes]) for i in range(n)]
        del self._buf[:n * self.frame_bytes]
        return out

    def flush(self) -> bytes | None:
        """The partial frame left over, padded with silence (None if empty)."""
        if not self._buf:
            return None
        out = bytes(self._buf) + bytes(self.frame_bytes - len(self._buf))
        self._buf.clear()
        return out


def frame_bytes(rate: int, ms: int) -> int:
    return rate * ms // 1000 * 2


def silence(rate: int, ms: int) -> bytes:
    return bytes(frame_bytes(rate, ms))


def tone(rate: int, ms: int, hz: float = 440.0, amp: int = 8000) -> bytes:
    t = np.arange(rate * ms // 1000) / rate
    return (amp * np.sin(2 * np.pi * hz * t)).astype("<i2").tobytes()
