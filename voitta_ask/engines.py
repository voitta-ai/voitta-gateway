"""Speech engines. Both work on mono pcm16le at 16 kHz.

piper:   Piper TTS (ONNX, CPU; ~30x faster than real time for a medium voice)
whisper: faster-whisper (CTranslate2), GPU if available
fake:    deterministic stand-ins for tests
"""
from __future__ import annotations

import ctypes
import glob
import logging
import os
import threading
from typing import Protocol

import numpy as np

from voitta_gateway.audio import Resampler, tone

log = logging.getLogger(__name__)
RATE = 16000


class TTS(Protocol):
    def synthesize(self, text: str) -> bytes: ...


class STT(Protocol):
    def transcribe(self, pcm: bytes, language: str | None) -> tuple[str, str | None]: ...


# ---------------------------------------------------------------- Piper
class PiperTTS:
    def __init__(self, model: str, length_scale: float | None = None):
        from piper import PiperVoice, SynthesisConfig

        self.voice = PiperVoice.load(model)
        self.syn = SynthesisConfig(length_scale=length_scale) if length_scale else None
        self.lock = threading.Lock()

    def synthesize(self, text: str) -> bytes:
        with self.lock:
            chunks = list(self.voice.synthesize(text, syn_config=self.syn))
        if not chunks:
            return b""
        pcm = b"".join(c.audio_int16_bytes for c in chunks)
        return Resampler(chunks[0].sample_rate, RATE)(pcm, last=True)


# ---------------------------------------------------------------- Whisper
def _preload_cuda_libs() -> None:
    """Make pip-installed cuBLAS/cuDNN (nvidia-* wheels) visible to CTranslate2 without
    LD_LIBRARY_PATH: load them by full path, retrying until their inter-dependencies resolve."""
    try:
        import nvidia.cublas
        import nvidia.cudnn
    except ImportError:
        return
    libs = []
    for pkg in (nvidia.cublas, nvidia.cudnn):
        for root in pkg.__path__:
            libs += sorted(glob.glob(os.path.join(root, "lib", "lib*.so*")))
    pending = libs
    for _ in range(len(libs)):
        failed = []
        for path in pending:
            try:
                ctypes.CDLL(path, mode=ctypes.RTLD_GLOBAL)
            except OSError:
                failed.append(path)
        if not failed or len(failed) == len(pending):
            break
        pending = failed


class WhisperSTT:
    def __init__(self, model: str = "small", device: str = "auto", compute_type: str = "default",
                 download_root: str | None = None, beam_size: int = 1):
        if device in ("cuda", "auto"):
            _preload_cuda_libs()
        from faster_whisper import WhisperModel

        self.model = WhisperModel(model, device=device, compute_type=compute_type, download_root=download_root)
        self.beam_size = beam_size
        self.lock = threading.Lock()

    def transcribe(self, pcm: bytes, language: str | None) -> tuple[str, str | None]:
        audio = np.frombuffer(pcm, dtype="<i2").astype(np.float32) / 32768.0
        if audio.size < RATE // 4:
            return "", language
        with self.lock:
            segments, info = self.model.transcribe(audio, language=language, beam_size=self.beam_size,
                                                   vad_filter=True, condition_on_previous_text=False)
            text = " ".join(s.text.strip() for s in segments).strip()
        return text, info.language


# ---------------------------------------------------------------- fakes
class FakeTTS:
    """A 300 Hz tone, 60 ms per character (capped), so prompts have a predictable length."""

    def synthesize(self, text: str) -> bytes:
        return tone(RATE, min(2000, 60 * len(text)), hz=300, amp=6000)


class FakeSTT:
    def __init__(self, text: str = "yes I will be there"):
        self.text = text
        self.heard: list[bytes] = []

    def transcribe(self, pcm: bytes, language: str | None) -> tuple[str, str | None]:
        self.heard.append(pcm)
        loud = pcm and np.abs(np.frombuffer(pcm, "<i2")).max() > 1000
        return (self.text if loud else ""), language


def build_tts(cfg) -> TTS:
    if cfg.engine == "fake":
        return FakeTTS()
    return PiperTTS(cfg.model, cfg.length_scale)


def build_stt(cfg) -> STT:
    if cfg.engine == "fake":
        return FakeSTT()
    return WhisperSTT(cfg.model, cfg.device, cfg.compute_type, cfg.download_root, cfg.beam_size)
