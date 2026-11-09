"""End-of-answer detection on 20 ms frames of pcm16le.

Energy based, with an adaptive noise floor: good enough on a phone or Signal
call, where the far end already does noise suppression. Times are counted in
audio received, not wall clock, so it behaves the same however fast frames
arrive.
"""
from __future__ import annotations

import numpy as np


class Endpointer:
    def __init__(self, *, rate: int = 16000, end_silence_s: float = 1.2, no_speech_s: float = 8.0,
                 max_answer_s: float = 30.0, min_speech_s: float = 0.3, ignore_first_s: float = 0.2,
                 min_rms: float = 350.0):
        self.rate = rate
        self.end_silence_s, self.no_speech_s, self.max_answer_s = end_silence_s, no_speech_s, max_answer_s
        self.min_speech_s, self.ignore_first_s, self.min_rms = min_speech_s, ignore_first_s, min_rms
        self.floor = 100.0
        self.elapsed = 0.0
        self.voiced = 0.0
        self.silence = 0.0
        self.started = False
        self.reason: str | None = None

    @property
    def done(self) -> bool:
        return self.reason is not None

    def push(self, frame: bytes) -> bool:
        """Feed one frame; returns True once the answer is over (see `reason`)."""
        if self.done or not frame:
            return self.done
        s = np.frombuffer(frame, dtype="<i2").astype(np.float64)
        dur = len(s) / self.rate
        self.elapsed += dur
        rms = float(np.sqrt(np.mean(s * s))) if len(s) else 0.0
        voiced = rms > max(self.min_rms, 4 * self.floor)
        if not voiced:
            self.floor = 0.95 * self.floor + 0.05 * rms
        if self.elapsed <= self.ignore_first_s:
            return False
        if voiced:
            self.voiced += dur
            self.silence = 0.0
            if self.voiced >= self.min_speech_s:
                self.started = True
        else:
            self.silence += dur
        if self.started and self.silence >= self.end_silence_s:
            self.reason = "end_of_speech"
        elif not self.started and self.elapsed >= self.no_speech_s:
            self.reason = "no_speech"
        elif self.elapsed >= self.max_answer_s:
            self.reason = "max_duration"
        return self.done
