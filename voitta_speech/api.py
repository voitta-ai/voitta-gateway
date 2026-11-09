"""HTTP API over the speech engines, mounted inside the voitta-ask process.

    POST /v1/speech/transcribe   raw audio body (any container PyAV reads) -> {text, language, duration_s}
    POST /v1/speech/synthesize   {"text": ...} -> audio/wav (pcm16 16 kHz mono)

Used by the gateway to transcribe inbound voice clips (Slack clips, Signal
voice notes) without running any model itself, and by agents that want to
send speech as an audio file.
"""
from __future__ import annotations

import asyncio
import logging
import time

from fastapi import Depends, FastAPI, Request
from fastapi.responses import Response

from .engines import STT, TTS, to_wav

log = logging.getLogger(__name__)
MAX_AUDIO = 25 * 1024 * 1024
MAX_TEXT = 2000


class SpeechError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


def mount(app: FastAPI, tts: TTS, stt: STT, client_dep, error_cls=SpeechError) -> None:
    """Register the speech routes on `app` (directly, so they show in its route list)."""
    r = app

    @r.post("/v1/speech/transcribe")
    async def transcribe(request: Request, language: str | None = None, who: str = Depends(client_dep)):
        data = await request.body()
        if not data:
            raise error_cls(422, "empty", "empty audio body")
        if len(data) > MAX_AUDIO:
            raise error_cls(413, "too_large", f"audio is limited to {MAX_AUDIO} bytes")
        t0 = time.monotonic()
        try:
            text, lang, duration = await asyncio.to_thread(stt.transcribe_file, data, language)
        except Exception as e:
            log.warning("transcription failed: %s", e)
            raise error_cls(422, "undecodable", f"could not decode or transcribe the audio: {e}")
        return {"text": text, "language": lang, "duration_s": round(duration, 2),
                "elapsed_s": round(time.monotonic() - t0, 2)}

    @r.post("/v1/speech/synthesize")
    async def synthesize(request: Request, who: str = Depends(client_dep)):
        try:
            body = await request.json()
        except Exception:
            raise error_cls(400, "bad_json", "request body must be JSON")
        text = str((body or {}).get("text") or "").strip()
        if not text:
            raise error_cls(422, "bad_request", "text is required")
        if len(text) > MAX_TEXT:
            raise error_cls(422, "text_too_long", f"text is limited to {MAX_TEXT} characters")
        pcm = await asyncio.to_thread(tts.synthesize, text)
        return Response(to_wav(pcm), media_type="audio/wav")

