"""voitta-speech: speech engines (TTS, STT) and a small HTTP API over them.

The engines are shared by voitta-ask (live calls) and the gateway's voice-clip
transcription; the HTTP router is mounted inside the voitta-ask process so the
models are loaded once.
"""
