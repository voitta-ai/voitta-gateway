"""Pipecat FrameSerializer for voitta-media/1.

Use it with Pipecat's FastAPIWebsocketTransport (the agent is the WebSocket
server; the gateway connects in):

    serializer = VoittaMediaSerializer()
    transport = FastAPIWebsocketTransport(websocket, FastAPIWebsocketParams(
        audio_in_enabled=True, audio_out_enabled=True, add_wav_header=False,
        serializer=serializer))

Gateway messages other than audio and DTMF (connected, start, mark, error,
stop) arrive in the pipeline as InputTransportMessageFrame, so the app can
greet on `start` or track what was played via `mark`. Send a mark with
OutputTransportMessageFrame({"event": "mark", "name": "..."}).
"""
from __future__ import annotations

import base64
import json

from pipecat.audio.dtmf.types import KeypadEntry
from pipecat.audio.utils import create_stream_resampler
from pipecat.frames.frames import (
    AudioRawFrame,
    CancelFrame,
    EndFrame,
    Frame,
    InputAudioRawFrame,
    InputDTMFFrame,
    InputTransportMessageFrame,
    InterruptionFrame,
    OutputTransportMessageFrame,
    OutputTransportMessageUrgentFrame,
)
from pipecat.processors.frame_processor import FrameProcessorSetup
from pipecat.serializers.base_serializer import FrameSerializer


class VoittaMediaSerializer(FrameSerializer):
    def __init__(self, hang_up_on_end: bool = True, params: FrameSerializer.InputParams | None = None):
        super().__init__(params or FrameSerializer.InputParams())
        self.hang_up_on_end = hang_up_on_end
        self.call: dict = {}
        self.gateway_rate = 16000
        self._pipeline_rate = 16000
        self._in_rs = create_stream_resampler()
        self._out_rs = create_stream_resampler()
        self._ended = False

    async def setup(self, setup: FrameProcessorSetup):
        self._pipeline_rate = setup.audio_in_sample_rate or self._pipeline_rate

    async def serialize(self, frame: Frame) -> str | bytes | None:
        if isinstance(frame, InterruptionFrame):
            return '{"event":"clear"}'
        if isinstance(frame, AudioRawFrame):
            pcm = frame.audio
            if frame.sample_rate != self.gateway_rate:
                pcm = await self._out_rs.resample(pcm, frame.sample_rate, self.gateway_rate)
            if not pcm:
                return None
            return json.dumps({"event": "media", "payload": base64.b64encode(pcm).decode()})
        if isinstance(frame, (EndFrame, CancelFrame)):
            if self.hang_up_on_end and not self._ended:
                self._ended = True
                return json.dumps({"event": "hangup", "reason": "agent_end"})
            return None
        if isinstance(frame, (OutputTransportMessageFrame, OutputTransportMessageUrgentFrame)):
            if self.should_ignore_frame(frame):
                return None
            return json.dumps(frame.message)
        return None

    async def deserialize(self, data: str | bytes) -> Frame | None:
        if isinstance(data, bytes):
            return None
        try:
            msg = json.loads(data)
        except json.JSONDecodeError:
            return None
        ev = msg.get("event")
        if ev == "media":
            pcm = base64.b64decode(msg.get("payload") or "")
            if self.gateway_rate != self._pipeline_rate:
                pcm = await self._in_rs.resample(pcm, self.gateway_rate, self._pipeline_rate)
            if not pcm:
                return None
            return InputAudioRawFrame(audio=pcm, num_channels=1, sample_rate=self._pipeline_rate)
        if ev == "dtmf":
            try:
                return InputDTMFFrame(KeypadEntry(str(msg.get("digit"))))
            except ValueError:
                return None
        if ev == "start":
            self.call = msg
            self.gateway_rate = int((msg.get("media_format") or {}).get("sample_rate") or 16000)
        if ev == "stop":
            self._ended = True                     # the call is over: don't send hangup back
        return InputTransportMessageFrame(message=msg)
