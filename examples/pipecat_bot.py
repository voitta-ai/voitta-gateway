"""A Pipecat voice agent behind the gateway.

    pip install -e ".[pipecat]"                       # echo mode needs nothing else
    pip install "pipecat-ai[deepgram,anthropic,cartesia,silero]"   # assistant mode
    VOITTA_MEDIA_SECRET=... uvicorn examples.pipecat_bot:app --port 8765

MODE=echo (default) loops the caller's audio through a Pipecat pipeline: it
proves the serializer and transport without any API keys. MODE=assistant runs
Deepgram -> Claude -> Cartesia and opens with the AI disclosure.
"""
from __future__ import annotations

import os

from fastapi import FastAPI, WebSocket
from loguru import logger
from pipecat.frames.frames import (
    Frame,
    InputAudioRawFrame,
    InputTransportMessageFrame,
    OutputAudioRawFrame,
    TTSSpeakFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.transports.websocket.fastapi import FastAPIWebsocketParams, FastAPIWebsocketTransport

from pipecat.workers.runner import WorkerRunner

from voitta_gateway.agent import verify
from voitta_gateway.agent.pipecat import VoittaMediaSerializer

DISCLOSURE = "Hi, you are talking to Voitta's AI assistant. How can I help?"
app = FastAPI()


class Echo(FrameProcessor):
    """Caller audio straight back out: the audio path test."""

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, InputAudioRawFrame):
            await self.push_frame(OutputAudioRawFrame(audio=frame.audio, sample_rate=frame.sample_rate,
                                                      num_channels=frame.num_channels))
        else:
            await self.push_frame(frame, direction)


class GreetOnStart(FrameProcessor):
    """Speak the disclosure once the gateway says the call is answered (`start`)."""

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, InputTransportMessageFrame) and frame.message.get("event") == "start":
            logger.info(f"call {frame.message.get('call_id')} from {frame.message.get('from')}")
            await self.push_frame(TTSSpeakFrame(DISCLOSURE))
        await self.push_frame(frame, direction)


def assistant_processors() -> list[FrameProcessor]:
    from pipecat.processors.aggregators.llm_context import LLMContext
    from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
    from pipecat.services.anthropic.llm import AnthropicLLMService
    from pipecat.services.cartesia.tts import CartesiaTTSService
    from pipecat.services.deepgram.stt import DeepgramSTTService

    stt = DeepgramSTTService(api_key=os.environ["DEEPGRAM_API_KEY"])
    llm = AnthropicLLMService(api_key=os.environ["ANTHROPIC_API_KEY"],
                              model=os.environ.get("CLAUDE_MODEL", "claude-sonnet-5-5"))
    tts = CartesiaTTSService(api_key=os.environ["CARTESIA_API_KEY"],
                             voice_id=os.environ.get("CARTESIA_VOICE", "71a7ad14-091c-4e8e-a314-022ece01c121"))
    context = LLMContext([{"role": "system", "content":
                           "You are Voitta's phone assistant. Be brief: this is a voice call. "
                           "You already told the caller you are an AI."}])
    agg = LLMContextAggregatorPair(context)
    return [stt, agg.user(), llm, GreetOnStart(), tts, agg.assistant()]


@app.websocket("/media")
async def media(ws: WebSocket):
    secret = os.environ.get("VOITTA_MEDIA_SECRET")
    q = ws.query_params
    if secret and not verify(secret, q.get("call_id", ""), q.get("ts", "0"), q.get("sig", "")):
        await ws.close(code=4403)
        return
    await ws.accept()
    transport = FastAPIWebsocketTransport(ws, FastAPIWebsocketParams(
        audio_in_enabled=True, audio_out_enabled=True, add_wav_header=False,
        serializer=VoittaMediaSerializer()))
    middle = assistant_processors() if os.environ.get("MODE") == "assistant" else [Echo()]
    task = PipelineWorker(Pipeline([transport.input(), *middle, transport.output()]),
                        params=PipelineParams(audio_in_sample_rate=16000, audio_out_sample_rate=16000))

    @transport.event_handler("on_client_disconnected")
    async def _gone(_transport, _client):
        await task.cancel()

    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(task)
    await runner.run()
