"""The smallest voitta-media/1 agent: beeps, then echoes the caller back.

    VOITTA_MEDIA_SECRET=... python examples/echo_agent.py --port 8765

Use it for the Signal and phone spikes: if you hear yourself, audio works
end to end in both directions. Press # (phone) to hang up from the agent side.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
from urllib.parse import parse_qs, urlparse

import numpy as np
import websockets

from voitta_gateway.agent import verify

log = logging.getLogger("echo-agent")


def beep(rate: int, ms: int = 300, hz: float = 660) -> bytes:
    t = np.arange(rate * ms // 1000) / rate
    return (6000 * np.sin(2 * np.pi * hz * t)).astype("<i2").tobytes()


async def handle(ws, secret: str | None) -> None:
    q = parse_qs(urlparse(ws.request.path).query)
    call_id = (q.get("call_id") or [""])[0]
    if secret and not verify(secret, call_id, (q.get("ts") or ["0"])[0], (q.get("sig") or [""])[0]):
        log.warning("rejecting connection with a bad signature")
        await ws.close(code=4403)
        return
    log.info("call %s connected", call_id)
    async for raw in ws:
        msg = json.loads(raw)
        ev = msg.get("event")
        if ev == "start":
            rate = msg["media_format"]["sample_rate"]
            log.info("start: %s -> %s via %s at %d Hz", msg.get("from"), msg.get("to"), msg.get("channel"), rate)
            await ws.send(json.dumps({"event": "media", "payload": base64.b64encode(beep(rate)).decode()}))
            await ws.send(json.dumps({"event": "mark", "name": "beep"}))
        elif ev == "media":
            await ws.send(json.dumps({"event": "media", "payload": msg["payload"]}))
        elif ev == "mark":
            log.info("mark %s %s", msg.get("name"), "cleared" if msg.get("cleared") else "played")
        elif ev == "dtmf":
            log.info("dtmf %s", msg.get("digit"))
            if msg.get("digit") == "#":
                await ws.send(json.dumps({"event": "hangup", "reason": "caller_pressed_hash"}))
        elif ev in ("stop", "error", "connected"):
            log.info("%s %s", ev, {k: v for k, v in msg.items() if k != "event"})
    log.info("call %s closed", call_id)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")
    secret = os.environ.get("VOITTA_MEDIA_SECRET")
    if not secret:
        log.warning("VOITTA_MEDIA_SECRET not set: accepting unsigned connections")
    async with websockets.serve(lambda ws: handle(ws, secret), args.host, args.port, max_size=2 ** 22):
        log.info("echo agent on ws://%s:%d", args.host, args.port)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
