"""The gateway embedded as a library: echo every text message back, no HTTP listener.

    pip install voitta-gateway            # no [service] extra: fastapi is not needed
    python examples/embedded_echo.py --config gateway.yaml

The config is the ordinary gateway.yaml; only adapters that need public HTTP
(the phone, Slack calls) are refused. This process becomes the one owner of the
configured Signal account and Slack app, so nothing else may run against them.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal

from voitta_gateway import embed

log = logging.getLogger("embedded-echo")


async def main(path: str) -> None:
    gw = embed.build(path, consumer="echo")

    async def on_message(event: dict) -> None:
        d = event["data"]
        text = " ".join(p.get("text", "") for p in d["parts"] if p.get("type") == "text").strip()
        if not text:
            return
        result = gw.send({"to": {"conversation_id": d["conversation_id"]},
                          "parts": [{"type": "text", "text": f"echo: {text}"}],
                          "reply_to": d["message_id"]})
        log.info("queued %s", result["message_id"])

    gw.on_event(on_message, types=["message.received"])
    await gw.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    await gw.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", "-c", default="gateway.yaml")
    args = ap.parse_args()
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(main(args.config))
