"""Wire config -> gateway -> adapters -> HTTP listeners.

Two listeners on purpose: the private API (commands, events, admin) and the
public ingress (Telnyx webhooks + media). Only the ingress faces the internet.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import signal

import uvicorn
from fastapi import FastAPI

from .api import build_api
from .calls import CallManager
from .config import Config, host_port
from .core import Gateway
from .outbox import Outbox

log = logging.getLogger(__name__)


class _Server(uvicorn.Server):
    """uvicorn's own handlers stop only the server that installed them last; run() stops both."""

    @contextlib.contextmanager
    def capture_signals(self):
        yield


def build(cfg: Config, *, signal_port_factory=None, telnyx_http=None) -> tuple[Gateway, FastAPI, FastAPI | None]:
    gw = Gateway(cfg)
    gw.outbox = Outbox(gw)
    gw.calls = CallManager(gw)
    a = cfg.adapters
    if a.signal:
        from .adapters.signal import SignalAdapter
        s = SignalAdapter(a.signal, port_factory=signal_port_factory)
        gw.messaging["signal"] = s
        if a.signal.calls:
            gw.voice["signal"] = s
    ingress = None

    def _ingress() -> FastAPI:
        nonlocal ingress
        if ingress is None:
            ingress = FastAPI(title="voitta-gateway ingress", docs_url=None, redoc_url=None, openapi_url=None)
        return ingress

    if a.slack:
        from .adapters.slack import SlackAdapter
        sl = SlackAdapter(a.slack, cfg.secret(a.slack.bot_token_secret), cfg.secret(a.slack.app_token_secret))
        gw.messaging["slack"] = sl
        if a.slack.calls:
            gw.voice["slack"] = sl
            sl.mount(_ingress())
    if a.phone:
        from .adapters.telnyx import TelnyxAdapter
        t = TelnyxAdapter(a.phone, cfg.secret(a.phone.api_key_secret),
                          cfg.secret(a.phone.public_key_secret) if a.phone.public_key_secret else None,
                          http=telnyx_http)
        if a.phone.verify_signatures and not a.phone.public_key_secret:
            raise ValueError("adapters.phone.verify_signatures needs public_key_secret")
        gw.voice["phone"] = t
        t.mount(_ingress())
    api = build_api(gw)
    return gw, api, ingress


async def run(cfg: Config) -> None:
    gw, api, ingress = build(cfg)
    await gw.start()
    gw.outbox.start()
    servers = []
    host, port = host_port(cfg.listen.api)
    servers.append(_Server(uvicorn.Config(api, host=host, port=port, log_level="info",
                                                 proxy_headers=False)))
    if ingress is not None:
        if not cfg.listen.ingress:
            raise ValueError("listen.ingress is required when the phone adapter is on")
        ih, ip = host_port(cfg.listen.ingress)
        servers.append(_Server(uvicorn.Config(ingress, host=ih, port=ip, log_level="info",
                                                     proxy_headers=True, ws_max_size=2 ** 22)))
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):    # SIGHUP: the terminal / screen closed
        loop.add_signal_handler(sig, lambda s=sig: [setattr(x, "should_exit", True) for x in servers]
                                and log.info("got %s, shutting down", signal.Signals(s).name))
    log.info("api on %s%s", cfg.listen.api, f", ingress on {cfg.listen.ingress}" if ingress else "")
    try:
        await asyncio.gather(*(s.serve() for s in servers))
    finally:
        await gw.calls.stop()
        await gw.outbox.stop()
        for adapter in {*gw.messaging.values(), *gw.voice.values()}:
            try:
                await adapter.stop()
            except Exception:
                log.exception("stopping %s", adapter.name)
