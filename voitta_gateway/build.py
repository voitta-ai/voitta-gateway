"""The one builder both shapes use: config -> Gateway with its adapters wired.

No web framework here. The service (`app.build`) passes an `ingress` factory
so the adapters that need public HTTP (phone webhooks, Slack call pages) can
mount on it; the library (`embed.build`) passes none, and those adapters are
refused with a message saying so.
"""
from __future__ import annotations

from typing import Callable

from .calls import CallManager
from .config import Config
from .core import Gateway
from .outbox import Outbox


def build_gateway(cfg: Config, *, ingress: Callable[[], object] | None = None,
                  signal_port_factory=None, telnyx_http=None) -> Gateway:
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
    if a.slack:
        if a.slack.calls and ingress is None:
            raise ValueError("adapters.slack.calls needs the service: callees join through an HTTP page")
        from .adapters.slack import SlackAdapter
        sl = SlackAdapter(a.slack, cfg.secret(a.slack.bot_token_secret), cfg.secret(a.slack.app_token_secret))
        gw.messaging["slack"] = sl
        if a.slack.calls:
            gw.voice["slack"] = sl
            sl.mount(ingress())
    if a.phone:
        if ingress is None:
            raise ValueError("adapters.phone needs the service: the provider reaches it by webhook")
        from .adapters.telnyx import TelnyxAdapter
        t = TelnyxAdapter(a.phone, cfg.secret(a.phone.api_key_secret),
                          cfg.secret(a.phone.public_key_secret) if a.phone.public_key_secret else None,
                          http=telnyx_http)
        if a.phone.verify_signatures and not a.phone.public_key_secret:
            raise ValueError("adapters.phone.verify_signatures needs public_key_secret")
        gw.voice["phone"] = t
        t.mount(ingress())
    retval = gw
    return retval
