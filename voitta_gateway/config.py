"""gateway.yaml: which adapters run, with which accounts, and where their events go.

Secrets never live in the YAML. Fields ending in `_secret` name an environment
variable (or a line in the optional env file); `Config.secret()` resolves them.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, field_validator


class MediaFormat(BaseModel):
    encoding: Literal["pcm16le"] = "pcm16le"
    sample_rate: int = 16000
    channels: Literal[1] = 1


class Route(BaseModel):
    """Where calls go: the agent's media WebSocket and what to do when it fails."""
    media_url: str                                  # ws:// or wss://, the agent is the server
    media_secret_secret: str                        # HMAC key shared with the agent
    answer: Literal["auto", "manual"] = "auto"      # manual: the agent POSTs /answer
    connect_timeout_s: float = 3.0                  # agent unreachable -> fallback
    grace_s: float = 5.0                            # agent socket lost mid-call -> reconnect window
    fallback: str = "reject"                        # reject | hangup | transfer:+15551234567
    media_format: MediaFormat = MediaFormat()
    frame_ms: int = 20
    max_buffer_s: float = 60.0                      # agent audio queued ahead of playback


class SignalConfig(BaseModel):
    account: str                                    # E.164 number of the Signal account
    aci: str | None = None                          # our own UUID; looked up if omitted
    spawn: bool = True                              # start signal-cli ourselves
    binary: str = "signal-cli"
    data_dir: str | None = None                     # signal-cli --config dir (holds the keys)
    tcp: str = "127.0.0.1:7583"                     # JSON-RPC socket of the daemon
    send_read_receipts: bool = True
    groups: Literal["mention", "all", "ignore"] = "mention"
    calls: bool = False
    route: str | None = None
    attachments_dir: str | None = None              # where signal-cli saves attachments
    pulse_latency_ms: int = 40


class SlackCalls(BaseModel):
    """Outbound voice calls on Slack: a DM with a Join link that opens our browser page.

    Slack bots cannot join huddles, and apps outside Slack's partner program are meant to
    start Calls API calls from a slash command, so the default card is an ordinary message
    with a Join button; `card: calls_api` tries Slack's native call card first."""
    public_base_url: str                            # where callees' browsers reach the ingress
    route: str | None = None                        # default agent route for these calls
    ring_timeout_s: float = 60.0                    # not joined by then -> ended, unanswered
    card: Literal["button", "calls_api"] = "button"
    title: str = "Voitta"


class SlackConfig(BaseModel):
    bot_token_secret: str
    app_token_secret: str
    channels: Literal["mention", "all"] = "mention"  # DMs always count; @here/@channel count too
    bots: Literal["ignore", "mention"] = "mention"   # other bots' posts: dropped, or only when they mention us
    min_interval_s: float = 1.1                     # per channel, Slack allows ~1 msg/s
    calls: SlackCalls | None = None


class PhoneConfig(BaseModel):
    provider: Literal["telnyx"] = "telnyx"
    api_key_secret: str
    public_key_secret: str | None = None            # Ed25519 key for webhook signatures
    verify_signatures: bool = True
    connection_id: str | None = None                # Call Control app id, needed for dialing out
    numbers: list[str] = []
    route: str | None = None
    public_base_url: str                            # https://gw.example.com (ingress)
    codec: Literal["L16", "PCMU"] = "L16"
    l16_byte_order: Literal["big", "little"] = "big"
    api_base: str = "https://api.telnyx.com/v2"


class Adapters(BaseModel):
    signal: SignalConfig | None = None
    slack: SlackConfig | None = None
    phone: PhoneConfig | None = None


class Consumer(BaseModel):
    token_secret: str
    scopes: list[str] = ["events:read"]             # events:read messages:send calls:control admin
    events: list[str] = ["*"]                       # type patterns, e.g. message.*


class Person(BaseModel):
    signal: list[str] = []                          # ACI (UUID) and/or E.164
    slack: list[str] = []                           # U123 or T123/U123
    phone: list[str] = []                           # E.164


class Policy(BaseModel):
    inbound_allowlist: list[str] | Literal["*"] = "*"   # person ids; unknown senders -> held
    calls_allow_unknown: bool = False
    opt_out_keywords: list[str] = ["STOP", "UNSUBSCRIBE"]
    block_emergency_numbers: bool = True
    emergency_numbers: list[str] = ["911", "112", "999", "000", "110", "119", "933"]


class Speech(BaseModel):
    """Optional: transcribe inbound voice clips (Slack clips, Signal voice notes) through the
    voitta-speech API (mounted in voitta-ask). The gateway itself runs no models."""
    url: str                                        # e.g. http://127.0.0.1:8791
    token_secret: str
    timeout_s: float = 60.0
    language: str | None = None                     # None = detect


class Listen(BaseModel):
    api: str = "127.0.0.1:8700"                     # private: commands, events, admin
    ingress: str | None = None                      # public: Telnyx webhooks + media, Slack call pages


class Config(BaseModel):
    listen: Listen = Listen()
    data_dir: str = "./var"
    env_file: str | None = None
    adapters: Adapters = Adapters()
    routes: dict[str, Route] = {}
    consumers: dict[str, Consumer] = {}
    persons: dict[str, Person] = {}
    policy: Policy = Policy()
    speech: Speech | None = None
    idempotency_ttl_s: int = 24 * 3600
    max_send_attempts: int = 5

    _env: dict[str, str] = {}

    @field_validator("routes")
    @classmethod
    def _routes_ok(cls, v):
        for name, r in v.items():
            if not r.media_url.startswith(("ws://", "wss://")):
                raise ValueError(f"route {name}: media_url must be ws:// or wss://")
        return v

    def model_post_init(self, __context) -> None:
        slack_calls = self.adapters.slack.calls if self.adapters.slack else None
        for a in (self.adapters.signal, self.adapters.phone, slack_calls):
            if a and a.route and a.route not in self.routes:
                raise ValueError(f"adapter route {a.route!r} is not defined under routes")
        if self.env_file and Path(self.env_file).exists():
            for line in Path(self.env_file).read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    self._env[k.strip()] = v.strip().strip('"').strip("'")

    def secret(self, name: str | None) -> str | None:
        if not name:
            return None
        value = os.environ.get(name) or self._env.get(name)
        if value is None:
            raise KeyError(f"secret {name} is not set (environment or env_file)")
        return value

    @property
    def data_path(self) -> Path:
        p = Path(self.data_dir)
        p.mkdir(parents=True, exist_ok=True)
        return p


def load(path: str | Path) -> Config:
    raw = yaml.safe_load(Path(path).read_text()) or {}
    cfg = Config.model_validate(raw)
    if cfg.env_file and not Path(cfg.env_file).is_absolute():
        cfg.env_file = str(Path(path).parent / cfg.env_file)
        cfg.model_post_init(None)
    return cfg


def host_port(s: str) -> tuple[str, int]:
    host, _, port = s.rpartition(":")
    return host or "0.0.0.0", int(port)
