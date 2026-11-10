"""gateway.yaml: which adapters run, with which accounts, and where their events go.

Secrets never live in the YAML. Fields ending in `_secret` name an environment
variable (or a line in the optional env file); `Config.secret()` resolves them.
"""
from __future__ import annotations

import os
import unicodedata
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, field_validator


def clean_address(value: str) -> str:
    """An account string with every invisible character removed.

    Unicode format characters (category Cf: bidi controls such as U+202C, zero
    widths, the BOM) ride along when a number is pasted from a chat client or a
    contact card. They print as nothing, so the account looks right, and
    signal-cli answers "not registered" for it. Re-registering the account is
    the wrong, destructive response to that error; stripping the characters
    here is the fix."""
    retval = "".join(ch for ch in value if unicodedata.category(ch) != "Cf").strip()
    return retval


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
    # Off by default: a read receipt tells the sender a person saw the message.
    # Delivery receipts are not covered by this switch -- signal-cli sends them
    # whenever it receives, for every conversation including groups that are not
    # listed under `conversations`, and no flag turns that off.
    send_read_receipts: bool = False
    groups: Literal["mention", "all", "ignore"] = "mention"
    calls: bool = False
    route: str | None = None
    direct_consumer: str | None = None              # gets 1:1 messages; unset = every consumer
    attachments_dir: str | None = None              # where signal-cli saves attachments
    pulse_latency_ms: int = 40

    @field_validator("account", "aci")
    @classmethod
    def _clean(cls, v):
        return clean_address(v) if v else v


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
    direct_consumer: str | None = None              # gets DMs; unset = every consumer
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

    @field_validator("numbers")
    @classmethod
    def _clean(cls, v):
        return [clean_address(n) for n in v]


class Adapters(BaseModel):
    signal: SignalConfig | None = None
    slack: SlackConfig | None = None
    phone: PhoneConfig | None = None


class Consumer(BaseModel):
    token_secret: str
    scopes: list[str] = ["events:read"]             # events:read messages:send calls:control admin
    events: list[str] = ["*"]                       # type patterns, e.g. message.*
    names: list[str] = []                           # name keywords that address this consumer
    label: str | None = None                        # prefixed to its outbound text ("label: ...")


class Person(BaseModel):
    signal: list[str] = []                          # ACI (UUID) and/or E.164
    slack: list[str] = []                           # U123 or T123/U123
    phone: list[str] = []                           # E.164

    @field_validator("signal", "slack", "phone")
    @classmethod
    def _clean(cls, v):
        return [clean_address(a) for a in v]


class Conversation(BaseModel):
    """A group conversation the gateway listens to. Groups (Signal groups, Slack
    channels) are closed unless listed here: an unlisted one produces no event,
    no held row and no stored conversation.

    Gating happens here, at read time, because the platforms offer no gate
    earlier: on Signal anyone holding our profile key can add the account to a
    group and it is a full member at once, with no pending invite to decline."""
    channel: Literal["signal", "slack"]
    key: str                                        # signal: group id; slack: channel id
    consumers: list[str] = []                       # who serves it: gets its events, may act there;
                                                    # the first is the default addressee (#1 item 3).
                                                    # The label is also the send alias
                                                    # (`to: {conversation: label}`).

    @field_validator("key")
    @classmethod
    def _clean(cls, v):
        return clean_address(v)


class Policy(BaseModel):
    # Person ids allowed to reach the agents; anyone else is held for approval.
    # Empty by default; "*" lets every sender through and has to be chosen.
    inbound_allowlist: list[str] | Literal["*"] = []
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
    conversations: dict[str, Conversation] = {}     # label -> group conversation; unlisted groups are closed
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
        for label, c in self.conversations.items():
            for name in c.consumers:
                if name not in self.consumers:
                    raise ValueError(f"conversation {label}: consumer {name!r} is not defined under consumers")
        for name, a in (("signal", self.adapters.signal), ("slack", self.adapters.slack)):
            if a and a.direct_consumer and a.direct_consumer not in self.consumers:
                raise ValueError(f"adapters.{name}.direct_consumer {a.direct_consumer!r} is not defined under consumers")
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
