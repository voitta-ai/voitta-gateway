"""What the core expects from a platform adapter."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..core import Gateway


class PermanentError(Exception):
    """The platform refused; retrying will not help (bad recipient, unsupported...)."""


class Unsupported(PermanentError):
    pass


@dataclass
class Inbound:
    """One message from a platform, already normalized by the adapter."""
    channel: str
    conversation_key: dict                 # what the adapter needs to send back here
    kind: str                              # direct | group
    sender: dict                           # {"id", "number"?, "name"?}
    platform_id: str                       # unique within the conversation
    time_ms: int
    parts: list[dict]
    reply_to_platform: str | None = None
    thread_platform: str | None = None
    addressed: bool = True                 # mentioned / DM; groups may require it
    extra: dict[str, Any] = field(default_factory=dict)


class MessagingAdapter:
    name: str = ""
    account: str = ""
    capabilities: dict = {}
    min_interval_s: float = 0.0            # per conversation, outbound pacing

    async def start(self, gw: "Gateway") -> None: ...
    async def stop(self) -> None: ...

    async def send(self, key: dict, parts: list[dict], reply_to: dict | None) -> tuple[str, str | None]:
        """Returns (platform_id, thread_platform_id)."""
        raise Unsupported("send")

    async def edit(self, key: dict, platform_id: str, parts: list[dict]) -> None:
        raise Unsupported("edit")

    async def delete(self, key: dict, platform_id: str) -> None:
        raise Unsupported("delete")

    async def react(self, key: dict, target: dict, emoji: str, remove: bool) -> None:
        raise Unsupported("reactions")

    async def typing(self, key: dict, on: bool) -> None:
        raise Unsupported("typing")

    def direct_key(self, address: str) -> dict:
        """Conversation key for a 1:1 conversation with `address` (agent-initiated sends)."""
        raise Unsupported("starting conversations")


class CallAdapter:
    name: str = ""
    call_capabilities: dict = {}

    async def dial(self, call_id: str, to: str) -> str:
        raise Unsupported("outbound calls")

    async def answer(self, call_id: str, platform_id: str) -> None: ...
    async def reject(self, call_id: str, platform_id: str, reason: str) -> None: ...
    async def hangup(self, call_id: str, platform_id: str) -> None: ...

    async def send_dtmf(self, call_id: str, platform_id: str, digits: str) -> None:
        raise Unsupported("dtmf")

    async def transfer(self, call_id: str, platform_id: str, to: str) -> None:
        raise Unsupported("transfer")
