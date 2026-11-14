"""Private HTTP API: REST commands, the event stream (SSE + polling), admin.

Every request carries `Authorization: Bearer <token>`; each consumer in
gateway.yaml has its own token and scopes:
  events:read  messages:send  calls:control  admin
"""
from __future__ import annotations

import asyncio
import hmac
import json
import math
from typing import Any

from fastapi import Depends, FastAPI, Header, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

from .core import ApiError, Gateway
from .events import event_seq, matches


class Principal:
    def __init__(self, name: str, scopes: list[str], events: list[str]):
        self.name, self.scopes, self.events = name, scopes, events


def build_api(gw: Gateway) -> FastAPI:
    app = FastAPI(title="voitta-gateway", version="0.1.0")
    tokens: list[tuple[str, Principal]] = []
    for name, c in gw.cfg.consumers.items():
        tokens.append((gw.cfg.secret(c.token_secret), Principal(name, c.scopes, c.events)))

    @app.exception_handler(ApiError)
    async def _api_error(_: Request, e: ApiError):
        return JSONResponse({"error": {"code": e.code, "message": e.message}}, status_code=e.status)

    @app.exception_handler(RequestValidationError)
    async def _bad_request(_: Request, e: RequestValidationError):
        detail = "; ".join(f"{'.'.join(str(p) for p in x['loc'])}: {x['msg']}" for x in e.errors())
        return JSONResponse({"error": {"code": "bad_request", "message": detail}}, status_code=422)

    def principal(authorization: str = Header(default="")) -> Principal:
        token = authorization.removeprefix("Bearer ").strip()
        for t, p in tokens:
            if token and hmac.compare_digest(t, token):
                return p
        raise ApiError(401, "unauthorized", "missing or unknown bearer token")

    def need(scope: str):
        def dep(p: Principal = Depends(principal)) -> Principal:
            if scope not in p.scopes and "admin" not in p.scopes:
                raise ApiError(403, "forbidden", f"token lacks scope {scope}")
            return p
        return dep

    async def body(request: Request) -> dict[str, Any]:
        try:
            data = await request.json()
        except Exception:
            raise ApiError(400, "bad_json", "request body must be JSON")
        if not isinstance(data, dict):
            raise ApiError(400, "bad_json", "request body must be a JSON object")
        return data

    def idempotent(p: Principal, data: dict, fn):
        key = data.get("idempotency_key")
        if key:
            prior = gw.store.idem_get(p.name, key, gw.cfg.idempotency_ttl_s)
            if prior is not None:
                return JSONResponse({**prior, "duplicate": True}, status_code=200)
        result = fn()
        if key:
            gw.store.idem_put(p.name, key, result)
        return JSONResponse(result, status_code=202)

    async def idempotent_async(p: Principal, data: dict, coro_fn):
        key = data.get("idempotency_key")
        if key:
            prior = gw.store.idem_get(p.name, key, gw.cfg.idempotency_ttl_s)
            if prior is not None:
                return JSONResponse({**prior, "duplicate": True}, status_code=200)
        result = await coro_fn()
        if key:
            gw.store.idem_put(p.name, key, result)
        return JSONResponse(result, status_code=202)

    # ------------------------------------------------------------ meta
    @app.get("/v1/health")
    async def health():
        return {"ok": True, "channels": {k: v for k, v in gw.channel_up.items()}}

    @app.get("/v1/channels")
    async def channels(p: Principal = Depends(need("events:read"))):
        return {"channels": gw.channels()}

    @app.get("/v1/conversations")
    async def conversations(p: Principal = Depends(need("events:read"))):
        return {"conversations": gw.conversations(p.name)}

    @app.get("/v1/conversations/{label}/members")
    async def members(label: str, p: Principal = Depends(need("events:read"))):
        return await gw.members(label, p.name)

    @app.get("/v1/identities/{person_id}")
    async def identity(person_id: str, p: Principal = Depends(need("events:read"))):
        return {"person_id": person_id, "addresses": gw.identity.addresses(person_id)}

    # ------------------------------------------------------------ messages
    @app.post("/v1/messages")
    async def send(request: Request, data: dict = Depends(body),
                   p: Principal = Depends(need("messages:send"))):
        if "to" not in data:
            raise ApiError(422, "bad_target", "to is required")
        wait = data.get("wait_s")
        if wait is not None:                              # checked before anything is queued
            if isinstance(wait, bool) or not isinstance(wait, (int, float)) or not math.isfinite(wait):
                raise ApiError(422, "bad_wait", "wait_s must be a finite number of seconds")
            wait = min(max(float(wait), 0.0), 30.0)
        resp = idempotent(p, data, lambda: gw.send_message(p.name, data))
        if not wait:
            return resp
        # Synchronous mode for run-and-exit consumers: hold the answer until the platform took the
        # message (200) or refused it for good (502), else 202. Retries in between are the outbox's;
        # a retried POST with the same idempotency_key waits on the same message.
        result = json.loads(resp.body)
        if "conversation_id" not in result:
            raise ApiError(409, "key_reused", "that idempotency_key was used for another kind of request")
        mid = result["message_id"]
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait
        while True:
            row = gw.store.message(mid)
            if row is None:                               # the key outlived its message (data reset)
                raise ApiError(409, "message_gone", "the message this idempotency_key queued no longer "
                                                    "exists; send again with a new key")
            if row["status"] in ("sent", "delivered", "read", "deleted"):
                return JSONResponse({**result, "status": row["status"]}, status_code=200)
            if row["status"] == "failed":
                raise ApiError(502, "send_failed", row["error"] or "the platform refused the message")
            if loop.time() >= deadline or await request.is_disconnected():
                return JSONResponse({**result, "status": row["status"]}, status_code=202)  # still in flight
            await asyncio.sleep(0.1)

    @app.get("/v1/messages/{mid}")
    async def message(mid: str, p: Principal = Depends(need("events:read"))):
        row = gw.store.message(mid)
        if not row:
            raise ApiError(404, "unknown_message", mid)
        conv = gw.store.conversation_row(row["conversation_id"])
        gw.check_scope(p.name, conv["channel"], json.loads(conv["platform_key"]), conv["kind"])
        return {k: row[k] for k in ("id", "conversation_id", "direction", "status", "error")}

    @app.post("/v1/messages/{mid}/edit")
    async def edit(mid: str, data: dict = Depends(body), p: Principal = Depends(need("messages:send"))):
        return idempotent(p, data, lambda: gw.message_op(mid, "edit", {"parts": data.get("parts") or []}, p.name))

    @app.post("/v1/messages/{mid}/delete")
    async def delete(mid: str, data: dict = Depends(body), p: Principal = Depends(need("messages:send"))):
        return idempotent(p, data, lambda: gw.message_op(mid, "delete", {}, p.name))

    @app.post("/v1/messages/{mid}/reactions")
    async def react(mid: str, data: dict = Depends(body), p: Principal = Depends(need("messages:send"))):
        if not data.get("emoji"):
            raise ApiError(422, "bad_emoji", "emoji is required")
        return idempotent(p, data, lambda: gw.message_op(
            mid, "react", {"emoji": data["emoji"], "remove": bool(data.get("remove"))}, p.name))

    @app.post("/v1/conversations/{cid}/typing")
    async def typing(cid: str, data: dict = Depends(body), p: Principal = Depends(need("messages:send"))):
        await gw.typing(cid, bool(data.get("on", True)), p.name)
        return {"ok": True}

    @app.get("/v1/conversations/{cid}/history")
    async def history(cid: str, thread: str | None = None, limit: int = 25,
                      p: Principal = Depends(need("events:read"))):
        """Recent messages of a conversation, or of the thread a message belongs to (Slack)."""
        return {"conversation_id": cid, "messages": await gw.history(cid, thread, limit, p.name)}

    # ------------------------------------------------------------ attachments
    @app.post("/v1/attachments")
    async def upload(request: Request, p: Principal = Depends(need("messages:send")),
                     x_filename: str | None = Header(default=None)):
        data = await request.body()
        if not data:
            raise ApiError(422, "empty", "empty upload")
        ctype = request.headers.get("content-type", "application/octet-stream")
        return gw.store.put_file(data, ctype, x_filename)

    @app.get("/v1/attachments/{aid}")
    async def download(aid: str, p: Principal = Depends(need("events:read"))):
        row = gw.store.file(aid)
        if not row:
            raise ApiError(404, "unknown_attachment", aid)
        return FileResponse(row["path"], media_type=row["content_type"] or "application/octet-stream",
                            filename=row["filename"] or aid)

    # ------------------------------------------------------------ events
    def _filter(p: Principal, types: str | None, events: list[dict]) -> list[dict]:
        """A consumer's events: its type patterns, and on a shared account only what is addressed to it
        (#1 item 3). `addressed_to` absent or null means every consumer; admin tokens see everything."""
        wanted = [t.strip() for t in types.split(",")] if types else None
        return [e for e in events if matches(e["type"], p.events) and (not wanted or matches(e["type"], wanted))
                and (e["data"].get("addressed_to") in (None, p.name) or "admin" in p.scopes)]

    @app.get("/v1/events/poll")
    async def poll(request: Request, after: str | None = None, types: str | None = None, limit: int = 200,
                   wait: float = 0, p: Principal = Depends(need("events:read"))):
        cursor = gw.events.last_seq() if after == "now" else event_seq(after)
        reset = cursor > gw.events.last_seq()        # an id from another log (gateway data was reset)
        if reset:
            cursor = 0
        end = asyncio.get_running_loop().time() + min(wait, 30)
        while wait and gw.events.last_seq() <= cursor and not await request.is_disconnected():
            left = end - asyncio.get_running_loop().time()
            if left <= 0:
                break
            await gw.events.wait(cursor, min(left, 1.0))     # short slices: notice a client that left
        batch = gw.events.after(cursor, min(limit, 1000))
        nxt = batch[-1]["id"] if batch else f"evt_{cursor}"
        out = {"events": _filter(p, types, batch), "next": nxt}
        if reset:
            out["reset"] = True
        return out

    @app.get("/v1/events")
    async def stream(request: Request, after: str | None = None, types: str | None = None,
                     last_event_id: str | None = Header(default=None),
                     p: Principal = Depends(need("events:read"))):
        """Server-sent events. Reconnect with Last-Event-ID (or ?after=) to resume without gaps.
        `after=now` starts at the end of the log."""
        if after == "now":
            cursor = gw.events.last_seq()
        else:
            cursor = event_seq(last_event_id or after)
        if cursor > gw.events.last_seq():            # an id from another log: replay this one
            cursor = 0

        async def gen():
            nonlocal cursor
            yield ": voitta-gateway event stream\n\n"
            while not await request.is_disconnected():
                batch = gw.events.after(cursor)
                for e in _filter(p, types, batch):
                    yield f"id: {e['id']}\nevent: {e['type']}\ndata: {json.dumps(e)}\n\n"
                if batch:
                    cursor = event_seq(batch[-1]["id"])
                    continue
                await gw.events.wait(cursor, 15)
                if gw.events.last_seq() <= cursor:
                    yield ": keepalive\n\n"

        return StreamingResponse(gen(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    # ------------------------------------------------------------ calls
    def calls():
        if gw.calls is None:
            raise ApiError(422, "no_voice", "no voice adapters configured")
        return gw.calls

    @app.post("/v1/calls")
    async def start_call(data: dict = Depends(body), p: Principal = Depends(need("calls:control"))):
        return await idempotent_async(p, data, lambda: calls().start_call(data))

    @app.get("/v1/calls/{call_id}")
    async def get_call(call_id: str, p: Principal = Depends(need("events:read"))):
        c = calls().calls.get(call_id)
        if c:
            return calls().describe(c)
        row = gw.store.one("SELECT * FROM calls WHERE id=?", call_id)
        if not row:
            raise ApiError(404, "unknown_call", call_id)
        return {"call_id": row["id"], "channel": row["channel"], "direction": row["direction"],
                "state": row["state"], "peer": row["peer"], "person_id": row["person_id"],
                "route": row["route"], "reason": row["reason"], "answered": row["answered"] is not None}

    @app.post("/v1/calls/{call_id}/answer")
    async def answer(call_id: str, p: Principal = Depends(need("calls:control"))):
        return await calls().answer(call_id)

    @app.post("/v1/calls/{call_id}/reject")
    async def reject(call_id: str, p: Principal = Depends(need("calls:control"))):
        return await calls().reject(call_id)

    @app.post("/v1/calls/{call_id}/hangup")
    async def hangup(call_id: str, p: Principal = Depends(need("calls:control"))):
        return await calls().hangup(call_id, "hangup_api")

    @app.post("/v1/calls/{call_id}/dtmf")
    async def dtmf(call_id: str, data: dict = Depends(body), p: Principal = Depends(need("calls:control"))):
        digits = str(data.get("digits", ""))
        if not digits or any(ch not in "0123456789*#ABCDw" for ch in digits):
            raise ApiError(422, "bad_digits", "digits: 0-9 * # A-D, w = pause")
        return await calls().send_dtmf(call_id, digits)

    @app.post("/v1/calls/{call_id}/transfer")
    async def transfer(call_id: str, data: dict = Depends(body), p: Principal = Depends(need("calls:control"))):
        if not data.get("to"):
            raise ApiError(422, "bad_target", "to is required")
        return await calls().transfer(call_id, data["to"], data.get("mode", "blind"))

    @app.post("/v1/calls/{call_id}/recording")
    async def recording(call_id: str, p: Principal = Depends(need("calls:control"))):
        raise ApiError(422, "unsupported", "recording is not implemented yet")

    # ------------------------------------------------------------ admin
    @app.get("/v1/held")
    async def held(p: Principal = Depends(need("admin"))):
        return {"held": gw.held()}

    @app.post("/v1/held/{hid}/approve")
    async def approve(hid: str, p: Principal = Depends(need("admin"))):
        return {"released": gw.approve(hid)}

    @app.post("/v1/held/{hid}/discard")
    async def discard(hid: str, p: Principal = Depends(need("admin"))):
        gw.discard(hid)
        return {"ok": True}

    @app.get("/v1/optouts")
    async def optouts(p: Principal = Depends(need("admin"))):
        return {"optouts": [dict(r) for r in gw.store.q("SELECT * FROM optouts ORDER BY created")]}

    @app.delete("/v1/optouts/{channel}/{address}")
    async def optin(channel: str, address: str, p: Principal = Depends(need("admin"))):
        gw.store.x("DELETE FROM optouts WHERE channel=? AND address=?", channel, address)
        return {"ok": True}

    return app
