from __future__ import annotations

import asyncio
import os
import time

import httpx
import pytest
import uvicorn

from voitta_gateway.app import build
from voitta_gateway.config import Config

from .fakes import free_port
from .spec_check import VIOLATIONS, gateway_api, response_hook, streams

TOKEN = "test-token"
MEDIA_SECRET = "media-secret"


async def serve(app, port: int | None = None) -> tuple[uvicorn.Server, asyncio.Task, str]:
    port = port or free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    task = asyncio.create_task(server.serve())
    for _ in range(200):
        if server.started:
            break
        await asyncio.sleep(0.02)
    return server, task, f"http://127.0.0.1:{port}"


class Running:
    def __init__(self, gw, api_url, ingress_url, client, servers):
        self.gw, self.api_url, self.ingress_url, self.client, self.servers = gw, api_url, ingress_url, client, servers
        self.cursor = "evt_0"

    async def wait_event(self, short_type: str, timeout: float = 5.0, pred=None, from_start=False) -> dict:
        cursor = "evt_0" if from_start else self.cursor
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            r = await self.client.get("/v1/events/poll", params={"after": cursor, "wait": 0.5})
            body = r.json()
            for e in body["events"]:
                if e["type"] == f"ai.voitta.{short_type}.v1" and (pred is None or pred(e)):
                    self.cursor = e["id"]
                    return e
            cursor = body["next"]
        raise AssertionError(f"no {short_type} event within {timeout}s")

    async def events(self, short_type: str) -> list[dict]:
        r = await self.client.get("/v1/events/poll", params={"after": "evt_0", "limit": 1000})
        return [e for e in r.json()["events"] if e["type"] == f"ai.voitta.{short_type}.v1"]


@pytest.fixture
async def gateway(tmp_path):
    started: list[Running] = []

    async def start(cfg_dict: dict, **build_kw) -> Running:
        os.environ["TEST_TOKEN"] = TOKEN
        os.environ["TEST_MEDIA_SECRET"] = MEDIA_SECRET
        cfg_dict = {"data_dir": str(tmp_path / "var"),
                    "consumers": {"agent": {"token_secret": "TEST_TOKEN", "scopes": ["admin"], "events": ["*"]}},
                    **cfg_dict}
        cfg = Config.model_validate(cfg_dict)
        gw, api, ingress = build(cfg, **build_kw)
        await gw.start()
        gw.outbox.start()
        servers = []
        s, t, api_url = await serve(api)
        servers.append((s, t))
        ingress_url = None
        if ingress is not None:
            s2, t2, ingress_url = await serve(ingress)
            servers.append((s2, t2))
        client = httpx.AsyncClient(base_url=api_url, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=10,
                                   event_hooks={"response": [response_hook(gateway_api())]})
        r = Running(gw, api_url, ingress_url, client, servers)
        started.append(r)
        return r

    yield start

    for r in started:                                   # every event this gateway emitted must match the spec
        for event in r.gw.events.after(0, 1_000_000):
            VIOLATIONS.add(streams().check_event(event), "events")
    for r in started:
        await r.client.aclose()
        await r.gw.calls.stop()
        await r.gw.outbox.stop()
        for a in {*r.gw.messaging.values(), *r.gw.voice.values()}:
            try:
                await a.stop()
            except Exception:
                pass
        for s, t in r.servers:
            s.should_exit = True
            await t


@pytest.fixture(autouse=True)
def spec_conformance():
    """Fail the test if any response, event or media message broke spec/*.yaml."""
    VIOLATIONS.items.clear()
    yield
    found, VIOLATIONS.items[:] = list(VIOLATIONS.items), []
    assert not found, "spec violations:\n  " + "\n  ".join(dict.fromkeys(found))
