"""The specs are well-formed and cover exactly the routes the code serves.

Payload conformance (every JSON response, every event, every media message) is checked in
all other tests by the `spec_conformance` fixture in conftest.py.
"""
import re

from fastapi.routing import APIRoute, APIWebSocketRoute
from jsonschema import Draft202012Validator
from openapi_spec_validator import validate
from openapi_spec_validator.readers import read_from_filename

from voitta_ask.service import AskConfig, AskService, build_app
from voitta_ask.engines import FakeSTT, FakeTTS
from voitta_gateway.api import build_api
from voitta_gateway.config import Config
from voitta_gateway.core import Gateway

from .spec_check import SPEC, ask_api, gateway_api, streams


def served(app) -> set[tuple[str, str]]:
    out = set()
    for r in app.routes:
        if isinstance(r, APIRoute) and not r.path.startswith(("/docs", "/redoc", "/openapi")):
            for m in r.methods - {"HEAD"}:
                out.add((m, re.sub(r"\{[^}]+\}", "{}", r.path)))
    return out


def test_openapi_documents_are_valid():
    for name in ("gateway.openapi.yaml", "ask.openapi.yaml"):
        doc, _ = read_from_filename(str(SPEC / name))
        validate(doc)


def test_asyncapi_payload_schemas_are_valid_json_schema():
    for name, schema in streams().doc["components"]["schemas"].items():
        Draft202012Validator.check_schema(schema)
    # every event type the code can emit has a schema (and nothing else does)
    names = {m["name"] for m in streams().doc["components"]["messages"].values() if m["name"].startswith("ai.")}
    import pathlib
    src = "".join(p.read_text() for p in pathlib.Path("voitta_gateway").rglob("*.py"))
    emitted = set(re.findall(r'(?:append|_event)\(\s*(?:c, )?"([a-z_]+\.[a-z_]+)"', src))
    emitted |= {"channel.down", "reaction.added"}              # second arm of `x if cond else y`
    assert {f"ai.voitta.{t}.v1" for t in emitted} == names


def test_gateway_routes_match_spec(tmp_path, monkeypatch):
    monkeypatch.setenv("T", "x")
    gw = Gateway(Config.model_validate({"data_dir": str(tmp_path), "consumers": {"c": {"token_secret": "T"}}}))
    assert served(build_api(gw)) == gateway_api().operations()


def test_ask_routes_match_spec(monkeypatch):
    monkeypatch.setenv("T", "x")
    cfg = AskConfig.model_validate({"gateway": {"token_secret": "T"}, "media_secret_secret": "T",
                                    "tts": {"engine": "fake"}, "stt": {"engine": "fake"}})
    app = build_app(AskService(cfg, tts=FakeTTS(), stt=FakeSTT()))
    assert served(app) == ask_api().operations()
    assert any(isinstance(r, APIWebSocketRoute) and r.path == "/media" for r in app.routes)


async def test_error_shape_for_bad_query(gateway):
    """Malformed parameters use the same Error shape as everything else (checked by the hook)."""
    gw = await gateway({})
    r = await gw.client.get("/v1/events/poll", params={"wait": "soon"})
    assert r.status_code == 422 and r.json()["error"]["code"] == "bad_request"
