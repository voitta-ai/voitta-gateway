"""Check live traffic against spec/*.yaml (OpenAPI 3.1 and AsyncAPI 3.1 payload schemas)."""
from __future__ import annotations

import re
from functools import cache
from pathlib import Path
from urllib.parse import quote

import yaml
from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

SPEC = Path(__file__).parent.parent / "spec"
METHODS = {"get", "post", "put", "delete", "patch"}


class Doc:
    def __init__(self, name: str):
        self.doc = yaml.safe_load((SPEC / name).read_text())
        self.uri = f"urn:voitta:{name}"
        self.registry = Registry().with_resource(self.uri, Resource(self.doc, specification=DRAFT202012))
        self._validators: dict[str, Draft202012Validator] = {}

    def validator(self, pointer: str) -> Draft202012Validator:
        if pointer not in self._validators:
            self._validators[pointer] = Draft202012Validator(
                {"$ref": f"{self.uri}#{quote(pointer, safe='/~')}"}, registry=self.registry)
        return self._validators[pointer]

    def errors(self, pointer: str, instance) -> list[str]:
        return [f"{'/'.join(map(str, e.absolute_path)) or '<root>'}: {e.message}"
                for e in self.validator(pointer).iter_errors(instance)]


class OpenApi(Doc):
    def __init__(self, name: str):
        super().__init__(name)
        self.routes: list[tuple[str, re.Pattern, str]] = []
        for path, item in self.doc["paths"].items():
            rx = re.compile("^" + re.sub(r"\{[^}]+\}", "[^/]+", path) + "$")
            for method in item:
                if method in METHODS:
                    self.routes.append((method.upper(), rx, path))

    def operations(self) -> set[tuple[str, str]]:
        """(METHOD, path with params normalized to {}) for route-parity checks."""
        return {(m, re.sub(r"\{[^}]+\}", "{}", p)) for m, _, p in self.routes}

    def check_response(self, method: str, path: str, status: int, body) -> list[str]:
        for m, rx, p in self.routes:
            if m == method and rx.match(path):
                break
        else:
            return [f"{method} {path} is not in {self.uri}"]
        responses = self.doc["paths"][p][method.lower()]["responses"]
        key = str(status) if str(status) in responses else "default"
        if key not in responses:
            return [f"{method} {p}: status {status} is not in the spec"]
        resp = responses[key]
        if "$ref" in resp:                                   # components/responses/X
            ref = resp["$ref"].removeprefix("#")
            resp = self.doc
            for part in ref.strip("/").split("/"):
                resp = resp[part]
            pointer = ref + "/content/application~1json/schema"
        else:
            esc = p.replace("~", "~0").replace("/", "~1")
            pointer = f"/paths/{esc}/{method.lower()}/responses/{key}/content/application~1json/schema"
        if "application/json" not in resp.get("content", {}):
            return [f"{method} {p} {status}: spec has no JSON body for this response"]
        return [f"{method} {p} {status}: {e}" for e in self.errors(pointer, body)]


class AsyncApi(Doc):
    def check_event(self, event: dict) -> list[str]:
        errs = self.errors("/components/schemas/Event", event)
        if errs:                                              # say which schema it should have matched
            name = "".join(w.capitalize() for w in re.split(r"[._]", event.get("type", "")
                           .removeprefix("ai.voitta.").rsplit(".v", 1)[0])) + "Event"
            if name in self.doc["components"]["schemas"]:
                errs = self.errors(f"/components/schemas/{name}", event)
        return [f"{event.get('type')} {event.get('id')}: {e}" for e in errs]

    def check_media(self, msg: dict, direction: str) -> list[str]:
        schema = "GatewayToAgent" if direction == "to_agent" else "AgentToGateway"
        return [f"media {msg.get('event')} ({direction}): {e}" for e in self.errors(f"/components/schemas/{schema}", msg)]


@cache
def gateway_api() -> OpenApi:
    return OpenApi("gateway.openapi.yaml")


@cache
def ask_api() -> OpenApi:
    return OpenApi("ask.openapi.yaml")


@cache
def streams() -> AsyncApi:
    return AsyncApi("voitta.asyncapi.yaml")


class Violations:
    """Collects spec violations seen during a test; the fixture fails the test at teardown."""

    def __init__(self):
        self.items: list[str] = []

    def add(self, errs: list[str], kind: str = "") -> None:
        self.items.extend(errs)
        if kind:
            COUNTS[kind] = COUNTS.get(kind, 0) + 1


COUNTS: dict[str, int] = {}
VIOLATIONS = Violations()


def response_hook(api: OpenApi):
    async def hook(response):
        if not response.headers.get("content-type", "").startswith("application/json"):
            return
        await response.aread()
        VIOLATIONS.add(api.check_response(response.request.method, response.request.url.path,
                                          response.status_code, response.json()), "responses")
    return hook
