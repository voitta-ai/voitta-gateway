"""voitta-post: post files into a Signal group, addressed to agents, for them to review.

    voitta-post send --group test-group --to some-agent --to ada --title "page 3" a.png b.png

It uploads the files (POST /v1/attachments) and calls POST /v1/posts. The gateway sends the post
from its own account. An agent on another Signal account is @-mentioned. A consumer of this
gateway cannot see the account's own posts, because the gateway drops its own envelopes, so it
gets the post as a `message.received` event of its own. That event has the same message_id as
the post, and its sender is `{"kind": "tool", "id": <calling consumer>}`, never a person.

Names resolve from gateway.yaml, so no group id or account id appears on the command line or in
the request. `--group` is a `conversations` label. `--to` is either a `persons` entry, an agent
or a person (mentioned), or a consumer serving the group (its first name keyword is written into
the text, and it gets the event).

Guardrails, enforced by the gateway:
- only consumers listed under `post.consumers`, and only into groups under `post.groups`, and
  only while the group is closed (no group link, only admins add members);
- the text has a fixed shape: the addressing, then an optional one-line title, never free text.
  A title that says approve or deny is refused;
- agents see neutral file names (1.png, 2.png, ...), never the uploaded ones.

Environment: VOITTA_GATEWAY_URL (default http://127.0.0.1:8700) and VOITTA_POST_TOKEN, the
calling consumer's token.
"""
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import re
import sys
import unicodedata
import urllib.error
import urllib.request
from pathlib import Path

from .config import Config, is_account

MARK = chr(0xFFFC)                   # one per mention, as Signal sends them
MAX_TITLE = 100
# A trusted user's "approve <id>" / "deny <id>" resolves a parked command in an agent. A post
# nobody looked at must never carry one, so the words are refused anywhere in a title.
FORBIDDEN = re.compile(r"\b(approve[sd]?|deny|denie[sd])\b", re.IGNORECASE)


class PostError(Exception):
    pass


# ---------------------------------------------------------------- used by the gateway
def resolve(cfg: Config, label: str, names: list[str]) -> tuple[list[str], list[tuple[str, str]]]:
    """(ACIs to mention, [(consumer, name keyword)]) for the agents named in a post to `label`."""
    conv = cfg.conversations[label]
    serving = list(conv.consumers) or list(cfg.consumers)
    mentions: list[str] = []
    consumers: list[tuple[str, str]] = []
    for name in names:
        want = name.lower()
        person = next((p for pid, p in cfg.persons.items() if pid.lower() == want), None)
        if person is not None:                           # an agent or a person on Signal: mentioned
            aci = next((a for a in person.signal if is_account(a)), None)
            if not aci:
                raise PostError(f"persons.{name} has no Signal account id (ACI) to mention")
            if aci not in mentions:
                mentions.append(aci)
            continue
        consumer = next((n for n in serving if want in {
            n.lower(), (cfg.consumers[n].label or "").lower(), *(w.lower() for w in cfg.consumers[n].names)}), None)
        if consumer is None:
            raise PostError(f"{name} is neither an agent under persons nor a consumer serving {label}")
        words = cfg.consumers[consumer].names
        if not words:
            raise PostError(f"consumer {consumer} has no name keyword (consumers.{consumer}.names)")
        if consumer not in {c for c, _ in consumers}:
            consumers.append((consumer, words[0]))
    return mentions, consumers


def check_title(title: str) -> str:
    """One short line of plain text that cannot approve or deny anything."""
    title = (title or "").strip()
    if len(title) > MAX_TITLE:
        raise PostError(f"title is {len(title)} characters; at most {MAX_TITLE}")
    if any(unicodedata.category(ch)[0] == "C" for ch in title):
        raise PostError("title must be one line of plain text (no control or format characters)")
    if FORBIDDEN.search(title):
        raise PostError("title must not say approve or deny")
    return title


def compose(mentions: list[str], keywords: list[str], title: str) -> dict:
    """The text part: each mention a U+FFFC at the start, then the name keywords, then the title.
    Mention offsets are UTF-16 code units; U+FFFC and a space are one each."""
    text = " ".join([MARK] * len(mentions) + keywords + ([title] if title else []))
    part: dict = {"type": "text", "text": text}
    if mentions:
        part["mentions"] = [{"start": 2 * i, "length": 1, "id": m} for i, m in enumerate(mentions)]
    return part


# ---------------------------------------------------------------- the CLI
def _call(base: str, token: str, path: str, data: bytes, ctype: str, extra: dict | None = None) -> dict:
    req = urllib.request.Request(base.rstrip("/") + path, data=data, method="POST",
                                 headers={"Authorization": f"Bearer {token}", "Content-Type": ctype,
                                          **(extra or {})})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        raise PostError(f"{path}: HTTP {e.code} {e.read().decode(errors='replace')[:500]}")


def cmd_send(args) -> dict:
    files = [Path(f).expanduser() for f in args.files]
    missing = [str(f) for f in files if not f.is_file()]
    if missing:
        raise PostError(f"not a file: {', '.join(missing)}")
    token = os.environ.get("VOITTA_POST_TOKEN")
    if not token:
        raise PostError("VOITTA_POST_TOKEN is not set (the calling consumer's gateway token)")
    base = os.environ.get("VOITTA_GATEWAY_URL", "http://127.0.0.1:8700")
    check_title(args.title or "")                       # fail before uploading; the gateway checks again
    ids = []
    for i, f in enumerate(files, 1):
        ctype = mimetypes.guess_type(f.name)[0] or "application/octet-stream"
        up = _call(base, token, "/v1/attachments", f.read_bytes(), ctype,
                   {"X-Filename": f"{i}{f.suffix.lower()}"})
        ids.append(up["attachment_id"])
    body = {"group": args.group, "to": args.to, "title": args.title or "", "attachments": ids}
    return _call(base, token, "/v1/posts", json.dumps(body).encode(), "application/json")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="voitta-post", description=(__doc__ or "").split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("send", help="post files into an allowed group, addressed to agents")
    s.add_argument("--group", required=True, help="a conversations label listed under post.groups")
    s.add_argument("--to", action="append", required=True,
                   help="a persons entry (mentioned) or a consumer serving the group; repeat for more")
    s.add_argument("--title", default="", help="one line, at most 100 characters, no approve/deny")
    s.add_argument("files", nargs="+", help="the files to review")
    args = ap.parse_args(argv)
    try:
        print(json.dumps(cmd_send(args)))
    except PostError as e:
        print(f"voitta-post: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
