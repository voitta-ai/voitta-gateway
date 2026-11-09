"""Call someone, read them a text, print what they answered.

    ASK_URL=http://127.0.0.1:8790 ASK_TOKEN=... python examples/ask.py +15551112222 "Are you free at ten?"
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import httpx


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("to", help="E.164 number or Signal ACI")
    ap.add_argument("text")
    ap.add_argument("--via", default="signal", choices=["signal", "phone"])
    ap.add_argument("--language", default="en")
    args = ap.parse_args()
    url, token = os.environ.get("ASK_URL", "http://127.0.0.1:8790"), os.environ["ASK_TOKEN"]
    with httpx.Client(base_url=url, headers={"Authorization": f"Bearer {token}"}, timeout=150) as c:
        r = c.post("/v1/asks", json={"to": args.to, "via": args.via, "text": args.text, "language": args.language})
        if r.status_code >= 400:
            sys.exit(f"refused: {r.status_code} {r.text}")
        ask = r.json()
        print(f"calling {args.to} ({ask['ask_id']})")
        seen, deadline = None, time.monotonic() + 300
        while time.monotonic() < deadline:
            ask = c.get(f"/v1/asks/{ask['ask_id']}", params={"wait": 5}).json()
            if ask["state"] != seen:
                print(f"  {ask['state']}")
                seen = ask["state"]
            if ask["state"] in ("done", "no_answer", "failed"):
                break
        if ask["state"] == "done":
            print(f"\nanswer ({ask['answer_s']} s, {ask['end_reason']}): {ask['transcript']!r}")
        else:
            print(f"\n{ask['state']}: {ask.get('end_reason') or ask.get('error')}")


if __name__ == "__main__":
    main()
