"""voitta-post: post files into a Signal group as a person, addressed to agents.

A post the gateway sends from its own account cannot ask its agents anything:
the gateway drops its own messages before routing (they are sync copies), and
agents on other accounts may ignore the gateway's account by design (loop
guards). So this tool posts from a different account: a signal-cli linked as a
secondary device to a person's own Signal account. The post is then exactly
what that person would send from their phone, and every agent treats it so.

    voitta-post -c gateway.yaml link                      # once: scan the QR in Signal
    voitta-post -c gateway.yaml send --group test-group \\
        --to some-agent --to ada --title "page 3, second pass" a.png b.png

Names resolve from gateway.yaml, so neither group ids nor account ids appear on
the command line: `--group` is a `conversations` label, and `--to` is a person
marked `agent: true` (mentioned) or a consumer serving the group (its first name
keyword is written into the text, or under `groups: mention` the gateway's
account is mentioned too).

The linked device can read and post anywhere the person can, and agents take
what it posts as the person's own words (a trusted user's "approve <id>"
resolves a parked command). So the tool is held tight:
- only groups listed under `post.groups` in gateway.yaml, and only while closed
  (no group link, only admins add members);
- the text is fixed-shape: the addressing, then an optional one-line title, never
  free text; a title that says approve or deny is refused;
- no linked device is an error; it never falls back to the gateway's account.

The linked device has its own signal-cli data directory, separate from the
gateway's account. It receives everything sent to the person's account, so
`send` receives first with attachments ignored and the output discarded: that
keeps the device active and its group state current, and keeps no messages.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import unicodedata
from pathlib import Path

from . import config
from .config import is_account

MARK = chr(0xFFFC)                   # one per mention, as Signal sends them
MAX_TITLE = 100
# A trusted user's "approve <id>" / "deny <id>" resolves a parked command in an agent. A post
# nobody looked at must never carry one, so the words are refused anywhere in a title.
FORBIDDEN = re.compile(r"\b(approve[sd]?|deny|denie[sd])\b", re.IGNORECASE)


class PostError(Exception):
    pass


class Cli:
    """signal-cli against the linked device's own data directory."""

    def __init__(self, binary: str, data_dir: Path):
        self.binary = binary
        self.data_dir = data_dir

    def run(self, *args: str, account: str | None = None, json_out: bool = False,
            quiet: bool = False, timeout: float = 120) -> str:
        cmd = [self.binary, "--config", str(self.data_dir)]
        if account:
            cmd += ["-a", account]
        if json_out:
            cmd += ["-o", "json"]
        cmd += list(args)
        p = subprocess.run(cmd, stdout=subprocess.DEVNULL if quiet else subprocess.PIPE,
                           stderr=subprocess.PIPE, text=True, timeout=timeout)
        if p.returncode != 0:
            raise PostError(f"signal-cli {args[0]} failed: {(p.stderr or '').strip()[-500:]}")
        return p.stdout or ""

    def account(self) -> str:
        """The one account linked into this data directory. None linked is an error: there is no
        fallback to any other account."""
        if not self.data_dir.exists():
            raise PostError(f"no linked device in {self.data_dir}; run `voitta-post link` first")
        out = self.run("listAccounts", json_out=True)
        accounts = [a.get("number") for a in json.loads(out or "[]")] if out.strip() else []
        if len(accounts) != 1:
            raise PostError(f"{self.data_dir} holds {len(accounts)} linked accounts; expected exactly one")
        return accounts[0]


def _cli(cfg: config.Config, args) -> Cli:
    binary = cfg.adapters.signal.binary if cfg.adapters.signal else "signal-cli"
    return Cli(binary, Path(args.signal_dir).expanduser())


def resolve(cfg: config.Config, label: str, names: list[str]) -> tuple[str, list[str], list[str]]:
    """(group id, mention ids, name keywords) for an allowed group label and the agents named."""
    if label not in cfg.post.groups:
        allowed = ", ".join(cfg.post.groups) or "none (post.groups is empty)"
        raise PostError(f"group {label!r} is not in post.groups; allowed: {allowed}")
    conv = cfg.conversations[label]                      # config validation: a listed Signal group
    sig = cfg.adapters.signal
    serving = list(conv.consumers) or list(cfg.consumers)
    mentions: list[str] = []
    keywords: list[str] = []
    for name in names:
        want = name.lower()
        person = next((p for pid, p in cfg.persons.items() if pid.lower() == want), None)
        if person is not None:
            if not person.agent:
                raise PostError(f"{name} is a person, not an agent (persons.{name}.agent is not true)")
            aci = next((a for a in person.signal if is_account(a)), None)
            if not aci:
                raise PostError(f"persons.{name} has no Signal account id (ACI) to mention")
            mentions.append(aci)
            continue
        consumer = next((n for n in serving if want in {
            n.lower(), (cfg.consumers[n].label or "").lower(), *(w.lower() for w in cfg.consumers[n].names)}), None)
        if consumer is None:
            raise PostError(f"{name} is neither an agent under persons nor a consumer serving {label}")
        words = cfg.consumers[consumer].names
        if not words:
            raise PostError(f"consumer {consumer} has no name keyword (consumers.{consumer}.names)")
        keywords.append(words[0])
        if sig is not None and sig.groups not in ("names", "all") and sig.account not in mentions:
            mentions.append(sig.account)                 # groups: mention -- the name alone does not address it
    return conv.key, mentions, keywords


def check_title(title: str) -> str:
    """One short line of plain text that cannot approve or deny anything."""
    title = title.strip()
    if len(title) > MAX_TITLE:
        raise PostError(f"title is {len(title)} characters; at most {MAX_TITLE}")
    if any(unicodedata.category(ch)[0] == "C" for ch in title):
        raise PostError("title must be one line of plain text (no control or format characters)")
    if FORBIDDEN.search(title):
        raise PostError("title must not say approve or deny: agents read this post as the person's own words")
    return title


def closed_check(cli: Cli, account: str, group_id: str) -> None:
    """Refuse a group the linked account is not in, or one that is open: a link, or members
    other than admins able to add members. The same test the gateway applies to admit: members."""
    out = cli.run("listGroups", "-d", "-g", group_id, account=account, json_out=True, timeout=60)
    groups = json.loads(out or "[]") if out.strip() else []
    g = next((x for x in groups if x.get("id") == group_id), None)
    if g is None or not g.get("isMember", True):
        raise PostError("the linked account is not a member of that group")
    reasons = []
    if g.get("groupInviteLink"):
        reasons.append("the group link is on")
    if g.get("permissionAddMember") != "ONLY_ADMINS":
        reasons.append("members other than admins can add members")
    if reasons:
        raise PostError("refused, the group is not closed: " + "; ".join(reasons))


def compose(mentions: list[str], keywords: list[str], title: str) -> tuple[str, list[str]]:
    """The text and its signal-cli --mention ranges: each mention a U+FFFC at the start, then the
    name keywords, then the title. Offsets are UTF-16 code units; U+FFFC and a space are one each."""
    head = [MARK] * len(mentions) + keywords
    text = " ".join(head + ([title] if title else []))
    ranges = [f"{2 * i}:1:{m}" for i, m in enumerate(mentions)]
    return text, ranges


def cmd_send(cfg: config.Config, args) -> dict:
    files = [Path(f).expanduser() for f in args.files]
    missing = [str(f) for f in files if not f.is_file()]
    if missing:
        raise PostError(f"not a file: {', '.join(missing)}")
    if not files:
        raise PostError("nothing to review: give at least one file")
    group_id, mentions, keywords = resolve(cfg, args.group, args.to)
    text, ranges = compose(mentions, keywords, check_title(args.title or ""))
    cli = _cli(cfg, args)
    account = cli.account()
    cli.run("receive", "--timeout", "5", "--ignore-attachments", "--ignore-stories",
            account=account, quiet=True, timeout=180)
    closed_check(cli, account, group_id)
    send = ["send", "-g", group_id, "-m", text, "--attachment", *(str(f) for f in files)]
    if ranges:
        send += ["--mention", *ranges]
    out = cli.run(*send, account=account, json_out=True, timeout=300)
    ts = None
    try:
        ts = json.loads(out).get("timestamp")
    except (ValueError, AttributeError):
        pass
    return {"group": args.group, "to": args.to, "files": [f.name for f in files], "timestamp": ts}


def cmd_link(cfg: config.Config, args) -> dict:
    """Link a new signal-cli device to a person's Signal account: print the link as a QR code
    (Signal > Settings > Linked devices > +), wait until it is scanned, then fetch groups."""
    cli = _cli(cfg, args)
    cli.data_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(cli.data_dir, 0o700)                       # holds the person's account keys
    p = subprocess.Popen([cli.binary, "--config", str(cli.data_dir), "link", "-n", args.name],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    uri = (p.stdout.readline() if p.stdout else "").strip()
    if not uri.startswith("sgnl://"):
        p.kill()
        raise PostError(f"signal-cli link did not print a link: {uri or (p.stderr.read() if p.stderr else '')}")
    if shutil.which("qrencode"):
        subprocess.run(["qrencode", "-t", "ANSIUTF8", uri], check=False)
    else:
        print(uri)
    print("Scan in Signal: Settings > Linked devices > Link new device. Waiting...", file=sys.stderr)
    if p.wait() != 0:
        raise PostError(f"linking failed: {(p.stderr.read() if p.stderr else '').strip()[-500:]}")
    account = cli.account()
    cli.run("receive", "--timeout", "15", "--ignore-attachments", "--ignore-stories",
            account=account, quiet=True, timeout=300)
    out = cli.run("listGroups", account=account, json_out=True)
    have = {g.get("id") for g in json.loads(out or "[]")} if out.strip() else set()
    member_of = sorted(k for k in cfg.post.groups if cfg.conversations[k].key in have)
    return {"linked": True, "post_groups_member_of": member_of}


def main() -> None:
    ap = argparse.ArgumentParser(prog="voitta-post",
                                 description="Post files into a Signal group as a person, addressed to agents.")
    ap.add_argument("--config", "-c", default=os.environ.get("VOITTA_GATEWAY_CONFIG", "gateway.yaml"),
                    help="gateway.yaml: groups (post.groups), agents and consumers resolve from it")
    ap.add_argument("--signal-dir", default=os.environ.get("VOITTA_POST_SIGNAL_DIR"),
                    help="the linked device's signal-cli data directory (default: operator-signal "
                         "next to the config)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    lk = sub.add_parser("link", help="link this machine to your Signal account (once)")
    lk.add_argument("--name", default="voitta-post", help="device name shown in Signal")
    sd = sub.add_parser("send", help="post files to an allowed group, addressed to agents")
    sd.add_argument("--group", "-g", required=True, help="a label under post.groups in gateway.yaml")
    sd.add_argument("--to", "-t", action="append", required=True,
                    help="an agent: a persons entry with agent: true, or a consumer serving the group")
    sd.add_argument("--title", default="", help=f"optional one-line title, at most {MAX_TITLE} characters")
    sd.add_argument("files", nargs="+")
    args = ap.parse_args()
    if not args.signal_dir:
        args.signal_dir = str(Path(args.config).expanduser().resolve().parent / "operator-signal")
    try:
        cfg = config.load(Path(args.config).expanduser())
        out = cmd_link(cfg, args) if args.cmd == "link" else cmd_send(cfg, args)
    except (PostError, subprocess.TimeoutExpired) as e:
        raise SystemExit(f"voitta-post: {e}")
    print(json.dumps(out))


if __name__ == "__main__":
    main()
