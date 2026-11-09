"""Generate the gateway architecture diagrams.

    python docs/diagrams.py            # writes docs/out/*.svg (+ *.png if cairosvg)

Each function draws one figure; build_docx.py imports FIGURES to embed them.
Status badges (svgkit.STATUS): LIVE = tested end to end on real Signal calls;
TESTED = tested end to end against simulated platforms; PARTIAL; PLANNED.
"""
from __future__ import annotations

from pathlib import Path

from svgkit import MUTED, PALETTE, SVG, Sequence

OUT = Path(__file__).parent / "out"
GREEN = "#047857"


def corner_status(s: SVG, status: str, note: str) -> None:
    """Status of a whole figure, top right."""
    s.badge(s.w - 20, 14, status)
    s.text(s.w - 92, 18, note, size=11.5, anchor="end", color=MUTED, italic=True)


# ----------------------------------------------------------------------------
# 1. Overall architecture, as built
# ----------------------------------------------------------------------------
def architecture() -> SVG:
    s = SVG(1300, 870, "voitta-gateway — architecture and build status")

    # Config band
    s.group(40, 20, 1220, 82, "Config & secrets", "config", sub="per deployment")
    s.box(230, 36, 320, 54, "gateway.yaml", ["adapters · accounts · routes · policy · consumers"],
          kind="config", title_size=13, status="live")
    s.box(570, 36, 320, 54, "secrets (.env)", ["tokens · API keys · media secrets · signal-cli data"],
          kind="config", title_size=13, status="live")
    s.box(910, 36, 330, 54, "identity map", ["person_id -> Signal ACI · Slack user · E.164 (lookup)"],
          kind="config", title_size=13, status="tested")

    # Gateway container
    s.text(240, 120, "voitta-gateway  (one Python process on a Linux server · SQLite)",
           size=14, weight="bold", anchor="start")
    s.rect(230, 128, 870, 500, "host", rx=14, sw=2)

    # Platforms
    s.text(115, 200, "Platforms", size=13, weight="bold", color=MUTED)
    s.box(40, 212, 150, 56, "Signal", ["text · calls"], kind="platform")
    s.box(40, 292, 150, 56, "Slack", ["text · clips · calls*"], kind="platform")
    s.box(40, 372, 150, 56, "WhatsApp · SMS", ["Telegram ..."], kind="later", dashed=True)
    s.text(1200, 200, "Platforms", size=13, weight="bold", color=MUTED)
    s.box(1140, 212, 120, 68, "Phone", ["PSTN"], kind="platform")
    s.box(1140, 300, 120, 68, "WhatsApp", ["calling"], kind="later", dashed=True)

    # Messaging layer
    s.group(250, 160, 380, 282, "Messaging layer", "msg", sub="text · media · reactions · async")
    s.box(270, 212, 170, 56, "Signal adapter", ["signal-cli JSON-RPC (TCP)"],
          kind="msg", title_size=13, status="tested")
    s.box(270, 292, 170, 56, "Slack adapter", ["Socket Mode · watchdog"], kind="msg", title_size=13,
          status="tested")
    s.box(270, 372, 170, 56, "more adapters", ["WhatsApp · SMS · ..."], kind="later", dashed=True,
          title_size=13, status="planned")
    s.box(466, 290, 152, 138, "Message",
          ["conversation_id", "sender · person_id", "parts[] · attachments",
           "reply_to · edit · delete", "status: queued->read"], kind="msg", title_size=13,
          line_size=10.5, status="tested")
    s.arrow([(190, 240), (270, 240)], both=True)
    s.arrow([(190, 320), (270, 320)], both=True)
    s.arrow([(190, 400), (270, 400)], both=True, dashed=True, color="#a1a1aa")
    s.arrow([(440, 254), (453, 254), (453, 305), (466, 305)])
    s.arrow([(440, 330), (466, 330)])
    s.arrow([(440, 400), (466, 400)], dashed=True, color="#a1a1aa")

    # Voice layer
    s.group(660, 160, 420, 282, "Voice layer", "voice", sub="real-time audio")
    s.box(676, 212, 124, 68, "Signal calls", ["signal-cli + tunnel", "Pulse 48 kHz"],
          kind="voice", title_size=12.5, line_size=10.5, status="live")
    s.box(808, 212, 124, 68, "Slack calls", ["DM card -> Join page", "browser WS pcm16"],
          kind="voice", title_size=12.5, line_size=10.5, status="tested")
    s.box(940, 212, 124, 68, "Phone", ["Telnyx Call Control", "media stream (L16)"],
          kind="voice", title_size=12.5, line_size=10.5, status="tested")
    s.box(680, 318, 380, 110, "CallSession",
          ["built: initiated -> ringing -> answered -> ended (reason)",
           "built: agent connect first · grace period · fallback",
           "planned: legs in a bridge (warm transfer, conference)",
           "planned: answering-machine detection · recording"],
          kind="voice", title_size=13, line_size=11, status="partial")
    for x in (738, 870, 1002):
        s.arrow([(x, 280), (x, 318)], both=True)
    s.arrow([(440, 240), (676, 240)], both=True, label="callEvent · startCall",
            label_at=(558, 234))
    s.arrow([(60, 212), (60, 146), (646, 146), (646, 262), (676, 262)], both=True, color=GREEN,
            label="Signal call audio · RingRTC / SRTP", label_at=(330, 150), label_color=GREEN)
    s.arrow([(40, 320), (28, 320), (28, 134), (848, 134), (848, 212)], both=True, color=GREEN,
            label="Slack call audio · callee's browser · WebSocket", label_at=(560, 138), label_color=GREEN)
    s.arrow([(1140, 246), (1064, 246)], both=True, color=GREEN)
    s.text(1102, 236, "PSTN", size=10.5, color=GREEN, bg=True)
    s.arrow([(1140, 334), (1060, 290)], dashed=True, color="#a1a1aa")

    # Gateway core: three parts with different status
    s.arrow([(440, 442), (440, 456)])
    s.arrow([(870, 442), (870, 456)])
    for x, label, st in ((250, "event log · SQLite (WAL) · replay", "live"),
                         (530, "policy: allowlist · opt-out · 911 block", "tested"),
                         (810, "outbox · files · clip transcripts", "tested")):
        s.rect(x, 456, 270, 34, "core")
        s.text(x + 135, 478, label, size=11.5, color="#be185d", weight="bold")
        s.badge(x + 262, 456, st)

    # API band
    s.group(250, 502, 830, 120, "Gateway API", "api")
    s.box(262, 536, 262, 78, "Commands · REST",
          ["calls: start / answer / hangup (live)", "messages · blocks · history (tested)",
           "spec/gateway.openapi.yaml"], kind="api", title_size=12.5, line_size=10.5, status="live")
    s.box(534, 536, 262, 78, "Events · stream",
          ["CloudEvents · 20 types · per-conv seq", "SSE + long-poll · cursor replay",
           "spec/voitta.asyncapi.yaml"], kind="api", title_size=12.5, line_size=10.5, status="live")
    s.box(806, 536, 262, 78, "Media · WS per call",
          ["voitta-media/1 · gateway connects", "start · media · mark · clear · stop",
           "spec/voitta.asyncapi.yaml"], kind="api", title_size=12.5, line_size=10.5, status="live")

    # Consumers
    s.group(250, 672, 830, 124, "Rest of the system", "consumer")
    s.box(262, 708, 192, 76, "Agent core (text)", ["manhattan-logic bot", "port: phase 3"],
          kind="later", dashed=True, title_size=12.5, status="planned")
    s.box(464, 708, 192, 76, "voitta-ask + speech", ["text -> call -> transcript",
          "speech API: Piper · Whisper GPU"], kind="consumer", title_size=12.5, line_size=10.5, status="live")
    s.box(666, 708, 192, 76, "Pipecat bot", ["our serializer · echo mode", "STT -> LLM -> TTS (keys)"],
          kind="consumer", title_size=12.5, status="tested")
    s.box(868, 708, 192, 76, "Other services", ["CRM · schedulers", "dashboards"],
          kind="later", dashed=True, title_size=12.5, status="planned")
    s.arrow([(393, 672), (393, 614)], label="HTTP + token", label_at=(393, 648))
    s.arrow([(665, 614), (665, 672)], label="SSE / long-poll", label_at=(665, 648))
    s.arrow([(937, 614), (937, 672)], both=True, label="WS (gateway connects)", label_at=(937, 648))

    s.arrow([(1000, 102), (1000, 128)], dashed=True, color="#52525b")
    s.text(1260, 852, "* Slack bots cannot join huddles: a Slack call is our own page, opened from a DM",
           size=11, anchor="end", color=MUTED, italic=True)
    s.legend(40, 836)
    return s


# ----------------------------------------------------------------------------
# 2. Inbound message -> reply
# ----------------------------------------------------------------------------
def seq_message() -> SVG:
    s = SVG(1200, 560, "Inbound message and reply")
    corner_status(s, "tested", "simulated signal-cli; not yet live")
    q = Sequence(s, [
        ("u", "Person\nSignal app", "platform"),
        ("a", "Signal adapter\nsignal-cli daemon", "msg"),
        ("g", "Gateway core + API\nevent log · identity", "core"),
        ("c", "Agent core\nconsumer", "consumer"),
    ], top=40)
    q.msg("u", "a", "message (E2EE, decrypted by signal-cli)")
    q.msg("a", "g", "JSON-RPC notification -> normalize -> Message")
    q.note("g", "g", "persist first · dedupe · policy (allowlist / opt-out) · person_id lookup", pad=150)
    q.msg("g", "c", "event message.received {id: evt_42, conversation, message_id, parts}", dashed=True)
    q.msg("c", "g", "POST /v1/messages {conversation, reply_to, parts, idempotency_key: \"reply:evt_42\"}")
    q.note("g", "g", "same conversation it came from · durable outbox · rate limits", pad=150)
    q.msg("g", "a", "send (JSON-RPC)")
    q.msg("a", "u", "message")
    q.msg("a", "g", "send result -> sent · delivery / read receipts", dashed=True)
    q.msg("g", "c", "event message.status {sent -> delivered -> read}", dashed=True)
    q.heads(q.y)
    s.h = int(q.y + 24)
    return s


# ----------------------------------------------------------------------------
# 3. Inbound call
# ----------------------------------------------------------------------------
def seq_call() -> SVG:
    s = SVG(1200, 960, "Inbound call")
    corner_status(s, "tested", "simulated signal-cli and Telnyx; live calls so far were outbound")
    q = Sequence(s, [
        ("u", "Caller\nphone / Signal app", "platform"),
        ("v", "Voice adapter\nTelnyx stream · signal-cli + tunnel", "voice"),
        ("g", "Gateway core + API", "core"),
        ("p", "Voice agent (WS server)\nvoitta-ask · Pipecat", "consumer"),
    ], top=40)
    q.msg("u", "v", "call offer (Telnyx webhook / Signal callEvent RINGING_INCOMING)")
    q.msg("v", "g", "incoming call -> CallSession (state: ringing)")
    q.note("g", "g", "policy check · person_id lookup · route -> agent media URL", pad=140)
    q.msg("g", "p", "event call.ringing {call_id, from, route}  (for observers)", dashed=True)
    q.msg("g", "p", "open WS to agent media URL (signed) · connected {call_id}")
    q.msg("p", "g", "WS accepted = agent ready")
    q.note("g", "g", "agent not connected within N s -> fallback (reject / hang up / transfer)",
           kind="core", pad=140)
    q.msg("g", "v", "answer (Telnyx answer + stream · signal-cli acceptCall)")
    q.msg("v", "g", "answered / CONNECTED -> call.answered", dashed=True)
    q.msg("g", "p", "start {call_id, media_format}  (agent may speak only after this)")
    q.msg("p", "g", "media: disclosure \"You are talking to an AI assistant ...\"", color=GREEN)
    q.msg("g", "v", "resample once -> native format", color=GREEN)
    q.msg("v", "u", "audio", color=GREEN)
    q.msg("u", "v", "speech", color=GREEN)
    q.msg("v", "g", "native audio (8 kHz PCMU / L16 16 kHz / 48 kHz Pulse)", color=GREEN)
    q.msg("g", "p", "media frames in the format announced in start", color=GREEN)
    q.msg("p", "g", "clear (barge-in) · mark -> echoed when played or cleared")
    q.note("g", "p", "agent WS drops -> grace period -> hang up or fallback", kind="core", pad=95)
    q.msg("u", "v", "hangup")
    q.msg("v", "g", "ended (reason)")
    q.msg("g", "p", "stop {reason} · close WS")
    q.msg("g", "p", "event call.ended {duration, reason}", dashed=True)
    q.heads(q.y)
    s.h = int(q.y + 24)
    return s


# ----------------------------------------------------------------------------
# 4. voitta-ask: text in, spoken, answer transcribed (measured on the live call)
# ----------------------------------------------------------------------------
def seq_ask() -> SVG:
    s = SVG(1240, 1100, "voitta-ask flow")
    corner_status(s, "live", "real Signal calls on .212; times from the call to Nadya")
    q = Sequence(s, [
        ("cl", "Client\nany service · examples/ask.py", "consumer"),
        ("ask", "voitta-ask\nPiper · Whisper (GPU)", "consumer"),
        ("gw", "voitta-gateway", "core"),
        ("sig", "signal-cli + tunnel\nRingRTC · Pulse 48 kHz", "voice"),
        ("u", "Callee\nSignal app", "platform"),
    ], top=40, head_w=196)
    q.msg("cl", "ask", "POST /v1/asks {to, text}  -> 202 {ask_id, state: calling}")
    q.note("ask", "ask", "synthesize the text before dialing (Piper, ~0.15 s)", kind="consumer", pad=150)
    q.msg("ask", "gw", "POST /v1/calls {to, via: signal, route: ask}")
    q.msg("gw", "ask", "WS connect (signed) · connected", color=GREEN)
    q.msg("gw", "sig", "startCall")
    q.msg("sig", "u", "ring  (t = 0.5 s)")
    q.msg("gw", "ask", "event call.ringing (long-poll; a watchdog also checks GET /v1/calls)", dashed=True)
    q.msg("u", "sig", "picks up  (t = 8.5 s)")
    q.msg("sig", "gw", "callEvent CONNECTED -> open Pulse devices")
    q.msg("gw", "ask", "start {pcm16le, 16 kHz, 20 ms}")
    q.msg("ask", "gw", "media: spoken text + beep · mark \"prompt\"", color=GREEN)
    q.msg("gw", "u", "audio  (16 -> 48 kHz, played in real time)", color=GREEN)
    q.msg("gw", "ask", "mark \"prompt\" played -> listening  (t = 19.3 s)")
    q.msg("u", "gw", "answer", color=GREEN)
    q.msg("gw", "ask", "media frames  (48 -> 16 kHz, 20 ms)", color=GREEN)
    q.note("ask", "ask", "1.5 s of silence after speech -> answer over", kind="consumer", pad=150)
    q.msg("ask", "gw", "media \"Thank you. Goodbye.\" · mark \"bye\"", color=GREEN)
    q.note("ask", "ask", "transcribe meanwhile: Whisper small on the RTX 3090, 0.3 s", kind="consumer", pad=150)
    q.msg("gw", "ask", "mark \"bye\" played")
    q.msg("ask", "gw", "hangup")
    q.msg("gw", "sig", "hangupCall")
    q.msg("gw", "ask", "stop · event call.ended", dashed=True)
    q.msg("cl", "ask", "GET /v1/asks/{id}?wait=60")
    q.msg("ask", "cl", "done · transcript  (t = 27.5 s)")
    q.heads(q.y)
    s.h = int(q.y + 24)
    return s


# ----------------------------------------------------------------------------
# 4b. Outbound Slack call through the browser join page
# ----------------------------------------------------------------------------
def seq_slack_call() -> SVG:
    s = SVG(1240, 900, "Slack call")
    corner_status(s, "tested", "simulated Slack; join page in headless Chrome; not yet live")
    q = Sequence(s, [
        ("ag", "Agent\nvoitta-ask · Pipecat", "consumer"),
        ("gw", "voitta-gateway\nAPI · Slack adapter · ingress", "core"),
        ("sl", "Slack\nWeb API", "platform"),
        ("br", "Callee's browser\njoin page", "platform"),
    ], top=40, head_w=210)
    q.msg("ag", "gw", "POST /v1/calls {to: U..., via: slack, route}")
    q.msg("gw", "ag", "WS connect (signed) · connected", color=GREEN)
    q.msg("gw", "sl", "conversations.open · chat.postMessage (card with Join button)")
    q.msg("sl", "br", "DM notification · click Join -> opens the page")
    q.msg("gw", "ag", "event call.ringing  (ring timeout 60 s -> missed)", dashed=True)
    q.msg("br", "gw", "GET /slack/call/{id}?t=<per-call token>  (HTML)")
    q.note("sl", "br", "page: click Join -> microphone permission · AudioWorklet capture + player", kind="neutral", pad=60)
    q.msg("br", "gw", "WS /slack/call/{id}/audio · hello {rate: 48000}")
    q.msg("gw", "sl", "chat.update: card -> \"On a call\"", dashed=True)
    q.msg("gw", "ag", "start {pcm16le, 16 kHz}  · event call.answered")
    q.msg("br", "gw", "mic: pcm16 48 kHz, binary, 20 ms", color=GREEN)
    q.msg("gw", "ag", "media frames 16 kHz", color=GREEN)
    q.msg("ag", "gw", "media · mark · clear", color=GREEN)
    q.msg("gw", "br", "pcm16 48 kHz (paced) · {event: clear}", color=GREEN)
    q.note("gw", "br", "page reload: same call resumes (10 s grace) · tab closed: remote_hangup", kind="core", pad=95)
    q.msg("ag", "gw", "hangup")
    q.msg("gw", "br", "{event: ended} · close")
    q.msg("gw", "sl", "chat.update: card -> \"Call ended · 0:27\"", dashed=True)
    q.msg("gw", "ag", "stop · event call.ended", dashed=True)
    q.heads(q.y)
    s.h = int(q.y + 24)
    return s


# ----------------------------------------------------------------------------
# 5. The POC as it runs on .212
# ----------------------------------------------------------------------------
def deployment_poc() -> SVG:
    s = SVG(1200, 720, "POC deployment on .212")
    s.text(40, 34, "saturn (192.168.88.212) · Ubuntu 22.04 · RTX 3090 24 GB · 16 cores · 62 GB",
           size=15, weight="bold", anchor="start")
    s.rect(30, 46, 830, 640, "host", rx=14, sw=2)

    s.box(60, 80, 360, 100, "manhattan-logic (existing)", ["Claude bot on the 212 number",
          "signal-cli 0.14.9 native · --http :18080", "stopped while the gateway owns the account"],
          kind="neutral", title_size=13)
    s.box(460, 80, 370, 100, "voitta-ask", ["screen voitta-ask · 127.0.0.1:8791",
          "Piper en_US-lessac-medium (CPU)", "faster-whisper small (GPU, int8_float16)"],
          kind="consumer", title_size=13, status="live")
    s.box(60, 230, 360, 110, "voitta-gateway", ["screen voitta-test · venv · 127.0.0.1:8700",
          "SQLite event log · route \"ask\"", "spawns signal-cli; dies with it (PDEATHSIG)"],
          kind="core", title_size=13, status="live")
    s.box(460, 230, 370, 110, "signal-cli 0.14.9 (JVM build)", ["bundled JRE 25 · --tcp :7583",
          "receive-mode manual · call events", "native 0.14.9 cannot place calls (#2144)"],
          kind="msg", title_size=13, status="live")
    s.box(60, 390, 360, 100, "PulseAudio (user session)", ["per-call null sinks:",
          "signal_input_<id> · signal_output_<id>", "gateway: parec / pacat at 48 kHz"],
          kind="voice", title_size=13, status="live")
    s.box(460, 390, 370, 100, "signal-call-tunnel", ["RingRTC 2.65.1: has CVE-2026-113380",
          "(its patches do not apply to >= 2.72)", "not on PATH: only the gateway uses it"],
          kind="voice", title_size=13, status="partial")
    s.box(60, 540, 770, 70, "~/.local/share/signal-cli · account +1 212 707 1132 (primary device)",
          ["one daemon at a time: manhattan-logic's OR the gateway's (switch: stop one, start the other)"],
          kind="config", title_size=13)

    s.arrow([(600, 180), (300, 230)], both=True, label="API + media WS", label_at=(450, 200))
    s.arrow([(420, 285), (460, 285)], both=True)
    s.text(440, 274, "spawns", size=10.5, color=MUTED, bg=True)
    s.arrow([(645, 340), (645, 390)], label="per call", label_at=(690, 368))
    s.arrow([(460, 440), (420, 440)], both=True, color=GREEN)
    s.arrow([(240, 340), (240, 390)], both=True, color=GREEN, label="parec / pacat", label_at=(300, 368),
            label_color=GREEN)
    s.line([(60, 130), (44, 130), (44, 575), (60, 575)], dashed=True, color="#a1a1aa")
    s.arrow([(830, 300), (846, 300), (846, 575), (830, 575)], dashed=True, color="#a1a1aa")
    s.text(48, 520, "uses", size=10.5, color=MUTED, anchor="start", bg=True)
    s.text(842, 520, "uses", size=10.5, color=MUTED, anchor="end", bg=True)
    s.text(445, 650, "switch: stop manhattan-logic -> start gateway -> call -> stop gateway -> ./start.sh  (~1 min)",
           size=12, color=MUTED, italic=True)

    s.text(1025, 74, "Outside", size=14, weight="bold", color=MUTED)
    s.box(900, 90, 250, 80, "Client", ["ssh + curl or examples/ask.py"], kind="platform", title_size=13)
    s.box(900, 240, 250, 80, "Signal servers", ["messages · call signalling · TURN"], kind="platform",
          title_size=13)
    s.box(900, 400, 250, 80, "Callees", ["Roman · Nadya (Signal app)"], kind="platform", title_size=13)
    s.arrow([(900, 130), (830, 130)], label="POST /v1/asks", label_at=(865, 120))
    s.arrow([(830, 270), (900, 270)], both=True)
    s.arrow([(830, 440), (870, 440), (870, 300), (900, 300)], both=True, color=GREEN,
            label="SRTP audio", label_at=(870, 380), label_color=GREEN)
    s.arrow([(1025, 320), (1025, 400)], both=True)
    s.legend(40, 704, ("live", "partial"))
    return s


# ----------------------------------------------------------------------------
# 6. Target deployment (designed)
# ----------------------------------------------------------------------------
def deployment() -> SVG:
    s = SVG(1200, 640, "Target deployment on one Linux server")
    s.text(40, 34, "Linux server · Docker Compose (image written, not built yet)", size=15,
           weight="bold", anchor="start")
    s.badge(640, 29, "planned")
    s.rect(30, 46, 820, 568, "host", rx=14, sw=2)

    s.box(60, 80, 500, 100, "voitta-gateway", ["Python · FastAPI + asyncio",
          "REST · events SSE / long-poll · media WS client to agents",
          "Slack + phone adapters in-process · Pulse audio bridge"], kind="core", status="live")
    s.box(600, 80, 220, 100, "SQLite (WAL) · files", ["event log · outbox", "id maps · consent",
          "attachments"], kind="neutral", title_size=13, status="live")
    s.box(60, 220, 360, 80, "signal-cli daemon", ["JSON-RPC · receive-mode=manual",
          "subscribeReceive + subscribeCallEvents"], kind="msg", status="live")
    s.box(60, 340, 360, 90, "signal-call-tunnel  x N", ["Rust / RingRTC · 1 per call",
          "per-call Pulse devices (in the container)", "needs a RingRTC >= 2.72 port of its patches"],
          kind="voice", status="partial")
    s.box(460, 220, 360, 90, "public ingress (Caddy, TLS)", ["Telnyx webhooks + media WSS",
          "ed25519 signature check", "nothing else exposed"], kind="api", status="tested")
    s.box(460, 340, 360, 90, "later: own SIP server", ["LiveKit SIP · FreeSWITCH · Asterisk",
          "multi-carrier · conferences · recording"], kind="later", dashed=True, status="planned")
    s.box(60, 470, 760, 100, "voice agents · agent core (any host)",
          ["voitta-ask (GPU host) · Pipecat bots · manhattan-logic as a consumer",
           "events + commands via the API (bearer token); media: gateway connects in (signed)"],
          kind="consumer")

    s.arrow([(240, 220), (240, 180)], both=True, label="JSON-RPC", label_at=(290, 204))
    s.arrow([(240, 300), (240, 340)], label="spawns", label_at=(290, 324))
    s.arrow([(420, 385), (434, 385), (434, 180)], both=True, color=GREEN,
            label="Pulse 48 kHz", label_at=(372, 452), label_color=GREEN)
    s.arrow([(450, 470), (450, 180)], both=True, label="API · media WS", label_at=(520, 452))
    s.arrow([(560, 130), (600, 130)], both=True)
    s.arrow([(520, 220), (520, 180)], both=True)
    s.arrow([(640, 340), (640, 310)], dashed=True, color="#a1a1aa")

    s.text(1025, 74, "External", size=14, weight="bold", color=MUTED)
    for y, t, sub in ((90, "Signal servers", "chat + call signalling, TURN"),
                      (190, "Slack", "Socket Mode wss · Web API"),
                      (290, "Telnyx", "Call Control · media streaming"),
                      (390, "STT / LLM / TTS", "local GPU or hosted APIs")):
        s.box(900, y, 250, 64, t, [sub], kind="platform", title_size=13)
    s.arrow([(60, 260), (44, 260), (44, 56), (880, 56), (880, 122), (900, 122)],
            both=True, color="#1d4ed8", label="Signal: chat (signal-cli) + call media (tunnel)",
            label_at=(320, 60), label_color="#1d4ed8")
    s.line([(60, 385), (44, 385), (44, 260)], color="#1d4ed8")
    s.arrow([(500, 80), (500, 66), (866, 66), (866, 222), (900, 222)], both=True,
            label="Slack: outbound wss", label_at=(690, 70))
    s.arrow([(820, 300), (900, 315)], both=True, color=GREEN)
    s.arrow([(820, 515), (900, 422)], both=True)
    s.legend(40, 628)
    return s


# ----------------------------------------------------------------------------
# 7. Roadmap with progress
# ----------------------------------------------------------------------------
def roadmap() -> SVG:
    s = SVG(1200, 380, "Roadmap")
    phases = [
        ("0 · Signal call spike", "voice", "live", [
            "tunnel built (RingRTC 2.65.1)", "outbound calls + echo: done",
            "voitta-ask on real calls: done", "inbound: out of scope"]),
        ("1 · Phone baseline", "voice", "tested", [
            "Telnyx adapter built", "signed webhooks + L16 media", "needs a Telnyx account",
            "latency vs Signal: open"]),
        ("2 · Freeze the API", "api", "partial", [
            "OpenAPI + AsyncAPI v1: done", "checked in every test: done",
            "calls: Signal live, others tested", "manhattan-logic: not yet"]),
        ("3 · Slack", "msg", "tested", [
            "messaging hardened (shmobster)", "voice clips -> transcripts",
            "calls via browser join page", "live: needs the Slack app"]),
        ("4 · More channels", "later", "planned", [
            "WhatsApp (msg + calling)", "SMS via trunk", "Telegram / Discord",
            "warm transfer · conference"]),
    ]
    x, w, gap = 30, 214, 13
    for title, kind, status, items in phases:
        f, st = PALETTE[kind]
        pts = [(x, 40), (x + w - 18, 40), (x + w, 75), (x + w - 18, 110), (x, 110), (x + 18, 75)]
        d = " ".join(f"{a},{b}" for a, b in pts)
        s.items.append(f'<polygon points="{d}" fill="{f}" stroke="{st}" stroke-width="1.6"/>')
        s.text(x + w / 2 + 4, 80, title, size=13.5, weight="bold", color=st)
        s.badge(x + w / 2 + 34, 132, status)
        for i, it in enumerate(items):
            s.text(x + 22, 168 + i * 24, "• " + it, size=12, anchor="start", color="#27272a")
        x += w + gap
    s.text(600, 296, "Voice first: the CallSession interface comes from working calls, "
           "not from a design up front.", size=12.5, color=MUTED, italic=True)
    s.legend(20, 340)
    return s


FIGURES = {
    "architecture": (architecture, "Architecture, with what is built and how it was tested"),
    "seq-message": (seq_message, "Inbound message and reply"),
    "seq-call": (seq_call, "Inbound call (same flow for phone and Signal)"),
    "seq-ask": (seq_ask, "voitta-ask: text spoken on a call, answer transcribed (live)"),
    "seq-slack-call": (seq_slack_call, "Outbound Slack call through the browser join page"),
    "deployment-poc": (deployment_poc, "The POC as it runs on .212"),
    "deployment": (deployment, "Target deployment on one Linux server"),
    "roadmap": (roadmap, "Roadmap and progress"),
}


def build(out: Path = OUT) -> dict[str, Path]:
    return {name: fn().save(out, name) for name, (fn, _) in FIGURES.items()}


if __name__ == "__main__":
    for name, p in build().items():
        print(p)
