"""Build docs/out/voitta-gateway.docx (approach, architecture, POC status).

    python docs/build_docx.py

Regenerates the diagrams first (docs/diagrams.py), then embeds the PNG copies.
Requires: python-docx, cairosvg.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Pt, RGBColor

import diagrams
from svgkit import STATUS

OUT = Path(__file__).parent / "out"
DOCX = OUT / "voitta-gateway.docx"
ACCENT = RGBColor(0x1D, 0x4E, 0xD8)


# ---------------------------------------------------------------- helpers
def shade(cell, hex_fill: str):
    tc = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), hex_fill)
    tc.append(shd)


def para(doc, text, style=None, bold_prefix=None, size=None, italic=False):
    p = doc.add_paragraph(style=style)
    if bold_prefix:
        p.add_run(bold_prefix).bold = True
    r = p.add_run(text)
    r.italic = italic
    if size:
        r.font.size = Pt(size)
    return p


def bullets(doc, items, style="List Bullet"):
    for it in items:
        if isinstance(it, tuple):
            para(doc, it[1], style=style, bold_prefix=it[0])
        else:
            para(doc, it, style=style)


def status_line(doc, status: str, text: str):
    """A paragraph that starts with a coloured status tag, e.g. at the top of a section."""
    fill, label = STATUS[status][:2]
    p = doc.add_paragraph()
    r = p.add_run(f" {label} ")
    r.bold = True
    r.font.size = Pt(8.5)
    r.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
    rpr = r._element.get_or_add_rPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), fill.lstrip("#"))
    rpr.append(shd)
    t = p.add_run("  " + text)
    t.italic = True
    t.font.size = Pt(9.5)
    p.paragraph_format.keep_with_next = True
    return p


def code(doc, text):
    for line in text.strip("\n").split("\n"):
        p = doc.add_paragraph()
        p.paragraph_format.space_after = Pt(0)
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.left_indent = Cm(0.5)
        r = p.add_run(line if line else " ")
        r.font.name = "Courier New"
        r._element.rPr.rFonts.set(qn("w:eastAsia"), "Courier New")
        r.font.size = Pt(8.5)
    doc.add_paragraph().paragraph_format.space_after = Pt(2)


def table(doc, header, rows, widths=None, status_col: int | None = None):
    t = doc.add_table(rows=1, cols=len(header))
    t.style = "Table Grid"
    t.alignment = WD_TABLE_ALIGNMENT.CENTER
    for i, h in enumerate(header):
        c = t.rows[0].cells[i]
        c.text = ""
        r = c.paragraphs[0].add_run(h)
        r.bold = True
        r.font.size = Pt(9.5)
        shade(c, "E4E4E7")
    for row in rows:
        cells = t.add_row().cells
        for i, v in enumerate(row):
            cells[i].text = ""
            if status_col is not None and i == status_col and v in STATUS:
                fill, label = STATUS[v][:2]
                r = cells[i].paragraphs[0].add_run(label)
                r.bold = True
                r.font.size = Pt(8.5)
                r.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
                cells[i].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
                shade(cells[i], fill.lstrip("#"))
            else:
                r = cells[i].paragraphs[0].add_run(str(v))
                r.font.size = Pt(9.5)
    if widths:
        t.autofit = False
        layout = OxmlElement("w:tblLayout")
        layout.set(qn("w:type"), "fixed")
        t._tbl.tblPr.append(layout)
        for i, gc in enumerate(t._tbl.tblGrid.findall(qn("w:gridCol"))):
            gc.set(qn("w:w"), str(int(Cm(widths[i]).twips)))
        for row in t.rows:
            for i, w in enumerate(widths):
                row.cells[i].width = Cm(w)
    for i, row in enumerate(t.rows):
        trPr = row._tr.get_or_add_trPr()
        trPr.append(OxmlElement("w:cantSplit"))
        if i == 0:
            trPr.append(OxmlElement("w:tblHeader"))
    doc.add_paragraph()
    return t


def figure(doc, name, width_cm=17.0):
    caption = diagrams.FIGURES[name][1]
    doc.add_picture(str(OUT / f"{name}.png"), width=Cm(width_cm))
    doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run(f"Figure: {caption}  (vector: out/{name}.svg)")
    r.italic = True
    r.font.size = Pt(9)
    r.font.color.rgb = RGBColor(0x52, 0x52, 0x5B)


# ---------------------------------------------------------------- content
def build() -> Path:
    diagrams.build(OUT)

    doc = Document()
    sec = doc.sections[0]
    sec.left_margin = sec.right_margin = Cm(2.0)
    sec.top_margin = sec.bottom_margin = Cm(1.8)
    base = doc.styles["Normal"]
    base.font.name = "Arial"
    base.element.rPr.rFonts.set(qn("w:eastAsia"), "Arial")
    base.font.size = Pt(10.5)
    for lvl, size in ((1, 16), (2, 13), (3, 11.5)):
        st = doc.styles[f"Heading {lvl}"]
        st.font.size = Pt(size)
        st.font.color.rgb = ACCENT if lvl == 1 else RGBColor(0x18, 0x18, 0x1B)
        st.font.name = "Arial"
        for attr in ("w:asciiTheme", "w:hAnsiTheme", "w:eastAsiaTheme", "w:cstheme"):
            st.element.rPr.rFonts.attrib.pop(qn(attr), None)

    # ---- title
    t = doc.add_paragraph()
    r = t.add_run("voitta-gateway")
    r.bold = True
    r.font.size = Pt(24)
    r.font.color.rgb = ACCENT
    para(doc, "Messaging and voice connectivity layer: approach, architecture and POC status", size=13)
    para(doc, f"Version 2 · {dt.date.today():%d %B %Y} · working name; the repository is voitta-sip, "
              "but SIP is only one of the transports.", size=9, italic=True)

    # ---- 1
    doc.add_heading("1. Summary", 1)
    para(doc, "One module connects Voitta agents to the outside world: Signal, Slack, ordinary phone "
              "calls, and later WhatsApp, SMS or Telegram. It runs on a Linux server, holds all platform "
              "accounts and keys, and exposes one API to the rest of the system.")
    bullets(doc, [
        ("The proof of concept works on real calls. ", "On .212 the gateway placed Signal calls from the "
         "212 number; a first voice agent, voitta-ask, read a text to the callee and returned their spoken "
         "answer as text, end to end in 17 to 28 seconds."),
        ("Two layers, not one. ", "Messaging (text, media, reactions; asynchronous) and voice (real-time "
         "audio) have different shapes. They share config, identity and the API, but not the abstraction."),
        ("Three API channels, now specified formally. ", "REST commands (OpenAPI 3.1), one replayable "
         "event stream and a WebSocket per call for audio (AsyncAPI 3.1). Every test checks the traffic "
         "against these specs."),
        ("Thin gateway, separate agents. ", "The gateway translates platforms and enforces policy; agents "
         "such as voitta-ask decide what to say and talk to it only through the API."),
        ("Slack in both layers. ", "Messaging hardened with shmobster's lessons, voice clips arrive with "
         "transcripts, and outbound voice calls run through a browser join page (Slack bots cannot join huddles). "
         "Built and tested end to end against a simulated Slack and in headless Chrome; not yet on a live "
         "workspace (section 7)."),
        ("Scope for now: outbound calls on the 212 number. ", "Incoming calls and other numbers are out of "
         "scope. The bot (manhattan-logic) and the gateway cannot hold the account at the same time, so they "
         "switch by hand (section 9)."),
    ])

    # ---- 2 status
    doc.add_heading("2. Status (8 November 2026)", 1)
    para(doc, "Every component in this document carries one of four tags:")
    table(doc, ["Tag", "Meaning"], [
        ["live", "Built; tested end to end on real Signal calls on .212."],
        ["tested", "Built; tested end to end in the automated suite against simulated platforms "
                   "(a JSON-RPC server standing in for signal-cli, signed Telnyx webhooks and media, a stubbed "
                   "Slack client), over real sockets. Not yet run against the real service."],
        ["partial", "Built in part; what is missing is stated next to it."],
        ["planned", "Designed in this document, not built."],
    ], widths=[2.4, 14.6], status_col=0)

    doc.add_heading("2.1 Components", 2)
    table(doc, ["Component", "Status", "Evidence / what is missing"], [
        ["Signal calls: outbound, audio both ways", "live",
         "3 answered calls on .212 (echo, voitta-ask to Roman, voitta-ask to Nadya)"],
        ["Signal calls: inbound", "tested", "simulated only; no live inbound call yet"],
        ["Signal messaging (send, receive, receipts, reactions, edits, groups)", "tested",
         "simulated signal-cli; one message sent live via manhattan-logic's daemon"],
        ["Slack messaging: Socket Mode, watchdog, safe downloads, bots, @here, blocks, buttons, history",
         "tested", "simulated Slack Web API; needs the Voitta Slack app installed (manifest in deploy/)"],
        ["Slack voice clips -> transcripts (Slack's own, else voitta-speech)", "tested",
         "simulated Slack; real AAC/Opus clips transcribed on .212 by the real models"],
        ["Slack calls: DM card + browser join page", "tested",
         "page run in headless Chrome with a fake microphone; needs a public HTTPS URL for live use"],
        ["voitta-speech API (transcribe, synthesize)", "live", "running in voitta-ask on .212; 0.1-0.3 s per clip"],
        ["Phone via Telnyx (signed webhooks, L16 media, DTMF, dial-out)", "tested",
         "simulated Telnyx incl. big-endian audio; needs a Telnyx account"],
        ["Event log, SSE + long-poll, replay, cursor reset", "live", "SQLite (WAL); used by voitta-ask live"],
        ["Policy: allowlist + held queue, STOP opt-out, emergency block", "tested",
         "live: only the caller allowlist"],
        ["Durable outbox, pacing, retries, idempotency", "tested", "survives restarts (persisted first)"],
        ["Media protocol voitta-media/1", "live", "start, media, mark, clear, stop, hangup used on live calls"],
        ["CallSession", "partial", "single leg; no warm transfer, conference, recording or AMD"],
        ["voitta-ask (text -> call -> transcript)", "live",
         "Piper TTS (CPU) + faster-whisper small (GPU); over Slack calls: tested"],
        ["Pipecat serializer + example bot", "tested", "echo mode in a real Pipecat pipeline; LLM mode needs keys"],
        ["Formal specs (OpenAPI 3.1 x2, AsyncAPI 3.1)", "live",
         "validated by official tools; all test traffic checked against them"],
        ["Docker image + Compose + Caddy", "planned", "files written; image never built"],
        ["signal-call-tunnel with RingRTC >= 2.72", "partial",
         "built only at 2.65.1 (CVE-2026-113380); patches need porting"],
        ["manhattan-logic as a gateway consumer", "planned", "phase 3"],
    ], widths=[6.0, 2.0, 9.0], status_col=1)

    doc.add_heading("2.2 POC results", 2)
    table(doc, ["Call", "Rings", "Picked up", "Listening", "Answer ends", "Transcript ready", "Result"], [
        ["Roman", "0.3 s", "5.0 s", "9.9 s", "16.7 s", "17.0 s", "\"Hello, one, two, three, four, five.\""],
        ["Nadya", "0.5 s", "8.5 s", "19.3 s", "27.2 s", "27.5 s",
         "\"Search Google for Asian pairs prices.\" (pears: homophone)"],
    ], widths=[1.7, 1.5, 1.7, 1.9, 1.9, 2.1, 6.2])
    bullets(doc, [
        "Times are seconds from the client's request. Almost all of it is the call itself: picking up, "
        "hearing the text, answering, and 1.5 s of silence that marks the end of the answer.",
        "Machine time is small: Piper synthesizes a sentence in about 0.15 s; Whisper small transcribes "
        "an 8-second answer in about 0.3 s on the RTX 3090.",
        "Automated suite: 46 tests pass in about 40 s (unit, end to end, spec conformance, the join page in "
        "headless Chrome); four more run the real models on .212 and pass.",
    ])

    # ---- 3 usual approach
    doc.add_heading("3. How this is usually solved", 1)
    para(doc, "", bold_prefix="Messaging")
    bullets(doc, [
        "One adapter per platform, translating to a common message model, with per-platform "
        "capabilities declared (threads, edits, reactions, calls).",
        "Off-the-shelf options: Matrix + mautrix bridges, Matterbridge, Bot Framework, Rasa channels; "
        "hosted: Twilio Conversations, Infobip. For two platforms, running a Matrix homeserver is more "
        "operational weight than it saves.",
    ])
    para(doc, "", bold_prefix="Voice")
    bullets(doc, [
        "The common AI voice stack: a phone number from a carrier (Telnyx, Twilio), a media server "
        "(LiveKit SIP, FreeSWITCH, Asterisk, Jambonz) or the carrier's own media streaming, and a voice-agent "
        "framework (Pipecat, LiveKit Agents) running speech-to-text, an LLM and text-to-speech.",
        "The useful abstraction is a call session: call events plus a two-way audio stream. Twilio Media "
        "Streams, Telnyx, Vonage and Jambonz all expose roughly this over WebSocket, which is what our media "
        "protocol follows.",
    ])

    # ---- 4 architecture
    doc.add_heading("4. Architecture", 1)
    figure(doc, "architecture")

    doc.add_heading("4.1 Config and secrets", 2)
    status_line(doc, "live", "gateway.yaml, ask.yaml and .env run the POC on .212.")
    bullets(doc, [
        ("gateway.yaml: ", "which adapters run, the account each uses, routes (where a call's audio goes), "
         "policy, and the consumers with their scopes."),
        ("Secrets: ", "every *_secret field names an environment variable or a line in the env file; "
         "nothing secret is in the YAML. The signal-cli data directory holds the account's keys and is a "
         "secret too."),
        ("Identity map: ", "person_id to platform addresses: Signal ACI (UUID) or E.164, Slack team/user, "
         "phone E.164. A lookup only: sessions stay per conversation, so private context does not leak "
         "between channels."),
    ])
    code(doc, """
adapters:
  signal: {account: "+12127071132", calls: true, route: ask, groups: mention}
  slack:  {bot_token_secret: SLACK_BOT_TOKEN, app_token_secret: SLACK_APP_TOKEN}
  phone:  {api_key_secret: TELNYX_API_KEY, public_key_secret: TELNYX_PUBLIC_KEY,
           numbers: ["+15550002222"], public_base_url: "https://gw.example.com", codec: L16}
routes:
  ask: {media_url: "ws://127.0.0.1:8791/media", media_secret_secret: ASK_MEDIA_SECRET,
        connect_timeout_s: 3, grace_s: 5, fallback: hangup}
consumers:
  ask-service: {token_secret: TOKEN_ASK, scopes: [events:read, calls:control],
                events: ["call.*"]}
persons:
  p_roman: {signal: ["+15551112222"]}
policy: {inbound_allowlist: [p_roman], block_emergency_numbers: true}
""")

    doc.add_heading("4.2 Messaging layer", 2)
    status_line(doc, "tested", "built; tested against simulated signal-cli and Slack, not yet on live accounts.")
    bullets(doc, [
        ("Signal adapter: ", "signal-cli daemon over JSON-RPC on a TCP socket, started with "
         "--receive-mode=manual; the gateway calls subscribeReceive so nothing is received before it "
         "listens, and writes each notification to the database before processing it. Message ids are "
         "author ACI + sent timestamp. In groups the agent sees only messages that mention it (configurable)."),
        ("Slack adapter: ", "Socket Mode (outbound WebSocket only, no public ingress). DMs open on demand; a "
         "reply to a channel message goes into its thread; about one message per second per channel. Section 7 "
         "covers what it took from shmobster and the voice side."),
        ("Common Message: ", "conversation_id, sender (with kind: human or bot) and person_id, parts (text, "
         "file, sticker; outbound also blocks), reply_to, "
         "edits and deletes, and an outbound status that only moves forward: queued, sent, delivered, read "
         "or failed."),
        ("Capabilities: ", "each adapter declares what it supports (GET /v1/channels); unsupported operations "
         "are refused up front with 422 unsupported."),
    ])

    doc.add_heading("4.3 Voice layer", 2)
    status_line(doc, "live", "Signal outbound calls live; inbound and phone tested with simulators.")
    bullets(doc, [
        ("Signal calls: ", "signal-cli (0.14.2 and later) starts one signal-call-tunnel process per call, "
         "built on Signal's RingRTC. The gateway must call subscribeCallEvents, or incoming calls are ignored. "
         "Audio never passes through signal-cli: the tunnel creates per-call PulseAudio devices "
         "(signal_input_<id>, signal_output_<id>) and the gateway reads and writes them at 48 kHz with "
         "parec and pacat. So the gateway, signal-cli and the tunnels share one host and one audio server."),
        ("Phone (Telnyx): ", "Call Control webhooks (Ed25519-signed) and a bidirectional media WebSocket "
         "carrying L16 at 16 kHz, so no media server of our own is needed. L16 is big-endian on the wire; "
         "the gateway converts, and detects and corrects a wrong byte-order setting from the first speech."),
        ("CallSession: ", "one state machine for all channels: initiated, ringing, answered, ended with a "
         "reason. The agent's media socket is opened while the call still rings; if it cannot be reached the "
         "call is rejected or handed to a fallback before anyone hears anything. If it drops mid-call the "
         "gateway reconnects for a grace period, then hangs up or transfers. Planned: calls as legs in a "
         "bridge (warm transfer, conferences), answering-machine detection, recording."),
        ("Audio format: ", "announced, not assumed: the gateway resamples once at its edge (Signal 48 kHz, "
         "phone 8 or 16 kHz) to the format stated in the media start message, 16 kHz pcm16 by default."),
    ])

    doc.add_heading("4.4 Gateway core", 2)
    status_line(doc, "tested", "event log live; policy and outbox tested.")
    bullets(doc, [
        ("Policy: ", "unknown senders are held without content until approved; STOP opts a contact out on "
         "that channel; calls to emergency numbers are refused; unknown callers are rejected unless allowed. "
         "This lives in the gateway because it is the only place that sees every channel."),
        ("Event log and outbox: ", "SQLite in WAL mode, enough for one host; Redis or Postgres is the "
         "scale-out path. Every event is written before anyone is told about it; outbound operations are "
         "written before they are sent and retried with backoff."),
        ("Attachments: ", "stored by content hash; uploaded with POST /v1/attachments and referenced by id."),
    ])

    # ---- 5 API
    doc.add_heading("5. API for the rest of the system", 1)
    status_line(doc, "live", "specified in spec/; validated with openapi-spec-validator and the AsyncAPI CLI.")
    table(doc, ["File", "Describes"], [
        ["spec/gateway.openapi.yaml", "26 REST operations of the gateway: messages, attachments, calls, "
                                     "events polling, admin (held messages, opt-outs); scopes per operation"],
        ["spec/voitta.asyncapi.yaml", "the event stream (19 CloudEvents types, each with a payload schema) "
                                     "and voitta-media/1 (11 messages, both directions)"],
        ["spec/ask.openapi.yaml", "voitta-ask: create an ask, read it (optionally waiting for the result)"],
    ], widths=[5.0, 12.0])
    para(doc, "The specs are part of the tests. A test fails if the routes the code serves differ from "
              "the spec, if an emitted event type has no schema, or if any JSON response, event or media "
              "message seen in an end-to-end test does not validate. Malformed requests return the same "
              "error shape as every other error.")

    doc.add_heading("5.1 Commands (REST)", 2)
    code(doc, """
POST /v1/messages     {to, parts[], reply_to?, idempotency_key?}
                      -> 202 {message_id, status: queued}
POST /v1/messages/{id}/edit | delete | reactions
POST /v1/calls        {to, via: signal|phone, route?}  -> 202 {call_id, state: initiated}
GET  /v1/calls/{id}   state, reason, answered
POST /v1/calls/{id}/answer | reject | hangup | dtmf | transfer
GET  /v1/events       SSE stream (resume with Last-Event-ID)
GET  /v1/events/poll  ?after=evt_N&types=call.*&wait=20
GET  /v1/channels · /v1/identities/{person_id} · /v1/held · /v1/optouts
""")
    bullets(doc, [
        ("Replies go back where the message came from: ", "to: {conversation_id}. A person_id target is only "
         "for conversations the agent starts; the gateway then picks a channel."),
        ("Idempotency keys come from the event being answered ", "(reply:evt_42): events are delivered at "
         "least once, so a repeated event produces one reply, not two."),
        ("Scoped tokens: ", "one per consumer: events:read, messages:send, calls:control, admin."),
    ])

    doc.add_heading("5.2 Events", 2)
    code(doc, """
{"specversion": "1.0", "id": "evt_42", "source": "/gateway/signal",
 "type": "ai.voitta.call.ended.v1", "subject": "call_01a1bddd...",
 "time": "2026-11-08T21:14:03.120Z",        // when it happened on the platform
 "ingesttime": "2026-11-08T21:14:03.131Z",  // when the gateway got it
 "data": {"call_id": "call_01a1bddd...", "channel": "signal", "direction": "outbound",
          "from": "+12127071132", "to": "+15551230000", "person_id": null,
          "reason": "ask_done", "duration_s": 19.0, "answered": true}}
""")
    bullets(doc, [
        "Conversation events also carry seq, a per-conversation counter, so consumers can see gaps.",
        "Read them as SSE (resume with Last-Event-ID) or by long-polling with a cursor. A cursor beyond the end "
        "of the log, left over from a previous database, replays the log from the start and says reset: true. "
        "That case caused the first hung ask in the POC (section 7).",
    ])

    doc.add_heading("5.3 Media (WebSocket per call)", 2)
    para(doc, "The gateway connects to the agent (the agent is the WebSocket server), as Twilio and Telnyx "
              "do, so existing agent frameworks fit. The URL carries call_id, a timestamp and an HMAC "
              "signature. Messages are JSON text with base64 audio.")
    code(doc, """
# while the call rings
g->a {"event":"connected","protocol":"voitta-media/1","call_id":"call_..."}
# once it is answered
g->a {"event":"start","frame_ms":20,"channel":"signal","direction":"outbound",
      "media_format":{"encoding":"pcm16le","sample_rate":16000,"channels":1},
      "capabilities":{"dtmf":false,"transfer":[]}, "from":..., "to":...}
g->a {"event":"media","seq":17,"ts_ms":340,"payload":"<20 ms>"}
a->g {"event":"media","payload":"<any length>"}      # queued, played in real time
a->g {"event":"mark","name":"prompt"}
g->a {"event":"mark","name":"prompt","cleared":false}
a->g {"event":"clear"}                               # barge-in; pending marks: cleared
a->g {"event":"hangup","reason":"ask_done"}
g->a {"event":"stop","reason":"ask_done"}
""")

    doc.add_heading("5.4 Flows", 2)
    figure(doc, "seq-message")
    figure(doc, "seq-call")

    # ---- 6 voitta-ask
    doc.add_heading("6. First agent: voitta-ask", 1)
    status_line(doc, "live", "two answered calls on .212 with real models.")
    para(doc, "A client sends a text and a recipient; voitta-ask calls them, reads the text, records the "
              "spoken answer, transcribes it and returns the text. It is a separate service that uses the "
              "gateway only through its public API and media protocol; the gateway did not change to support it "
              "beyond generic features (after=now, cursor reset, the answered flag on calls).")
    bullets(doc, [
        ("States: ", "calling, ringing, speaking, listening, transcribing, then done, no_answer or failed."),
        ("End of the answer: ", "energy-based detection with an adaptive noise floor: 1.5 s of silence after "
         "speech, 10 s without any speech, or 30 s in total; a hang-up also ends it."),
        ("Never hangs: ", "besides call events, a watchdog checks the call's state every 5 s, and a ring "
         "deadline (90 s) hangs up an unanswered call."),
        ("Models: ", "Piper en_US-lessac-medium on the CPU; faster-whisper small on the GPU (int8_float16). "
         "Both are behind a small interface; tests use deterministic stand-ins."),
    ])
    figure(doc, "seq-ask")

    # ---- 7 slack
    doc.add_heading("7. Slack: messaging and voice", 1)
    status_line(doc, "tested", "built and tested against a simulated Slack and in headless Chrome; not yet live.")
    para(doc, "Slack gives bots no audio: apps cannot join huddles (they only see who is in one), and the "
              "Calls API is a directory of third-party calls, not a media path. So Slack voice is built from what "
              "the platform does allow: voice clips as files, and calls on our own page, announced in Slack.")
    doc.add_heading("7.1 Messaging, hardened with shmobster's lessons", 2)
    para(doc, "shmobster (voitta-ai/shmobster) has run on Slack for months with Bolt and Socket Mode. "
              "What it learned, and what the adapter now does:")
    table(doc, ["shmobster lesson", "In the gateway"], [
        ["A Socket Mode session can reconnect forever without delivering anything (13 days, 52,707 sessions).",
         "Watchdog on two signals: no pong for 60 s, or 6 sessions in 5 minutes. It rebuilds the client with "
         "backoff and reports channel.down / up."],
        ["The bot token must not leave Slack; an unauthorised download is a 200 with the sign-in page.",
         "Downloads follow redirects by hand, send the token only to https *.slack.com, and refuse text/html."],
        ["Agents share workspaces; a voice listener posts as a bot.",
         "Other bots' posts arrive only when they mention us, marked sender.kind = bot (configurable)."],
        ["@here and @channel address everyone in the room, the agent included.", "They count as addressed."],
        ["Approval cards with buttons; a click is acknowledged within 3 s.",
         "blocks message parts out; interaction.received events in (acknowledged on receipt)."],
        ["Replies need the thread so far.", "GET /v1/conversations/{id}/history, with sender kind self / bot / human."],
        ["Two programs on one app's tokens both answer every event.",
         "A separate Voitta app; manifest in deploy/slack-app-manifest.yaml with only the scopes used."],
    ], widths=[8.0, 9.0])
    doc.add_heading("7.2 Voice clips", 2)
    bullets(doc, [
        "A clip (subtype slack_audio) arrives as an audio file part with clip: true, its duration and a transcript.",
        "Slack's own transcript is used when it is complete; otherwise the gateway sends the audio to voitta-speech "
        "(the speech API now mounted in voitta-ask, same models) and records transcript_source. The gateway runs "
        "no models itself, and the same path transcribes Signal voice notes.",
        "Only senders that pass policy are transcribed: a stranger's clip is held untouched.",
        "Verified on .212 with the real models: AAC in m4a and Opus in webm both transcribed word for word, in "
        "about 0.1 to 0.3 s per clip.",
    ])
    doc.add_heading("7.3 Calls through a browser join page", 2)
    bullets(doc, [
        "POST /v1/calls with via: slack DMs the callee a card with a Join button. The link opens a page served on "
        "the gateway's ingress, protected by a per-call token.",
        "The page captures the microphone with an AudioWorklet (echo cancellation and noise suppression on), "
        "sends 20 ms pcm16 frames over a WebSocket, and plays the agent's audio from a small jitter buffer that "
        "clear empties at once (barge-in).",
        "It plugs into the same media protocol as Signal and phone calls: voitta-ask asks over Slack unchanged.",
        "Not joined within the ring timeout: missed. Page reloaded: the call resumes (10 s grace). Tab closed: "
        "remote_hangup. The card in the DM follows the call: ringing, on a call, ended with its length, missed.",
        "card: calls_api posts Slack's native call card instead (calls.add); if Slack refuses it, the call is ended "
        "there and the Join button is used.",
        "Needs a public HTTPS address for live use: browsers only give microphone access on https or localhost. "
        "No TURN server is needed, because the page talks plain WebSocket to the gateway.",
    ])
    figure(doc, "seq-slack-call")

    # ---- 8 lessons
    doc.add_heading("8. What the POC taught us", 1)
    table(doc, ["Finding", "Consequence"], [
        ["Signal does not ring for calls from unknown numbers; the call times out silently.",
         "Callees must know the number first (a message exchange or a contact). Send a short heads-up "
         "message before the first call."],
        ["The native signal-cli 0.14.9 build cannot place calls (TURN lookup fails, issue #2144). 0.14.10 fixes "
         "it but migrates the account database one way (version 31 to 32), after which 0.14.9 refuses to start.",
         "The gateway runs the JVM build of 0.14.9 with a bundled Java 25; manhattan-logic keeps the native "
         "build on the same data. Upgrade both together later."],
        ["The tunnel's patches do not apply to RingRTC 2.72 or later (Signal rewrote the audio code they touch).",
         "The POC runs on RingRTC 2.65.1, which has CVE-2026-113380; kept off PATH and limited to allowlisted "
         "callers. Porting the patches is a prerequisite for production."],
        ["Only one signal-cli daemon can hold an account.",
         "The bot and the gateway switch by hand (about a minute). Decide: a separate voice number, or "
         "manhattan-logic as a gateway consumer (section 9)."],
        ["A gateway that exits uncleanly left its signal-cli running beside the bot's.",
         "signal-cli now dies with the gateway (Linux parent-death signal); SIGHUP shuts the gateway down cleanly."],
        ["After a database reset an agent's event cursor pointed past the end of the log; it missed call.ended "
         "and waited forever.",
         "The gateway replays from the start and flags reset; voitta-ask also polls the call state and has a "
         "ring deadline. Regression tests cover both."],
        ["Whisper small heard \"Asian pairs\" for \"Asian pears\".",
         "Try a larger Whisper model (the GPU has room) or pass expected vocabulary as a prompt."],
        ["faster-whisper 1.2.1 cannot decode files with PyAV 19 (it passes an argument PyAV removed).",
         "voitta-speech decodes with PyAV directly; caught by the real-model test of clip formats."],
        ["Chrome's sandboxed audio service cannot read a fake-microphone file; the page then records silence.",
         "The browser test disables that sandbox feature; the page itself is unaffected."],
        ["Shared host: ports 8765 and 8790 were already taken.",
         "All ports are configurable; .212 uses 8700 (gateway), 7583 (signal-cli) and 8791 (voitta-ask)."],
    ], widths=[8.0, 9.0])

    # ---- 8 deployment
    doc.add_heading("9. Deployment", 1)
    doc.add_heading("9.1 The POC on .212", 2)
    status_line(doc, "live", "as run for the calls in section 2.2.")
    figure(doc, "deployment-poc")
    bullets(doc, [
        "Everything runs as the user in screen sessions; nothing is exposed beyond localhost.",
        "Switching the 212 account: stop manhattan-logic, start the gateway, call, stop the gateway, "
        "start manhattan-logic with its start.sh. Messages that arrive meanwhile go to the gateway.",
    ])
    doc.add_heading("9.2 Target deployment", 2)
    status_line(doc, "planned", "Dockerfile, Compose file and Caddy config are written; the image has not been built.")
    figure(doc, "deployment")
    bullets(doc, [
        "One Linux host per Signal number: gateway, signal-cli, the tunnels and PulseAudio in one container, "
        "because the call audio goes through a shared audio server.",
        "Public exposure only for the phone adapter: one TLS endpoint for Telnyx webhooks and media, with "
        "signature checks. Signal and Slack connect outward only.",
        "Agents run anywhere (voitta-ask on a GPU host); they reach the gateway through its API, and the "
        "gateway connects to their media endpoints.",
    ])

    # ---- 9 review
    doc.add_heading("10. Review against current practice (November 2026)", 1)
    para(doc, "Before building, the design was checked against published practice and platform documentation. "
              "These changes were made then and are now in the code:")
    table(doc, ["Area", "Original idea", "Changed to", "Why"], [
        ["Audio format", "Fixed PCM16 16 kHz", "Announced per stream; resample once",
         "Phone is 8 kHz, Signal 48 kHz; wrong-rate audio fails silently"],
        ["Media protocol", "Agent connects; binary frames", "Gateway connects; JSON; start only after answer; "
         "mark reports cleared", "Reuses agent frameworks; no audio into an unanswered call"],
        ["Phone path", "SIP trunk + own media server", "Telnyx Call Control + media streaming",
         "No media server to run; L16 16 kHz both ways"],
        ["Replies", "To person_id; gateway picks channel", "To the source conversation",
         "A Slack question must not be answered on Signal"],
        ["Idempotency", "Client-chosen key", "Derived from the triggering event",
         "At-least-once delivery would otherwise double-reply"],
        ["Business logic", "None in gateway", "Policy, opt-out, consent in gateway",
         "Must apply before any model sees content"],
        ["Identity", "Merge person across channels", "Lookup only", "Merging leaks private context"],
        ["Event envelope", "Custom; time = ingest", "CloudEvents; time = platform time; seq",
         "Standard; exposes gaps and reordering"],
        ["Signal calls", "acceptCall is enough", "subscribeCallEvents; gateway owns the audio",
         "Without it calls are ignored; audio bypasses signal-cli"],
        ["Spec", "OpenAPI", "OpenAPI + AsyncAPI", "OpenAPI does not describe WebSockets"],
    ], widths=[2.6, 3.3, 4.3, 6.8])

    doc.add_heading("10.1 Platform status", 2)
    table(doc, ["Platform", "Messaging", "Voice for a bot", "Notes"], [
        ["Signal", "signal-cli JSON-RPC", "Experimental (0.14.2+); works",
         "Tunnel built by us; 1:1 only; unknown callers do not ring; latest signal-cli 0.14.10"],
        ["Slack", "Socket Mode", "No media API",
         "Apps cannot join huddles; voice = clips (files) and our own join page"],
        ["Phone (PSTN)", "SMS via carrier", "Mature", "Telnyx Call Control + media streaming"],
        ["WhatsApp", "Cloud API", "Calling API (GA Jul 2025)",
         "Business-initiated calls not available for US/Canada numbers"],
        ["Telegram", "Bot API", "User account only", "py-tgcalls / NTgCalls"],
        ["Discord", "Bot API", "Yes, DAVE E2EE required", "Since March 2026"],
        ["Teams", "Bot Framework", "Windows/.NET only", "Application-hosted media; preview"],
        ["Google Meet", "-", "Media API shutting down", "Endpoints off 15 Jan 2027"],
    ], widths=[2.4, 3.0, 3.6, 8.0])

    doc.add_heading("10.2 Compliance", 2)
    bullets(doc, [
        ("Signal terms: ", "no bulk messaging, auto-messaging or auto-dialing. Use a dedicated number; call "
         "only people who expect it."),
        ("AI disclosure: ", "EU AI Act Article 50 (since 2 August 2026) and US TCPA rules for AI voices "
         "(FCC 24-17). The voitta-ask demo introduced itself as an assistant; make it a fixed opening."),
        ("Recording consent: ", "voitta-ask records the answer to transcribe it. About 11 to 13 US states "
         "require all-party consent; say so in the opening."),
        ("Emergency numbers: ", "outbound calls and transfers to them are refused (built)."),
    ])

    # ---- 10 roadmap
    doc.add_heading("11. Roadmap", 1)
    figure(doc, "roadmap")
    table(doc, ["Phase", "Status", "Done when"], [
        ["0 · Signal call spike", "live",
         "Done: outbound calls, echo and voitta-ask on real calls. Inbound calls: out of scope for now."],
        ["1 · Phone baseline", "tested",
         "Adapter built and simulated. Left: a Telnyx account and number, a live call, latency compared "
         "with Signal."],
        ["2 · Freeze the API", "partial",
         "Specs v1 written and enforced in tests. Left: the phone path live, a generated client library."],
        ["3 · Slack", "tested",
         "Messaging, voice clips and calls built and simulated; join page tested in headless Chrome. Left: install "
         "the Voitta app, a public HTTPS address for the join page, a live call."],
        ["4 · More channels", "planned", "WhatsApp, SMS, Telegram; warm transfer and conferences."],
    ], widths=[4.0, 2.0, 11.0], status_col=1)

    # ---- 11 next
    doc.add_heading("12. Next steps and open questions", 1)
    bullets(doc, [
        ("Scope decision (8 Nov 2026): ", "outbound calls only, on the 212 number. Incoming calls and "
         "additional numbers are out of scope for now; the hand switch between manhattan-logic and the gateway "
         "stays until manhattan-logic becomes a gateway consumer (phase 3)."),
        ("Port the tunnel patches to RingRTC 2.72 or later ", "before any use beyond the allowlisted test "
         "contacts (CVE-2026-113380)."),
        ("Upgrade signal-cli to 0.14.10 everywhere at once ", "(database migration is one way); then the "
         "native build can place calls too."),
        ("Slack, to go live: ", "create the Voitta app from deploy/slack-app-manifest.yaml, put its two tokens "
         "in .env, and give the ingress a public HTTPS name (a reverse proxy or tunnel to .212) for the join page."),
        ("Telnyx: ", "open an account and buy a number to run phase 1."),
        ("Transcription quality: ", "try Whisper medium or large-v3 on the RTX 3090, and a vocabulary prompt."),
        ("Make the disclosure and recording notice a fixed part of every outbound call.", ""),
        ("Production host and packaging: ", "build the Docker image, or run as systemd units on .212."),
    ])

    # ---- sources
    doc.add_heading("Sources", 1)
    src = [
        "signal-cli releases, PR #1932 (calls): github.com/AsamK/signal-cli",
        "signal-cli issue #2144 (native build cannot fetch TURN servers): github.com/AsamK/signal-cli/issues/2144",
        "signal-cli issue #2155 (tunnel packaging): github.com/AsamK/signal-cli/issues/2155",
        "signal-cli call tunnel protocol: github.com/AsamK/signal-cli/blob/master/docs/CALL_TUNNEL.md",
        "signal-call-tunnel (patched RingRTC): github.com/hara-seihun/signal-call-tunnel",
        "RingRTC CVE-2026-113380: helpnetsecurity.com/2026/10/22/signal-ringrtc-vulnerability-cve-2026-113380/",
        "Signal terms: signal.org/legal/",
        "Slack Socket Mode vs HTTP: docs.slack.dev/apis/events-api/comparing-http-socket-mode/",
        "Slack Calls API: docs.slack.dev/apis/web-api/using-the-calls-api/",
        "Slack files and clips: docs.slack.dev/messaging/working-with-files/",
        "shmobster (Slack lessons): github.com/voitta-ai/shmobster",
        "Telnyx media streaming: developers.telnyx.com/docs/voice/programmable-voice/media-streaming",
        "Twilio Media Streams messages: twilio.com/docs/voice/media-streams/websocket-messages",
        "Pipecat transports: docs.pipecat.ai/pipecat/learn/transports",
        "Piper TTS: github.com/OHF-Voice/piper1-gpl · faster-whisper: github.com/SYSTRAN/faster-whisper",
        "CloudEvents 1.0: github.com/cloudevents/spec · AsyncAPI 3.1: asyncapi.com/docs · OpenAPI 3.1: spec.openapis.org",
        "WhatsApp Calling API: developers.facebook.com/docs/whatsapp/cloud-api/calling/",
        "FCC 24-17 (AI voices under TCPA): fcc.gov/document/fcc-makes-ai-generated-voices-robocalls-illegal",
        "EU AI Act Article 50 timing: joneswalker.com (Aug 2026 AI law blog)",
    ]
    for s in src:
        para(doc, s, style="List Bullet", size=9)

    OUT.mkdir(parents=True, exist_ok=True)
    doc.save(DOCX)
    return DOCX


if __name__ == "__main__":
    print(build())
