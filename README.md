# voitta-gateway

One API in front of Signal, Slack and the phone network for Voitta agents.
Design, status and POC results: [docs/out/voitta-gateway.docx](docs/out/voitta-gateway.docx).
Formal API: [spec/](spec/) (OpenAPI 3.1 for the REST APIs, AsyncAPI 3.1 for events and media).

**Status (8 Nov 2026):** outbound Signal calls and voitta-ask work live on .212; messaging,
Slack, inbound calls and the phone (Telnyx) path are built and tested against simulators.

```
Signal  ─┐                    ┌─ REST commands      (send, react, call, answer, hangup…)
Slack   ─┼─ voitta-gateway ───┼─ event stream       (SSE / polling, replay by id)
Phone   ─┘   (one process)    └─ media WebSocket    (per call; the gateway connects to the agent)
```

| Channel | Messaging | Voice |
|---|---|---|
| Signal | signal-cli JSON-RPC daemon | signal-call-tunnel + PulseAudio (experimental, Linux) |
| Slack | Socket Mode (watchdog, blocks, buttons, history) | voice clips -> transcripts; outbound calls via a browser join page |
| Phone | — | Telnyx Call Control + media streaming, L16 16 kHz |

## Run it

```bash
uv venv && uv pip install -e ".[dev]"
cp gateway.example.yaml gateway.yaml && cp .env.example .env    # fill both in
voitta-gateway -c gateway.yaml
python examples/echo_agent.py                                   # a voice agent to route calls to
```

Production is one Linux host via [deploy/](deploy/) (Docker Compose: gateway + signal-cli +
signal-call-tunnel + PulseAudio in one container, Caddy for the public ingress).
First live tests: [docs/SPIKE.md](docs/SPIKE.md).

## voitta-ask: call someone, read a text, get the answer as text

A voice agent behind the gateway ([voitta_ask/](voitta_ask/), API in
[spec/ask.openapi.yaml](spec/ask.openapi.yaml)). Piper speaks the text; faster-whisper transcribes the answer.

```bash
uv pip install -e ".[ask]"
voitta-ask -c ask.yaml                     # route "ask" in gateway.yaml points at its /media
ASK_TOKEN=... python examples/ask.py +15551112222 "Will you join the review at ten?"
```

## API (private listener, `Authorization: Bearer <consumer token>`)

```
GET  /v1/health · /v1/channels · /v1/identities/{person_id}
POST /v1/messages                     {to, parts[], reply_to?, idempotency_key?} -> 202 queued
POST /v1/messages/{id}/edit|delete|reactions
POST /v1/conversations/{id}/typing    {on}
GET  /v1/conversations/{id}/history  ?thread=<message_id>&limit=25   (Slack)
POST /v1/attachments                  raw body (+ X-Filename)  ·  GET /v1/attachments/{id}
GET  /v1/events                       SSE; resume with Last-Event-ID or ?after=evt_N (after=now)
GET  /v1/events/poll                  ?after=&types=message.*,call.*&wait=10
POST /v1/calls                        {to: "+1…" | {person_id}, via: phone|signal, route?} -> 202
POST /v1/calls/{id}/answer|reject|hangup|dtmf|transfer
GET  /v1/held · POST /v1/held/{id}/approve|discard · GET/DELETE /v1/optouts   (admin)
```

- **Replies go back where the message came from**: `to: {conversation_id}`. `to: {person_id}` is
  only for conversations the agent starts; the gateway then picks a channel.
- **Idempotency**: derive the key from the event you're answering (`reply:evt_42`); events are
  delivered at least once.
- **Events** are CloudEvents 1.0: `type` like `ai.voitta.message.received.v1`, `time` = when it
  happened on the platform, `ingesttime` = when the gateway got it, `seq` = per conversation.
  Types: `message.received|status|edited|deleted|held`, `reaction.added|removed`,
  `contact.opted_out|opted_in`, `call.ringing|answered|dtmf|state|transferred|ended`,
  `channel.up|down`, `operation.done|failed`.
- **Policy lives here**: unknown senders are held (no content in the event) until approved;
  `STOP` opts out; calls to emergency numbers are refused.

## Media protocol `voitta-media/1`

The gateway connects to the route's `media_url` with `?call_id=…&ts=…&sig=…` (HMAC-SHA256 of
`call_id.ts` with the route secret; check it with `voitta_gateway.agent.verify`). JSON text frames:

```
g->a {"event":"connected","protocol":"voitta-media/1","call_id":"call_…"}    while it still rings
g->a {"event":"start","call_id","channel","direction","from","to","person_id",
      "media_format":{"encoding":"pcm16le","sample_rate":16000,"channels":1},"frame_ms":20,
      "capabilities":{"dtmf":true,"transfer":["blind"]}}                       once answered
g->a {"event":"media","seq":17,"ts_ms":340,"payload":"<base64 20 ms>"}
a->g {"event":"media","payload":"<base64, any length>"}                       queued, played in real time
a->g {"event":"mark","name":"m1"}   ->  g->a {"event":"mark","name":"m1","cleared":false|true}
a->g {"event":"clear"}                                                         barge-in
a->g {"event":"hangup","reason":"done"}
g->a {"event":"dtmf","digit":"5"} · {"event":"error","code":"buffer_full",…} · {"event":"stop","reason":…}
```

Audio sent before `start` is dropped. If the agent's socket drops mid-call the gateway
reconnects for `grace_s`, then falls back (hang up or transfer). Pipecat agents use
`voitta_gateway.agent.pipecat.VoittaMediaSerializer` ([examples/pipecat_bot.py](examples/pipecat_bot.py)).

## Slack

App: create it from [deploy/slack-app-manifest.yaml](deploy/slack-app-manifest.yaml) (a separate app from
shmobster), put `SLACK_BOT_TOKEN` / `SLACK_APP_TOKEN` in `.env`.

- **Messaging** over Socket Mode, with shmobster's lessons: a watchdog for sessions that reconnect
  without working, the bot token sent only to `https://*.slack.com`, other bots heard only when they
  mention us (`sender.kind: bot`), `@here` counts as addressed, Block Kit out (`{"type": "blocks"}`
  parts), button clicks in (`interaction.received`), thread history on request.
- **Voice clips** arrive as audio parts with a `transcript`: Slack's own if complete, otherwise from
  voitta-speech (`speech:` in gateway.yaml points at voitta-ask, which serves `/v1/speech/*`).
- **Calls** (`adapters.slack.calls`): `POST /v1/calls {"to": "U…", "via": "slack"}` DMs a card with a
  Join button; the page (served on the ingress) streams the microphone over a WebSocket into the same
  media protocol, so voitta-ask works unchanged. Slack bots cannot join huddles. Needs a public HTTPS
  `public_base_url` (browsers allow the microphone only on https or localhost).

## Tests

```bash
pytest -q        # 46 tests: units, end to end, spec conformance, the Slack join page in headless Chrome
VOITTA_ASK_REAL=run/ask.yaml pytest tests/test_ask_real.py   # real Piper + Whisper (GPU host)
```

The end-to-end tests drive the real adapters over real sockets: a JSON-RPC TCP server standing
in for signal-cli, signed webhooks and a media WebSocket standing in for Telnyx, and both a
scripted agent and the Pipecat example bot on the agent side. Every JSON response, event and
media message they produce is validated against [spec/](spec/), and the routes the code serves
must match the specs exactly.

## v0 limits

- State is SQLite (WAL) in `data_dir`: right for one host; the design doc's Redis/Postgres is the
  scale-out path.
- Not implemented yet: recording, warm transfer and conferences (calls are single-leg), answering
  machine detection, Slack typing (no API), Signal group calls.
