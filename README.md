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
| Signal | signal-cli JSON-RPC daemon | signal-call-tunnel + PulseAudio (experimental, Linux; not in the default image) |
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
PulseAudio in one container, Caddy for the public ingress). Signal voice calls need
signal-call-tunnel, which only the `with-signal-calls` build target includes; it is experimental.
The tunnel is built against RingRTC >= 2.72 with the patches in `deploy/tunnel/` (see the
Dockerfile header).
First live tests: [docs/SPIKE.md](docs/SPIKE.md).

The service needs the `service` extra (FastAPI, uvicorn): `pip install "voitta-gateway[service]"`.
`[dev]` includes it.

## Embed it (library mode)

One long-running agent that owns its own identities can run the same core in-process,
with no HTTP listener and without the `service` extra:

```python
from voitta_gateway import embed

gw = embed.build("gateway.yaml", consumer="leads")    # or a Config
gw.on_event(handler, types=["message.*"], after="now") # async handler(event), same CloudEvents envelope
await gw.start()
gw.send({"to": {"conversation_id": cid}, "parts": [{"type": "text", "text": "hi"}]})  # POST /v1/messages body
await gw.stop()
```

`app.build` (the service) and `embed.build` share one builder (`build.build_gateway`).
Events come from the same persisted log, so `after=<event id>` resumes where a previous run
stopped. Adapters that need public HTTP (`adapters.phone`, Slack calls) are service-only and
refused at build time. **One owner per identity** holds in both shapes: an embedded gateway
runs its account's signal-cli and its Slack app's Socket Mode connection, so no other process
(service or library) may use either. Example: [examples/embedded_echo.py](examples/embedded_echo.py).

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
- **Read the event stream after every send.** `202 queued` means accepted, not sent: the outcome
  arrives later as `message.status` (`sent`, or `failed` with an error). A consumer that runs and
  exits finds its own failures on its next poll, so it persists `next` on every 200, even when
  `events` is empty (on a shared account most events belong to other consumers).
- **Policy lives here**: unknown senders are held (no content in the event) until approved;
  `STOP` opts out; calls to emergency numbers are refused.
- **Closed by default**: `policy.inbound_allowlist` is empty, so every sender is held until
  listed or approved; groups and Slack channels produce nothing unless listed under
  `conversations`, and from then on leave nothing in the gateway's store: no raw envelope,
attachment, conversation, held row or event. Unlisting a group stops new storage and drops
its unprocessed envelopes; what was stored while it was listed stays (signal-cli's own
attachment directory is outside the gateway's reach;
gating happens at read time because Signal offers none earlier: anyone
  holding the account's profile key can add it to a group as a full member);
  `send_read_receipts` is off. Delivery receipts are not affected: signal-cli sends them for
  everything it receives, listed or not. Account strings in the config (Signal account,
  phone numbers, person addresses, conversation keys) are stripped of invisible Unicode
  format characters when loaded; a pasted number carrying U+202C otherwise makes
  signal-cli report "not registered", and re-registering is the wrong fix.

## Several agents on one account

One Signal account (or Slack app) can serve several consumers. `conversations:` maps each group
to the consumers that serve it, default first; its label is also the alias they send to, so group
ids stay in gateway.yaml. DMs go to the adapter's `direct_consumer`.

```yaml
consumers:
  shmobster: {token_secret: TOKEN_SHMOBSTER, scopes: [events:read, messages:send], names: [ada], label: Ada}
  leads:     {token_secret: TOKEN_LEADS,     scopes: [events:read, messages:send], names: [leadbot]}
conversations:
  team: {channel: signal, key: "<group-id>", consumers: [leads, shmobster]}
adapters:
  signal: {account: "+15550001111", direct_consumer: shmobster}
```

- **Who gets an inbound event** (`addressed_to`): the consumer that sent the message it quotes,
  reacts to or edits; else the consumer whose name keyword is in the text (whole word, `@`
  optional); else the conversation's default. Only that consumer's stream carries it; admin tokens
  see everything. `message.status` and `operation.*` go to the consumer that sent the message.
- **Where a consumer may act**: sends, reactions, typing and history need a conversation the
  consumer serves; edits and deletes, also that it sent the message. Admin tokens are exempt.
- **`label`** is prefixed to the consumer's text (`Ada: ...`), so people can tell agents apart.
- **DMs** go to the consumer that last sent into that conversation, else `direct_consumer`. Any
  consumer may start a DM that has no messages yet; only the consumers serving a DM act in it.
  An operator's admin send does not take a DM or a thread over. One person is one DM whichever
  address reaches them: a send by number joins the DM their messages arrive in (by ACI) when
  `persons` lists both, or once signal-cli has reported their number on an inbound message.
  Edits, deletes and reactions on a person's message go to the consumer that message went to.
- **Calls** are routed by `routes:`, not by this map: give message consumers `events` patterns
  without `call.*`.
- A listed group whose entry names no `consumers`, and DMs with no `direct_consumer` set, reach
  every consumer as before. `message.status` and `operation.*` always go only to the consumer that
  sent the message.

## Consumer contract

- **Text is final when it reaches the gateway.** The consumer renders Markdown and splits; the
  gateway does neither. Signal style ranges go on the text part, `styles: [{start, length, style}]`,
  in UTF-16 code units. A send over `max_text_bytes` (Signal: 2000, in UTF-8 bytes, which is what
  signal-cli measures) is `422 too_long`; past it signal-cli would cut the text and attach the
  rest. Count neither in characters. A consumer `label` is prefixed before the limit is checked,
  and styles are shifted to match. An edit cannot carry files.
- **Run-and-exit consumers poll.** `GET /v1/events/poll?after=<next>` with the cursor kept on disk;
  persisting `next` is the ack. Events are at-least-once with stable ids, and sends take an
  `idempotency_key` (per consumer, 24 h), so a timed-out POST can be retried safely.
- **Signal messages** carry `timestamp` (the sender's sent timestamp, Signal's id for the message) and `mentions: [{id, start, length,
  self}]` exactly as signal-cli reports them: UTF-16 offsets, one U+FFFC per mention in the text.
  To mention someone, put one U+FFFC in the text and add `mentions: [{id, start, length: 1}]` to
  the text part, with the person's `sender.id` and the U+FFFC's UTF-16 offset; anything else is
  `422 bad_mentions`, because an off-by-one mention tags the wrong person. The label shift applies.
  React, edit or delete by `message_id`; the gateway keeps the platform ids.
- **Signal history** is the gateway's own record (Signal has no history API): the messages it
  delivered as events and the ones it sent, kept current through edits and deletes, from the time
  it started recording. Group chatter that did not address the agent is not kept.
- **Identity**: trust checks key on `sender.id` (an ACI on Signal). `person_id` comes from the
  operator's `persons:` map in gateway.yaml and is for correlation; it is on every message,
  reaction, edit, delete, held and status event (status: the other side of a direct conversation).

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
