# Slack in both layers: plan, decisions, status

Status on 8 November 2026: **built and tested against a simulated Slack and in headless Chrome;
not yet on a live workspace.** Details and figures: section 7 of
[out/voitta-gateway.docx](out/voitta-gateway.docx).

## Facts that shaped it

- Slack apps cannot join huddles; the only official huddle signals are who is in one.
- The Calls API (`calls.add`, the `call` block) lists a call that runs elsewhere; it carries no audio.
- Voice clips are ordinary files (`subtype: slack_audio`), often with Slack's own transcript.
- shmobster (voitta-ai/shmobster) already runs on Slack: Bolt over Socket Mode, mentions + DMs +
  @here, thread replies as markdown blocks, approval buttons, and two hard-won lessons: Socket Mode
  can reconnect forever without working, and file downloads must not leak the bot token.

## Decisions (made at implementation time)

| Question | Decision | Why |
|---|---|---|
| Which Slack app | A new **Voitta** app from `deploy/slack-app-manifest.yaml` | Two programs on one app's tokens both answer every event |
| Live-call audio path | **Our own join page, plain WebSocket to the gateway**, instead of LiveKit | No new service or account; no TURN needed (TCP WebSocket); same media protocol as Signal and phone, so agents are unchanged. LiveKit stays an option if browsers on bad networks need WebRTC |
| Call card | **DM with a Join button** by default; `card: calls_api` tries Slack's native call card first and falls back | Slack documents Calls API use for non-partner apps via slash commands; the button works everywhere |
| Who transcribes clips | **voitta-speech**: the engines moved out of voitta-ask into a package with an HTTP API, mounted in voitta-ask (one copy of the models on the GPU) | The gateway runs no models; every consumer gets text; Signal voice notes get it for free |
| Huddles | **Not built** | No official API; the workarounds drive a real user account |

## Phases

| Phase | Scope | Status |
|---|---|---|
| S1 Messaging | watchdog, safe downloads, bot posts with `sender.kind`, @here, `blocks` parts, `interaction.received`, history API, manifest | tested (simulated Slack) |
| S2 Voice clips | audio parts with `clip`, `duration_s`, `transcript` (Slack's, else voitta-speech); policy before transcription | tested; real AAC/Opus clips transcribed on .212 |
| S3 Calls | `via: slack`, DM card, join page (AudioWorklet capture + jitter-buffered player, barge-in), ring timeout, reload grace, card updates | tested; page run in headless Chrome with a fake microphone |

## To go live

1. Create the Voitta app from the manifest; put `SLACK_BOT_TOKEN` and `SLACK_APP_TOKEN` in `.env`.
2. Add `adapters.slack` to gateway.yaml on .212 (see `gateway.example.yaml`); invite the bot.
3. For calls: a public HTTPS name for the gateway's ingress (reverse proxy or tunnel to .212),
   set as `adapters.slack.calls.public_base_url`, and `listen.ingress`.
