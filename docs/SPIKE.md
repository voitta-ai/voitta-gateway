# First live tests (roadmap phases 0 and 1)

What has been tested without real accounts: every adapter end to end against fakes
(see `tests/`). What only a live run can show: that signal-call-tunnel builds against a
patched RingRTC, that Signal call audio flows through PulseAudio in the container, and how
Telnyx actually encodes L16.

You need: a Linux x86_64 server with Docker, a **spare** phone number for Signal (never a
personal one: Signal's terms forbid automated use and the account may be banned), and your own
phone with Signal installed.

## 0. Build

```bash
git clone … voitta-sip && cd voitta-sip
docker build -f deploy/Dockerfile -t voitta-gateway .
```

The tunnel stage downloads ~100 MB of prebuilt WebRTC and needs several GB of disk.
It moves RingRTC from the 2.65.1 pinned by both tunnel repos to `v2.74.0`, because versions
before 2.72.0 have CVE-2026-113380 (the other side may keep hearing you after hangup).
**This bump has not been compiled yet.** If it fails:

- try `--build-arg RINGRTC_TAG=v2.72.1` (closest to the pinned API);
- if the tunnel's code needs changes for the newer RingRTC, that is the first real work item;
- building with 2.65.1 (`RINGRTC_TAG=v2.65.1` after removing the version check) is acceptable only
  for a closed test where nobody but your own phone can call the number.

## 1. Configure

```bash
cp gateway.example.yaml gateway.yaml     # set adapters.signal.account, persons.p_roman.signal = your number
cp .env.example .env                     # fill TOKEN_OPS, VOITTA_MEDIA_SECRET (openssl rand -hex 32)
alias dc='docker compose -f deploy/docker-compose.yml'
```

## 2. Register the spare Signal number, then start

```bash
alias sc='dc run --rm gateway signal-cli --config /signal'
sc -a +15550001111 register --captcha "$(cat captcha.txt)"   # captcha: signalcaptchas.org/registration/generate.html
sc -a +15550001111 verify 123-456
sc -a +15550001111 updateProfile --given-name "Voitta" --family-name "Assistant"

dc up -d --build
dc exec -d gateway python /app/examples/echo_agent.py --host 127.0.0.1 --port 8765
export T=$(grep TOKEN_OPS .env | cut -d= -f2)
curl -s -H "Authorization: Bearer $T" localhost:8700/v1/channels | jq '.channels[] | {channel, up}'
```

Run it as the **primary** device first: calls on linked devices only work from signal-cli 0.14.9.

## 3. Messages

From your phone, send "hello" to the spare number, then:

```bash
curl -s -H "Authorization: Bearer $T" 'localhost:8700/v1/events/poll?types=message.*' | jq '.events[-1].data'
CID=…   # data.conversation_id
curl -s -H "Authorization: Bearer $T" localhost:8700/v1/messages \
     -d "{\"to\":{\"conversation_id\":\"$CID\"},\"parts\":[{\"type\":\"text\",\"text\":\"hi from the gateway\"}]}"
```

Pass: the reply arrives; `message.status` goes queued → sent → delivered → read.

## 4. The Signal call (the actual spike)

Call the spare number from the Signal app on your **phone** (not Desktop first).

| Check | Pass | If not |
|---|---|---|
| `call.ringing` then `call.answered` events | both, within ~2 s | `subscribeCallEvents` missing? check gateway log for `callEvent` |
| `dc exec gateway pactl list short sinks` during the call | `sink_for_signal_input_<id>` and `signal_output_<id>` exist | tunnel did not start: `SIGNAL_CALL_TUNNEL_BIN`, tunnel stderr in the signal-cli log |
| You hear a beep, then yourself | yes, both directions | one direction silent: swap test with `paplay`/`parecord` on the device names (SPIKE notes in the tunnel README) |
| Echo delay (clap test) | note the number; < 400 ms round trip is good | try `pulse_latency_ms: 20` |
| Hang up on the phone | `call.ended`, agent gets `stop` | |
| `POST /v1/calls/{id}/hangup` mid-call | the phone call ends | |
| Kill the echo agent mid-call | after `grace_s` the call ends with `agent_lost` | |
| Call from Signal Desktop | connects (Desktop-originated calls had issues in other projects) | note it, not a blocker |

Then swap the echo agent for the Pipecat bot: `MODE=echo uvicorn examples.pipecat_bot:app --port 8765`,
and finally `MODE=assistant` with Deepgram, Anthropic and Cartesia keys.

## 5. Phone baseline (phase 1, Telnyx)

1. Telnyx portal: buy a number; create a **Call Control application** with webhook URL
   `https://gw.example.com/telnyx/webhook` (API v2); assign the number; copy the API key and
   the public key (Keys & Credentials → Public Key) into `.env`.
2. DNS `gw.example.com` → the server; set the host in `deploy/Caddyfile`.
3. Uncomment `adapters.phone` in `gateway.yaml` (`connection_id` = the application id for
   outbound), then `dc --profile phone up -d`.
4. Call the number: beep + echo. Watch the log for
   `Telnyx L16 audio looks byte-swapped` — if it appears, set `l16_byte_order` as it says.
5. Outbound: `curl … localhost:8700/v1/calls -d '{"to":"+1your-cell","via":"phone"}'`.
6. Compare latency and quality with the Signal call.
