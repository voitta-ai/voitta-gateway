#!/bin/sh
# A private PulseAudio for this container. signal-call-tunnel creates and removes its
# per-call null sinks on it; the gateway reads and writes them with parec/pacat.
set -e
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/tmp/runtime-voitta}"
mkdir -p "$XDG_RUNTIME_DIR" && chmod 700 "$XDG_RUNTIME_DIR"
pulseaudio --daemonize=yes --exit-idle-time=-1 --disallow-exit \
    --load="module-native-protocol-unix" --log-target=stderr
for i in $(seq 1 50); do pactl info >/dev/null 2>&1 && break; sleep 0.1; done
pactl info >/dev/null || { echo "pulseaudio did not start" >&2; exit 1; }
exec "$@"
