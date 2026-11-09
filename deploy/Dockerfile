# voitta-gateway for one Linux x86_64 host: gateway + signal-cli (native) + signal-call-tunnel + PulseAudio.
# The gateway, signal-cli and the tunnels must share one audio server, so they share one container.
#
#   docker build -f deploy/Dockerfile -t voitta-gateway .
#
# The tunnel stage is the slow one (RingRTC + a ~100 MB prebuilt WebRTC download, several GB of disk).

# ---------------------------------------------------------------- signal-call-tunnel
FROM rust:1-bookworm AS tunnel
RUN apt-get update && apt-get install -y --no-install-recommends \
        git clang cmake pkg-config libpulse-dev python3 curl ca-certificates protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
# hara-seihun's build carries the linked-device fix, SIGTERM teardown and the pipe audio backend;
# visigoth/signal-call-tunnel is the original. Both pin RingRTC 2.65.1, which predates the fix for
# CVE-2026-113380 (2.72.0), so RingRTC is moved to RINGRTC_TAG below and the build refuses anything older.
ARG TUNNEL_REPO=https://github.com/hara-seihun/signal-call-tunnel.git
ARG TUNNEL_REF=6fe3298844
ARG RINGRTC_TAG=v2.74.0
WORKDIR /src
RUN git clone --recurse-submodules "$TUNNEL_REPO" tunnel \
    && cd tunnel && git checkout "$TUNNEL_REF" && git submodule update --init --recursive \
    && cd third-party/ringrtc && git fetch --tags --depth 1 origin "refs/tags/$RINGRTC_TAG:refs/tags/$RINGRTC_TAG" \
    && git checkout "$RINGRTC_TAG" \
    && v=$(sed -n '/\[workspace.package\]/,/^\[/s/^version *= *"\(.*\)"/\1/p' Cargo.toml) \
    && echo "RingRTC $v" \
    && python3 -c "import sys; v=tuple(map(int,'$v'.split('.'))); sys.exit(0 if v>=(2,72,0) else 'RingRTC $v < 2.72.0 (CVE-2026-113380)')"
RUN cd tunnel/signal-call-tunnel && cargo build --release \
    && cp target/release/signal-call-tunnel /usr/local/bin/

# ---------------------------------------------------------------- signal-cli (GraalVM native, no JRE)
FROM debian:bookworm-slim AS signalcli
ARG SIGNAL_CLI_VERSION=0.14.10
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -fsSL -o /tmp/sc.tgz \
       "https://github.com/AsamK/signal-cli/releases/download/v${SIGNAL_CLI_VERSION}/signal-cli-${SIGNAL_CLI_VERSION}-Linux-native.tar.gz" \
    && mkdir -p /opt/signal-cli && tar xzf /tmp/sc.tgz -C /opt/signal-cli \
    && find /opt/signal-cli -type f -name signal-cli -exec install -m 0755 {} /usr/local/bin/signal-cli \;

# ---------------------------------------------------------------- runtime
FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
        pulseaudio pulseaudio-utils libpulse0 tini ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 voitta
COPY --from=tunnel /usr/local/bin/signal-call-tunnel /usr/local/bin/
COPY --from=signalcli /usr/local/bin/signal-cli /usr/local/bin/
ENV SIGNAL_CALL_TUNNEL_BIN=/usr/local/bin/signal-call-tunnel \
    XDG_RUNTIME_DIR=/tmp/runtime-voitta \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY voitta_gateway ./voitta_gateway
RUN pip install --no-cache-dir .
COPY examples ./examples
COPY deploy/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN mkdir -p /data /signal && chown voitta:voitta /data /signal   # named volumes inherit this
USER voitta
VOLUME ["/data", "/signal"]
EXPOSE 8700 8800
ENTRYPOINT ["tini", "--", "/usr/local/bin/entrypoint.sh"]
CMD ["voitta-gateway", "--config", "/etc/voitta/gateway.yaml"]
