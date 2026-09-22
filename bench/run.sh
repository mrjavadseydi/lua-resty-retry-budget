#!/bin/sh
# Amplification (wrk) and happy-path overhead, inside the OpenResty image.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
IMAGE=${OPENRESTY_IMAGE:-openresty/openresty:1.31.1.1-3-jammy}
WARMUP=${WARMUP:-12}
DURATION=${DURATION:-15}
THREADS=${THREADS:-2}
CONNS=${CONNS:-64}

docker run --rm \
    -v "$ROOT":/src \
    -w /src \
    -e WARMUP="$WARMUP" \
    -e DURATION="$DURATION" \
    -e THREADS="$THREADS" \
    -e CONNS="$CONNS" \
    "$IMAGE" \
    bash -lc '
set -eu
export DEBIAN_FRONTEND=noninteractive
export PATH=/usr/local/openresty/nginx/sbin:/usr/local/openresty/bin:$PATH
apt-get update -qq
apt-get install -y -qq curl ca-certificates git make gcc libssl-dev zlib1g-dev >/dev/null
if ! apt-get install -y -qq wrk >/dev/null 2>&1; then
    git clone --depth 1 https://github.com/wg/wrk.git /tmp/wrk-src
    make -C /tmp/wrk-src -j"$(nproc)"
    cp /tmp/wrk-src/wrk /usr/local/bin/wrk
fi
opm get ledgetech/lua-resty-http >/dev/null

mkdir -p /tmp/nginx/logs /tmp/nginx/client_body_temp /tmp/nginx/proxy_temp \
    /tmp/nginx/fastcgi_temp /tmp/nginx/uwsgi_temp /tmp/nginx/scgi_temp
nginx -p /tmp/nginx -c /src/bench/nginx.conf
cleanup() {
    nginx -p /tmp/nginx -c /src/bench/nginx.conf -s quit >/dev/null 2>&1 || true
}
trap cleanup EXIT

ready=0
for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
    if curl -sf --max-time 1 http://127.0.0.1:8080/health >/dev/null; then
        ready=1
        break
    fi
    sleep 0.2
done
if [ "$ready" != 1 ]; then
    echo "nginx did not start" >&2
    cat /tmp/nginx/logs/error.log >&2 || true
    exit 1
fi

echo "warmup_sec $WARMUP"
echo "duration_sec $DURATION"
echo "threads $THREADS"
echo "connections $CONNS"
echo "nproc $(getconf _NPROCESSORS_ONLN)"
wrk -t"$THREADS" -c"$CONNS" -d"${WARMUP}s" http://127.0.0.1:8080/budget >/dev/null

run_case() {
    name=$1
    path=$2
    curl -sf --max-time 2 http://127.0.0.1:8080/reset >/dev/null
    echo "== $name"
    wrk -t"$THREADS" -c"$CONNS" -d"${DURATION}s" -s /src/bench/wrk.lua \
        "http://127.0.0.1:8080$path"
    echo "-- nginx"
    curl -sf --max-time 2 http://127.0.0.1:8080/counters
    if [ "$name" = "budget" ]; then
        echo "-- window"
        curl -sf --max-time 2 http://127.0.0.1:8080/budget-stats
    fi
}

run_case budget /budget
run_case naive /naive

echo "== happy"
curl -sf --max-time 120 http://127.0.0.1:8080/happy
'
