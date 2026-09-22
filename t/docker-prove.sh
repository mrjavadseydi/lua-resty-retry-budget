#!/bin/sh
# Run the Test::Nginx suite inside the OpenResty image.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
IMAGE=${OPENRESTY_IMAGE:-openresty/openresty:1.31.1.1-3-jammy}

docker run --rm \
    -v "$ROOT":/src \
    -w /src \
    "$IMAGE" \
    bash -lc '
set -eu
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq git ca-certificates \
    libtest-base-perl libtext-diff-perl libtest-longstring-perl \
    liblist-moreutils-perl libwww-perl \
    >/dev/null
if [ ! -d /opt/test-nginx ]; then
    git clone --depth 1 https://github.com/openresty/test-nginx.git /opt/test-nginx
fi
opm get ledgetech/lua-resty-http
export PATH=/usr/local/openresty/nginx/sbin:/usr/local/openresty/bin:$PATH
export PERL5LIB=/opt/test-nginx/lib:${PERL5LIB:-}
export RETRY_BUDGET_LIB=/src/lib/?.lua
prove -I/opt/test-nginx/lib -r t
'
