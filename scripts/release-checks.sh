#!/usr/bin/env bash
# Release checks: this repository's full checks. scripts/promote.sh runs this file, from the commit being
# promoted, in a fresh worktree next to the checkout, and records a green run as a receipt for that tree.
set -euo pipefail
# Mirrors the Makefile `ci` target (and CONTRIBUTING.md "Before a pull request"): unit suite,
# eventing boundaries, the SQLAlchemy matrix, OpenTofu tests, then integration and both LocalStack
# end-to-end examples against compose. Steps needing no engine run first.
export PATH="$HOME/.local/bin:$PATH"
command -v uv >/dev/null || { echo "release-checks: uv is required (https://docs.astral.sh/uv/)" >&2; exit 2; }
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test   # LocalStack's; keeps real credentials out
unset FACTBUS_PG_URL FACTBUS_MYSQL_URL LOCALSTACK_ENDPOINT

uv venv -q --clear --python 3.12 .release-venv
uv pip install -q -p .release-venv/bin/python -e '.[dev]'
py=.release-venv/bin/python

# Checksum-pinned OpenTofu (the version foundry-agent-toolkit's installer pins).
tools="$PWD/.release-tools"; mkdir -p "$tools"; export PATH="$tools:$PATH"
if ! command -v tofu >/dev/null; then
  curl -fsSL -o "$tools/tofu.zip" \
    https://github.com/opentofu/opentofu/releases/download/v1.13.1/tofu_1.13.1_linux_amd64.zip
  echo "8ccbc6f8ee21d2827715f3c6e08a9b3e0209b1e62057c05067ef117e047c1a80  $tools/tofu.zip" | sha256sum -c --quiet -
  python3 -c "import zipfile,sys; zipfile.ZipFile(sys.argv[1]).extract('tofu', sys.argv[2])" "$tools/tofu.zip" "$tools"
  chmod +x "$tools/tofu"; fi

"$py" -m pytest
python3 scripts/check_eventing_boundaries.py
(cd iac/modules/consumer && tofu init -backend=false -input=false >/dev/null && tofu test)
(cd examples/product && tofu init -backend=false -input=false >/dev/null && tofu test)
# The matrix's unit arms; without engine URLs it exits 3 (integration arms not run), rerun below.
set +e; PYTHON=$(uv python find 3.12) bash scripts/test_sqlalchemy_matrix.sh; code=$?; set -e
[ "$code" = 0 ] || [ "$code" = 3 ] || exit "$code"

# Docker. A machine with the daemon installed but not running (a cloud sandbox) gets it started; Docker Hub
# images come through a mirror there, because shared egress IPs hit Docker Hub's anonymous rate limit.
if ! docker info >/dev/null 2>&1 && [ "$(id -u)" = 0 ] && command -v dockerd >/dev/null; then
  mirror=--registry-mirror=https://mirror.gcr.io
  grep -qs registry-mirrors /etc/docker/daemon.json && mirror=
  (dockerd $mirror >/tmp/release-checks-dockerd.log 2>&1 &)
  for _ in $(seq 30); do docker info >/dev/null 2>&1 && break; sleep 1; done
fi
docker info >/dev/null 2>&1 || { echo "UNPROVEN: the remaining steps need a Docker daemon"; exit 3; }
# compose.yaml on free ports under its own project, removed on exit.
free() { python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1', 0)); print(s.getsockname()[1])"; }
export COMPOSE_PROJECT_NAME="factbus-release-$$"
export FACTBUS_LOCALSTACK_PORT=$(free) FACTBUS_POSTGRES_PORT=$(free) FACTBUS_MYSQL_PORT=$(free)
trap 'docker compose down -v --remove-orphans >/dev/null 2>&1 || true' EXIT
docker compose up -d --wait
export LOCALSTACK_ENDPOINT="http://localhost:$FACTBUS_LOCALSTACK_PORT"
export FACTBUS_PG_URL="postgresql+psycopg://factbus:factbus@localhost:$FACTBUS_POSTGRES_PORT/factbus"
export FACTBUS_MYSQL_URL="mysql+pymysql://factbus:factbus@localhost:$FACTBUS_MYSQL_PORT/factbus"
make PY="$py" LOCALSTACK_ENDPOINT="$LOCALSTACK_ENDPOINT" FACTBUS_PG_URL="$FACTBUS_PG_URL" \
  FACTBUS_MYSQL_URL="$FACTBUS_MYSQL_URL" integration e2e e2e-product
PYTHON=$(uv python find 3.12) bash scripts/test_sqlalchemy_matrix.sh
