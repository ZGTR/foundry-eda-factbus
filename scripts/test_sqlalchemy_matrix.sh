#!/usr/bin/env bash
# Run the kit's unit and integration suites on every SQLAlchemy version it supports,
# plus the SQL claim store's (`SqlClaimStore`, behind `once`) tests as named arms.
#
# Adopters run SQLAlchemy 1.4.36 and 2.0. A kit tested on 2.0 alone imported `Engine`
# from top-level `sqlalchemy`, which exists on 2.0 only, and failed to import inside a
# 1.4 service (F-B0-1). Each leg below gets its own venv so one
# leg's resolution never leaks into the other.
#
# Every leg first asserts the version it actually installed: a leg that silently
# resolved 2.0 while claiming 1.4 would report a green 1.4 run that never happened.
#
# Integration URLs are the README's. PostgreSQL on 1.4 goes through psycopg2, because
# the `postgresql+psycopg` dialect is new in 2.0; the database is the same one.
#
# Exit codes: 0 every arm ran and passed; 1 an arm failed; 2 an instrument refused
# (wrong installed version, no pass count); 3 unit arms passed but the integration arms
# were not run because an engine URL is absent -- a named skip, never a pass.
#
#   bash scripts/test_sqlalchemy_matrix.sh
#   FACTBUS_PG_URL=... FACTBUS_MYSQL_URL=... LOCALSTACK_ENDPOINT=... \
#     bash scripts/test_sqlalchemy_matrix.sh

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LEGS=("1.4.36" "2.0.36")
PYTHON="${PYTHON:-python3.12}"
RAN=0
FAILED=0
REFUSED=0
SKIPPED=0
SUMMARY=()

have_integration_urls() {
  [[ -n "${FACTBUS_PG_URL:-}" && -n "${FACTBUS_MYSQL_URL:-}" && -n "${LOCALSTACK_ENDPOINT:-}" ]]
}

make_venv() {
  local version="$1" venv="$2" override
  override="$(mktemp)"
  printf 'SQLAlchemy==%s\n' "$version" > "$override"
  uv venv -q --clear --python "$PYTHON" "$venv" || return 1
  uv pip install -q -p "$venv/bin/python" --override "$override" -e '.[dev]' || return 1
  if [[ "$version" == 1.4.* ]]; then
    uv pip install -q -p "$venv/bin/python" 'psycopg2-binary==2.9.10' || return 1
  fi
  rm -f "$override"
}

# Runs one suite; records RAN/FAILED/REFUSED and a summary line naming the pass count.
run_arm() {
  local leg="$1" name="$2" out status passed
  shift 2
  out="$(mktemp)"
  "$@" > "$out" 2>&1
  status=$?
  RAN=$((RAN + 1))
  passed="$(sed -n 's/^\([0-9][0-9]*\) passed.*/\1/p; s/.* \([0-9][0-9]*\) passed.*/\1/p' "$out" | tail -1)"
  if [[ $status -ne 0 ]]; then
    FAILED=$((FAILED + 1))
    SUMMARY+=("FAIL  sqlalchemy $leg  $name  exit=$status  $(tail -1 "$out")")
    tail -25 "$out"
  elif [[ -z "$passed" || "$passed" -lt 1 ]]; then
    REFUSED=$((REFUSED + 1))
    SUMMARY+=("REFUSE sqlalchemy $leg  $name  exit 0 with no pass count: $(tail -1 "$out")")
  else
    SUMMARY+=("PASS  sqlalchemy $leg  $name  $passed passed")
  fi
  rm -f "$out"
}

for leg in "${LEGS[@]}"; do
  venv="$ROOT/.venv-matrix-sa${leg//./}"
  if ! make_venv "$leg" "$venv"; then
    REFUSED=$((REFUSED + 1))
    SUMMARY+=("REFUSE sqlalchemy $leg  venv could not be built")
    continue
  fi
  installed="$("$venv/bin/python" -c 'import sqlalchemy; print(sqlalchemy.__version__)')"
  if [[ "$installed" != "$leg" ]]; then
    REFUSED=$((REFUSED + 1))
    SUMMARY+=("REFUSE sqlalchemy $leg  leg installed $installed instead; its results would describe the wrong version")
    continue
  fi
  run_arm "$leg" "unit" "$venv/bin/python" -m pytest -q -p no:cacheprovider
  # The claim store's own arms: already inside the suites above, run again by name so a
  # leg that silently collected no claim test REFUSES (no pass count) instead of passing.
  run_arm "$leg" "claims-unit" "$venv/bin/python" -m pytest -q -p no:cacheprovider \
    tests/unit/test_sql_claims.py tests/unit/test_once.py -k "sql"
  if have_integration_urls; then
    pg_url="$FACTBUS_PG_URL"
    if [[ "$leg" == 1.4.* ]]; then
      pg_url="${pg_url/+psycopg:/+psycopg2:}"
    fi
    run_arm "$leg" "integration" env FACTBUS_PG_URL="$pg_url" \
      "$venv/bin/python" -m pytest -q -p no:cacheprovider tests/integration
    run_arm "$leg" "claims-integration" env FACTBUS_PG_URL="$pg_url" \
      "$venv/bin/python" -m pytest -q -p no:cacheprovider tests/integration/test_sql_claims.py
  else
    SKIPPED=$((SKIPPED + 2))
    SUMMARY+=("SKIP  sqlalchemy $leg  integration  engine URLs absent (see README)")
    SUMMARY+=("SKIP  sqlalchemy $leg  claims-integration  engine URLs absent (see README)")
  fi
done

printf '%s\n' "${SUMMARY[@]}"
TOTAL=$(( ${#LEGS[@]} * 4 ))
echo "${#LEGS[@]} legs x 4 suites = $TOTAL arms; $RAN executed, $SKIPPED skipped, $FAILED failed, $REFUSED refused"
if [[ $((RAN + SKIPPED + REFUSED)) -lt $TOTAL && $REFUSED -eq 0 ]]; then
  echo "REFUSE: arms unaccounted for"
  exit 2
fi
[[ $FAILED -gt 0 ]] && exit 1
[[ $REFUSED -gt 0 ]] && exit 2
[[ $SKIPPED -gt 0 ]] && exit 3
echo "PASS: every arm executed"
exit 0
