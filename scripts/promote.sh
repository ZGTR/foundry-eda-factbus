#!/usr/bin/env bash
# promote.sh: move the release branch (main or master) to a commit already on `develop`, by
# fast-forward. Land-once flow (ZGTR/mshaker-alphazed-workspace
# docs/plans/2026-11-23-land-once-branch-flow.md): every change lands once, by PR into `develop`;
# the release branch is a pointer that only this script moves. Releases are OWNER-TRIGGERED
# (decision D4 = d, 24 November 2026): run it only when the owner says release, after this
# repository's own release gate passed on that commit (AGENTS.md, "Land once").
#
#   bash scripts/promote.sh                  dry run: promote origin/develop's tip
#   bash scripts/promote.sh <rev> --apply    push it (a plain push: git refuses a non-fast-forward)
#
# Exit 0 promoted, already there, or a dry run that would promote; 2 refused.
set -euo pipefail
rev=""; apply=0
for a in "$@"; do case "$a" in --apply) apply=1 ;; -*) echo "unknown flag $a" >&2; exit 2 ;; *) rev="$a" ;; esac; done
rel=$(git ls-remote --heads origin main master | sed 's#.*refs/heads/##' | head -1)
[ -n "$rel" ] || { echo "[promote] no main or master on origin" >&2; exit 2; }
git fetch -q --no-tags origin "+refs/heads/develop:refs/remotes/origin/develop" "+refs/heads/$rel:refs/remotes/origin/$rel"
sha=$(git rev-parse --verify "${rev:-origin/develop}^{commit}")
cur=$(git rev-parse "origin/$rel")
if [ "$sha" = "$cur" ]; then echo "[promote] $rel is already at ${sha:0:12}."; exit 0; fi
if ! git merge-base --is-ancestor "$sha" origin/develop; then
  echo "[promote] REFUSED: ${sha:0:12} is not on develop. Only commits that landed on develop are promoted." >&2; exit 2; fi
if ! git merge-base --is-ancestor "$cur" "$sha"; then
  echo "[promote] REFUSED: $rel has $(git rev-list --count "$sha..$cur") commit(s) that ${sha:0:12} lacks: not a fast-forward." >&2
  echo "[promote] Reconcile once: a PR into develop that merges origin/$rel, then promote." >&2; exit 2; fi
echo "[promote] $rel: ${cur:0:12} -> ${sha:0:12} (fast-forward, $(git rev-list --count "$cur..$sha") commit(s))"
if [ "$apply" != 1 ]; then echo "[promote] dry run. Re-run with --apply to push."; exit 0; fi
git push --no-verify origin "$sha:refs/heads/$rel"
live=$(git ls-remote origin "refs/heads/$rel" | cut -f1)
[ "$live" = "$sha" ] || { echo "[promote] read-back mismatch: origin $rel is $live" >&2; exit 2; }
echo "[promote] done: origin $rel = $sha."
