#!/usr/bin/env bash
# promote.sh: move the release branch (main or master) to a commit already on `develop`, by
# fast-forward, and only when that commit's TREE passed this repository's release checks.
# Land-once flow (ZGTR/mshaker-alphazed-workspace docs/plans/2026-11-23-land-once-branch-flow.md):
# every change lands once, by PR into `develop`; the release branch is a pointer that only this
# script moves. Releases are OWNER-TRIGGERED (decision D4 = d): run it only when the owner says release.
#
#   bash scripts/promote.sh [<rev>]             dry run: what would move, and whether a receipt exists
#   bash scripts/promote.sh [<rev>] --check     run the release checks on <rev> and record a receipt
#   bash scripts/promote.sh [<rev>] --apply     promote; runs the checks first if no green receipt exists
#
# The release checks are `scripts/release-checks.sh` AT THAT COMMIT, run in a fresh worktree next to
# this checkout (so sibling checkouts resolve as they do here). A green run is recorded as
# release/<tree>.json on the `receipts` branch, so any machine can promote a tree another one checked.
# A tree identical to the release branch's needs no checks. A repository without release-checks.sh
# refuses; PROMOTE_UNCHECKED="<reason>" promotes anyway and records the reason in the receipt.
#
# Exit 0 promoted, checked, already there, or a dry run; 1 the checks failed; 2 refused.
set -euo pipefail
rev=""; mode=dry
for a in "$@"; do case "$a" in --apply) mode=apply ;; --check) mode=check ;; -*) echo "unknown flag $a" >&2; exit 2 ;; *) rev="$a" ;; esac; done
rel=$(git ls-remote --heads origin main master | sed 's#.*refs/heads/##' | head -1)
[ -n "$rel" ] || { echo "[promote] no main or master on origin" >&2; exit 2; }
git fetch -q --no-tags origin "+refs/heads/develop:refs/remotes/origin/develop" "+refs/heads/$rel:refs/remotes/origin/$rel"
git fetch -q --no-tags origin "+refs/heads/receipts:refs/remotes/origin/receipts" 2>/dev/null || true
sha=$(git rev-parse --verify "${rev:-origin/develop}^{commit}")
cur=$(git rev-parse "origin/$rel")
tree=$(git rev-parse "$sha^{tree}")
receipt="release/$tree.json"

has_receipt() { git cat-file -p "origin/receipts:$receipt" 2>/dev/null | grep -qE '"(exit": 0|unchecked": ")'; }

record() { # record <json>: add release/<tree>.json to the receipts branch (one retry on a race)
  local body="$1" i blob parent idx newtree commit
  for i in 1 2; do
    git fetch -q --no-tags origin "+refs/heads/receipts:refs/remotes/origin/receipts" 2>/dev/null || true
    blob=$(printf '%s\n' "$body" | git hash-object -w --stdin)
    idx=$(mktemp); rm -f "$idx"
    parent=$(git rev-parse -q --verify origin/receipts 2>/dev/null || true)
    if [ -n "$parent" ]; then GIT_INDEX_FILE=$idx git read-tree "$parent"; fi
    GIT_INDEX_FILE=$idx git update-index --add --cacheinfo "100644,$blob,$receipt"
    newtree=$(GIT_INDEX_FILE=$idx git write-tree); rm -f "$idx"
    commit=$(git -c user.name="${GIT_AUTHOR_NAME:-promote}" -c user.email="${GIT_AUTHOR_EMAIL:-promote@localhost}" \
      commit-tree "$newtree" ${parent:+-p "$parent"} -m "release receipt ${tree:0:12}")
    if git push -q --no-verify origin "$commit:refs/heads/receipts" 2>/dev/null; then
      git fetch -q --no-tags origin "+refs/heads/receipts:refs/remotes/origin/receipts"; return 0; fi
  done
  echo "[promote] could not publish $receipt to the receipts branch" >&2; return 2
}

run_checks() {
  if ! git cat-file -e "$sha:scripts/release-checks.sh" 2>/dev/null; then
    if [ -n "${PROMOTE_UNCHECKED:-}" ]; then
      echo "[promote] no scripts/release-checks.sh; PROMOTE_UNCHECKED: $PROMOTE_UNCHECKED"
      record "$(printf '{"tree": "%s", "commit": "%s", "unchecked": "%s", "at": "%s"}' "$tree" "$sha" "${PROMOTE_UNCHECKED//\"/\'}" "$(date -u +%FT%TZ)")"
      return 0; fi
    echo "[promote] REFUSED: ${sha:0:12} has no scripts/release-checks.sh, so nothing can say its tree is green." >&2
    echo "[promote] Land one by PR into develop (this repository's full checks), or set PROMOTE_UNCHECKED=\"<reason>\"." >&2
    return 2; fi
  local top wt code
  top=$(git rev-parse --show-toplevel)
  wt="$(dirname "$top")/.release-$(basename "$top")-${tree:0:12}"
  rm -rf "$wt"; git worktree prune
  git worktree add -q --detach "$wt" "$sha"
  echo "[promote] running scripts/release-checks.sh on ${sha:0:12} in $wt"
  set +e; (cd "$wt" && bash scripts/release-checks.sh); code=$?; set -e
  git worktree remove --force "$wt" || true
  if [ "$code" != 0 ]; then echo "[promote] release checks FAILED (exit $code) on ${sha:0:12}: not promoted." >&2; return 1; fi
  record "$(printf '{"tree": "%s", "commit": "%s", "exit": 0, "checks": "scripts/release-checks.sh", "at": "%s"}' "$tree" "$sha" "$(date -u +%FT%TZ)")"
  echo "[promote] release checks green; receipt $receipt recorded."
}

if [ "$mode" = check ]; then run_checks; exit $?; fi
if [ "$sha" = "$cur" ]; then echo "[promote] $rel is already at ${sha:0:12}."; exit 0; fi
if ! git merge-base --is-ancestor "$sha" origin/develop; then
  echo "[promote] REFUSED: ${sha:0:12} is not on develop. Only commits that landed on develop are promoted." >&2; exit 2; fi
if ! git merge-base --is-ancestor "$cur" "$sha"; then
  echo "[promote] REFUSED: $rel has $(git rev-list --count "$sha..$cur") commit(s) that ${sha:0:12} lacks: not a fast-forward." >&2
  echo "[promote] Reconcile once: a PR into develop that merges origin/$rel, then promote." >&2; exit 2; fi
echo "[promote] $rel: ${cur:0:12} -> ${sha:0:12} (fast-forward, $(git rev-list --count "$cur..$sha") commit(s))"
if [ "$tree" = "$(git rev-parse "$cur^{tree}")" ]; then echo "[promote] same tree as $rel: no checks needed."
elif has_receipt; then echo "[promote] receipt $receipt found on the receipts branch."
elif [ "$mode" = apply ]; then run_checks || exit $?
else echo "[promote] no receipt for tree ${tree:0:12} yet: --apply will run scripts/release-checks.sh first."; fi
if [ "$mode" != apply ]; then echo "[promote] dry run. Re-run with --apply to push."; exit 0; fi
git push --no-verify origin "$sha:refs/heads/$rel"
live=$(git ls-remote origin "refs/heads/$rel" | cut -f1)
[ "$live" = "$sha" ] || { echo "[promote] read-back mismatch: origin $rel is $live" >&2; exit 2; }
echo "[promote] done: origin $rel = $sha."
