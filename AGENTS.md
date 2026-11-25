# foundry-eda-factbus agent guide

## Land once (owner, 25 November 2026)

- Every change lands once: branch from `origin/develop`, run this repository's checks, open a PR into `develop`, merge it when green, and stop. Never open a PR into `main`, never push `main`, never back-merge.
- `main` is a pointer. It moves only by `bash scripts/promote.sh <sha> --apply` (a fast-forward of a commit already on `develop`), and only when the owner says release (decision D4). It refuses a tree without a green release receipt: it runs `scripts/release-checks.sh` (this repository's full checks) on that commit first, or reuses a receipt another machine recorded on the `receipts` branch; a repository without that file refuses until one lands (`PROMOTE_UNCHECKED="<reason>"` is the recorded escape).
- If `main` ever holds a commit `develop` lacks, land it once by a PR into `develop` that merges `origin/main` (its tree should equal `develop`'s), then promote.
- `.claude/hooks/land-once-guard.py` (Claude PreToolUse) refuses a PR into `main`, a push to it and API file writes to it; Codex follows this section. Hotfix escape: `LAND_ONCE_ALLOW="<reason>"` in the command.
- Plan: `ZGTR/mshaker-alphazed-workspace` `docs/plans/2026-11-23-land-once-branch-flow.md`.
