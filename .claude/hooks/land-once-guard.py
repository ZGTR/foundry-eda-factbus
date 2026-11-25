#!/usr/bin/env python3
"""PreToolUse guard: nothing lands on a land-once repository's release branch except promotion.

Land-once (owner, 2026-11-24/25; ZGTR/mshaker-alphazed-workspace docs/plans/2026-11-23-land-once-branch-flow.md):
every change lands once, by PR into `develop`; `main`/`master` moves only by the repository's promote script,
when the owner says release (D4 = d). Keyed on the TARGET repository, so it holds whichever repository the
session started in: GitHub MCP owner/repo, `gh ... -R owner/repo`, or this checkout's `origin` (read from the
git config file). The harness's hooks/trunk_pr_guard.py carries the same map for local sessions.

Blocks (exit 2, reason to the agent): a PR into the release branch (create, or retarget), a `git push` whose
destination is the release branch, and API file writes to it. The promote script's own push is not a tool call.
Hotfix escape, with a reason the transcript keeps: LAND_ONCE_ALLOW="<reason>" in the command.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import sys
from pathlib import Path

# repository (lower case) -> release branch
LAND_ONCE = {
    "zgtr/foundry-mobileapps": "main", "developeralphazed/alphazed-alqosh": "master",
    "zgtr/alphazed-printables": "master", "zgtr/foundry-abstract-graph": "main", "zgtr/foundry-abstract-mcp": "main",
    "zgtr/foundry-agent": "main", "zgtr/foundry-agent-oss": "main", "zgtr/foundry-agent-toolkit": "main",
    "zgtr/foundry-assets": "main", "zgtr/foundry-data-gateway": "main", "zgtr/foundry-design": "main",
    "zgtr/foundry-eda-factbus": "main", "zgtr/foundry-eda-notifications": "main",
    "zgtr/foundry-eda-notifications-email": "main", "zgtr/foundry-marketer-and-salesperson": "main",
    "zgtr/foundry-marketing-websites": "main", "zgtr/foundry-memory": "main", "zgtr/foundry-rag": "main",
    "zgtr/foundry-research": "main", "zgtr/foundry-service-template": "main", "zgtr/foundry-webapps": "main",
    "zgtr/foundry-wingman": "main", "zgtr/foundry-workflow": "main", "zgtr/mshaker-alphazed-workspace": "main",
    "zgtr/mshaker-foundry-services": "main", "developeralphazed/alphazed-content-graph": "main",
    "developeralphazed/alphazed-database": "master",
}
PROMOTE = {"zgtr/foundry-mobileapps": "npm run promote:main -- <sha> --apply",
           "developeralphazed/alphazed-alqosh": "python3 scripts/ci/promote.py <sha> --apply"}
REMOTE = re.compile(r"[:/]([^/:\s]+)/([^/\s]+?)(?:\.git)?/?$")


def origin_repo(cwd: str | None) -> str | None:
    """owner/repo of this checkout's `origin`, from the git config file (worktrees via commondir)."""
    try:
        here = Path(cwd or os.getcwd()).resolve()
        for d in (here, *here.parents):
            dot = d / ".git"
            if dot.is_dir():
                gitdir = dot
            elif dot.is_file():
                text = dot.read_text(encoding="utf-8", errors="replace").strip()
                if not text.startswith("gitdir:"):
                    return None
                gitdir = (d / text.split(":", 1)[1].strip()).resolve()
            else:
                continue
            if (gitdir / "commondir").is_file():
                gitdir = (gitdir / (gitdir / "commondir").read_text(encoding="utf-8").strip()).resolve()
            inside = False
            for line in (gitdir / "config").read_text(encoding="utf-8", errors="replace").splitlines():
                line = line.strip()
                if line.startswith("["):
                    inside = line.replace(" ", "") == '[remote"origin"]'
                elif inside and line.startswith("url"):
                    m = REMOTE.search(line.split("=", 1)[1].strip())
                    return f"{m.group(1)}/{m.group(2)}" if m else None
            return None
    except OSError:
        return None
    return None


def refusal(repo: str | None, branch: str | None, how: str) -> str | None:
    key = (repo or "").lower()
    rel = LAND_ONCE.get(key)
    if not rel or (branch or "").rsplit(":", 1)[-1].removeprefix("refs/heads/").lstrip("+") != rel:
        return None
    promote = PROMOTE.get(key, "bash scripts/promote.sh <sha> --apply")
    return (f"land-once: {how} targets `{rel}` in {repo}. Land the change once, by a PR into `develop`. "
            f"`{rel}` moves only by `{promote}` (a fast-forward), and only when the owner says release. "
            f"Hotfix only: LAND_ONCE_ALLOW=\"<reason>\" in the command.")


def bash(command: str, cwd: str | None) -> str | None:
    if "LAND_ONCE_ALLOW=" in command:
        return None
    for part in re.split(r"&&|\|\||;|\n|\|", command):
        try:
            t = shlex.split(part)
        except ValueError:
            t = part.split()
        if len(t) >= 3 and t[0].rsplit("/", 1)[-1] == "gh" and t[1] == "pr" and t[2] in ("create", "edit"):
            repo = base = None
            for i, w in enumerate(t):
                nxt = t[i + 1] if i + 1 < len(t) else None
                if w in ("-R", "--repo"):
                    repo = nxt
                elif w.startswith("--repo="):
                    repo = w.split("=", 1)[1]
                elif w in ("-B", "--base"):
                    base = nxt
                elif w.startswith("--base="):
                    base = w.split("=", 1)[1]
            reason = refusal(repo or origin_repo(cwd), base, f"this `gh pr {t[2]}`")
            if reason:
                return reason
        if "git" in t and "push" in t[t.index("git"):]:
            args = t[t.index("push", t.index("git")) + 1:]
            for ref in [a for a in args if not a.startswith("-")][1:]:
                reason = refusal(origin_repo(cwd), ref.split(":", 1)[-1], "this `git push`")
                if reason:
                    return reason
    return None


def violation(event: dict) -> str | None:
    tool = str(event.get("tool_name") or "")
    inp = event.get("tool_input") or {}
    if tool == "Bash":
        return bash(str(inp.get("command") or ""), event.get("cwd"))
    repo = f"{inp.get('owner', '')}/{inp.get('repo', '')}"
    if tool.endswith(("__create_pull_request", "__update_pull_request")):
        return refusal(repo, str(inp.get("base") or ""), "this pull request")
    if tool.endswith(("__push_files", "__create_or_update_file", "__delete_file")):
        return refusal(repo, str(inp.get("branch") or ""), "this file write")
    return None


def main() -> int:
    try:
        event = json.load(sys.stdin)
        reason = violation(event) if isinstance(event, dict) else None
    except (ValueError, TypeError, AttributeError):
        return 0  # never trap a session on a malformed event
    if reason:
        print(reason, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
