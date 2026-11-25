"""Self-test for land-once-guard.py: python3 .claude/hooks/test_land_once_guard.py"""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HOOK = Path(__file__).resolve().parent / "land-once-guard.py"


def checkout(slug: str) -> str:
    root = Path(tempfile.mkdtemp(prefix="land-once-"))
    (root / ".git").mkdir()
    (root / ".git" / "config").write_text(f'[remote "origin"]\n\turl = https://github.com/{slug}.git\n')
    return str(root)


def run(event: dict) -> int:
    return subprocess.run([sys.executable, str(HOOK)], input=json.dumps(event), capture_output=True, text=True).returncode


def bash(command: str, cwd: str | None = None) -> dict:
    return {"tool_name": "Bash", "tool_input": {"command": command}, "cwd": cwd}


class LandOnceGuard(unittest.TestCase):
    def test_blocks(self):
        web = checkout("ZGTR/foundry-webapps")
        for event in (
            {"tool_name": "mcp__github__create_pull_request", "tool_input": {"owner": "ZGTR", "repo": "foundry-webapps", "base": "main"}},
            {"tool_name": "mcp__github__update_pull_request", "tool_input": {"owner": "developeralphazed", "repo": "alphazed-database", "base": "master"}},
            {"tool_name": "mcp__github__push_files", "tool_input": {"owner": "ZGTR", "repo": "foundry-mobileapps", "branch": "main"}},
            bash("gh pr create --base main --fill", web),
            bash("gh pr create -R ZGTR/foundry-rag -B main --fill", checkout("ZGTR/foundry-memory")),
            bash("git push origin HEAD:main", web),
            bash("git push origin main", web),
        ):
            self.assertEqual(run(event), 2, event)

    def test_allows(self):
        web = checkout("ZGTR/foundry-webapps")
        for event in (
            {"tool_name": "mcp__github__create_pull_request", "tool_input": {"owner": "ZGTR", "repo": "foundry-webapps", "base": "develop"}},
            {"tool_name": "mcp__github__create_pull_request", "tool_input": {"owner": "ZGTR", "repo": "not-in-the-map", "base": "main"}},
            bash("gh pr create --base develop --fill", web),
            bash("git push -u origin claude/x", web),
            bash("bash scripts/promote.sh --apply", web),
            bash('LAND_ONCE_ALLOW="hotfix" git push origin hotfix/x:main', web),
            bash("git push origin main", checkout("ZGTR/not-in-the-map")),
            {"tool_name": "Read", "tool_input": {"file_path": "main"}},
        ):
            self.assertEqual(run(event), 0, event)

    def test_malformed_events_fail_open(self):
        for bad in (b"nope", b'"x"', b"[1]"):
            self.assertEqual(subprocess.run([sys.executable, str(HOOK)], input=bad, capture_output=True).returncode, 0)


if __name__ == "__main__":
    unittest.main()
