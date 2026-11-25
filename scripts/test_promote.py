"""scripts/promote.sh against real throwaway repos: a bare origin plus clones.

Run: python3 scripts/test_promote.py (or pytest). Every push, refusal and receipt is git's own behaviour.
"""
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "promote.sh"
ENV = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t",
       "GIT_COMMITTER_EMAIL": "t@t", "GIT_CONFIG_NOSYSTEM": "1"}
ENV.pop("PROMOTE_UNCHECKED", None)


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, env=ENV, capture_output=True, text=True, check=True).stdout.strip()


def promote(cwd, *args, **env):
    return subprocess.run(["bash", str(SCRIPT), *args], cwd=cwd, env={**ENV, **env}, capture_output=True, text=True)


class Promote(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="promote-"))
        self.origin = self.root / "origin.git"
        git(self.root, "init", "-q", "--bare", "-b", "develop", str(self.origin))
        self.a = self.root / "a"
        git(self.root, "clone", "-q", str(self.origin), str(self.a))
        (self.a / "f.txt").write_text("one\n")
        git(self.a, "add", ".")
        git(self.a, "commit", "-q", "-m", "one")
        git(self.a, "push", "-q", "origin", "HEAD:refs/heads/develop", "HEAD:refs/heads/main")
        self.b = self.root / "b"
        git(self.root, "clone", "-q", str(self.origin), str(self.b))
        self.marker = self.root / "checks-ran"

    def land(self, files):
        git(self.a, "fetch", "-q", "origin")
        git(self.a, "checkout", "-q", "-B", "work", "origin/develop")
        for name, text in files.items():
            path = self.a / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        git(self.a, "add", ".")
        git(self.a, "commit", "-q", "-m", "change")
        git(self.a, "push", "-q", "origin", "HEAD:refs/heads/develop")
        return git(self.a, "rev-parse", "HEAD")

    def checks(self, code):
        return f'echo ran >> "{self.marker}"\nexit {code}\n'

    def main(self):
        return git(self.a, "ls-remote", "origin", "refs/heads/main").split()[0]

    def test_same_tree_promotes_without_checks(self):
        git(self.a, "commit", "-q", "--allow-empty", "-m", "bookkeeping")
        git(self.a, "push", "-q", "origin", "HEAD:refs/heads/develop")
        r = promote(self.b, "--apply")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.main(), git(self.a, "rev-parse", "HEAD"))

    def test_no_release_checks_refuses_and_unchecked_is_recorded(self):
        before = self.main()
        sha = self.land({"f.txt": "two\n"})
        r = promote(self.b, "--apply")
        self.assertEqual(r.returncode, 2)
        self.assertIn("no scripts/release-checks.sh", r.stderr)
        self.assertEqual(self.main(), before)
        r = promote(self.b, "--apply", PROMOTE_UNCHECKED="docs-only hotfix")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.main(), sha)
        tree = git(self.a, "rev-parse", f"{sha}^{{tree}}")
        git(self.a, "fetch", "-q", "origin", "+refs/heads/receipts:refs/remotes/origin/receipts")
        self.assertIn("docs-only hotfix", git(self.a, "show", f"origin/receipts:release/{tree}.json"))

    def test_red_checks_block_and_record_nothing(self):
        before = self.main()
        self.land({"f.txt": "two\n", "scripts/release-checks.sh": self.checks(1)})
        r = promote(self.b, "--apply")
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertEqual(self.main(), before)
        self.assertTrue(self.marker.exists(), "the checks really ran")
        self.assertEqual(git(self.a, "ls-remote", "origin", "refs/heads/receipts"), "")

    def test_a_receipt_from_one_clone_lets_another_promote_without_rerunning(self):
        sha = self.land({"f.txt": "two\n", "scripts/release-checks.sh": self.checks(0)})
        r = promote(self.a, sha, "--check")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(self.marker.read_text().count("ran"), 1)
        r = promote(self.b, sha)  # dry run sees the receipt
        self.assertIn("receipt release/", r.stdout)
        r = promote(self.b, sha, "--apply")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(self.marker.read_text().count("ran"), 1, "the receipt was trusted, no second run")
        self.assertEqual(self.main(), sha)

    def test_apply_runs_missing_checks_then_promotes(self):
        sha = self.land({"f.txt": "two\n", "scripts/release-checks.sh": self.checks(0)})
        r = promote(self.b, "--apply")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertTrue(self.marker.exists())
        self.assertEqual(self.main(), sha)
        self.assertEqual([p for p in self.root.iterdir() if p.name.startswith(".release-")], [], "worktree removed")

    def test_refuses_a_commit_not_on_develop(self):
        git(self.a, "checkout", "-q", "-b", "side")
        (self.a / "f.txt").write_text("side\n")
        git(self.a, "commit", "-q", "-am", "side")
        r = promote(self.a, git(self.a, "rev-parse", "HEAD"), "--apply", PROMOTE_UNCHECKED="x")
        self.assertEqual(r.returncode, 2)
        self.assertIn("not on develop", r.stderr)


if __name__ == "__main__":
    unittest.main()
