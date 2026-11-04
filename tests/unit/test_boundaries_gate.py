"""The architecture gate runs green on the real tree and every clause is proved by blinding."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_GATE = Path(__file__).resolve().parents[2] / "scripts" / "check_eventing_boundaries.py"
_spec = importlib.util.spec_from_file_location("check_eventing_boundaries", _GATE)
gate = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = gate  # dataclasses resolve their module through sys.modules
_spec.loader.exec_module(gate)


def test_gate_is_green_on_the_real_tree():
    assert gate.main([]) == 0


def test_every_clause_goes_green_when_blinded():
    assert gate.blinding_table() == 0


def test_k1_real_imports_include_no_contract_package():
    _, stats = gate.analyse(gate.SOURCE)
    assert stats.import_roots["factbus"] >= gate.MIN_INTERNAL_IMPORTS
    assert not any(gate._is_contract_or_product(root) for root in stats.import_roots)


def test_clause_10_finds_no_environment_read_in_src_and_flags_one_when_present(tmp_path):
    """Regression for F1: no factbus module reads the process environment. Positive
    control: the same scan on a copy of src with one `os.environ` read added flags it."""
    findings, _ = gate.analyse(gate.SOURCE)
    assert [f for f in findings if f.clause == 10] == []

    import shutil

    copy = tmp_path / "src"
    shutil.copytree(gate.SOURCE / "factbus", copy / "factbus", ignore=shutil.ignore_patterns("__pycache__"))
    target = copy / "factbus" / "sql_producer.py"
    target.write_text(target.read_text(encoding="utf-8") + '\n\nimport os\nREGION = os.environ.get("AWS_REGION")\n',
                      encoding="utf-8")
    flagged, _ = gate.analyse(copy)
    assert [(f.clause, f.path) for f in flagged if f.clause == 10] == [(10, "factbus/sql_producer.py")]


def test_clause_10_sees_through_an_aliased_os_import(tmp_path):
    package = tmp_path / "src" / "factbus"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "settings.py").write_text(
        'import os as settings\n\n\ndef region():\n    return settings.getenv("AWS_REGION")\n', encoding="utf-8"
    )
    flagged, _ = gate.analyse(tmp_path / "src")
    assert [(f.clause, f.line) for f in flagged] == [(10, 5)]


@pytest.mark.parametrize(
    "source, line",
    [
        ('import os.path\n\n\ndef region():\n    return os.environ.get("AWS_REGION")\n', 5),
        ('import os\n\n\ndef home():\n    return os.path.expanduser("~")\n', 5),
        ('from os import *\n', 1),
        ('from os.path import expandvars\n', 1),
        ('import posix\n\n\ndef region():\n    return posix.environ[b"AWS_REGION"]\n', 5),
        ('import os.path as osp\n\n\ndef home():\n    return osp.expandvars("$HOME")\n', 5),
        ('from os import path\n\n\ndef home():\n    return path.expanduser("~")\n', 5),
        ('from os import path as p2\n\n\ndef home():\n    return p2.expandvars("$HOME")\n', 5),
        ('import posixpath\n\n\ndef home():\n    return posixpath.expandvars("$HOME")\n', 5),
    ],
    ids=[
        "import-os.path", "os.path.expanduser", "from-os-star", "from-os.path-expandvars", "posix",
        "import-os.path-as", "from-os-import-path", "from-os-import-path-as", "posixpath",
    ],
)
def test_clause_10_sees_the_other_ways_to_read_the_environment(tmp_path, source, line):
    """F1 review round 3 (Medium): `import os.path` binds `os`, and the gate missed it.
    Each case is one read the gate must flag."""
    package = tmp_path / "src" / "factbus"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "settings.py").write_text(source, encoding="utf-8")
    flagged, _ = gate.analyse(tmp_path / "src")
    assert [(f.clause, f.line) for f in flagged] == [(10, line)]


def test_clause_10_leaves_os_path_alone_when_it_reads_no_environment(tmp_path):
    """Control for the cases above: `import os.path` and `os.path.join` read nothing."""
    package = tmp_path / "src" / "factbus"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "paths.py").write_text(
        'import os.path\nimport os.path as osp\nfrom os import path\n\n'
        'JOINED = (os.path.join("a", "b"), osp.join("a"), path.join("b"))\n',
        encoding="utf-8",
    )
    flagged, _ = gate.analyse(tmp_path / "src")
    assert flagged == []
