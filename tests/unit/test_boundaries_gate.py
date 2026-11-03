"""The architecture gate runs green on the real tree and every clause is proved by blinding."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

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
