#!/usr/bin/env python3
"""factbus's architecture gate: ten clauses over `src/factbus`, read with `ast`.

WHY THIS GATE CARRIES LAYERING. The kit's design meant to reuse a service kit's
`check_layering.py` and `check_size_limits.py` (assumption A7). Measured on this
library on 2026-09-25: both REFUSE, exit 2, because their floors are sized for a
service (layering: "only 15 module(s) examined ..., floor is 20"; size limits: "16
module(s) and 0 handler(s) classified; floors are 20/3"). A7's falsifier names the
fallback, and this is it: clause 7 below.

Clauses (a finding names the clause, file and line):

1. boto3 or botocore imported outside `adapters`.
2. A boto3 client built without an explicit `config=` (a default client re-sends a
   failed request inside the SDK, invisibly; see `adapters/eventbridge.py`).
3. An event-type string literal (`<word>.<word>.<word>`) anywhere in `src`: the kit
   knows no event type.
4. An adapter constructed at module level, or held in a module-level registry
   (service locator). Construction belongs to a composition root.
5. A transport type passed into `domain`: a domain function parameter named for a raw
   SQS or Lambda record.
6. K-1: an import of a contract or product package (`CONTRACT_ROOTS`,
   `PRODUCT_PREFIXES`). The kit routes on three envelope keys and
   imports no contract model.
7. Layer direction: `domain` imports only `domain`; `ports` adds `domain`;
   `application` adds `ports`; `adapters` sees `adapters`, `ports`, `domain`. The inner
   three import no SDK or ORM.
8. `domain` reads a clock (`datetime.now`, `time.monotonic`, ...). The backoff, version
   and breaker rules take time as a parameter; a domain rule that reads the wall clock
   cannot be tested to the second and hides a dependency the composition root owns.
9. `domain`, `ports` or `application` blocks the thread itself (`time.sleep`,
   `asyncio.sleep`). The rate guard's wait goes through the injected `Sleep` port, so a
   test runs a paced batch without waiting and the composition root decides what
   blocking means.
10. Any module reads the process environment (`os.environ`, `os.getenv`, `os.putenv`,
   `os.path.expandvars`/`expanduser`, the same names imported from `os` or `os.path`, a
   star import of either, `posix`/`nt` and `posixpath`/`ntpath`, under any alias,
   `import os.path [as p]` and `from os import path` included). factbus takes every setting (region, endpoint,
   table names) as an argument, so a service's one typed settings object stays the only
   reader of its environment (notifications design plan, slice F1).

Before any verdict on the real tree, the gate runs its fixtures: every offending
fixture must produce exactly the finding for its own clause and nothing else, and the
conforming fixture must produce none. `--blinding` then disables each clause in turn
and prints baseline / blinded / restore for its fixture.

Exit codes: 0 clean, 1 finding, 2 broken instrument.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "src"
FIXTURES = ROOT / "scripts" / "gate_fixtures" / "eventing_boundaries"
PACKAGE = "factbus"

CLAUSES = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

SDK_ROOTS = frozenset({"boto3", "botocore"})
INNER_FORBIDDEN_ROOTS = SDK_ROOTS | frozenset({"sqlalchemy", "alembic", "psycopg", "pymysql"})
#: Example product names; a fork that vendors factbus lists its own product packages here.
CONTRACT_ROOTS = frozenset({"acme_contracts", "acme_events"})
PRODUCT_PREFIXES = ("acme_",)
ALLOWED_LAYERS = {
    "domain": frozenset({"domain"}),
    "ports": frozenset({"ports", "domain"}),
    "application": frozenset({"application", "ports", "domain"}),
    "adapters": frozenset({"adapters", "ports", "domain"}),
}
EVENT_TYPE = re.compile(r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$")
RAW_RECORD_PARAMS = frozenset({"record", "records", "raw_record", "sqs_record", "message", "messages", "lambda_event"})
CONSTRUCTOR_CALLS = frozenset({"client", "resource", "create_engine", "Session"})
CLOCK_MODULES = frozenset({"datetime", "date", "time"})
CLOCK_CALLS = frozenset({"now", "utcnow", "today", "time", "time_ns", "monotonic", "monotonic_ns", "perf_counter"})
SLEEP_MODULES = frozenset({"time", "asyncio"})
INNER_LAYERS = frozenset({"domain", "ports", "application"})
ENV_NAMES = frozenset({"environ", "environb", "getenv", "getenvb", "putenv", "unsetenv"})

#: Floors on the production scan. A scan that saw nothing reports a clean tree it never
#: read. Hand-written.
MIN_MODULES = 12
MIN_INTERNAL_IMPORTS = 10
MIN_CLIENT_CONSTRUCTIONS = 2

#: Each offending fixture's directory name maps to the one clause it must trip.
EXPECTED_OFFENDERS = {
    "c1_sdk_outside_adapters": 1,
    "c2_client_without_config": 2,
    "c3_event_type_literal": 3,
    "c4_module_level_client": 4,
    "c5_raw_record_in_domain": 5,
    "c6a_imports_acme_contracts": 6,
    "c6b_imports_acme_events": 6,
    "c7_domain_imports_application": 7,
    "c8_domain_reads_clock": 8,
    "c9_application_sleeps": 9,
    "c10_reads_environment": 10,
}


@dataclass(frozen=True)
class Finding:
    clause: int
    path: str
    line: int
    detail: str

    def __str__(self) -> str:
        return f"clause {self.clause}: {self.path}:{self.line}: {self.detail}"


@dataclass
class ScanStats:
    modules: int = 0
    internal_imports: int = 0
    client_constructions: int = 0
    import_roots: Counter | None = None


def _layer(module: Path, package_root: Path) -> str | None:
    parts = module.relative_to(package_root).parts
    return parts[0] if len(parts) > 1 and parts[0] in ALLOWED_LAYERS else None


def _imports(tree: ast.AST) -> Iterable[tuple[str, int]]:
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name, node.lineno
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            yield node.module, node.lineno


def _is_contract_or_product(root: str) -> bool:
    if root == PACKAGE:
        return False
    return root in CONTRACT_ROOTS or root.startswith(PRODUCT_PREFIXES)


def _call_name(call: ast.Call) -> str | None:
    func = call.func
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return None


def _reads_clock(call: ast.Call) -> bool:
    """`datetime.now()`, `datetime.datetime.now()`, `time.monotonic()` and the like."""
    func = call.func
    if not isinstance(func, ast.Attribute) or func.attr not in CLOCK_CALLS:
        return False
    base = func.value
    name = base.attr if isinstance(base, ast.Attribute) else base.id if isinstance(base, ast.Name) else None
    return name in CLOCK_MODULES


def _sleeps(call: ast.Call) -> bool:
    """`time.sleep(...)` or `asyncio.sleep(...)`; a call to an injected `sleep` is a Name."""
    func = call.func
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "sleep"
        and isinstance(func.value, ast.Name)
        and func.value.id in SLEEP_MODULES
    )


#: Modules that expose the process environment under the same names as `os`.
ENV_MODULES = frozenset({"os", "posix", "nt"})
#: `os.path` functions that read the environment to expand their argument.
ENV_PATH_NAMES = frozenset({"expandvars", "expanduser"})


def _os_aliases(tree: ast.AST) -> set[str]:
    """Every name bound to the `os` module (or `posix`/`nt`): `import os`, `import os as
    anything`, and `import os.path`, which binds `os` too."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Import):
            continue
        for alias in node.names:
            root = alias.name.split(".", 1)[0]
            if root not in ENV_MODULES:
                continue
            if alias.asname is None:
                names.add(root)
            elif alias.name == root:
                names.add(alias.asname)
    return names


#: Modules that are `os.path` under another name.
PATH_MODULES = frozenset({"posixpath", "ntpath"})


def _path_aliases(tree: ast.AST) -> set[str]:
    """Every name bound to `os.path` itself: `import os.path as p`, `from os import path
    [as p]`, `import posixpath`/`ntpath` (under any alias)."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "os.path" and alias.asname:
                    names.add(alias.asname)
                elif alias.name in PATH_MODULES:
                    names.add(alias.asname or alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module == "os" and node.level == 0:
            names.update(alias.asname or alias.name for alias in node.names if alias.name == "path")
    return names


def _reads_environment(node: ast.AST, os_names: set[str], path_names: frozenset[str] = frozenset()) -> str | None:
    """`os.environ`, `os.getenv(...)` (under any alias of `os`) and the same names imported
    from `os`; `request.environ` is not the process's."""
    if (
        isinstance(node, ast.Attribute)
        and node.attr in ENV_NAMES
        and isinstance(node.value, ast.Name)
        and node.value.id in os_names
    ):
        return f"{node.value.id}.{node.attr}"
    if (
        isinstance(node, ast.Attribute)
        and node.attr in ENV_PATH_NAMES
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "path"
        and isinstance(node.value.value, ast.Name)
        and node.value.value.id in os_names
    ):
        return f"{node.value.value.id}.path.{node.attr}"
    if (
        isinstance(node, ast.Attribute)
        and node.attr in ENV_PATH_NAMES
        and isinstance(node.value, ast.Name)
        and node.value.id in path_names
    ):
        return f"{node.value.id}.{node.attr}"
    if isinstance(node, ast.ImportFrom) and node.module in ENV_MODULES and node.level == 0:
        names = sorted(alias.name for alias in node.names if alias.name in ENV_NAMES | {"*"})
        if names:
            return f"from {node.module} import {', '.join(names)}"
    if isinstance(node, ast.ImportFrom) and node.module in {"os.path"} | PATH_MODULES and node.level == 0:
        names = sorted(alias.name for alias in node.names if alias.name in ENV_PATH_NAMES | {"*"})
        if names:
            return f"from {node.module} import {', '.join(names)}"
    return None


def _adapter_classes(package_root: Path) -> set[str]:
    names: set[str] = set()
    for module in (package_root / "adapters").rglob("*.py"):
        for node in ast.parse(module.read_text(encoding="utf-8")).body:
            if isinstance(node, ast.ClassDef):
                names.add(node.name)
    return names


def _docstring_nodes(tree: ast.AST) -> set[int]:
    ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", [])
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                ids.add(id(body[0].value))
    return ids


def analyse(source_root: Path, *, disabled: frozenset[int] = frozenset()) -> tuple[list[Finding], ScanStats]:
    package_root = source_root / PACKAGE
    if not package_root.is_dir():
        raise FileNotFoundError(f"no {PACKAGE} package under {source_root}")
    adapters = _adapter_classes(package_root) if (package_root / "adapters").is_dir() else set()
    findings: list[Finding] = []
    stats = ScanStats(import_roots=Counter())
    modules = sorted(p for p in package_root.rglob("*.py") if "__pycache__" not in p.parts)
    for module in modules:
        stats.modules += 1
        rel = str(module.relative_to(source_root))
        tree = ast.parse(module.read_text(encoding="utf-8"), filename=rel)
        layer = _layer(module, package_root)

        def add(clause: int, line: int, detail: str, rel: str = rel) -> None:
            if clause not in disabled:
                findings.append(Finding(clause, rel, line, detail))

        for name, line in _imports(tree):
            root = name.split(".")[0]
            stats.import_roots[root] += 1
            if root in SDK_ROOTS and layer != "adapters":
                add(1, line, f"imports {name} outside adapters")
            if _is_contract_or_product(root):
                add(6, line, f"imports {name}; the kit imports no contract or product model (K-1)")
            if root == PACKAGE:
                stats.internal_imports += 1
                target = name.split(".")
                target_layer = target[1] if len(target) > 1 else None
                if layer and target_layer in ALLOWED_LAYERS and target_layer not in ALLOWED_LAYERS[layer]:
                    add(7, line, f"{layer} imports {name}")
            elif layer in ("domain", "ports", "application") and root in INNER_FORBIDDEN_ROOTS:
                add(7, line, f"{layer} imports {name}, an SDK or ORM")

        docstrings = _docstring_nodes(tree)
        os_names = _os_aliases(tree)
        path_names = frozenset(_path_aliases(tree))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings:
                if EVENT_TYPE.match(node.value):
                    add(3, node.lineno, f"event-type literal {node.value!r}")
            if layer == "domain" and isinstance(node, ast.Call) and _reads_clock(node):
                add(8, node.lineno, "domain reads a clock; take `now` as a parameter")
            if layer in INNER_LAYERS and isinstance(node, ast.Call) and _sleeps(node):
                add(9, node.lineno, f"{layer} blocks the thread itself; take a `Sleep` port")
            env_read = _reads_environment(node, os_names, path_names)
            if env_read:
                add(10, node.lineno, f"reads the environment ({env_read}); take the setting as an argument")
            if isinstance(node, ast.Call) and _call_name(node) == "client":
                stats.client_constructions += 1
                if not any(k.arg == "config" for k in node.keywords):
                    add(2, node.lineno, "boto3 client built without config=")
            if layer == "domain" and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for arg in node.args.args + node.args.kwonlyargs:
                    if arg.arg in RAW_RECORD_PARAMS:
                        add(5, node.lineno, f"domain function {node.name}() takes raw {arg.arg!r}")

        for node in tree.body:
            value = node.value if isinstance(node, (ast.Assign, ast.AnnAssign)) else None
            if value is None:
                continue
            calls = [value] if isinstance(value, ast.Call) else [
                n for n in ast.walk(value) if isinstance(n, ast.Call)
            ]
            for call in calls:
                name = _call_name(call)
                if name in CONSTRUCTOR_CALLS or name in adapters:
                    add(4, node.lineno, f"module-level construction of {name}()")
            if isinstance(value, (ast.Dict, ast.List, ast.Tuple, ast.Set)):
                held = {n.id for n in ast.walk(value) if isinstance(n, ast.Name)} & adapters
                if held:
                    add(4, node.lineno, f"module-level registry holds adapter(s) {sorted(held)}")
    return findings, stats


def _fixture_roots() -> tuple[Path, dict[str, Path]]:
    conforming = FIXTURES / "conforming"
    offending = {p.name: p for p in sorted((FIXTURES / "offending").iterdir()) if p.is_dir()}
    return conforming, offending


def run_controls() -> list[str]:
    """Both directions, before any verdict. Returns refusal reasons (empty when sound)."""
    problems: list[str] = []
    conforming, offending = _fixture_roots()
    if set(offending) != set(EXPECTED_OFFENDERS):
        problems.append(
            f"offending fixtures {sorted(offending)} differ from the hand-written set {sorted(EXPECTED_OFFENDERS)}"
        )
        return problems
    found, stats = analyse(conforming)
    if found:
        problems.append(f"the CONFORMING fixture produced findings: {[str(f) for f in found]}")
    if stats.client_constructions < 1:
        problems.append("the conforming fixture holds no boto3 client, so clause 2's quiet side is unproven")
    for name, clause in EXPECTED_OFFENDERS.items():
        found, _ = analyse(offending[name])
        clauses = sorted({f.clause for f in found})
        if clauses != [clause]:
            problems.append(f"fixture {name} must trip exactly clause {clause}; it tripped {clauses}")
    return problems


def blinding_table() -> int:
    """Per clause: baseline (red), clause disabled (must go green), restore (red again)."""
    _, offending = _fixture_roots()
    print("clause | fixture | baseline | blinded | restore | verdict")
    ok = True
    for name, clause in EXPECTED_OFFENDERS.items():
        baseline = len(analyse(offending[name])[0])
        blinded = len(analyse(offending[name], disabled=frozenset({clause}))[0])
        restore = len(analyse(offending[name])[0])
        good = baseline > 0 and blinded == 0 and restore == baseline
        ok &= good
        print(f"{clause} | {name} | {baseline} finding(s) | {blinded} | {restore} | {'PASS' if good else 'FAIL'}")
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--blinding", action="store_true", help="print the per-clause blinding table")
    args = parser.parse_args(argv)
    if args.blinding:
        return blinding_table()

    problems = run_controls()
    if problems:
        for problem in problems:
            print(f"REFUSED: {problem}", file=sys.stderr)
        return 2

    findings, stats = analyse(SOURCE)
    floors = [
        (stats.modules, MIN_MODULES, "module(s) scanned"),
        (stats.internal_imports, MIN_INTERNAL_IMPORTS, "internal import(s) resolved"),
        (stats.client_constructions, MIN_CLIENT_CONSTRUCTIONS, "boto3 client construction(s) seen"),
    ]
    for observed, floor, what in floors:
        if observed < floor:
            print(f"REFUSED: {observed} {what}, floor {floor}; a scan that read nothing is not clean", file=sys.stderr)
            return 2

    roots = ", ".join(f"{root}={n}" for root, n in sorted(stats.import_roots.items()))
    print(f"import roots in src ({sum(stats.import_roots.values())} imports): {roots}")
    if findings:
        for finding in findings:
            print(f"  {finding}", file=sys.stderr)
        print(f"FINDING: {len(findings)} boundary violation(s)", file=sys.stderr)
        return 1
    arms = len(CLAUSES) * 2
    print(
        f"PASS: {stats.modules} module(s), {stats.internal_imports} internal import(s), "
        f"{stats.client_constructions} client construction(s); {len(CLAUSES)} clauses x 2 fixture "
        f"directions = {arms} arms; {arms} executed, 0 skipped, 0 unaccounted "
        f"({len(EXPECTED_OFFENDERS)} offending fixtures, 1 conforming)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
