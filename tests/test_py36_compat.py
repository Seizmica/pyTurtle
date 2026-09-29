"""Guards that keep this branch runnable on Python 3.6.

The gateway that submits these jobs is pinned to 3.6, so `util/` and `jobs/`
must at least parse and import there -- enough to load config and validate a
job, which is the part that does not need PySpark.

These run on any interpreter: they inspect the source rather than execute it.
"""

import ast

import pytest

from util.config import REPO_ROOT

TARGET = (3, 6)
SOURCES = sorted(
    path
    for package in ("util", "jobs")
    for path in (REPO_ROOT / package).rglob("*.py")
    if "__pycache__" not in path.parts
)

# Present in the language but absent from 3.6's standard library.
BANNED_IMPORTS = {"dataclasses", "contextvars"}
# Parse fine, then fail at runtime on 3.6.
BANNED_ATTRIBUTES = {("re", "Match"), ("re", "Pattern")}
BANNED_KEYWORDS = {("subprocess", "run"): {"capture_output", "text"}}


def _tree(path):
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
def test_source_parses_as_python_36(path):
    """Catches the syntax 3.6 does not have: walrus, positional-only, f-string =."""
    try:
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path), feature_version=TARGET)
    except SyntaxError as exc:
        pytest.fail(f"{path.name}:{exc.lineno} is not valid 3.6 syntax: {exc.msg}")


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
def test_no_future_annotations_import(path):
    """PEP 563 is 3.7+. Annotations are quoted by hand here instead."""
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.ImportFrom) and node.module == "__future__":
            assert not any(
                a.name == "annotations" for a in node.names
            ), f"{path.name}:{node.lineno}"


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
def test_runtime_annotations_are_quoted(path):
    """Without PEP 563, a signature annotation is evaluated at def time.

    Quoting keeps `dict[str, str]`, `X | None` and TYPE_CHECKING-only names
    from ever being looked up, which is what makes them safe on 3.6. Local
    variable annotations are exempt: PEP 526 never evaluates those.
    """
    offenders = []

    def check(node, where):
        if node is None or (isinstance(node, ast.Constant) and isinstance(node.value, str)):
            return
        offenders.append(f"{path.name}:{node.lineno} ({where})")

    class Walker(ast.NodeVisitor):
        def __init__(self):
            self.depth = 0

        def visit_FunctionDef(self, node):
            args = node.args
            groups = [args.args, args.kwonlyargs, getattr(args, "posonlyargs", [])]
            for group in groups:
                for arg in group:
                    check(arg.annotation, f"argument {arg.arg}")
            for extra in (args.vararg, args.kwarg):
                if extra is not None:
                    check(extra.annotation, f"argument {extra.arg}")
            check(node.returns, f"return of {node.name}")
            self.depth += 1
            for child in node.body:
                self.visit(child)
            self.depth -= 1

        visit_AsyncFunctionDef = visit_FunctionDef

        def visit_AnnAssign(self, node):
            if self.depth == 0:
                check(node.annotation, "class or module attribute")
            self.generic_visit(node)

    Walker().visit(_tree(path))
    assert not offenders, "unquoted annotations: " + ", ".join(offenders)


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
def test_no_post_36_stdlib(path):
    """Imports, attributes and keyword arguments 3.6 does not have."""
    problems = []
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] in BANNED_IMPORTS:
                    problems.append(f"line {node.lineno}: import {alias.name}")
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module.split(".")[0] in BANNED_IMPORTS:
                problems.append(f"line {node.lineno}: from {node.module}")
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            if (node.value.id, node.attr) in BANNED_ATTRIBUTES:
                problems.append(f"line {node.lineno}: {node.value.id}.{node.attr}")
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                banned = BANNED_KEYWORDS.get((func.value.id, func.attr), set())
                for keyword in node.keywords:
                    if keyword.arg in banned:
                        problems.append(f"line {node.lineno}: {func.attr}({keyword.arg}=)")

    assert not problems, f"{path.name} uses post-3.6 APIs: " + ", ".join(problems)
