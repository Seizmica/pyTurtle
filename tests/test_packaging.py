"""Tests for how the source tree is shipped to a cluster. No Spark required.

``submit.sh`` bundles ``util/`` and ``jobs/`` into a zip and passes it to
``spark-submit --py-files``. That import path behaves differently from a local
run, so it is exercised here rather than discovered at submission time.
"""

import os
import subprocess
import sys
import zipfile

import pytest

from util.config import REPO_ROOT

PACKAGES = ("util", "jobs")


def _bundle(tmp_path):
    """Build the archive the way submit.sh does."""
    archive = tmp_path / "etl.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        for package in PACKAGES:
            for source in sorted((REPO_ROOT / package).rglob("*.py")):
                if "__pycache__" in source.parts:
                    continue
                bundle.write(source, source.relative_to(REPO_ROOT).as_posix())
    return archive


@pytest.mark.parametrize("package", PACKAGES)
def test_shipped_packages_are_not_namespace_packages(package):
    """zipimport cannot load PEP 420 namespace packages, so these are required."""
    assert (
        REPO_ROOT / package / "__init__.py"
    ).is_file(), f"{package}/__init__.py is missing; a --py-files zip would fail to import"


def test_every_job_imports_from_inside_a_zip(tmp_path):
    """The driver's import path in cluster mode: the zip, and nothing else.

    Run in a subprocess rooted outside the repo, so a module resolved from the
    working directory cannot mask one missing from the archive.
    """
    archive = _bundle(tmp_path)
    jobs = sorted(p.stem for p in (REPO_ROOT / "jobs").glob("*.py") if p.stem != "__init__")
    program = "import util.runner, util.config\n" + "".join(f"import jobs.{job}\n" for job in jobs)

    result = subprocess.run(
        [sys.executable, "-c", program],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(archive)},
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
