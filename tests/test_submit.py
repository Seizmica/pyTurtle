"""How submit.sh passes the optional run date through to the job.

spark-submit and zip are replaced by stubs, so this needs bash but no Spark.
"""

import os
import shutil
import stat
import subprocess

import pytest

from util.config import REPO_ROOT

pytestmark = pytest.mark.skipif(
    os.name == "nt" or shutil.which("bash") is None,
    reason="needs a POSIX bash; on Windows `bash` may resolve to WSL",
)


def _stub(path, body):
    path.write_text("#!/usr/bin/env bash\n" + body + "\n", encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


@pytest.fixture
def submit(tmp_path):
    bin_dir = tmp_path / "bin"
    spark_home = tmp_path / "spark"
    (spark_home / "bin").mkdir(parents=True)
    bin_dir.mkdir()
    # The argv after the job script is what the job's argparse sees.
    _stub(spark_home / "bin" / "spark-submit", 'printf "%s\\n" "$@"')
    _stub(bin_dir / "zip", "exit 0")

    def run(*args):
        result = subprocess.run(
            ["bash", str(REPO_ROOT / "submit.sh"), *args],
            env=dict(
                os.environ,
                SPARK_HOME=str(spark_home),
                PATH="{}{}{}".format(bin_dir, os.pathsep, os.environ["PATH"]),
            ),
            # capture_output= and text= are 3.7+; these are the 3.6 spellings.
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        assert result.returncode == 0, result.stderr
        argv = result.stdout.splitlines()
        return argv[argv.index("jobs/customer.py") + 1 :]

    return run


def test_an_explicit_run_date_is_passed_through(submit):
    assert submit("customer", "dev", "2026-06-28") == ["--env", "dev", "--run-date", "2026-06-28"]


def test_the_run_date_can_be_left_out(submit):
    assert submit("customer", "dev") == ["--env", "dev"]


def test_extra_args_without_a_run_date_are_not_mistaken_for_one(submit):
    assert submit("customer", "dev", "--dry-run") == ["--env", "dev", "--dry-run"]


def test_a_run_date_and_extra_args_together(submit):
    assert submit("customer", "dev", "2026-06-28", "--dry-run") == [
        "--env",
        "dev",
        "--run-date",
        "2026-06-28",
        "--dry-run",
    ]
