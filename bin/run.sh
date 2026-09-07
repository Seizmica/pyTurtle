#!/usr/bin/env bash
# Entry point for a deployed bundle. Runs from the release root so the relative
# `configs/` and `sql/` paths in a job config resolve.
#
#   ./bin/run.sh --config configs/jobs/customer_daily.yaml --env prod --run-date 2026-06-28
#
# The engine itself comes from the wheel in wheels/ — install it once per host:
#   pip install wheels/*.whl
# or point PYTHON at an interpreter that already has it.
set -euo pipefail

release_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$release_root"

PYTHON="${PYTHON:-python3}"

if ! "$PYTHON" -c "import etl_framework" >/dev/null 2>&1; then
    echo "etl_framework is not importable by $PYTHON." >&2
    echo "Install the bundled wheel first:  $PYTHON -m pip install $release_root/wheels/*.whl" >&2
    exit 1
fi

exec "$PYTHON" -m etl_framework.main "$@"
