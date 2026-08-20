"""CLI entry point for the ETL framework."""

from __future__ import annotations

import argparse
import sys
from typing import Any

from .config.loader import load_config
from .core.pipeline import run_pipeline


def _parse_overrides(pairs: list[str]) -> dict[str, Any]:
    """Turn ``--param a.b=c`` pairs into a nested override dict."""
    overrides: dict[str, Any] = {}
    for pair in pairs:
        if "=" not in pair:
            raise SystemExit(f"Invalid --param '{pair}', expected key=value")
        key, value = pair.split("=", 1)
        node = overrides
        parts = key.split(".")
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = value
    return overrides


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etl-run", description="Run an ETL job.")
    parser.add_argument("--config", required=True, help="Path to job config file")
    parser.add_argument("--env", default=None, help="Environment name")
    parser.add_argument("--config-root", default="configs", help="Root dir for base.yaml and env/")
    parser.add_argument(
        "--run-date", default=None, help="Run date, injected as sql_params.run_date"
    )
    parser.add_argument(
        "--param", action="append", default=[], metavar="KEY=VALUE", help="Override config value"
    )
    parser.add_argument(
        "--dry-run", default="false", help="Validate and plan without writing (true/false)"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    overrides = _parse_overrides(args.param)
    if args.run_date:
        overrides.setdefault("sql_params", {})["run_date"] = args.run_date

    cfg = load_config(
        args.config,
        env=args.env,
        config_root=args.config_root,
        overrides=overrides,
    )
    dry_run = args.dry_run.lower() in ("1", "true", "yes")
    run_pipeline(cfg, dry_run=dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
