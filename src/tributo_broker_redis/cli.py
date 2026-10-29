"""Provider-owned validation and production consume CLI."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
from pathlib import Path
from typing import Any

from tributo_broker_redis.config import normalize_config
from tributo_broker_redis.plugin import RedisBrokerPlugin
from tributo_broker_redis.runtime import RedisBrokerRuntime

_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


def _configure_logging(level: str | None) -> None:
    selected = (level or os.environ.get("TRIBUTO_LOG_LEVEL") or "INFO").upper()
    if selected not in _LOG_LEVELS:
        raise ValueError("log level must be one of: " + ", ".join(_LOG_LEVELS))
    logging.basicConfig(
        level=getattr(logging, selected),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger().setLevel(getattr(logging, selected))


def _read_config(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("provider config root must be an object")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Tributo Redis Streams provider")
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate", help="validate provider config")
    validate.add_argument("--config", type=Path, required=True)
    validate.add_argument("--check-connectivity", action="store_true")
    consume = commands.add_parser("consume", help="run the provider consume loop")
    consume.add_argument("--config", type=Path, required=True)
    consume.add_argument("--once", action="store_true")
    consume.add_argument(
        "--log-level",
        choices=_LOG_LEVELS,
        default=None,
        type=str.upper,
        help="consumer log level (default: TRIBUTO_LOG_LEVEL or INFO)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    raw = _read_config(args.config)
    if args.command == "validate":
        RedisBrokerPlugin().validate_config(
            raw,
            check_connectivity=args.check_connectivity,
        )
        print("Redis broker configuration is valid")
        return 0

    _configure_logging(args.log_level)
    if normalize_config(raw).protocol.profile == "aimodel-v2":
        runtime = RedisBrokerPlugin().create_runtime(raw)
    else:
        runtime = RedisBrokerRuntime(normalize_config(raw))

    def _stop(_signum: int, _frame: object) -> None:
        runtime.close()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    try:
        runtime.start()
        if args.once:
            runtime.run_once()
        else:
            runtime.run_forever()
        return 0
    finally:
        runtime.close()


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]
