#!/usr/bin/env python3
"""CLI for the optional keyBy experiment; control and observation live in sibling modules."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence


if __package__:
    from .keyby_common import (
        LabError,
    )
    from .keyby_control import (
        disable_keyby,
        enable_keyby,
        rebuild_flink,
        reset_flink_job,
        stop_with_savepoint,
        submit_keyby,
    )
    from .keyby_evidence import (
        milestone_logs,
        observe_keyby,
        verify_keyby,
    )
else:  # Support python scripts/keyby_lab.py.
    from keyby_common import (
        LabError,
    )
    from keyby_control import (
        disable_keyby,
        enable_keyby,
        rebuild_flink,
        reset_flink_job,
        stop_with_savepoint,
        submit_keyby,
    )
    from keyby_evidence import (
        milestone_logs,
        observe_keyby,
        verify_keyby,
    )


# CLI entry point.
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Operate, switch, restart, and verify the Chapter 8 Flink keyBy lab"
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    savepoint = subparsers.add_parser(
        "savepoint",
        help="drain the one running lab job into a canonical savepoint",
    )
    savepoint.set_defaults(handler=stop_with_savepoint)

    rebuild = subparsers.add_parser(
        "rebuild",
        help="rebuild/recreate only Flink services after proving no job is active",
    )
    rebuild.set_defaults(handler=rebuild_flink)

    submit = subparsers.add_parser(
        "submit",
        help="restore the keyBy-enabled job from a required savepoint",
    )
    submit.add_argument("--savepoint", required=True)
    submit.set_defaults(handler=submit_keyby)

    keyby_on = subparsers.add_parser(
        "keyby-on",
        help="switch the sole running baseline job to the keyBy pipeline",
    )
    keyby_on.set_defaults(handler=enable_keyby)

    keyby_off = subparsers.add_parser(
        "keyby-off",
        help="switch the sole running keyBy job to the baseline pipeline",
    )
    keyby_off.set_defaults(handler=disable_keyby)

    flink_reset = subparsers.add_parser(
        "flink-reset",
        help="restart the sole running job in its current mode from a savepoint",
    )
    flink_reset.set_defaults(handler=reset_flink_job)

    observe = subparsers.add_parser(
        "observe",
        help="print the graph, HASH edge, subtask metrics, and shuffle rates",
    )
    observe.set_defaults(handler=observe_keyby)

    verify = subparsers.add_parser(
        "verify",
        help="read-only acceptance check for the live Chapter 8 experiment",
    )
    verify.set_defaults(handler=verify_keyby)

    logs = subparsers.add_parser(
        "logs",
        help="print a bounded tail of keyed-state milestone entries",
    )
    logs.set_defaults(handler=milestone_logs)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        handler: Callable[[argparse.Namespace], int] = args.handler
        return handler(args)
    except LabError as exc:
        print(f"keyby-lab: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
