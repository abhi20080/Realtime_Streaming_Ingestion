"""Savepoint-based keyBy mode transitions. Start with transition_pipeline()."""

from __future__ import annotations

import argparse
import re
import shlex
import sys
from collections.abc import Mapping
from typing import Any


if __package__:
    from .keyby_common import (
        COMMAND_TIMEOUT,
        JOB_NAME,
        LabError,
        SAVEPOINT_DIRECTORY,
        WAIT_TIMEOUT,
        _job_id,
        compose_command,
        job_keyby_enabled,
        list_jobs,
        require_exclusive_running_named_job,
        require_no_active_jobs,
        run_command,
    )
else:  # Support python scripts/keyby_lab.py.
    from keyby_common import (
        COMMAND_TIMEOUT,
        JOB_NAME,
        LabError,
        SAVEPOINT_DIRECTORY,
        WAIT_TIMEOUT,
        _job_id,
        compose_command,
        job_keyby_enabled,
        list_jobs,
        require_exclusive_running_named_job,
        require_no_active_jobs,
        run_command,
    )


def savepoint_command(job_id: str) -> list[str]:
    return compose_command(
        "exec",
        "-T",
        "jobmanager",
        "flink",
        "stop",
        "--drain",
        "--type",
        "canonical",
        "--savepointPath",
        SAVEPOINT_DIRECTORY,
        job_id,
    )


def restore_command(
    savepoint_path: str,
    *,
    keyby_enabled: bool,
    allow_non_restored_state: bool = False,
) -> list[str]:
    if keyby_enabled and allow_non_restored_state:
        raise LabError(
            "--allowNonRestoredState is permitted only when switching from "
            "the keyBy graph to the baseline graph"
        )
    flink_arguments = ["flink", "run", "-d"]
    if allow_non_restored_state:
        flink_arguments.append("--allowNonRestoredState")
    flink_arguments.extend(
        ["-s", savepoint_path, "-py", "/opt/flink/usrlib/job.py"]
    )
    return compose_command(
        "exec",
        "-T",
        "-e",
        f"ENABLE_KEYBY_LAB={'true' if keyby_enabled else 'false'}",
        "jobmanager",
        *flink_arguments,
    )


def rebuild_commands() -> tuple[list[str], ...]:
    wait = str(WAIT_TIMEOUT)
    return (
        compose_command("build", "jobmanager", "taskmanager"),
        compose_command(
            "up", "-d", "--force-recreate", "--no-deps", "--wait",
            "--wait-timeout", wait, "jobmanager",
        ),
        compose_command(
            "up", "-d", "--force-recreate", "--no-deps", "--wait",
            "--wait-timeout", wait, "taskmanager",
        ),
    )


# Savepoint-based mode changes.
def parse_savepoint_path(output: str) -> str:
    """Extract the completed savepoint URI from Flink CLI output."""

    matches = re.findall(
        r"Savepoint completed\.\s*Path:\s*([^\s]+)",
        output,
        flags=re.IGNORECASE,
    )
    if matches:
        return matches[-1].rstrip(".,;\"'")
    raise LabError(
        "Flink reported a successful stop, but its output contained no savepoint path"
    )


def create_drained_savepoint(job: Mapping[str, Any]) -> str:
    result = run_command(
        savepoint_command(_job_id(job)),
        capture=True,
        timeout=COMMAND_TIMEOUT,
    )
    combined = "\n".join(value for value in (result.stdout, result.stderr) if value)
    if combined.strip():
        print(combined.rstrip(), file=sys.stderr)
    return parse_savepoint_path(combined)


def stop_with_savepoint(_args: argparse.Namespace) -> int:
    job = require_exclusive_running_named_job(
        list_jobs(), JOB_NAME
    )
    path = create_drained_savepoint(job)
    # A single shell-assignment line makes this safe to capture and reuse:
    #   eval "$(make --no-print-directory keyby-savepoint 2>/dev/null)"
    print(f"SAVEPOINT={path}")
    return 0


def rebuild_flink_services() -> None:
    require_no_active_jobs(list_jobs())
    for command in rebuild_commands():
        run_command(command)


def rebuild_flink(_args: argparse.Namespace) -> int:
    rebuild_flink_services()
    print("Rebuilt and recreated only jobmanager and taskmanager; named volumes were retained.")
    return 0


def submit_keyby(args: argparse.Namespace) -> int:
    savepoint = args.savepoint.strip()
    if not savepoint:
        raise LabError("a non-empty savepoint path is required")
    require_no_active_jobs(list_jobs())
    run_command(restore_command(savepoint, keyby_enabled=True))
    return 0


def transition_pipeline(target_keyby: bool | None) -> int:
    """Switch mode, or restart the current mode when target_keyby is None."""

    job = require_exclusive_running_named_job(
        list_jobs(), JOB_NAME
    )
    current_keyby = job_keyby_enabled(job)
    desired_keyby = current_keyby if target_keyby is None else target_keyby

    if target_keyby is not None and current_keyby == desired_keyby:
        mode = "keyBy" if desired_keyby else "baseline"
        print(f"The {mode} pipeline is already running; no restart was performed.")
        return 0

    savepoint = create_drained_savepoint(job)
    # Print recovery information before any build or submission can fail.
    print(f"SAVEPOINT={savepoint}")

    discard_unmapped_state = current_keyby and not desired_keyby
    restore = restore_command(
        savepoint,
        keyby_enabled=desired_keyby,
        allow_non_restored_state=discard_unmapped_state,
    )
    print(
        "Recovery command if a later step fails: " + shlex.join(restore),
        file=sys.stderr,
    )
    if discard_unmapped_state:
        print(
            "WARNING: Flink's --allowNonRestoredState is required to remove "
            "the keyBy operator. In this controlled lab transition, its "
            "tenant-record-count state is intentionally not restored; the "
            "savepoint itself remains retained.",
            file=sys.stderr,
        )

    rebuild_flink_services()
    require_no_active_jobs(list_jobs())
    run_command(restore)

    action = "Restarted" if target_keyby is None else "Switched to"
    mode = "keyBy" if desired_keyby else "baseline"
    print(
        f"{action} the {mode} pipeline from {savepoint}; Kafka source "
        "positions were restored and named volumes were retained."
    )
    return 0


def enable_keyby(_args: argparse.Namespace) -> int:
    return transition_pipeline(True)


def disable_keyby(_args: argparse.Namespace) -> int:
    return transition_pipeline(False)


def reset_flink_job(_args: argparse.Namespace) -> int:
    return transition_pipeline(None)


