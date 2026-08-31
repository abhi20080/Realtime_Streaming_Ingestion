#!/usr/bin/env python3
"""Operate, switch, restart, and verify the Chapter 8 ``keyBy`` lab.

The control commands deliberately validate Flink's REST state before changing
anything.  Observation and verification are read-only and use Flink's REST API
plus Prometheus; no Python packages outside the standard library are required.

This script is control-plane code, not part of the record path. Read
``transition_pipeline`` for mode changes and ``verify_keyby`` for acceptance.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import shlex
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


# Fixed contract for this repository's keyBy experiment.
ROOT = Path(__file__).resolve().parents[1]
FLINK_URL = "http://localhost:18081"
PROMETHEUS_URL = "http://localhost:19090"
JOB_NAME = "kafka-flink-clickhouse-monitoring"
OPERATOR_NAME = "count-records-by-tenant"
OPERATOR_LABEL_PATTERN = "count_records_by_tenant.*"
SAVEPOINT_DIRECTORY = "file:///opt/flink/savepoints"
WINDOW = "15m"
PARALLELISM = 4
COMMAND_TIMEOUT = 180
WAIT_TIMEOUT = 180
LOG_SCAN_LINES = 5_000
LOG_LIMIT = 20

TERMINAL_JOB_STATES = {"CANCELED", "FAILED", "FINISHED", "SUSPENDED"}
KEYED_RECORDS_METRIC = "flink_taskmanager_job_task_operator_lab_keyed_records"
HOT_STATE_METRIC = (
    "flink_taskmanager_job_task_operator_lab_hot_tenant_state_count"
)
LOCAL_SHUFFLE_METRIC = "flink_taskmanager_job_task_numBytesInLocalPerSecond"
REMOTE_SHUFFLE_METRIC = "flink_taskmanager_job_task_numBytesInRemotePerSecond"


class LabError(RuntimeError):
    """An expected, user-actionable Chapter 8 workflow error."""


@dataclass(frozen=True)
class GraphEvidence:
    job: Mapping[str, Any]
    vertices: tuple[Mapping[str, Any], ...]
    keyed_vertices: tuple[Mapping[str, Any], ...]
    hash_edges: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class MetricEvidence:
    keyed_records: Mapping[str, float]
    hot_tenant_state: Mapping[str, float]
    local_shuffle_current: Mapping[str, float]
    remote_shuffle_current: Mapping[str, float]
    local_shuffle_recent: Mapping[str, float]
    remote_shuffle_recent: Mapping[str, float]


# Flink REST state and job-safety checks.
def fetch_json(
    base_url: str,
    path: str,
    *,
    query: Mapping[str, str] | None = None,
) -> Mapping[str, Any]:
    """Fetch one JSON object with errors phrased for a lab operator."""

    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    if query:
        url = f"{url}?{urllib.parse.urlencode(query)}"
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.load(response)
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        raise LabError(f"request failed for {url}: {exc}") from exc
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise LabError(f"endpoint did not return valid JSON: {url}") from exc
    if not isinstance(payload, Mapping):
        raise LabError(f"endpoint did not return a JSON object: {url}")
    return payload


def _job_id(job: Mapping[str, Any]) -> str:
    value = job.get("jid") or job.get("id")
    if not isinstance(value, str) or not value:
        raise LabError(f"Flink returned a job without an id: {job!r}")
    return value


def _job_state(job: Mapping[str, Any]) -> str:
    return str(job.get("state", "UNKNOWN")).upper()


def list_jobs() -> tuple[Mapping[str, Any], ...]:
    payload = fetch_json(FLINK_URL, "/jobs/overview")
    jobs = payload.get("jobs", [])
    if not isinstance(jobs, list) or not all(isinstance(job, Mapping) for job in jobs):
        raise LabError("Flink /jobs/overview returned an invalid jobs list")
    return tuple(jobs)


def active_jobs(jobs: Sequence[Mapping[str, Any]]) -> tuple[Mapping[str, Any], ...]:
    return tuple(job for job in jobs if _job_state(job) not in TERMINAL_JOB_STATES)


def describe_jobs(jobs: Sequence[Mapping[str, Any]]) -> str:
    if not jobs:
        return "none"
    return ", ".join(
        f"{job.get('name', '<unnamed>')}[{_job_id(job)}]={_job_state(job)}"
        for job in jobs
    )


def require_one_running_named_job(
    jobs: Sequence[Mapping[str, Any]], job_name: str
) -> Mapping[str, Any]:
    matches = tuple(
        job
        for job in jobs
        if job.get("name") == job_name and _job_state(job) == "RUNNING"
    )
    if len(matches) != 1:
        raise LabError(
            f"expected exactly one RUNNING job named {job_name!r}; "
            f"found {len(matches)} (active jobs: {describe_jobs(active_jobs(jobs))})"
        )
    return matches[0]


def require_exclusive_running_named_job(
    jobs: Sequence[Mapping[str, Any]], job_name: str
) -> Mapping[str, Any]:
    """Return the lab job only when it is the sole active Flink job.

    Mode switches recreate the JobManager and TaskManager, so stopping only the
    named lab job while another job remains active would be unsafe.
    """

    job = require_one_running_named_job(jobs, job_name)
    running = active_jobs(jobs)
    if len(running) != 1 or _job_id(running[0]) != _job_id(job):
        raise LabError(
            f"expected {job_name!r} to be the only active Flink job; "
            f"active jobs: {describe_jobs(running)}"
        )
    return job


def require_no_active_jobs(jobs: Sequence[Mapping[str, Any]]) -> None:
    running = active_jobs(jobs)
    if running:
        raise LabError(
            "expected no active Flink jobs; found: " + describe_jobs(running)
        )


# Compose and Flink command construction.
def compose_command(*arguments: str) -> list[str]:
    return [
        "docker",
        "compose",
        "--project-directory",
        str(ROOT),
        *arguments,
    ]


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


def run_command(
    command: Sequence[str],
    *,
    capture: bool = False,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(command),
            cwd=ROOT,
            check=True,
            text=True,
            capture_output=capture,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        raise LabError(f"required command is not installed: {command[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise LabError(
            f"command timed out after {timeout:g}s: {' '.join(command)}"
        ) from exc
    except subprocess.CalledProcessError as exc:
        details = "\n".join(
            value.strip()
            for value in (exc.stdout or "", exc.stderr or "")
            if value.strip()
        )
        suffix = f"\n{details}" if details else ""
        raise LabError(
            f"command failed with exit code {exc.returncode}: {' '.join(command)}{suffix}"
        ) from exc


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


def _plan_node_text(node: Mapping[str, Any]) -> str:
    return " ".join(
        str(node.get(field, ""))
        for field in ("description", "operator", "operator_strategy")
    )


def plan_nodes(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    """Return plan nodes from Flink 1.20's nested REST response."""

    document = payload.get("plan")
    if not isinstance(document, Mapping):
        raise LabError("Flink job plan returned an invalid plan object")
    nodes = document.get("nodes", [])
    if not isinstance(nodes, list) or not all(
        isinstance(node, Mapping) for node in nodes
    ):
        raise LabError("Flink job plan returned an invalid nodes list")
    return tuple(nodes)


def job_keyby_enabled(job: Mapping[str, Any]) -> bool:
    """Detect the feature from the submitted graph, not an ambient shell value."""

    job_id = _job_id(job)
    plan = fetch_json(FLINK_URL, f"/jobs/{job_id}/plan")
    return any(
        OPERATOR_NAME in _plan_node_text(node)
        for node in plan_nodes(plan)
    )


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


# Read-only graph and Prometheus evidence.
def collect_graph_evidence() -> GraphEvidence:
    job = require_one_running_named_job(list_jobs(), JOB_NAME)
    job_id = _job_id(job)
    details = fetch_json(FLINK_URL, f"/jobs/{job_id}")
    plan = fetch_json(FLINK_URL, f"/jobs/{job_id}/plan")

    raw_vertices = details.get("vertices", [])
    raw_nodes = plan_nodes(plan)
    if not isinstance(raw_vertices, list) or not all(
        isinstance(vertex, Mapping) for vertex in raw_vertices
    ):
        raise LabError("Flink job details returned an invalid vertices list")
    vertices = tuple(raw_vertices)
    keyed_nodes = tuple(
        node
        for node in raw_nodes
        if OPERATOR_NAME in _plan_node_text(node)
    )
    keyed_ids = {str(node.get("id")) for node in keyed_nodes}
    keyed_vertices = tuple(
        vertex
        for vertex in vertices
        if str(vertex.get("id")) in keyed_ids
    )

    edges: list[Mapping[str, Any]] = []
    for node in keyed_nodes:
        inputs = node.get("inputs", [])
        if not isinstance(inputs, list):
            continue
        for input_edge in inputs:
            if not isinstance(input_edge, Mapping):
                continue
            if str(input_edge.get("ship_strategy", "")).upper() == "HASH":
                edges.append(
                    {
                        **input_edge,
                        "target": node.get("id"),
                    }
                )
    return GraphEvidence(job, vertices, keyed_vertices, tuple(edges))


def prometheus_vector(query: str) -> tuple[Mapping[str, Any], ...]:
    payload = fetch_json(
        PROMETHEUS_URL,
        "/api/v1/query",
        query={"query": query},
    )
    if payload.get("status") != "success":
        raise LabError(f"Prometheus query failed: {query}")
    data = payload.get("data")
    if not isinstance(data, Mapping) or data.get("resultType") != "vector":
        raise LabError(f"Prometheus returned a non-vector result for: {query}")
    result = data.get("result", [])
    if not isinstance(result, list) or not all(isinstance(item, Mapping) for item in result):
        raise LabError(f"Prometheus returned an invalid vector for: {query}")
    return tuple(result)


def series_by_subtask(series: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    """Convert one Prometheus vector aggregated by subtask into a mapping."""

    values: dict[str, float] = {}
    for item in series:
        labels = item.get("metric", {})
        raw_value = item.get("value")
        if not isinstance(labels, Mapping) or not (
            isinstance(raw_value, list) and len(raw_value) == 2
        ):
            continue
        subtask = labels.get("subtask_index")
        if subtask is None:
            continue
        try:
            value = float(raw_value[1])
        except (TypeError, ValueError):
            continue
        if not math.isfinite(value):
            continue
        values[str(subtask)] = value
    return values


def collect_metric_evidence() -> MetricEvidence:
    keyed_records = series_by_subtask(
        prometheus_vector(
            "sum by (subtask_index) "
            f'({KEYED_RECORDS_METRIC}{{operator_name=~"{OPERATOR_LABEL_PATTERN}"}})'
        )
    )
    hot_state = series_by_subtask(
        prometheus_vector(
            "max by (subtask_index) "
            f'({HOT_STATE_METRIC}{{operator_name=~"{OPERATOR_LABEL_PATTERN}"}})'
        )
    )

    local_selector = (
        f'{LOCAL_SHUFFLE_METRIC}{{task_name=~"{OPERATOR_LABEL_PATTERN}"}}'
    )
    remote_selector = (
        f'{REMOTE_SHUFFLE_METRIC}{{task_name=~"{OPERATOR_LABEL_PATTERN}"}}'
    )
    local_current = series_by_subtask(
        prometheus_vector(f"sum by (subtask_index) ({local_selector})")
    )
    local_recent = series_by_subtask(
        prometheus_vector(
            "max by (subtask_index) "
            f"(max_over_time({local_selector}[{WINDOW}]))"
        )
    )
    remote_current = series_by_subtask(
        prometheus_vector(f"sum by (subtask_index) ({remote_selector})")
    )
    remote_recent = series_by_subtask(
        prometheus_vector(
            "max by (subtask_index) "
            f"(max_over_time({remote_selector}[{WINDOW}]))"
        )
    )

    return MetricEvidence(
        keyed_records=keyed_records,
        hot_tenant_state=hot_state,
        local_shuffle_current=local_current,
        remote_shuffle_current=remote_current,
        local_shuffle_recent=local_recent,
        remote_shuffle_recent=remote_recent,
    )


# Human-readable output and acceptance checks.
def _sorted_subtasks(values: Mapping[str, float]) -> list[tuple[str, float]]:
    return sorted(values.items(), key=lambda item: int(item[0]))


def print_graph(graph: GraphEvidence) -> None:
    print(f"JOB_ID={_job_id(graph.job)}")
    print(f"JOB_NAME={graph.job.get('name', '')}")
    print(f"JOB_STATE={_job_state(graph.job)}")
    for vertex in graph.vertices:
        print(
            "VERTEX "
            f"id={vertex.get('id', '')} "
            f"parallelism={vertex.get('parallelism', '')} "
            f"status={vertex.get('status', '')} "
            f"name={json.dumps(str(vertex.get('name', '')))}"
        )
    if graph.hash_edges:
        for edge in graph.hash_edges:
            print(
                "HASH_EDGE "
                f"source={edge.get('id', '')} "
                f"target={edge.get('target', '')} "
                f"strategy={str(edge.get('ship_strategy', '')).upper()} "
                f"exchange={edge.get('exchange', '')}"
            )
    else:
        print("HASH_EDGE none")


def print_metric(name: str, values: Mapping[str, float]) -> None:
    if not values:
        print(f"METRIC name={name} no_series=true")
        return
    for subtask, value in _sorted_subtasks(values):
        print(f"METRIC name={name} subtask={subtask} value={value:.15g}")


def print_shuffle(
    locality: str,
    sample: str,
    values: Mapping[str, float],
) -> None:
    if not values:
        print(f"SHUFFLE locality={locality} sample={sample} no_series=true")
        return
    for subtask, value in _sorted_subtasks(values):
        suffix = f" window={WINDOW}" if sample == "recent_max" else ""
        print(
            f"SHUFFLE locality={locality} sample={sample} subtask={subtask} "
            f"bytes_per_second={value:.15g}{suffix}"
        )


def collect_all_evidence() -> tuple[GraphEvidence, MetricEvidence]:
    graph = collect_graph_evidence()
    if not graph.keyed_vertices:
        raise LabError(
            f"running job has no vertex containing operator {OPERATOR_NAME!r}; "
            "was it submitted with ENABLE_KEYBY_LAB=true?"
        )
    return graph, collect_metric_evidence()


def observe_keyby(_args: argparse.Namespace) -> int:
    graph, metrics = collect_all_evidence()
    print_graph(graph)
    print_metric("lab_keyed_records", metrics.keyed_records)
    print_metric("lab_hot_tenant_state_count", metrics.hot_tenant_state)
    print_shuffle("local", "current", metrics.local_shuffle_current)
    print_shuffle("local", "recent_max", metrics.local_shuffle_recent)
    print_shuffle("remote", "current", metrics.remote_shuffle_current)
    print_shuffle("remote", "recent_max", metrics.remote_shuffle_recent)
    if not graph.hash_edges:
        raise LabError(
            f"no HASH input edge enters {OPERATOR_NAME!r}; the keyBy lab is not active"
        )
    return 0


def verification_errors(
    graph: GraphEvidence,
    metrics: MetricEvidence,
) -> list[str]:
    errors: list[str] = []
    expected_subtasks = {str(index) for index in range(PARALLELISM)}

    if len(graph.vertices) < 2:
        errors.append(f"expected at least 2 active vertices, found {len(graph.vertices)}")
    if not graph.keyed_vertices:
        errors.append("keyed vertex is missing")
    for vertex in graph.keyed_vertices:
        if vertex.get("parallelism") != PARALLELISM:
            errors.append(
                f"keyed vertex {vertex.get('id', '<unknown>')} has parallelism "
                f"{vertex.get('parallelism')!r}, expected {PARALLELISM}"
            )
    if not graph.hash_edges:
        errors.append("no HASH edge enters the keyed vertex")

    for name, values in (
        ("lab_keyed_records", metrics.keyed_records),
        ("lab_hot_tenant_state_count", metrics.hot_tenant_state),
        ("local recent shuffle", metrics.local_shuffle_recent),
        ("remote recent shuffle", metrics.remote_shuffle_recent),
    ):
        actual = set(values)
        if actual != expected_subtasks:
            errors.append(
                f"{name} subtasks are {sorted(actual)}, expected {sorted(expected_subtasks)}"
            )

    hot_owners = [
        subtask for subtask, value in metrics.hot_tenant_state.items() if value > 0
    ]
    if len(hot_owners) != 1:
        errors.append(
            "expected exactly one positive tenant-hot state gauge, "
            f"found {len(hot_owners)} ({hot_owners})"
        )
    keyed_total = sum(metrics.keyed_records.values())
    if not any(
        metrics.keyed_records.get(owner, 0) > keyed_total / 2
        for owner in hot_owners
    ):
        errors.append(
            "expected the tenant-hot owner to process a majority of records; "
            f"found {dict(_sorted_subtasks(metrics.keyed_records))}"
        )
    if not any(value > 0 for value in metrics.local_shuffle_recent.values()):
        errors.append("recent local shuffle rates contain no positive byte rate")
    nonzero_remote = {
        subtask: value
        for subtask, value in metrics.remote_shuffle_recent.items()
        if value != 0
    }
    if nonzero_remote:
        errors.append(f"recent remote shuffle rates are not zero: {nonzero_remote}")
    return errors


def verify_keyby(_args: argparse.Namespace) -> int:
    graph, metrics = collect_all_evidence()
    print_graph(graph)
    print_metric("lab_keyed_records", metrics.keyed_records)
    print_metric("lab_hot_tenant_state_count", metrics.hot_tenant_state)
    print_shuffle("local", "recent_max", metrics.local_shuffle_recent)
    print_shuffle("remote", "recent_max", metrics.remote_shuffle_recent)
    errors = verification_errors(graph, metrics)
    if errors:
        for error in errors:
            print(f"VERIFY_ERROR={error}", file=sys.stderr)
        raise LabError(f"Chapter 8 verification failed with {len(errors)} error(s)")
    owner = next(
        subtask for subtask, value in metrics.hot_tenant_state.items() if value > 0
    )
    print(
        "VERIFY_KEYBY=PASS "
        f"parallelism={PARALLELISM} hot_tenant_subtask={owner} "
        f"shuffle_window={WINDOW}"
    )
    return 0


def milestone_logs(_args: argparse.Namespace) -> int:
    result = run_command(
        compose_command(
            "logs", "--no-color", f"--tail={LOG_SCAN_LINES}", "taskmanager"
        ),
        capture=True,
    )
    lines = [
        line
        for line in result.stdout.splitlines()
        if "keyed_state_milestone" in line
    ]
    selected = lines[-LOG_LIMIT:]
    for line in selected:
        print(line)
    print(
        f"KEYED_STATE_MILESTONE_LOGS shown={len(selected)} "
        f"matched={len(lines)} scan_lines={LOG_SCAN_LINES}",
        file=sys.stderr,
    )
    return 0


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
