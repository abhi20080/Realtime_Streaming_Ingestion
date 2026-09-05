"""Flink REST access, job guards, and shared keyBy experiment contracts."""

from __future__ import annotations

import json
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
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


def compose_command(*arguments: str) -> list[str]:
    return [
        "docker",
        "compose",
        "--project-directory",
        str(ROOT),
        *arguments,
    ]


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


def _plan_node_text(node: Mapping[str, Any]) -> str:
    return " ".join(
        str(node.get(field, ""))
        for field in ("description", "operator", "operator_strategy")
    )


def plan_nodes(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    """Return plan nodes from Flink's nested REST response."""

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


