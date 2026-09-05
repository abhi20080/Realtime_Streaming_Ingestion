"""Read-only graph and metric evidence. Start with observe_keyby() or verify_keyby()."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Mapping, Sequence
from typing import Any


if __package__:
    from .keyby_common import (
        FLINK_URL,
        GraphEvidence,
        HOT_STATE_METRIC,
        JOB_NAME,
        KEYED_RECORDS_METRIC,
        LOCAL_SHUFFLE_METRIC,
        LOG_LIMIT,
        LOG_SCAN_LINES,
        LabError,
        MetricEvidence,
        OPERATOR_LABEL_PATTERN,
        OPERATOR_NAME,
        PARALLELISM,
        PROMETHEUS_URL,
        REMOTE_SHUFFLE_METRIC,
        WINDOW,
        _job_id,
        _job_state,
        _plan_node_text,
        compose_command,
        fetch_json,
        list_jobs,
        plan_nodes,
        require_one_running_named_job,
        run_command,
    )
else:  # Support python scripts/keyby_lab.py.
    from keyby_common import (
        FLINK_URL,
        GraphEvidence,
        HOT_STATE_METRIC,
        JOB_NAME,
        KEYED_RECORDS_METRIC,
        LOCAL_SHUFFLE_METRIC,
        LOG_LIMIT,
        LOG_SCAN_LINES,
        LabError,
        MetricEvidence,
        OPERATOR_LABEL_PATTERN,
        OPERATOR_NAME,
        PARALLELISM,
        PROMETHEUS_URL,
        REMOTE_SHUFFLE_METRIC,
        WINDOW,
        _job_id,
        _job_state,
        _plan_node_text,
        compose_command,
        fetch_json,
        list_jobs,
        plan_nodes,
        require_one_running_named_job,
        run_command,
    )


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


def _graph_verification_errors(graph: GraphEvidence) -> list[str]:
    errors: list[str] = []
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
    return errors


def _subtask_coverage_errors(metrics: MetricEvidence) -> list[str]:
    errors: list[str] = []
    expected_subtasks = {str(index) for index in range(PARALLELISM)}

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
    return errors


def _hot_tenant_errors(metrics: MetricEvidence) -> list[str]:
    errors: list[str] = []

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
    return errors


def _shuffle_errors(metrics: MetricEvidence) -> list[str]:
    errors: list[str] = []
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


def verification_errors(
    graph: GraphEvidence,
    metrics: MetricEvidence,
) -> list[str]:
    """Return ordered graph, coverage, ownership, and shuffle failures."""
    return [
        *_graph_verification_errors(graph),
        *_subtask_coverage_errors(metrics),
        *_hot_tenant_errors(metrics),
        *_shuffle_errors(metrics),
    ]


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


