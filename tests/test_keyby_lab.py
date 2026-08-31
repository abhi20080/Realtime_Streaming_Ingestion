from __future__ import annotations

import pytest

from scripts.keyby_lab import (
    GraphEvidence,
    LabError,
    MetricEvidence,
    active_jobs,
    job_keyby_enabled,
    parse_savepoint_path,
    plan_nodes,
    rebuild_commands,
    require_exclusive_running_named_job,
    require_no_active_jobs,
    require_one_running_named_job,
    restore_command,
    savepoint_command,
    transition_pipeline,
    verification_errors,
)


JOB_NAME = "kafka-flink-clickhouse-monitoring"


def test_flink_120_nested_plan_response_is_read() -> None:
    nodes = [{"id": "keyed", "inputs": [{"ship_strategy": "HASH"}]}]

    assert plan_nodes({"plan": {"nodes": nodes}}) == tuple(nodes)

    with pytest.raises(LabError, match="invalid plan object"):
        plan_nodes({"plan": []})


def test_savepoint_command_is_drained_canonical_and_path_is_parseable() -> None:
    command = savepoint_command("abc123")
    output = (
        "Suspending job with a CANONICAL savepoint.\n"
        "Savepoint completed. Path: "
        "file:/opt/flink/savepoints/savepoint-abc123\n"
    )

    assert command[-8:] == [
        "flink",
        "stop",
        "--drain",
        "--type",
        "canonical",
        "--savepointPath",
        "file:///opt/flink/savepoints",
        "abc123",
    ]
    assert "--drain" in command
    assert "canonical" in command
    assert parse_savepoint_path(output) == (
        "file:/opt/flink/savepoints/savepoint-abc123"
    )


def test_savepoint_parser_rejects_output_without_a_completed_path() -> None:
    with pytest.raises(LabError, match="no savepoint path"):
        parse_savepoint_path("Job was stopped, but no location was printed")


def test_restore_discards_unmapped_state_only_when_turning_off() -> None:
    savepoint = "file:/opt/flink/savepoints/savepoint-abc123"

    keyby_on = restore_command(savepoint, keyby_enabled=True)
    keyby_off = restore_command(
        savepoint,
        keyby_enabled=False,
        allow_non_restored_state=True,
    )
    assert "ENABLE_KEYBY_LAB=true" in keyby_on
    assert "ENABLE_KEYBY_LAB=false" in keyby_off
    assert "--allowNonRestoredState" in keyby_off
    assert "--allowNonRestoredState" not in keyby_on
    assert "-s" in keyby_on
    assert "-s" in keyby_off
    with pytest.raises(LabError, match="permitted only when switching"):
        restore_command(
            savepoint,
            keyby_enabled=True,
            allow_non_restored_state=True,
        )


def test_rebuild_targets_only_flink_services_and_preserves_volumes() -> None:
    commands = rebuild_commands()
    rendered = [" ".join(command) for command in commands]

    assert len(commands) == 3
    assert rendered[0].endswith("build jobmanager taskmanager")
    assert rendered[1].endswith("--wait-timeout 180 jobmanager")
    assert rendered[2].endswith("--wait-timeout 180 taskmanager")
    assert all("--no-deps" in command for command in commands[1:])
    assert all("-v" not in command for command in commands)
    assert all("down" not in command for command in commands)


def test_job_guards_distinguish_running_named_and_terminal_jobs() -> None:
    jobs = [
        {"jid": "running", "name": JOB_NAME, "state": "RUNNING"},
        {"jid": "old", "name": JOB_NAME, "state": "FINISHED"},
    ]

    assert require_one_running_named_job(jobs, JOB_NAME)["jid"] == "running"
    assert [job["jid"] for job in active_jobs(jobs)] == ["running"]
    with pytest.raises(LabError, match="expected no active Flink jobs"):
        require_no_active_jobs(jobs)
    require_no_active_jobs([jobs[1]])


def test_mode_switch_guard_rejects_any_second_active_job() -> None:
    jobs = [
        {"jid": "running", "name": JOB_NAME, "state": "RUNNING"},
        {"jid": "restarting", "name": "another-job", "state": "RESTARTING"},
    ]

    with pytest.raises(LabError, match="only active Flink job"):
        require_exclusive_running_named_job(jobs, JOB_NAME)


def test_mode_detection_finds_operator_inside_a_chained_plan_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_fetch_json(base_url: str, path: str):
        assert path == "/jobs/job-id/plan"
        return {
            "plan": {
                "nodes": [
                    {
                        "description": (
                            "count-records-by-tenant -> "
                            "observe-watermark-and-lateness"
                        )
                    }
                ]
            }
        }

    monkeypatch.setattr("scripts.keyby_lab.fetch_json", fake_fetch_json)

    assert job_keyby_enabled({"jid": "job-id"})


def test_flink_reset_restores_the_detected_mode_without_discarding_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current_keyby = True
    running = {"jid": "job-id", "name": JOB_NAME, "state": "RUNNING"}
    job_lists = iter([(running,), ()])
    submitted: list[list[str]] = []

    monkeypatch.setattr("scripts.keyby_lab.list_jobs", lambda: next(job_lists))
    monkeypatch.setattr(
        "scripts.keyby_lab.job_keyby_enabled",
        lambda *_args: current_keyby,
    )
    monkeypatch.setattr(
        "scripts.keyby_lab.create_drained_savepoint",
        lambda *_args: "file:/opt/flink/savepoints/savepoint-reset",
    )
    monkeypatch.setattr(
        "scripts.keyby_lab.rebuild_flink_services", lambda *_args: None
    )
    monkeypatch.setattr(
        "scripts.keyby_lab.run_command", lambda command: submitted.append(command)
    )

    assert transition_pipeline(None) == 0
    assert len(submitted) == 1
    expected_flag = f"ENABLE_KEYBY_LAB={'true' if current_keyby else 'false'}"
    assert expected_flag in submitted[0]
    assert "--allowNonRestoredState" not in submitted[0]


def test_keyby_off_is_the_only_transition_that_discards_unmapped_state(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    running = {"jid": "job-id", "name": JOB_NAME, "state": "RUNNING"}
    job_lists = iter([(running,), ()])
    submitted: list[list[str]] = []

    monkeypatch.setattr("scripts.keyby_lab.list_jobs", lambda: next(job_lists))
    monkeypatch.setattr("scripts.keyby_lab.job_keyby_enabled", lambda *_args: True)
    monkeypatch.setattr(
        "scripts.keyby_lab.create_drained_savepoint",
        lambda *_args: "file:/opt/flink/savepoints/savepoint-off",
    )
    monkeypatch.setattr(
        "scripts.keyby_lab.rebuild_flink_services", lambda *_args: None
    )
    monkeypatch.setattr(
        "scripts.keyby_lab.run_command", lambda command: submitted.append(command)
    )

    assert transition_pipeline(False) == 0
    output = capsys.readouterr()
    assert "SAVEPOINT=file:/opt/flink/savepoints/savepoint-off" in output.out
    assert "WARNING" in output.err
    assert "ENABLE_KEYBY_LAB=false" in submitted[0]
    assert "--allowNonRestoredState" in submitted[0]


def test_same_mode_switch_is_idempotent_and_does_not_stop_the_job(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    running = {"jid": "job-id", "name": JOB_NAME, "state": "RUNNING"}
    monkeypatch.setattr("scripts.keyby_lab.list_jobs", lambda: (running,))
    monkeypatch.setattr("scripts.keyby_lab.job_keyby_enabled", lambda *_args: True)

    def unexpected_savepoint(*_args):
        raise AssertionError("an idempotent switch must not stop the running job")

    monkeypatch.setattr(
        "scripts.keyby_lab.create_drained_savepoint", unexpected_savepoint
    )

    assert transition_pipeline(True) == 0
    assert "already running" in capsys.readouterr().out


def test_transition_prints_savepoint_and_recovery_before_rebuild_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    running = {"jid": "job-id", "name": JOB_NAME, "state": "RUNNING"}
    monkeypatch.setattr("scripts.keyby_lab.list_jobs", lambda: (running,))
    monkeypatch.setattr("scripts.keyby_lab.job_keyby_enabled", lambda *_args: False)
    monkeypatch.setattr(
        "scripts.keyby_lab.create_drained_savepoint",
        lambda *_args: "file:/opt/flink/savepoints/savepoint-recover",
    )
    monkeypatch.setattr(
        "scripts.keyby_lab.rebuild_flink_services",
        lambda *_args: (_ for _ in ()).throw(LabError("build failed")),
    )

    with pytest.raises(LabError, match="build failed"):
        transition_pipeline(True)

    output = capsys.readouterr()
    assert "SAVEPOINT=file:/opt/flink/savepoints/savepoint-recover" in output.out
    assert "Recovery command" in output.err
    assert "ENABLE_KEYBY_LAB=true" in output.err


def _passing_graph() -> GraphEvidence:
    keyed = {
        "id": "keyed",
        "name": "count-records-by-tenant -> observe-watermark-and-lateness",
        "parallelism": 4,
        "status": "RUNNING",
    }
    return GraphEvidence(
        job={"jid": "job", "name": JOB_NAME, "state": "RUNNING"},
        vertices=(
            {"id": "source", "name": "Kafka Source", "parallelism": 4},
            keyed,
        ),
        keyed_vertices=(keyed,),
        hash_edges=({"id": "source", "target": "keyed", "ship_strategy": "HASH"},),
    )


def _passing_metrics() -> MetricEvidence:
    return MetricEvidence(
        keyed_records={"0": 100.0, "1": 120.0, "2": 9500.0, "3": 110.0},
        hot_tenant_state={"0": 0.0, "1": 0.0, "2": 9500.0, "3": 0.0},
        local_shuffle_current={str(index): 0.0 for index in range(4)},
        remote_shuffle_current={str(index): 0.0 for index in range(4)},
        local_shuffle_recent={"0": 100.0, "1": 120.0, "2": 999.0, "3": 110.0},
        remote_shuffle_recent={str(index): 0.0 for index in range(4)},
    )


def test_live_verification_accepts_recent_local_and_zero_remote_shuffle() -> None:
    assert verification_errors(_passing_graph(), _passing_metrics()) == []


def test_live_verification_reports_hot_owner_remote_and_parallelism_failures() -> None:
    graph = _passing_graph()
    wrong_keyed = {**graph.keyed_vertices[0], "parallelism": 3}
    broken_graph = GraphEvidence(
        graph.job,
        (graph.vertices[0], wrong_keyed),
        (wrong_keyed,),
        graph.hash_edges,
    )
    metrics = _passing_metrics()
    broken_metrics = MetricEvidence(
        {str(index): 100.0 for index in range(4)},
        {"0": 1.0, "1": 0.0, "2": 2.0, "3": 0.0},
        metrics.local_shuffle_current,
        metrics.remote_shuffle_current,
        metrics.local_shuffle_recent,
        {"0": 0.0, "1": 0.0, "2": 7.0, "3": 0.0},
    )

    errors = verification_errors(broken_graph, broken_metrics)

    assert any("parallelism" in error for error in errors)
    assert any("exactly one positive" in error for error in errors)
    assert any("tenant-hot owner" in error for error in errors)
    assert any("remote shuffle rates are not zero" in error for error in errors)
