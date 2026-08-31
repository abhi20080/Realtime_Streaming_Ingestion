from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_keyby_job_is_default_off() -> None:
    job = (ROOT / "flink/job.py").read_text(encoding="utf-8")

    assert 'os.getenv("ENABLE_KEYBY_LAB", "false")' in job
    assert "if ENABLE_KEYBY_LAB:" in job


def test_keyby_make_targets_use_the_controlled_workload() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")

    for target in (
        "keyby-on",
        "keyby-off",
        "flink-reset",
        "keyby-savepoint",
        "keyby-rebuild",
        "keyby-submit",
        "produce-keyby",
        "observe-keyby",
        "verify-keyby",
        "keyby-logs",
    ):
        assert f"{target}:" in makefile

    for option in (
        "--rate 500 --count 20000",
        "--duplicate-rate 0",
        "--late-rate 0",
        "--hot-tenant-rate 0.95",
        "--bad-json-rate 0",
        "--trace-sample-rate 0",
    ):
        assert option in makefile

    assert "SAVEPOINT" in makefile
    assert "scripts/keyby_lab.py" in makefile
    assert "allowNonRestoredState" not in makefile


def test_flink_dashboard_contains_bounded_keyby_evidence() -> None:
    path = (
        ROOT
        / "observability/grafana/provisioning/dashboards/json/flink.json"
    )
    dashboard = json.loads(path.read_text(encoding="utf-8"))
    panels = {panel["title"]: panel for panel in dashboard["panels"]}

    required = {
        "Before / after keyBy by subtask",
        "Keyed load and tenant-hot state",
        "keyBy shuffle input: local / remote",
    }
    assert required <= panels.keys()

    expressions = "\n".join(
        target["expr"]
        for title in required
        for target in panels[title]["targets"]
    )
    assert "lab_records_valid" in expressions
    assert "lab_keyed_records" in expressions
    assert "lab_hot_tenant_state_count" in expressions
    assert 'operator_name=~"count_records_by_tenant.*"' in expressions
    assert "flink_taskmanager_job_task_numBytesInLocalPerSecond" in expressions
    assert "flink_taskmanager_job_task_numBytesInRemotePerSecond" in expressions
    assert "tenant_id=" not in expressions
