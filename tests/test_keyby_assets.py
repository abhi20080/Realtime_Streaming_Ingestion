from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_keyby_job_is_default_off() -> None:
    from logic import keyby_lab_enabled

    assert keyby_lab_enabled({}) is False
    assert keyby_lab_enabled({"ENABLE_KEYBY_LAB": "false"}) is False
    assert keyby_lab_enabled({"ENABLE_KEYBY_LAB": "true"}) is True


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
