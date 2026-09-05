from __future__ import annotations

import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_compose_project_and_ports_are_isolated() -> None:
    compose_text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    compose = yaml.safe_load(compose_text)

    assert compose["name"] == "kafka-flink-clickhouse-lab-monitoring"
    assert "container_name" not in compose_text

    expected_host_ports = {
        "29092",
        "18080",
        "18081",
        "18123",
        "19000",
        "13000",
        "19090",
        "19404",
        "19249",
        "19250",
        "19363",
        "13100",
        "22345",
    }
    configured = {
        str(port).split(":")[-2]
        for service in compose["services"].values()
        for port in service.get("ports", [])
    }
    assert expected_host_ports <= configured
    assert all(
        str(port).startswith("127.0.0.1:")
        for service in compose["services"].values()
        for port in service.get("ports", [])
    )
    assert compose["services"]["kafka"]["environment"][
        "KAFKA_ADVERTISED_LISTENERS"
    ] == "PLAINTEXT://kafka:19092,PLAINTEXT_HOST://localhost:29092"

    assert compose["volumes"]
    assert all(name.startswith("monitoring_") for name in compose["volumes"])


def test_topics_are_explicit_and_auto_creation_is_disabled() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    kafka_environment = compose["services"]["kafka"]["environment"]
    init_command = compose["services"]["kafka-init"]["command"][0]

    assert kafka_environment["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] == "false"
    assert "telemetry.raw telemetry.dlq" in init_command


def test_required_versions_and_optional_logs_profile_are_pinned() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    services = compose["services"]

    assert services["kafka"]["image"] == "telemetry-monitoring-kafka:4.3.1-jmx"
    assert services["clickhouse"]["image"] == "clickhouse/clickhouse-server:26.3.30.9"
    assert services["jobmanager"]["image"] == "telemetry-monitoring-flink:2.2.1-py311"
    assert services["prometheus"]["image"] == "prom/prometheus:v3.14.0"
    assert services["grafana"]["image"] == "grafana/grafana:13.2.1"
    assert (
        services["grafana"]["environment"]["GF_PLUGINS_PREINSTALL_SYNC"]
        == "grafana-clickhouse-datasource@4.20.0"
    )
    assert services["loki"]["profiles"] == ["logs"]
    assert services["alloy"]["profiles"] == ["logs"]
    assert "/var/run/docker.sock:/var/run/docker.sock:ro" in services["alloy"]["volumes"]
    for service_name in (
        "kafka",
        "clickhouse",
        "jobmanager",
        "taskmanager",
        "prometheus",
        "grafana",
    ):
        assert "healthcheck" in services[service_name]


def test_flink_keeps_the_logging_api_compatible_with_its_bundled_binding() -> None:
    pom = (ROOT / "flink/pom.xml").read_text(encoding="utf-8")

    assert "<artifactId>slf4j-api</artifactId>" in pom
    assert "<version>1.7.36</version>" in pom


def test_passwords_are_generated_locally_and_never_committed_as_defaults() -> None:
    compose_text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    example_lines = (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
    datasource = (
        ROOT / "observability/grafana/provisioning/datasources/datasources.yml"
    ).read_text(encoding="utf-8")
    init_sql = (ROOT / "clickhouse/init.sql").read_text(encoding="utf-8")
    init_users = (ROOT / "clickhouse/init-users.sh").read_text(encoding="utf-8")

    password_settings = [
        line for line in compose_text.splitlines() if "PASSWORD:" in line
    ]
    assert password_settings
    assert all(":?Run make credentials first" in line for line in password_settings)
    assert all(
        not line.split("=", 1)[1]
        for line in example_lines
        if line and not line.startswith("#") and line.split("=", 1)[0].endswith("PASSWORD")
    )
    assert "username: $CLICKHOUSE_OBSERVER_USER" in datasource
    assert "password: $CLICKHOUSE_OBSERVER_PASSWORD" in datasource
    assert "IDENTIFIED WITH sha256_password BY '" not in init_sql
    assert "{observer_password:String}" in init_users
    assert "^[A-Za-z_][A-Za-z0-9_]*$" in init_users
    assert "--param_observer_password" in init_users


def test_all_dashboards_are_valid_json_with_stable_uids() -> None:
    dashboard_dir = ROOT / "observability/grafana/provisioning/dashboards"
    dashboards = []

    for path in dashboard_dir.rglob("*.json"):
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        assert dashboard["title"]
        assert dashboard["uid"]
        assert isinstance(dashboard["panels"], list)
        assert len(dashboard["panels"]) >= 8
        assert all(panel.get("datasource") for panel in dashboard["panels"])
        assert all(panel.get("targets") for panel in dashboard["panels"])
        dashboards.append(dashboard)

    assert len(dashboards) == 4
    assert len({dashboard["uid"] for dashboard in dashboards}) == 4


def test_kafka_partition_log_size_query_escapes_regex_for_promql() -> None:
    path = ROOT / "observability/grafana/provisioning/dashboards/json/kafka.json"
    dashboard = json.loads(path.read_text(encoding="utf-8"))
    panel = next(panel for panel in dashboard["panels"] if panel["id"] == 8)

    assert panel["targets"][0]["expr"] == (
        'kafka_log_log_size{topic=~"telemetry\\\\.(raw|dlq)"}'
    )


def test_documented_make_targets_exist() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    readme += (ROOT / "docs/OPERATIONS.md").read_text(encoding="utf-8")
    for target in (
        "help",
        "deploy",
        "teardown",
        "build",
        "up",
        "up-observability",
        "up-logs",
        "stop-logs",
        "init",
        "submit",
        "produce-baseline",
        "produce-small",
        "produce-observable",
        "observe",
        "lag",
        "dlq",
        "latency",
        "checkpoints",
        "credentials",
        "rotate-credentials",
        "complexity",
        "smoke",
        "reset",
    ):
        assert f"{target}:" in makefile
        assert f"make {target}" in readme



def test_flink_parallelism_and_metrics_scope_are_consistent() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    services = compose["services"]
    properties = yaml.safe_load(services["jobmanager"]["environment"]["FLINK_PROPERTIES"])
    assert services["taskmanager"]["environment"]["FLINK_PROPERTIES"] == services["jobmanager"]["environment"]["FLINK_PROPERTIES"]
    assert properties["taskmanager.numberOfTaskSlots"] == "${FLINK_PARALLELISM:-4}"
    assert properties["parallelism.default"] == "${FLINK_PARALLELISM:-4}"
    assert properties["metrics.reporter.prom.scope.variables.additional"] == "lab:kafka-flink-clickhouse-monitoring"
    assert set(properties["metrics.reporter.prom.scope.variables.excludes"].split(";")) == {
        "job_id", "task_id", "task_attempt_id", "task_attempt_num", "operator_id", "tm_id",
    }
    assert properties["execution.checkpointing.dir"] == "file:///opt/flink/checkpoints"
    assert properties["restart-strategy.fixed-delay.attempts"] == 100
    assert properties["heartbeat.timeout"] == "120s"
    assert properties["env.java.opts.taskmanager"] == "-Djdk.lang.Process.launchMechanism=fork"
    assert "http://jobmanager:8081/taskmanagers" in services["taskmanager"]["healthcheck"]["test"][1]
