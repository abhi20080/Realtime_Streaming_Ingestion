"""Exercise the user-facing command contract without touching a running lab."""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from producer.producer import parse_args
from scripts import keyby_lab

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def run_make(tmp_path):
    shutil.copy(ROOT / "Makefile", tmp_path / "Makefile")
    (tmp_path / ".env").touch()
    (tmp_path / "kafka").mkdir()
    (tmp_path / "kafka/jmx_prometheus_javaagent-1.6.0.jar").write_bytes(b"cached-jar")
    calls = tmp_path / "docker-calls.jsonl"
    fake_docker = tmp_path / "docker"
    fake_docker.write_text(f"#!{sys.executable}\n" + '''
import json
import os
import sys
with open(os.environ["FAKE_DOCKER_CALLS"], "a") as output:
    output.write(json.dumps(sys.argv[1:]) + "\\n")
if sys.argv[-3:] == ["flink", "list", "-r"] and os.environ.get("FAKE_RUNNING") == "1":
    print("kafka-flink-clickhouse-monitoring")
''')
    fake_docker.chmod(0o700)

    def invoke(target, *, running=False):
        environment = dict(os.environ)
        environment.update(
            PATH=f"{tmp_path}{os.pathsep}{os.defpath}",
            FAKE_DOCKER_CALLS=str(calls),
            FAKE_RUNNING="1" if running else "0",
        )
        subprocess.run(
            [shutil.which("make"), "--no-print-directory", target],
            cwd=tmp_path, env=environment, check=True, capture_output=True, text=True,
        )
        return [json.loads(line) for line in calls.read_text().splitlines()]

    return invoke


@pytest.mark.parametrize("running", [False, True])
def test_deploy_initializes_and_submits_only_when_needed(run_make, running):
    commands = run_make("deploy", running=running)
    assert any(command[-3:] == ["--profile", "tools", "build"] for command in commands)
    assert any(command[-3:] == ["run", "--rm", "kafka-init"] for command in commands)
    submissions = [command for command in commands if "-py" in command]
    assert len(submissions) == (0 if running else 1)


def test_teardown_preserves_volumes_and_stop_logs_only_stops_collectors(run_make):
    commands = run_make("teardown")
    assert commands == [["compose", "--profile", "logs", "down"]]
    commands = run_make("stop-logs")
    assert commands[-1] == ["compose", "--profile", "logs", "stop", "alloy", "loki"]


@pytest.mark.parametrize(
    "target,count,rate,duplicate,late,hot,bad,trace",
    [
        ("produce-baseline", 1000, 100, 0, 0, 0, 0, 0),
        ("produce-small", 10000, 100, 0.03, 0.03, 0.70, 0, 0.01),
        ("produce-observable", 100000, 500, 0.05, 0.10, 0.80, 0.005, 0.001),
        ("produce-keyby", 20000, 500, 0, 0, 0.95, 0, 0),
    ],
)
def test_workload_commands_keep_their_documented_behavior(
    run_make, target, count, rate, duplicate, late, hot, bad, trace,
):
    command, = run_make(target)
    args = parse_args(command[command.index("producer") + 1:])
    assert (args.count, args.rate, args.duplicate_rate, args.late_rate,
            args.hot_tenant_rate, args.bad_json_rate, args.trace_sample_rate) == (
        count, rate, duplicate, late, hot, bad, trace,
    )


@pytest.mark.parametrize("command,handler", [
    ("keyby-on", "enable_keyby"), ("keyby-off", "disable_keyby"),
    ("flink-reset", "reset_flink_job"), ("savepoint", "stop_with_savepoint"),
    ("rebuild", "rebuild_flink"), ("observe", "observe_keyby"),
    ("verify", "verify_keyby"), ("logs", "milestone_logs"),
])
def test_keyby_cli_dispatches_to_the_selected_operation(monkeypatch, command, handler):
    calls = []
    monkeypatch.setattr(keyby_lab, handler, lambda args: calls.append(args.command) or 0)
    assert keyby_lab.main([command]) == 0
    assert calls == [command]


def test_keyby_submit_passes_savepoint_and_requires_it(monkeypatch):
    paths = []
    monkeypatch.setattr(keyby_lab, "submit_keyby", lambda args: paths.append(args.savepoint) or 0)
    assert keyby_lab.main(["submit", "--savepoint", "file:/test-savepoint"]) == 0
    assert paths == ["file:/test-savepoint"]
    with pytest.raises(SystemExit) as error:
        keyby_lab.main(["submit"])
    assert error.value.code == 2


@pytest.mark.parametrize("script", ["producer/producer.py", "scripts/keyby_lab.py"])
def test_direct_cli_help_works_without_runtime_dependencies(script):
    result = subprocess.run(
        [sys.executable, str(ROOT / script), "--help"],
        cwd=ROOT, capture_output=True, text=True, check=True,
    )
    assert "usage:" in result.stdout
