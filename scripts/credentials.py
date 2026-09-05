#!/usr/bin/env python3
"""Generate local credentials and rotate passwords in running lab containers."""

from __future__ import annotations

import argparse
import os
import re
import secrets
import subprocess
import tempfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
DATASOURCE_PATH = (
    ROOT / "observability/grafana/provisioning/datasources/datasources.yml"
)
COMPOSE_PROJECT = "kafka-flink-clickhouse-lab-monitoring"
ENVIRONMENT_ORDER = (
    "CLICKHOUSE_USER",
    "CLICKHOUSE_PASSWORD",
    "CLICKHOUSE_OBSERVER_USER",
    "CLICKHOUSE_OBSERVER_PASSWORD",
    "GRAFANA_ADMIN_USER",
    "GRAFANA_ADMIN_PASSWORD",
    "FLINK_PARALLELISM",
    "OUT_OF_ORDER_SECONDS",
    "WATERMARK_IDLE_SECONDS",
    "LOG_LEVEL",
    "PRODUCER_STATS_INTERVAL_SECONDS",
)
NON_SECRET_DEFAULTS = {
    "CLICKHOUSE_USER": "flink",
    "CLICKHOUSE_OBSERVER_USER": "observer",
    "GRAFANA_ADMIN_USER": "admin",
    "FLINK_PARALLELISM": "4",
    "OUT_OF_ORDER_SECONDS": "10",
    "WATERMARK_IDLE_SECONDS": "30",
    "LOG_LEVEL": "INFO",
    "PRODUCER_STATS_INTERVAL_SECONDS": "5",
}
SECRET_KEYS = (
    "CLICKHOUSE_PASSWORD",
    "CLICKHOUSE_OBSERVER_PASSWORD",
    "GRAFANA_ADMIN_PASSWORD",
)
SAFE_USER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class CredentialError(RuntimeError):
    """A credential operation failed without exposing secret command arguments."""


def _run(command: Sequence[str], operation: str) -> str:
    result = subprocess.run(
        list(command),
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        details = (result.stderr or result.stdout).strip()
        suffix = f": {details}" if details else ""
        raise CredentialError(f"{operation} failed{suffix}")
    return result.stdout.strip()


def _read_environment() -> dict[str, str]:
    values: dict[str, str] = {}
    if not ENV_PATH.exists():
        return values
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key] = value
    return values


def _write_environment(values: Mapping[str, str]) -> None:
    lines = [
        "# Generated local credentials. Keep this file out of version control.",
        *(f"{key}={values[key]}" for key in ENVIRONMENT_ORDER),
        "",
    ]
    descriptor, temporary_name = tempfile.mkstemp(prefix=".env.", dir=ROOT, text=True)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write("\n".join(lines))
        os.chmod(temporary_name, 0o600)
        os.replace(temporary_name, ENV_PATH)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def _new_environment(current: Mapping[str, str]) -> dict[str, str]:
    values = dict(NON_SECRET_DEFAULTS)
    values.update({key: value for key, value in current.items() if value})
    for key in SECRET_KEYS:
        values[key] = secrets.token_urlsafe(32)
    return values


def _container_id(service: str) -> str | None:
    output = _run(
        (
            "docker",
            "ps",
            "--filter",
            f"label=com.docker.compose.project={COMPOSE_PROJECT}",
            "--filter",
            f"label=com.docker.compose.service={service}",
            "--format",
            "{{.ID}}",
        ),
        f"finding the {service} container",
    )
    identifiers = output.splitlines()
    if len(identifiers) > 1:
        raise CredentialError(f"found multiple running {service} containers")
    return identifiers[0] if identifiers else None


def _container_environment(container: str, key: str) -> str:
    return _run(
        ("docker", "exec", container, "sh", "-c", f'printf %s "${{{key}:-}}"'),
        f"reading {key} from a running container",
    )


def _provisioned_observer_value(field: str) -> str:
    for line in DATASOURCE_PATH.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith(f"{field}:"):
            value = stripped.split(":", 1)[1].strip()
            return "" if value.startswith("$") else value
    raise CredentialError(f"could not find the provisioned ClickHouse {field}")


def _current_credentials(
    clickhouse_container: str | None,
    grafana_container: str | None,
) -> dict[str, str]:
    values = _read_environment()
    if clickhouse_container:
        values["CLICKHOUSE_USER"] = _container_environment(
            clickhouse_container, "CLICKHOUSE_USER"
        )
        values["CLICKHOUSE_PASSWORD"] = _container_environment(
            clickhouse_container, "CLICKHOUSE_PASSWORD"
        )
    if grafana_container:
        values["GRAFANA_ADMIN_USER"] = _container_environment(
            grafana_container, "GF_SECURITY_ADMIN_USER"
        )
        values["GRAFANA_ADMIN_PASSWORD"] = _container_environment(
            grafana_container, "GF_SECURITY_ADMIN_PASSWORD"
        )
        observer_user = _container_environment(
            grafana_container, "CLICKHOUSE_OBSERVER_USER"
        )
        observer_password = _container_environment(
            grafana_container, "CLICKHOUSE_OBSERVER_PASSWORD"
        )
        values["CLICKHOUSE_OBSERVER_USER"] = (
            observer_user or _provisioned_observer_value("username")
        )
        values["CLICKHOUSE_OBSERVER_PASSWORD"] = (
            observer_password or _provisioned_observer_value("password")
        )
    values.setdefault(
        "CLICKHOUSE_OBSERVER_USER", NON_SECRET_DEFAULTS["CLICKHOUSE_OBSERVER_USER"]
    )
    values.setdefault("CLICKHOUSE_OBSERVER_PASSWORD", "")
    return values


def _validate_usernames(values: Mapping[str, str]) -> None:
    for key in ("CLICKHOUSE_USER", "CLICKHOUSE_OBSERVER_USER"):
        if not SAFE_USER.fullmatch(values[key]):
            raise CredentialError(f"{key} must be a simple ClickHouse identifier")


def _alter_clickhouse_password(
    container: str,
    *,
    auth_user: str,
    auth_password: str,
    target_user: str,
    new_password: str,
) -> None:
    _run(
        (
            "docker", "exec",
            "-e", f"ROTATION_AUTH_USER={auth_user}",
            "-e", f"ROTATION_AUTH_PASSWORD={auth_password}",
            "-e", f"ROTATION_TARGET_USER={target_user}",
            "-e", f"ROTATED_PASSWORD={new_password}",
            container,
            "sh", "-ec",
            "exec clickhouse-client "
            "--user \"$ROTATION_AUTH_USER\" "
            "--password \"$ROTATION_AUTH_PASSWORD\" "
            "--param_target_user \"$ROTATION_TARGET_USER\" "
            "--param_rotated_password \"$ROTATED_PASSWORD\" "
            "--query 'ALTER USER {target_user:Identifier} IDENTIFIED WITH "
            "sha256_password BY {rotated_password:String}'",
        ),
        f"rotating the ClickHouse password for {target_user}",
    )


def _reset_grafana_password(container: str, password: str) -> None:
    _run(
        (
            "docker", "exec", "-e", f"ROTATED_PASSWORD={password}", container,
            "sh", "-ec",
            "exec grafana cli --homepath /usr/share/grafana "
            "admin reset-admin-password \"$ROTATED_PASSWORD\"",
        ),
        "rotating the Grafana administrator password",
    )


def _best_effort(action: Callable[[], None]) -> None:
    try:
        action()
    except CredentialError:
        pass


def _rollback_rotation(
    clickhouse_container: str | None,
    grafana_container: str | None,
    current: Mapping[str, str],
    completed: set[str],
) -> None:
    if (
        clickhouse_container
        and "observer" in completed
        and current.get("CLICKHOUSE_OBSERVER_PASSWORD")
    ):
        _best_effort(
            lambda: _alter_clickhouse_password(
                clickhouse_container,
                auth_user=current["CLICKHOUSE_USER"],
                auth_password=current["CLICKHOUSE_PASSWORD"],
                target_user=current["CLICKHOUSE_OBSERVER_USER"],
                new_password=current["CLICKHOUSE_OBSERVER_PASSWORD"],
            )
        )
    if grafana_container and "grafana" in completed:
        _best_effort(
            lambda: _reset_grafana_password(
                grafana_container, current["GRAFANA_ADMIN_PASSWORD"]
            )
        )


def _rotate_running_services(
    clickhouse_container: str | None,
    grafana_container: str | None,
    current: Mapping[str, str],
    rotated: Mapping[str, str],
) -> None:
    completed: set[str] = set()
    try:
        if grafana_container:
            _reset_grafana_password(
                grafana_container, rotated["GRAFANA_ADMIN_PASSWORD"]
            )
            completed.add("grafana")
        if clickhouse_container:
            _alter_clickhouse_password(
                clickhouse_container,
                auth_user=current["CLICKHOUSE_USER"],
                auth_password=current["CLICKHOUSE_PASSWORD"],
                target_user=current["CLICKHOUSE_OBSERVER_USER"],
                new_password=rotated["CLICKHOUSE_OBSERVER_PASSWORD"],
            )
            completed.add("observer")
    except CredentialError:
        _rollback_rotation(
            clickhouse_container, grafana_container, current, completed
        )
        raise


def generate_credentials(_args: argparse.Namespace) -> int:
    current = _read_environment()
    if ENV_PATH.exists() and all(current.get(key) for key in SECRET_KEYS):
        print("Local credentials already exist in .env; no changes were made.")
        return 0
    if _container_id("clickhouse") or _container_id("grafana"):
        raise CredentialError(
            "running services were detected; use 'make rotate-credentials'"
        )
    _write_environment(_new_environment(current))
    print("Generated local credentials in .env with permissions 0600.")
    return 0


def rotate_credentials(_args: argparse.Namespace) -> int:
    clickhouse_container = _container_id("clickhouse")
    grafana_container = _container_id("grafana")
    current = _current_credentials(clickhouse_container, grafana_container)
    rotated = _new_environment(current)
    _validate_usernames(rotated)
    _rotate_running_services(
        clickhouse_container, grafana_container, current, rotated
    )
    try:
        _write_environment(rotated)
    except OSError as exc:
        _rollback_rotation(
            clickhouse_container,
            grafana_container,
            current,
            {"observer", "grafana"},
        )
        raise CredentialError(f"writing .env failed: {exc}") from exc
    print(
        "Rotated persisted credentials, staged the environment-managed password, "
        "and stored all values in .env with permissions 0600."
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    generate = subparsers.add_parser("generate", help="create .env when no lab is running")
    generate.set_defaults(handler=generate_credentials)
    rotate = subparsers.add_parser("rotate", help="rotate .env and any running services")
    rotate.set_defaults(handler=rotate_credentials)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        return args.handler(args)
    except CredentialError as exc:
        print(f"credentials: error: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
