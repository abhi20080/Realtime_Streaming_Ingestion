#!/usr/bin/env python3
"""Fast syntax checks for configuration-as-code assets."""

from __future__ import annotations

import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    yaml_paths = [
        ROOT / "docker-compose.yml",
        ROOT / "kafka/kafka.yml",
        ROOT / "observability/loki/loki.yml",
        ROOT / "observability/prometheus/prometheus.yml",
        ROOT / "observability/prometheus/rules.yml",
    ]
    yaml_paths.extend((ROOT / "observability/grafana/provisioning").rglob("*.yml"))
    yaml_paths.extend((ROOT / "observability/grafana/provisioning").rglob("*.yaml"))

    for path in yaml_paths:
        with path.open(encoding="utf-8") as stream:
            yaml.safe_load(stream)
        print(f"yaml ok: {path.relative_to(ROOT)}")

    dashboards = ROOT / "observability/grafana/provisioning/dashboards"
    for path in dashboards.rglob("*.json"):
        with path.open(encoding="utf-8") as stream:
            document = json.load(stream)
        if not document.get("title") or "panels" not in document:
            raise ValueError(f"dashboard is missing title/panels: {path}")
        print(f"json ok: {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
