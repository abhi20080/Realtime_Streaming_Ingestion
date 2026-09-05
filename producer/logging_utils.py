"""One-line JSON logs shared by the producer loop and Kafka statistics callback."""

from __future__ import annotations

import json
import sys
import threading
from datetime import datetime
from typing import Any, TextIO

if __package__:
    from .workload import format_utc, utc_now
else:  # Support the Docker/direct-script entry point.
    from workload import format_utc, utc_now

_LOG_LOCK = threading.Lock()


def emit_json_log(
    event: str,
    *,
    level: str = "INFO",
    stream: TextIO | None = None,
    timestamp: datetime | None = None,
    **fields: Any,
) -> None:
    """Write exactly one compact JSON object per log entry."""

    record = {
        "timestamp": format_utc(timestamp or utc_now()),
        "level": level,
        "event": event,
        **fields,
    }
    line = json.dumps(record, separators=(",", ":"), sort_keys=True, default=str)
    destination = stream or sys.stdout
    with _LOG_LOCK:
        destination.write(line + "\n")
        destination.flush()


