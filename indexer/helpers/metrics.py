"""Prometheus textfile metrics for an indexer invocation."""

import contextlib
import os
import queue
import re
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any

METRIC_NAME_RE = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")


def validate_job_name(job_name: str) -> str:
    if not METRIC_NAME_RE.fullmatch(job_name):
        raise ValueError("metrics job name must be a valid Prometheus metric prefix")
    return job_name


def record_submission(cfg: dict, documents: int, successful: bool) -> None:
    """Send a safe, aggregate batch outcome to the parent process when enabled."""
    context: dict[str, Any] | None = cfg.get("metrics_context")
    if context is None:
        return
    context["queue"].put(
        {
            "project": context["project"],
            "record_type": context["record_type"],
            "documents": documents if successful else 0,
            "errors": 0 if successful else 1,
        }
    )


def record_error(cfg: dict) -> None:
    context: dict[str, Any] | None = cfg.get("metrics_context")
    if context is not None:
        context["queue"].put(
            {
                "project": context["project"],
                "record_type": context["record_type"],
                "documents": 0,
                "errors": 1,
            }
        )


def drain_events(events_queue: Any) -> tuple[dict[tuple[str, str], int], int]:
    counts: dict[tuple[str, str], int] = defaultdict(int)
    errors = 0
    while True:
        try:
            event = events_queue.get_nowait()
        except queue.Empty:
            break
        counts[(event["project"], event["record_type"])] += event["documents"]
        errors += event["errors"]
    return dict(counts), errors


def render_metrics(
    job_name: str,
    success: bool,
    finished_unixtime: int,
    duration_seconds: float,
    errors: int,
    counts: dict[tuple[str, str], int],
) -> str:
    prefix = validate_job_name(job_name)
    lines = [
        f"# HELP {prefix}_last_run_success Whether the most recent run succeeded (1) or failed (0).",
        f"# TYPE {prefix}_last_run_success gauge",
        f"{prefix}_last_run_success {int(success)}",
        f"# HELP {prefix}_last_finished_unixtime Unix timestamp when the most recent run finished.",
        f"# TYPE {prefix}_last_finished_unixtime gauge",
        f"{prefix}_last_finished_unixtime {finished_unixtime}",
        f"# HELP {prefix}_last_run_duration_seconds Duration of the most recent run in seconds.",
        f"# TYPE {prefix}_last_run_duration_seconds gauge",
        f"{prefix}_last_run_duration_seconds {duration_seconds:.6f}",
        f"# HELP {prefix}_last_run_errors Number of indexing errors in the most recent run.",
        f"# TYPE {prefix}_last_run_errors gauge",
        f"{prefix}_last_run_errors {errors}",
        f"# HELP {prefix}_last_run_records_indexed Number of Solr documents accepted in the most recent run.",
        f"# TYPE {prefix}_last_run_records_indexed gauge",
    ]
    for (project, record_type), count in sorted(counts.items()):
        lines.append(
            f'{prefix}_last_run_records_indexed{{project="{project}",record_type="{record_type}"}} {count}'
        )
    return "\n".join(lines) + "\n"


def write_metrics_atomically(directory: str, job_name: str, content: str) -> None:
    destination = Path(directory)
    final_path = destination / f"{validate_job_name(job_name)}.prom"
    fd, temp_path = tempfile.mkstemp(
        prefix=f".{final_path.name}.", suffix=".tmp", dir=destination
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temp_path, final_path)
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temp_path)
        raise
