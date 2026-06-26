"""Shared utilities for parsing CLI options and computing metrics."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import List

from rich.console import Console
from rich.table import Table

console = Console()
error_console = Console(stderr=True)


_SIZE_UNITS = {
    "B": 1,
    "KB": 1_024,
    "KIB": 1_024,
    "MB": 1_024 ** 2,
    "MIB": 1_024 ** 2,
    "GB": 1_024 ** 3,
    "GIB": 1_024 ** 3,
    "TB": 1_024 ** 4,
    "TIB": 1_024 ** 4,
}

_DURATION_UNITS = {
    "s": 1,
    "m": 60,
    "h": 3600,
}


def parse_size(size_str: str) -> int:
    """Parse a human-readable size string (e.g. '10GB', '500MB') into bytes."""
    match = re.match(r"^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$", size_str)
    if not match:
        raise ValueError(f"Invalid size format: '{size_str}'. Expected e.g. '10GB', '500MB'.")
    value = float(match.group(1))
    unit = match.group(2).upper()
    if unit not in _SIZE_UNITS:
        raise ValueError(f"Unknown size unit: '{unit}'. Supported: {list(_SIZE_UNITS.keys())}")
    return int(value * _SIZE_UNITS[unit])


def parse_duration(dur_str: str) -> float:
    """Parse a duration string (e.g. '5m', '30s', '1h') into seconds."""
    match = re.match(r"^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$", dur_str)
    if not match:
        raise ValueError(f"Invalid duration format: '{dur_str}'. Expected e.g. '5m', '30s', '1h'.")
    value = float(match.group(1))
    unit = match.group(2).lower()
    if unit not in _DURATION_UNITS:
        raise ValueError(f"Unknown duration unit: '{unit}'. Supported: {list(_DURATION_UNITS.keys())}")
    return value * _DURATION_UNITS[unit]


@dataclass
class Operation:
    """Metrics for a single stress test operation (upload or download)."""

    client_id: int
    duration_seconds: float
    bytes_transferred: int
    throughput_mbs: float


def print_report(
    mode: str,
    total_duration: float,
    shared_state: dict,
    metrics_bucket: List[Operation],
    num_clients: int,
    verbose: bool,
    output: str = "table",
) -> None:
    """Print stress test results. mode is 'upload' or 'download'."""
    total_bytes = shared_state["total_bytes"]
    total_mbs = total_bytes / (1024 * 1024)
    avg_throughput = total_mbs / total_duration if total_duration > 0 else 0

    label = "uploaded" if mode == "upload" else "downloaded"
    title = f"{'Upload' if mode == 'upload' else 'Download'} Stress Test Results"

    if output == "json":
        report = {
            "mode": mode,
            "duration_seconds": round(total_duration, 3),
            "total_bytes": total_bytes,
            "total_mb": round(total_mbs, 2),
            "avg_throughput_mbs": round(avg_throughput, 2),
            "clients": num_clients,
            "operations": len(metrics_bucket),
            "details": [
                {
                    "client_id": op.client_id,
                    "bytes_transferred": op.bytes_transferred,
                    "duration_seconds": round(op.duration_seconds, 3),
                    "throughput_mbs": round(op.throughput_mbs, 2),
                }
                for op in metrics_bucket
            ],
        }
        console.print_json(json.dumps(report))
        return

    console.print()
    console.print(f"[bold green]═══ {title} ═══[/bold green]")
    console.print(f"  Duration:         {total_duration:.2f}s")
    console.print(f"  Total {label}: {total_mbs:.2f} MB")
    console.print(f"  Avg throughput:   {avg_throughput:.2f} MB/s")
    console.print(f"  Clients:          {num_clients}")
    console.print(f"  Operations:       {len(metrics_bucket)}")
    console.print()

    if verbose and metrics_bucket:
        # Aggregate per-client stats
        from collections import defaultdict

        client_stats: dict = defaultdict(lambda: {"ops": 0, "bytes": 0, "durations": []})
        for op in metrics_bucket:
            s = client_stats[op.client_id]
            s["ops"] += 1
            s["bytes"] += op.bytes_transferred
            s["durations"].append(op.duration_seconds)

        col_label = "Uploaded" if mode == "upload" else "Downloaded"
        table = Table(title="Per-client summary")
        table.add_column("Client", style="cyan")
        table.add_column("Ops", style="white")
        table.add_column(col_label, style="green")
        table.add_column("Avg op time", style="yellow")
        table.add_column("Avg throughput", style="magenta")

        for cid in sorted(client_stats):
            s = client_stats[cid]
            total_mb = s["bytes"] / (1024 * 1024)
            avg_dur = sum(s["durations"]) / len(s["durations"]) if s["durations"] else 0
            total_dur = sum(s["durations"])
            avg_tp = total_mb / total_dur if total_dur > 0 else 0
            table.add_row(
                str(cid),
                str(s["ops"]),
                f"{total_mb:.2f} MB",
                f"{avg_dur:.3f}s",
                f"{avg_tp:.2f} MB/s",
            )

        console.print(table)


def size_limit_reached(shared_state: dict, pending: int = 0) -> bool:
    """Check whether the cumulative transfer has hit the size cap."""
    max_bytes = shared_state["max_bytes"]
    if not max_bytes:
        return False
    with shared_state["lock"]:
        return shared_state["total_bytes"] + pending >= max_bytes
