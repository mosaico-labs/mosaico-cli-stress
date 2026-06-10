"""Shared utilities for parsing CLI options and computing metrics."""

from __future__ import annotations

import re
from ast import List
from dataclasses import dataclass

from rich.console import Console
from rich.table import Table

# --- Size Parsing ---

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


def format_size(size_bytes: int) -> str:
    """Format bytes to human-readable string."""
    if size_bytes >= 1_000_000_000:
        return f"{size_bytes / 1_000_000_000:.2f} GB"
    elif size_bytes >= 1_000_000:
        return f"{size_bytes / 1_000_000:.2f} MB"
    elif size_bytes >= 1_000:
        return f"{size_bytes / 1_000:.2f} KB"
    return f"{size_bytes} B"


def format_throughput(bytes_per_sec: float) -> str:
    """Format throughput as human-readable MB/s or GB/s."""
    if bytes_per_sec >= 1_000_000_000:
        return f"{bytes_per_sec / 1_000_000_000:.2f} GB/s"
    elif bytes_per_sec >= 1_000_000:
        return f"{bytes_per_sec / 1_000_000:.2f} MB/s"
    elif bytes_per_sec >= 1_000:
        return f"{bytes_per_sec / 1_000:.2f} KB/s"
    return f"{bytes_per_sec:.0f} B/s"


console = Console()
error_console = Console(stderr=True)

@dataclass
class DownloadOperation:
    """Metrics for a single topic download pass."""
    client_id: int
    duration_seconds: float
    bytes_downloaded: int
    throughput_mbs: float

def print_download_report(
    total_duration: float,
    shared_state: dict,
    metrics_bucket: List[DownloadOperation],
    num_clients: int,
    verbose: bool,
    output: str = "table",
) -> None:
    total_bytes = shared_state["total_bytes"]
    total_mbs = total_bytes / (1024 * 1024)
    avg_throughput = total_mbs / total_duration if total_duration > 0 else 0

    if output == "json":
        import json

        report = {
            "duration_seconds": round(total_duration, 3),
            "total_bytes": total_bytes,
            "total_mb": round(total_mbs, 2),
            "avg_throughput_mbs": round(avg_throughput, 2),
            "clients": num_clients,
            "operations": len(metrics_bucket),
            "details": [
                {
                    "client_id": op.client_id,
                    "bytes_downloaded": op.bytes_downloaded,
                    "duration_seconds": round(op.duration_seconds, 3),
                    "throughput_mbs": round(op.throughput_mbs, 2),
                }
                for op in metrics_bucket
            ],
        }
        console.print_json(json.dumps(report))
        return

    console.print()
    console.print("[bold green]═══ Download Stress Test Results ═══[/bold green]")
    console.print(f"  Duration:         {total_duration:.2f}s")
    console.print(f"  Total downloaded: {total_mbs:.2f} MB")
    console.print(f"  Avg throughput:   {avg_throughput:.2f} MB/s")
    console.print(f"  Clients:          {num_clients}")
    console.print(f"  Operations:       {len(metrics_bucket)}")
    console.print()

    if verbose and metrics_bucket:
        table = Table(title="Per-operation breakdown")
        table.add_column("Client", style="cyan")
        table.add_column("Downloaded", style="green")
        table.add_column("Duration", style="yellow")
        table.add_column("Throughput", style="magenta")

        for op in metrics_bucket:
            table.add_row(
                str(op.client_id),
                f"{op.bytes_downloaded / (1024 * 1024):.2f} MB",
                f"{op.duration_seconds:.2f}s",
                f"{op.throughput_mbs:.2f} MB/s",
            )
        console.print(table)

def download_size_limit_reached(shared_state: dict, pending: int = 0) -> bool:
    """Check whether the cumulative download has hit the size cap."""
    max_bytes = shared_state["max_bytes"]
    if not max_bytes:
        return False
    with shared_state["lock"]:
        return shared_state["total_bytes"] + pending >= max_bytes
