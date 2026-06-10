"""Download stress test command."""

from __future__ import annotations

import itertools
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import typer
from mosaicolabs import MosaicoClient

from mosaico_stress.connection import discover_resources, get_connect_kwargs
from mosaico_stress.utils import (
    DownloadOperation,
    console,
    download_size_limit_reached,
    error_console,
    parse_duration,
    parse_size,
    print_download_report,
)

FLUSH_EVERY = 100

def download_worker(
    client_id: int,
    resources: List[str],
    shared_state: dict,
    metrics_bucket: List[DownloadOperation],
    connect_kwargs: dict,
) -> None:
    """Download topics in round-robin until the stop event fires."""
    with MosaicoClient.connect(**connect_kwargs) as sdk_client:
        for resource in itertools.cycle(resources):
            if shared_state["stop_event"].is_set():
                break

            start_time = time.time()
            bytes_received = 0

            sequence_name, topic_name = resource.split("/", 1)
            handler = sdk_client.topic_handler(sequence_name, topic_name)
            if not handler:
                continue

            streamer = handler.get_data_streamer(
                handler.timestamp_ns_min,
                handler.timestamp_ns_max,
            )
            msg_count = 0

            for message in streamer:
                bytes_received += len(message.model_dump_json())
                msg_count += 1

                if msg_count % FLUSH_EVERY == 0:
                    if download_size_limit_reached(shared_state, bytes_received):
                        shared_state["stop_event"].set()
                        break
                    if shared_state["stop_event"].is_set():
                        break

            duration = time.time() - start_time

            metrics_bucket.append(
                DownloadOperation(
                    client_id=client_id,
                    duration_seconds=duration,
                    bytes_downloaded=bytes_received,
                    throughput_mbs=(bytes_received / (1024 * 1024)) / duration if duration > 0 else 0,
                )
            )

            with shared_state["lock"]:
                shared_state["total_bytes"] += bytes_received

            if download_size_limit_reached(shared_state):
                shared_state["stop_event"].set()
                break


app = typer.Typer(invoke_without_command=True)

@app.callback(invoke_without_command=True)
def download(
    client: int = typer.Option(..., "--client", help="Number of concurrent download clients."),
    size: Optional[str] = typer.Option(None, "--size", help="Maximum data volume (e.g. 10GB, 500MB)."),
    time_limit: Optional[str] = typer.Option(None, "--time", help="Maximum test duration (e.g. 5m, 30s, 1h)."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Include per-client statistics in the report."),
    output: str = typer.Option("table", "--output", "-o", help="Report format: table or json."),
) -> None:
    """
    Run a download stress test against the Mosaico platform.

    Downloads data from existing sequences/topics using multiple concurrent clients
    in round-robin fashion. The test stops when the first of --size or --time limits
    is reached. At least one of --size or --time must be provided.
    """
    if not size and not time_limit:
        error_console.print("[bold red]Error:[/bold red] At least one of --size or --time must be specified.")
        raise typer.Exit(code=1)

    max_bytes = parse_size(size) if size else None
    max_seconds = parse_duration(time_limit) if time_limit else None
    connect_kwargs = get_connect_kwargs()

    if output != "json":
        console.print("[bold cyan]Discovering available topics...[/bold cyan]")

    resources = discover_resources(connect_kwargs)

    if not resources:
        error_console.print("[bold red]Error:[/bold red] No topics found. Upload some data first.")
        raise typer.Exit(code=1)

    if output != "json":
        console.print(f"  Found {len(resources)} topic(s)")
        console.print("[bold cyan]Starting download stress test[/bold cyan]")
        console.print(f"  Clients:  {client}")
        console.print(f"  Max size: {max_bytes / (1024*1024):.1f} MB" if max_bytes else "  Max size: unlimited")
        console.print(f"  Max time: {max_seconds:.0f}s" if max_seconds else "  Max time: unlimited")
        console.print(f"  Target:   {connect_kwargs['host']}:{connect_kwargs['port']}")
        console.print()

    stop_event = threading.Event()
    shared_state = {
        "total_bytes": 0,
        "stop_event": stop_event,
        "max_bytes": max_bytes,
        "lock": threading.Lock(),
    }
    metrics_bucket: List[DownloadOperation] = []

    def _timer():
        stop_event.wait(timeout=max_seconds)
        stop_event.set()

    if max_seconds:
        timer_thread = threading.Thread(target=_timer, daemon=True)
        timer_thread.start()

    start = time.time()

    with ThreadPoolExecutor(max_workers=client) as pool:
        futures = [
            pool.submit(download_worker, i, resources, shared_state, metrics_bucket, connect_kwargs)
            for i in range(client)
        ]
        # Wait for stop signal
        stop_event.wait()

        # Let workers finish their current iteration gracefully
        for f in futures:
            f.result()

    total_duration = time.time() - start
    print_download_report(total_duration, shared_state, metrics_bucket, client, verbose, output)
