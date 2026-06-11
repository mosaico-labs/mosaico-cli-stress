"""Download stress test command."""

from __future__ import annotations

import itertools
import multiprocessing
import multiprocessing.synchronize
import time
from multiprocessing import Event, Lock, Process, Value
from typing import Any, List, Optional

import typer
from mosaicolabs import MosaicoClient

from mosaico_stress.connection import discover_resources, get_connect_kwargs
from mosaico_stress.utils import (
    Operation,
    console,
    error_console,
    parse_duration,
    parse_size,
    print_report,
)

FLUSH_EVERY = 100

def download_worker(
    client_id: int,
    resources: List[str],
    stop_event: multiprocessing.synchronize.Event,
    total_bytes: Any,
    lock: multiprocessing.synchronize.Lock,
    max_bytes: Optional[int],
    connect_kwargs: dict,
    result_queue: multiprocessing.Queue,
) -> None:
    """Download topics in round-robin until the stop event fires."""
    ops: List[dict] = []

    with MosaicoClient.connect(**connect_kwargs) as sdk_client:
        for resource in itertools.cycle(resources):
            if stop_event.is_set():
                break

            sequence_name, topic_name = resource.split("/", 1)
            handler = sdk_client.topic_handler(sequence_name, topic_name)
            if not handler:
                continue

            streamer = handler.get_data_streamer(
                handler.timestamp_ns_min,
                handler.timestamp_ns_max,
            )

            batch_start = time.time()
            batch_bytes = 0
            msg_count = 0

            for message in streamer:
                batch_bytes += message._to_pa_record_batch().nbytes
                msg_count += 1

                if msg_count % FLUSH_EVERY == 0:
                    # Record batch operation
                    batch_duration = time.time() - batch_start
                    ops.append({
                        "client_id": client_id,
                        "duration_seconds": batch_duration,
                        "bytes_transferred": batch_bytes,
                        "throughput_mbs": (batch_bytes / (1024 * 1024)) / batch_duration if batch_duration > 0 else 0,
                    })

                    with lock:
                        total_bytes.value += batch_bytes

                    if max_bytes and total_bytes.value >= max_bytes:
                        stop_event.set()
                        break
                    if stop_event.is_set():
                        break

                    # Reset for next batch
                    batch_start = time.time()
                    batch_bytes = 0

            # Flush remaining messages in partial batch
            if batch_bytes > 0:
                batch_duration = time.time() - batch_start
                ops.append({
                    "client_id": client_id,
                    "duration_seconds": batch_duration,
                    "bytes_transferred": batch_bytes,
                    "throughput_mbs": (batch_bytes / (1024 * 1024)) / batch_duration if batch_duration > 0 else 0,
                })
                with lock:
                    total_bytes.value += batch_bytes

            if max_bytes and total_bytes.value >= max_bytes:
                stop_event.set()
                break

    result_queue.put(ops)

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

    # Shared state across processes
    stop_event = Event()
    total_bytes = Value("q", 0)
    lock = Lock()
    result_queue = multiprocessing.Queue()

    # Spawn worker processes
    processes: List[Process] = []
    start = time.time()

    for i in range(client):
        p = Process(
            target=download_worker,
            args=(i, resources, stop_event, total_bytes, lock, max_bytes, connect_kwargs, result_queue),
        )
        p.start()
        processes.append(p)

    # Time limit watchdog
    if max_seconds:
        import threading

        def _timer():
            time.sleep(max_seconds)
            stop_event.set()

        threading.Thread(target=_timer, daemon=True).start()

    # Wait for all workers to finish
    for p in processes:
        p.join()

    total_duration = time.time() - start

    # Collect results from queue
    metrics_bucket: List[Operation] = []
    while not result_queue.empty():
        ops = result_queue.get_nowait()
        for op_dict in ops:
            metrics_bucket.append(Operation(**op_dict))

    shared_state = {
        "total_bytes": total_bytes.value,
        "max_bytes": max_bytes,
        "lock": lock,
    }

    print_report("download", total_duration, shared_state, metrics_bucket, client, verbose, output)
