"""Upload stress test command."""

from __future__ import annotations

import multiprocessing
import multiprocessing.synchronize
import random
import time
import uuid
from multiprocessing import Event, Lock, Process, Value
from typing import Any, List, Optional

import typer
from mosaicolabs import (
    IMU,
    Message,
    MosaicoClient,
    SessionLevelErrorPolicy,
    Vector3d,
)

from mosaico_stress.connection import get_connect_kwargs
from mosaico_stress.utils import (
    Operation,
    console,
    error_console,
    parse_duration,
    parse_size,
    print_report,
)

BATCH_SIZE = 100

def upload_worker(
    client_id: int,
    stop_event: multiprocessing.synchronize.Event,
    total_bytes: Any,
    lock: multiprocessing.synchronize.Lock,
    max_bytes: Optional[int],
    connect_kwargs: dict,
    sequence_prefix: str,
    result_queue: multiprocessing.Queue,
) -> None:
    """Upload random IMU data until the stop event fires."""
    ops: List[dict] = []

    with MosaicoClient.connect(**connect_kwargs) as sdk_client:
        seq_name = f"{sequence_prefix}_client{client_id}"

        with sdk_client.sequence_create(
            sequence_name=seq_name,
            metadata={"stress_test": "true", "client_id": str(client_id)},
            on_error=SessionLevelErrorPolicy.Delete,
        ) as seq_writer:
            topic_writer = seq_writer.topic_create(
                topic_name="stress/data",
                metadata={"type": "random_imu"},
                ontology_type=IMU,
            )

            if not topic_writer:
                result_queue.put(ops)
                return

            ts_ns = 1_700_000_000_000_000_000

            while not stop_event.is_set():
                start_time = time.time()
                bytes_sent = 0

                for _ in range(BATCH_SIZE):
                    msg = Message(
                        timestamp_ns=ts_ns,
                        data=IMU(
                            acceleration=Vector3d(
                                x=random.uniform(-10, 10),
                                y=random.uniform(-10, 10),
                                z=random.uniform(9.0, 10.0),
                            ),
                            angular_velocity=Vector3d(
                                x=random.uniform(-1, 1),
                                y=random.uniform(-1, 1),
                                z=random.uniform(-1, 1),
                            ),
                        ),
                    )
                    topic_writer.push(message=msg)
                    ts_ns += 10_000_000
                    bytes_sent += msg._to_pa_record_batch().nbytes

                duration = time.time() - start_time

                ops.append({
                    "client_id": client_id,
                    "duration_seconds": duration,
                    "bytes_transferred": bytes_sent,
                    "throughput_mbs": (bytes_sent / (1024 * 1024)) / duration if duration > 0 else 0,
                })

                with lock:
                    total_bytes.value += bytes_sent

                if max_bytes and total_bytes.value >= max_bytes:
                    stop_event.set()
                    break

    result_queue.put(ops)


def _cleanup_sequences(connect_kwargs: dict, sequence_prefix: str, num_clients: int) -> None:
    """Delete uploaded sequences after the test."""
    try:
        with MosaicoClient.connect(**connect_kwargs) as sdk_client:
            for i in range(num_clients):
                seq_name = f"{sequence_prefix}_client{i}"
                try:
                    sdk_client.sequence_delete(seq_name)
                except Exception:
                    pass
    except Exception as e:
        error_console.print(f"[yellow]Warning:[/yellow] Cleanup failed: {e}")

app = typer.Typer(invoke_without_command=True)


@app.callback(invoke_without_command=True)
def upload(
    client: int = typer.Option(..., "--client", help="Number of concurrent upload clients."),
    size: Optional[str] = typer.Option(None, "--size", help="Maximum data volume (e.g. 10GB, 500MB)."),
    time_limit: Optional[str] = typer.Option(None, "--time", help="Maximum test duration (e.g. 5m, 30s, 1h)."),
    sequence_name: Optional[str] = typer.Option(None, "--name", "-n", help="Base name for uploaded sequences (default: auto-generated)."),
    no_cleanup: bool = typer.Option(False, "--no-cleanup", help="Skip cleanup of uploaded data after the test."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Include per-client statistics in the report."),
    output: str = typer.Option("table", "--output", "-o", help="Report format: table or json."),
) -> None:
    """
    Run an upload stress test against the Mosaico platform.

    Generates random IMU data and uploads it using multiple concurrent clients.
    The test stops when the first of --size or --time limits is reached.
    At least one of --size or --time must be provided.
    """
    if not size and not time_limit:
        error_console.print("[bold red]Error:[/bold red] At least one of --size or --time must be specified.")
        raise typer.Exit(code=1)

    max_bytes = parse_size(size) if size else None
    max_seconds = parse_duration(time_limit) if time_limit else None
    connect_kwargs = get_connect_kwargs()
    sequence_prefix = sequence_name if sequence_name else f"stress_upload_{uuid.uuid4().hex[:8]}"

    if output != "json":
        console.print("[bold cyan]Starting upload stress test[/bold cyan]")
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
            target=upload_worker,
            args=(i, stop_event, total_bytes, lock, max_bytes, connect_kwargs, sequence_prefix, result_queue),
        )
        p.start()
        processes.append(p)

    # Time limit watchdog (in main process)
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

    print_report("upload", total_duration, shared_state, metrics_bucket, client, verbose, output)

    # Cleanup
    if not no_cleanup:
        if output != "json":
            console.print("[dim]Cleaning up uploaded sequences...[/dim]")
        _cleanup_sequences(connect_kwargs, sequence_prefix, client)
        if output != "json":
            console.print("[dim]Cleanup complete.[/dim]")
