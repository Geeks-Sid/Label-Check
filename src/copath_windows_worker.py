"""Windows-side CoPath query worker using the shared filesystem queue."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import inspect
import os
import shutil
import subprocess
import sys
import threading
import time
import traceback
import uuid
from pathlib import Path
from typing import Callable, Optional, Sequence

from copath_queue import (
    FUTURE_CLOCK_SKEW_SECONDS,
    PROTOCOL_VERSION,
    REQUEST_ID_PATTERN,
    QueueProtocolError,
    atomic_write_json,
    format_utc,
    initialize_queue,
    read_json,
    utc_now,
    validate_request,
    validate_result_csv,
)


HEARTBEAT_INTERVAL_SECONDS = 5
CLAIM_RECOVERY_SECONDS = 10 * 60
TERMINAL_RETENTION_SECONDS = 24 * 60 * 60


class WorkerJobError(RuntimeError):
    def __init__(self, code: str, safe_message: str):
        """
        Initialize a worker error with a machine code and safe message.

        Args:
            code (str): Input code used by the operation.
            safe_message (str): Input safe message used by the operation.

        Returns:
            None: The operation completes through its side effects and returns no value.
        """
        super().__init__(safe_message)
        self.code = code
        self.safe_message = safe_message


QueryRunner = Callable[..., None]


def _remove_work_path(path: Path) -> None:
    """
    Remove a temporary worker path without following a symbolic link.

    Args:
        path (Path): Path to the input file or directory.

    Returns:
        None: The operation completes through its side effects and returns no value.
    """
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
    elif path.is_dir():
        shutil.rmtree(path)


class CoPathWindowsWorker:
    def __init__(
        self,
        queue_root: Path,
        connection_string_file: Path,
        query_runner: Optional[QueryRunner] = None,
    ) -> None:
        """
        Initialize the Windows CoPath worker and its shared queue.

        Args:
            queue_root (Path): Directory used as the queue root.
            connection_string_file (Path): Input filename or path for the connection string file.
            query_runner (Optional[QueryRunner]): Input query runner used by the operation.

        Returns:
            None: The operation completes through its side effects and returns no value.

        Raises:
            ValueError: If validation or the underlying resource operation fails.
        """
        self.queue_root = Path(queue_root).resolve()
        self.connection_string_file = Path(connection_string_file).resolve()
        if not self.connection_string_file.is_file():
            raise ValueError("the CoPath connection-string file does not exist")
        self.paths = initialize_queue(self.queue_root)
        self.query_runner = query_runner or self._run_query_cli
        self.worker_id = uuid.uuid4().hex
        self._stop = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None

    def publish_heartbeat(self) -> None:
        """
        Publish a current Windows worker heartbeat artifact.

        Returns:
            None: The operation completes through its side effects and returns no value.
        """
        atomic_write_json(self.queue_root / "worker.json", {
            "version": PROTOCOL_VERSION,
            "worker_id": self.worker_id,
            "updated_at": format_utc(utc_now()),
        })

    def _heartbeat_loop(self) -> None:
        """
        Refresh the worker heartbeat until shutdown is requested.

        Returns:
            None: The operation completes through its side effects and returns no value.
        """
        while not self._stop.is_set():
            try:
                self.publish_heartbeat()
            except OSError:
                traceback.print_exc()
            self._stop.wait(HEARTBEAT_INTERVAL_SECONDS)

    def start_heartbeat(self) -> None:
        """
        Publish an initial heartbeat and start its refresh thread.

        Returns:
            None: The operation completes through its side effects and returns no value.
        """
        self.publish_heartbeat()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name="copath-heartbeat", daemon=True
        )
        self._heartbeat_thread.start()

    def stop(self) -> None:
        """
        Stop the worker heartbeat and release worker resources.

        Returns:
            None: The operation completes through its side effects and returns no value.
        """
        self._stop.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=HEARTBEAT_INTERVAL_SECONDS + 1)
        heartbeat = self.queue_root / "worker.json"
        try:
            payload = read_json(heartbeat)
            if payload.get("worker_id") == self.worker_id:
                heartbeat.unlink(missing_ok=True)
        except (FileNotFoundError, QueueProtocolError, OSError):
            pass

    def _run_query_cli(
        self, accessions: Sequence[str], output_path: Path, work_dir: Path,
        scope: str = "exact_accession",
    ) -> None:
        """
        Run the configured CoPath query command in an isolated work directory.

        Args:
            accessions (Sequence[str]): Identifier values to process.
            output_path (Path): Path to the output.
            work_dir (Path): Directory used as the work dir.
            scope (str): Requested query or API scope.

        Returns:
            None: The operation completes through its side effects and returns no value.

        Raises:
            WorkerJobError: If validation or the underlying resource operation fails.
        """
        input_path = work_dir / "accessions.csv"
        with input_path.open("x", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["AccessionID"])
            writer.writeheader()
            writer.writerows({"AccessionID": accession} for accession in accessions)
        script = Path(__file__).parent / "copath_utilities" / "query_copath_db.py"
        environment = os.environ.copy()
        environment["COPATH_CONNECTION_STRING_FILE"] = str(self.connection_string_file)
        result = subprocess.run(
            [
                sys.executable, str(script), str(input_path), "-i",
                "history" if scope == "patient_history" else "accession",
                "-c", "AccessionID", "-o", str(output_path),
            ],
            cwd=work_dir,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode or not output_path.is_file():
            details = (result.stdout + result.stderr).strip()
            if details:
                print(details, file=sys.stderr)
            raise WorkerJobError(
                "query_failed",
                "The CoPath database query failed. Check the Windows worker console.",
            )

    def recover_stale_processing(
        self, now: Optional[dt.datetime] = None, max_age_seconds: float = CLAIM_RECOVERY_SECONDS
    ) -> int:
        """
        Return stale processing requests to the pending queue.

        Args:
            now (Optional[dt.datetime]): Reference time or date used for the operation.
            max_age_seconds (float): Numeric limit, duration, or count controlling the operation.

        Returns:
            int: Number of processing requests recovered.
        """
        current_timestamp = (now or utc_now()).timestamp()
        recovered = 0
        for claimed in self.paths["processing"].glob("*.json"):
            request_id = claimed.stem
            if not REQUEST_ID_PATTERN.fullmatch(request_id) or claimed.is_symlink():
                continue
            try:
                if current_timestamp - claimed.stat().st_mtime <= max_age_seconds:
                    continue
                if (
                    (self.paths["results"] / f"{request_id}.csv").exists()
                    or (self.paths["errors"] / f"{request_id}.json").exists()
                    or (self.paths["requests"] / claimed.name).exists()
                ):
                    claimed.unlink(missing_ok=True)
                    continue
                os.replace(claimed, self.paths["requests"] / claimed.name)
                recovered += 1
            except FileNotFoundError:
                continue
        return recovered

    def cleanup_old_artifacts(
        self, now: Optional[dt.datetime] = None, max_age_seconds: float = TERMINAL_RETENTION_SECONDS
    ) -> int:
        """
        Remove terminal queue artifacts older than the retention period.

        Args:
            now (Optional[dt.datetime]): Reference time or date used for the operation.
            max_age_seconds (float): Numeric limit, duration, or count controlling the operation.

        Returns:
            int: Number of terminal artifacts removed.
        """
        current_timestamp = (now or utc_now()).timestamp()
        removed = 0
        for directory in ("results", "errors", "work"):
            for path in self.paths[directory].iterdir():
                try:
                    if current_timestamp - path.lstat().st_mtime <= max_age_seconds:
                        continue
                    _remove_work_path(path)
                    removed += 1
                except FileNotFoundError:
                    continue
        return removed

    def _publish_error(self, request_id: str, code: str, message: str) -> None:
        """
        Publish a safe terminal error artifact for a queue request.

        Args:
            request_id (str): Input request id used by the operation.
            code (str): Input code used by the operation.
            message (str): Input message used by the operation.

        Returns:
            None: The operation completes through its side effects and returns no value.
        """
        if (self.paths["results"] / f"{request_id}.csv").exists():
            return
        atomic_write_json(self.paths["errors"] / f"{request_id}.json", {
            "version": PROTOCOL_VERSION,
            "request_id": request_id,
            "code": code,
            "message": message,
        })

    def _publish_result(self, request_id: str, source: Path) -> None:
        """
        Atomically publish a validated query result CSV for a request.

        Args:
            request_id (str): Input request id used by the operation.
            source (Path): Input source used by the operation.

        Returns:
            None: The operation completes through its side effects and returns no value.
        """
        destination = self.paths["results"] / f"{request_id}.csv"
        temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
        try:
            shutil.copyfile(source, temporary)
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)

    def process_claimed(self, claimed: Path) -> None:
        """
        Process one claimed queue request and publish its terminal artifact.

        Args:
            claimed (Path): Input claimed used by the operation.

        Returns:
            None: The operation completes through its side effects and returns no value.

        Raises:
            WorkerJobError: If validation or the underlying resource operation fails.
        """
        request_id = claimed.stem
        work_dir = self.paths["work"] / request_id
        try:
            payload = read_json(claimed)
            _, created_at, accessions, scope = validate_request(payload, request_id)
            if (created_at - utc_now()).total_seconds() > FUTURE_CLOCK_SKEW_SECONDS:
                raise WorkerJobError("invalid_request", "The request creation time is invalid.")
            if (
                (self.paths["results"] / f"{request_id}.csv").exists()
                or (self.paths["errors"] / f"{request_id}.json").exists()
            ):
                return
            if work_dir.exists() or work_dir.is_symlink():
                _remove_work_path(work_dir)
            work_dir.mkdir()
            output_path = work_dir / "result.csv"
            if len(inspect.signature(self.query_runner).parameters) >= 4:
                self.query_runner(accessions, output_path, work_dir, scope)
            else:
                self.query_runner(accessions, output_path, work_dir)
            try:
                validate_result_csv(output_path, accessions, scope)
            except QueueProtocolError as exc:
                raise WorkerJobError(
                    "invalid_result", "The CoPath query returned an invalid result."
                ) from exc
            self._publish_result(request_id, output_path)
        except WorkerJobError as exc:
            self._publish_error(request_id, exc.code, exc.safe_message)
        except QueueProtocolError:
            self._publish_error(
                request_id, "invalid_request", "The queued CoPath request or result was invalid."
            )
        except Exception:
            traceback.print_exc()
            self._publish_error(
                request_id, "worker_error", "The Windows CoPath worker could not complete the request."
            )
        finally:
            claimed.unlink(missing_ok=True)
            if work_dir.exists() or work_dir.is_symlink():
                _remove_work_path(work_dir)

    def claim_one(self) -> Optional[Path]:
        """
        Claim the next eligible queue request by atomically moving it to processing.

        Returns:
            Optional[Path]: Claimed request path, or None when no request is ready.
        """
        requests = list(self.paths["requests"].glob("*.json"))
        def priority(path: Path) -> tuple:
            """
            Return the scheduling priority for a queue request path.

            Args:
                path (Path): Path to the input file or directory.

            Returns:
                tuple: Tuple used to order queue requests.
            """
            try:
                payload = read_json(path)
                return (payload.get("scope") == "patient_history", payload.get("created_at", ""), path.name)
            except Exception:
                return (False, "", path.name)
        for request in sorted(requests, key=priority):
            if not REQUEST_ID_PATTERN.fullmatch(request.stem) or request.is_symlink():
                continue
            claimed = self.paths["processing"] / request.name
            if claimed.exists():
                continue
            try:
                os.replace(request, claimed)
                return claimed
            except FileNotFoundError:
                continue
        return None

    def run_once(self) -> bool:
        """
        Claim and process at most one queue request.

        Returns:
            bool: True when a request was processed; otherwise False.
        """
        claimed = self.claim_one()
        if claimed is None:
            return False
        self.process_claimed(claimed)
        return True

    def run_forever(self) -> None:
        """
        Run worker maintenance and request processing until stopped.

        Returns:
            None: The operation completes through its side effects and returns no value.
        """
        self.recover_stale_processing()
        self.cleanup_old_artifacts()
        last_maintenance = time.monotonic()
        self.start_heartbeat()
        print(f"CoPath Windows worker is watching {self.queue_root}")
        try:
            while not self._stop.is_set():
                if not self.run_once():
                    self._stop.wait(0.25)
                if time.monotonic() - last_maintenance >= 60:
                    self.recover_stale_processing()
                    self.cleanup_old_artifacts()
                    last_maintenance = time.monotonic()
        except KeyboardInterrupt:
            print("Stopping CoPath Windows worker.")
        finally:
            self.stop()


def main() -> int:
    """
    Run the command-line entry point.

    Returns:
        int: Process exit status, where applicable.
    """
    parser = argparse.ArgumentParser(description="Run the Windows-side CoPath query worker")
    parser.add_argument("--queue", required=True, type=Path, help="Shared CoPath queue directory")
    parser.add_argument(
        "--connection-string-file", required=True, type=Path,
        help="File containing the Windows-authenticated ODBC connection string",
    )
    args = parser.parse_args()
    try:
        worker = CoPathWindowsWorker(args.queue, args.connection_string_file)
        worker.run_forever()
    except (OSError, ValueError, QueueProtocolError) as exc:
        print(f"Worker configuration error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
