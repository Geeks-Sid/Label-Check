"""Validated filesystem queue shared by Label-Check and its Windows worker."""

from __future__ import annotations

import csv
import datetime as dt
import json
import os
import re
import time
import uuid
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple


PROTOCOL_VERSION = 1
QUERY_SCOPES = ("exact_accession", "patient_history")
MAX_ACCESSIONS = 10_000
HEARTBEAT_MAX_AGE_SECONDS = 15
FUTURE_CLOCK_SKEW_SECONDS = 30
MAX_JSON_BYTES = 64 * 1024 * 1024
REQUEST_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")
ERROR_CODE_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
QUEUE_DIRECTORIES = ("requests", "processing", "results", "errors", "work")


class QueueProtocolError(RuntimeError):
    """A safe-to-display filesystem queue error."""


def utc_now() -> dt.datetime:
    """
    Return the current timezone-aware UTC datetime.

    Returns:
        dt.datetime: Current timezone-aware UTC datetime.
    """
    return dt.datetime.now(dt.timezone.utc)


def format_utc(value: dt.datetime) -> str:
    """
    Format a datetime as a normalized UTC timestamp.

    Args:
        value (dt.datetime): Input value to validate, transform, or persist.

    Returns:
        str: String representation or formatted value produced by the operation.
    """
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def parse_utc(value: object, field: str) -> dt.datetime:
    """
    Parse and normalize a UTC timestamp from queue data.

    Args:
        value (object): Input value to validate, transform, or persist.
        field (str): Field or column metadata used by the operation.

    Returns:
        dt.datetime: Timezone-aware UTC datetime.

    Raises:
        QueueProtocolError: If validation or the underlying resource operation fails.
    """
    if not isinstance(value, str):
        raise QueueProtocolError(f"{field} must be a UTC timestamp")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise QueueProtocolError(f"{field} must be a UTC timestamp") from exc
    if parsed.tzinfo is None:
        raise QueueProtocolError(f"{field} must include a UTC offset")
    return parsed.astimezone(dt.timezone.utc)


def validate_request_id(value: object) -> str:
    """
    Validate a queue request identifier.

    Args:
        value (object): Input value to validate, transform, or persist.

    Returns:
        str: Validated request ID string.

    Raises:
        QueueProtocolError: If validation or the underlying resource operation fails.
    """
    if not isinstance(value, str) or not REQUEST_ID_PATTERN.fullmatch(value):
        raise QueueProtocolError("request_id is invalid")
    return value


def validate_accessions(values: object) -> List[str]:
    """
    Validate, normalize, and de-duplicate requested accessions.

    Args:
        values (object): Input values to validate, transform, or persist.

    Returns:
        List[str]: Normalized unique accession strings.

    Raises:
        QueueProtocolError: If validation or the underlying resource operation fails.
    """
    if not isinstance(values, list) or not values:
        raise QueueProtocolError("accessions must be a non-empty list")
    if len(values) > MAX_ACCESSIONS:
        raise QueueProtocolError(f"a request may contain at most {MAX_ACCESSIONS} accessions")
    normalized: List[str] = []
    seen = set()
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise QueueProtocolError("a request contains an invalid accession ID")
        accession = value.strip()
        key = accession.casefold()
        if key in seen:
            raise QueueProtocolError("a request contains duplicate accession IDs")
        seen.add(key)
        normalized.append(accession)
    return normalized


def queue_paths(root: Path) -> Dict[str, Path]:
    """
    Return paths for each directory in the shared queue.

    Args:
        root (Path): Directory used as the root.

    Returns:
        Dict[str, Path]: Mapping from queue directory names to paths.
    """
    root = Path(root)
    return {name: root / name for name in QUEUE_DIRECTORIES}


def initialize_queue(root: Path) -> Dict[str, Path]:
    """
    Create and secure all directories in the shared queue.

    Args:
        root (Path): Directory used as the root.

    Returns:
        Dict[str, Path]: Mapping from queue directory names to initialized paths.

    Raises:
        QueueProtocolError: If validation or the underlying resource operation fails.
    """
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink() or not root.is_dir():
        raise QueueProtocolError("The CoPath queue root must be a regular directory")
    paths = queue_paths(root)
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
        if path.is_symlink() or not path.is_dir():
            raise QueueProtocolError(
                "The CoPath queue contains an unsafe protocol directory"
            )
    return paths


def atomic_write_json(path: Path, payload: Dict[str, object]) -> None:
    """
    Write JSON content through a temporary file and atomic replace.

    Args:
        path (Path): Path to the input file or directory.
        payload (Dict[str, object]): Input payload to validate, transform, or persist.

    Returns:
        None: The operation completes through its side effects and returns no value.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def read_json(path: Path) -> Dict[str, object]:
    """
    Read and validate a JSON queue artifact.

    Args:
        path (Path): Path to the input file or directory.

    Returns:
        Dict[str, object]: Decoded JSON object from the queue artifact.

    Raises:
        QueueProtocolError: If validation or the underlying resource operation fails.
    """
    if path.is_symlink() or not path.is_file():
        raise QueueProtocolError(f"{path.name} is not a regular file")
    try:
        if path.stat().st_size > MAX_JSON_BYTES:
            raise QueueProtocolError(f"{path.name} is too large")
        value = json.loads(path.read_text(encoding="utf-8"))
    except QueueProtocolError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise QueueProtocolError(f"{path.name} is malformed") from exc
    if not isinstance(value, dict):
        raise QueueProtocolError(f"{path.name} must contain a JSON object")
    return value


def validate_request(payload: Dict[str, object], expected_id: Optional[str] = None) -> Tuple[str, dt.datetime, List[str], str]:
    """
    Validate a queue request payload and return its normalized fields.

    Args:
        payload (Dict[str, object]): Input payload to validate, transform, or persist.
        expected_id (Optional[str]): Input expected id used by the operation.

    Returns:
        Tuple[str, dt.datetime, List[str], str]: Tuple containing the request ID, expiry time, accession list, and scope.

    Raises:
        QueueProtocolError: If validation or the underlying resource operation fails.
    """
    version = payload.get("version")
    if version not in (1, PROTOCOL_VERSION):
        raise QueueProtocolError("unsupported queue protocol version")
    request_id = validate_request_id(payload.get("request_id"))
    if expected_id is not None and request_id != expected_id:
        raise QueueProtocolError("request_id does not match its queue filename")
    created_at = parse_utc(payload.get("created_at"), "created_at")
    accessions = validate_accessions(payload.get("accessions"))
    scope = payload.get("scope", "exact_accession")
    if scope not in QUERY_SCOPES:
        raise QueueProtocolError("query scope is invalid")
    return request_id, created_at, accessions, str(scope)


def require_fresh_heartbeat(root: Path, now: Optional[dt.datetime] = None) -> Dict[str, object]:
    """
    Require a recent Windows worker heartbeat and return its payload.

    Args:
        root (Path): Directory used as the root.
        now (Optional[dt.datetime]): Reference time or date used for the operation.

    Returns:
        Dict[str, object]: Worker heartbeat payload when the worker is available and current.

    Raises:
        QueueProtocolError: If validation or the underlying resource operation fails.
    """
    heartbeat_path = Path(root) / "worker.json"
    try:
        payload = read_json(heartbeat_path)
        if payload.get("version") != PROTOCOL_VERSION:
            raise QueueProtocolError("unsupported worker protocol version")
        updated_at = parse_utc(payload.get("updated_at"), "updated_at")
    except (FileNotFoundError, QueueProtocolError) as exc:
        raise QueueProtocolError(
            "The Windows CoPath worker is offline. Start the worker and retry."
        ) from exc
    current = now or utc_now()
    age = (current - updated_at).total_seconds()
    if age > HEARTBEAT_MAX_AGE_SECONDS or age < -FUTURE_CLOCK_SKEW_SECONDS:
        raise QueueProtocolError(
            "The Windows CoPath worker is offline. Start the worker and retry."
        )
    return payload


def validate_result_csv(
    path: Path, requested: Iterable[str], scope: str = "exact_accession"
) -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Validate a worker result CSV and return its identifiers and rows.

    Args:
        path (Path): Path to the input file or directory.
        requested (Iterable[str]): Input requested used by the operation.
        scope (str): Requested query or API scope.

    Returns:
        Tuple[List[str], List[Dict[str, str]]]: Tuple containing returned identifiers and validated result rows.

    Raises:
        QueueProtocolError: If validation or the underlying resource operation fails.
    """
    if path.is_symlink() or not path.is_file():
        raise QueueProtocolError("The Windows CoPath worker returned an unsafe result file")
    try:
        with path.open("r", newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            if (
                not fields or "accession_id" not in fields or len(fields) != len(set(fields))
                or (scope == "patient_history" and "mrn" not in fields)
            ):
                raise QueueProtocolError("The Windows CoPath worker returned a malformed CSV")
            rows = []
            requested_set = {value.strip().casefold() for value in requested}
            for row in reader:
                if None in row:
                    raise QueueProtocolError("The Windows CoPath worker returned a malformed CSV")
                clean = {key: value or "" for key, value in row.items()}
                accession = clean["accession_id"].strip()
                if not accession or (
                    scope == "exact_accession"
                    and accession.casefold() not in requested_set
                ):
                    raise QueueProtocolError(
                        "The Windows CoPath worker returned an accession that was not requested"
                    )
                clean["accession_id"] = accession
                rows.append(clean)
    except QueueProtocolError:
        raise
    except (OSError, UnicodeError, csv.Error) as exc:
        raise QueueProtocolError("The Windows CoPath worker returned a malformed CSV") from exc
    return fields, rows


def _validate_error(path: Path, request_id: str) -> str:
    """
    Validate a worker error artifact and return its safe message.

    Args:
        path (Path): Path to the input file or directory.
        request_id (str): Input request id used by the operation.

    Returns:
        str: String representation or formatted value produced by the operation.

    Raises:
        QueueProtocolError: If validation or the underlying resource operation fails.
    """
    payload = read_json(path)
    if payload.get("version") != PROTOCOL_VERSION:
        raise QueueProtocolError("The Windows CoPath worker returned a malformed error")
    if payload.get("request_id") != request_id:
        raise QueueProtocolError("The Windows CoPath worker returned a mismatched error")
    code = payload.get("code")
    message = payload.get("message")
    if not isinstance(code, str) or not ERROR_CODE_PATTERN.fullmatch(code):
        raise QueueProtocolError("The Windows CoPath worker returned a malformed error")
    if (
        not isinstance(message, str)
        or not message.strip()
        or len(message) > 500
        or any(ord(character) < 32 and character not in "\t" for character in message)
    ):
        raise QueueProtocolError("The Windows CoPath worker returned a malformed error")
    return f"Windows CoPath worker error ({code}): {message.strip()}"


def _cleanup_job(paths: Dict[str, Path], request_id: str) -> None:
    """
    Remove terminal queue artifacts for a completed request.

    Args:
        paths (Dict[str, Path]): Input paths used by the operation.
        request_id (str): Input request id used by the operation.

    Returns:
        None: The operation completes through its side effects and returns no value.
    """
    for directory, suffix in (
        ("requests", ".json"), ("processing", ".json"),
        ("results", ".csv"), ("errors", ".json"),
    ):
        (paths[directory] / f"{request_id}{suffix}").unlink(missing_ok=True)


def submit_query(
    root: Path,
    accessions: Sequence[str],
    output_path: Path,
    timeout_seconds: float,
    *,
    poll_interval: float = 0.1,
    monotonic: Callable[[], float] = time.monotonic,
    scope: str = "exact_accession",
) -> None:
    """
    Publish one request and atomically consume its matching terminal artifact.

    Args:
        root (Path): Directory used as the root.
        accessions (Sequence[str]): Identifier values to process.
        output_path (Path): Path to the output.
        timeout_seconds (float): Numeric limit, duration, or count controlling the operation.
        poll_interval (float): Input poll interval used by the operation.
        monotonic (Callable[[], float]): Input monotonic used by the operation.
        scope (str): Requested query or API scope.

    Returns:
        None: The operation completes through its side effects and returns no value.

    Raises:
        QueueProtocolError: If validation or the underlying resource operation fails.
    """
    normalized = validate_accessions(list(accessions))
    if scope not in QUERY_SCOPES:
        raise QueueProtocolError("query scope is invalid")
    if timeout_seconds <= 0:
        raise QueueProtocolError("COPATH_QUERY_TIMEOUT_SECONDS must be greater than zero")
    paths = initialize_queue(root)
    require_fresh_heartbeat(root)
    request_id = uuid.uuid4().hex
    created_at = utc_now()
    request_path = paths["requests"] / f"{request_id}.json"
    result_path = paths["results"] / f"{request_id}.csv"
    error_path = paths["errors"] / f"{request_id}.json"
    atomic_write_json(request_path, {
        "version": PROTOCOL_VERSION,
        "request_id": request_id,
        "created_at": format_utc(created_at),
        "accessions": normalized,
        "scope": scope,
    })
    deadline = monotonic() + timeout_seconds
    terminal_seen = False
    try:
        while monotonic() < deadline:
            if error_path.exists() or error_path.is_symlink():
                terminal_seen = True
                if error_path.stat().st_mtime < created_at.timestamp() - 5:
                    raise QueueProtocolError("The Windows CoPath worker returned a stale error")
                raise QueueProtocolError(_validate_error(error_path, request_id))
            if result_path.exists() or result_path.is_symlink():
                terminal_seen = True
                if result_path.stat().st_mtime < created_at.timestamp() - 5:
                    raise QueueProtocolError("The Windows CoPath worker returned a stale result")
                fields, rows = validate_result_csv(result_path, normalized, scope)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                temporary = output_path.with_name(f".{output_path.name}.{uuid.uuid4().hex}.tmp")
                try:
                    with temporary.open("x", newline="", encoding="utf-8") as handle:
                        writer = csv.DictWriter(handle, fieldnames=fields)
                        writer.writeheader()
                        writer.writerows(rows)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.replace(temporary, output_path)
                finally:
                    temporary.unlink(missing_ok=True)
                return
            time.sleep(poll_interval)
        raise QueueProtocolError(
            f"Timed out after {timeout_seconds:g} seconds waiting for the Windows CoPath worker."
        )
    finally:
        if terminal_seen:
            _cleanup_job(paths, request_id)
        else:
            # A request not yet claimed can be cancelled safely. Once claimed,
            # leave its marker intact so worker recovery and retention still work.
            request_path.unlink(missing_ok=True)
