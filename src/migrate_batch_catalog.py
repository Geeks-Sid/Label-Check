"""Build batch_catalog.sqlite3 from legacy per-batch stage and queue CSVs."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path, PurePosixPath
from typing import Dict, Iterable, Optional

from batch_catalog import BatchCatalog, normalize_relative_path, public_batch_id


STAGE_FIELDS = ["QC", "Renamed"]
QUEUE_FIELDS = [
    "original_index", "status", "leased_by_id", "leased_at",
    "completed_by_id", "completed_at",
]


def parse_bool(value: object, source: Path) -> bool:
    normalized = str(value or "").strip().lower()
    if normalized not in {"true", "false"}:
        raise ValueError(f"{source}: boolean values must be True or False")
    return normalized == "true"


def read_csv(path: Path, expected_fields: Optional[list[str]] = None) -> tuple[list[str], list[dict]]:
    try:
        with path.open("r", newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            if expected_fields is not None and fields != expected_fields:
                raise ValueError(
                    f"{path}: expected columns {','.join(expected_fields)}; got {','.join(fields)}"
                )
            return fields, [{key: value or "" for key, value in row.items()} for row in reader]
    except (OSError, UnicodeError, csv.Error) as exc:
        raise ValueError(f"could not read {path}: {exc}") from exc


def legacy_queue_name(runtime_root: str, relative_path: str) -> str:
    runtime_path = str(PurePosixPath(runtime_root) / PurePosixPath(relative_path))
    return hashlib.sha256(runtime_path.encode("utf-8")).hexdigest()[:16] + ".csv"


def reconciled_queue(queue_rows: Iterable[dict], slides: list[dict]) -> list[dict]:
    current: Dict[int, dict] = {}
    for row in queue_rows:
        try:
            index = int(row["original_index"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"invalid queue original_index: {row}") from exc
        if index in current or row.get("status") not in {"pending", "leased", "completed"}:
            raise ValueError(f"invalid or duplicate queue row: {row}")
        current[index] = dict(row)
    result = []
    for index, slide in enumerate(slides):
        complete_value = str(slide.get("ParsingQCPassed") or "").strip()
        complete = bool(complete_value and complete_value.lower() != "false")
        row = dict(current.get(index, {"original_index": str(index), "status": "pending"}))
        if complete and row["status"] != "completed":
            row.update({"status": "completed", "leased_by_id": "", "leased_at": ""})
        elif not complete and row["status"] == "completed":
            row.update({"status": "pending", "completed_by_id": "", "completed_at": ""})
        result.append(row)
    return result


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def collect(args: argparse.Namespace) -> tuple[list[dict], list[tuple[Path, str]], list[str]]:
    batches_root = args.batches_root.resolve()
    queue_root = args.state_root.resolve() / "instance" / "batch_queues"
    records: list[dict] = []
    imported_files: list[tuple[Path, str]] = []
    warnings: list[str] = []
    mapped_queues: set[Path] = set()
    for batch_root in sorted(batches_root.glob("SS*/*")):
        if not batch_root.is_dir():
            continue
        relative_path = normalize_relative_path(batch_root.relative_to(batches_root).as_posix())
        stage_path = batch_root / "completed_stages.csv"
        if not stage_path.exists():
            warnings.append(f"Skipped {relative_path}: completed_stages.csv is missing")
            continue
        _, stage_rows = read_csv(stage_path, STAGE_FIELDS)
        if len(stage_rows) != 1:
            raise ValueError(f"{stage_path}: expected exactly one data row")
        qc_complete = parse_bool(stage_rows[0]["QC"], stage_path)
        renamed_complete = parse_bool(stage_rows[0]["Renamed"], stage_path)
        enriched_path = batch_root / "enriched.csv"
        fields, slides = read_csv(enriched_path)
        if "ParsingQCPassed" not in fields or not slides:
            raise ValueError(f"{enriched_path}: missing ParsingQCPassed or slide rows")
        queue_path = queue_root / legacy_queue_name(
            args.legacy_runtime_batches_root, relative_path
        )
        queue_rows: list[dict] = []
        if queue_path.exists():
            _, queue_rows = read_csv(queue_path, QUEUE_FIELDS)
            mapped_queues.add(queue_path.resolve())
            imported_files.append((queue_path, relative_path))
        else:
            warnings.append(f"Seeded queue from enriched.csv for {relative_path}")
        mapping_path = batch_root / "name_mapping.csv"
        renaming_status = "missing"
        mapping_mtime_ns = None
        if mapping_path.exists():
            _, mapping_rows = read_csv(mapping_path)
            mapping_mtime_ns = mapping_path.stat().st_mtime_ns
            approved = bool(mapping_rows) and all(
                str(row.get("Approved", "")).strip().lower() == "true"
                for row in mapping_rows
            )
            renaming_status = "approved" if approved else "ready"
        history_status = "not_needed"
        history_path = batch_root / "copath_history_job.json"
        history_mtime_ns = None
        if history_path.exists():
            history_mtime_ns = history_path.stat().st_mtime_ns
            try:
                payload = json.loads(history_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise ValueError(f"could not read {history_path}: {exc}") from exc
            if not isinstance(payload, dict) or payload.get("version") != 1:
                raise ValueError(f"{history_path}: invalid job document")
            history_status = str(payload.get("status", "not_needed"))
        imported_files.append((stage_path, relative_path))
        records.append(
            {
                "relative_path": relative_path,
                "qc_complete": qc_complete,
                "renamed_complete": renamed_complete,
                "slides": slides,
                "queue": reconciled_queue(queue_rows, slides),
                "enriched_mtime_ns": enriched_path.stat().st_mtime_ns,
                "mapping_mtime_ns": mapping_mtime_ns,
                "history_mtime_ns": history_mtime_ns,
                "renaming_status": renaming_status,
                "history_status": history_status,
            }
        )
    if queue_root.exists():
        for queue_path in sorted(queue_root.glob("*.csv")):
            if queue_path.resolve() not in mapped_queues:
                warnings.append(f"Unmatched queue retained: {queue_path}")
    return records, imported_files, warnings


def build_database(records: list[dict], temporary_instance: Path) -> Path:
    store = BatchCatalog()
    for record in records:
        public_id = store.upsert_batch(
            temporary_instance,
            record["relative_path"],
            qc_complete=record["qc_complete"],
            renamed_complete=record["renamed_complete"],
            slide_count=len(record["slides"]),
            enriched_mtime_ns=record["enriched_mtime_ns"],
            mapping_mtime_ns=record["mapping_mtime_ns"],
            history_mtime_ns=record["history_mtime_ns"],
            renaming_status=record["renaming_status"],
            history_status=record["history_status"],
            preserve_stages=False,
        )
        store.replace_queue(temporary_instance, public_id, record["queue"])
    store.set_metadata(temporary_instance, "migrated_at", dt.datetime.now(dt.timezone.utc).isoformat())
    database = store.database_path(temporary_instance)
    connection = sqlite3.connect(database)
    try:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
        batch_count = connection.execute("SELECT COUNT(*) FROM batches").fetchone()[0]
        if integrity != "ok" or foreign_keys or batch_count != len(records):
            raise RuntimeError("temporary catalog verification failed")
    finally:
        connection.close()
    return database


def archive_sources(instance_dir: Path, sources: list[tuple[Path, str]]) -> Path:
    timestamp = dt.datetime.now().strftime("%Y%m%dT%H%M%S")
    archive = instance_dir / "legacy_batch_state_archive" / timestamp
    manifest_rows = []
    for source, batch_key in sources:
        source = source.resolve()
        if source.name == "completed_stages.csv":
            scanner, batch = source.parent.parent.name, source.parent.name
            relative = Path("batches") / scanner / batch / source.name
        else:
            relative = Path("batch_queues") / source.name
        destination = archive / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        source_hash = sha256_file(source)
        if sha256_file(destination) != source_hash:
            raise RuntimeError(f"archive checksum mismatch: {source}")
        manifest_rows.append(
            {
                "source": str(source), "archive": relative.as_posix(),
                "batch": batch_key, "size": source.stat().st_size,
                "sha256": source_hash,
            }
        )
    manifest = archive / "manifest.json"
    manifest.write_text(json.dumps(manifest_rows, indent=2) + "\n", encoding="utf-8")
    for source, _ in sources:
        source.unlink()
    return manifest


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--batches-root", type=Path,
        default=Path(os.environ.get("LABEL_CHECK_BATCHES_HOST") or os.environ.get("LABEL_CHECK_BATCHES", r"D:\label_check_batches")),
    )
    result.add_argument(
        "--state-root", type=Path,
        default=Path(os.environ.get("LABEL_CHECK_STATE_HOST", r"D:\label_check_batches\state")),
    )
    result.add_argument("--legacy-runtime-batches-root", default="/data/label-check-batches")
    result.add_argument("--apply", action="store_true", help="write database and archive imported files")
    result.add_argument("--replace", action="store_true", help="replace an existing catalog")
    return result


def main(argv: Optional[list[str]] = None) -> int:
    args = parser().parse_args(argv)
    try:
        records, sources, warnings = collect(args)
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(f"Validated {len(records)} batch(es) and {len(sources)} legacy state file(s).")
    for warning in warnings:
        print(f"WARNING: {warning}")
    if not args.apply:
        print("Dry run complete. Re-run with --apply to migrate and archive.")
        return 0
    instance_dir = args.state_root.resolve() / "instance"
    final_database = instance_dir / "batch_catalog.sqlite3"
    if final_database.exists() and not args.replace:
        print(f"ERROR: {final_database} exists; use --replace to replace it", file=sys.stderr)
        return 2
    instance_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=instance_dir) as temporary:
        temporary_instance = Path(temporary)
        database = build_database(records, temporary_instance)
        if final_database.exists():
            backup = final_database.with_suffix(".sqlite3.pre_migration_backup")
            shutil.copy2(final_database, backup)
        os.replace(database, final_database)
    manifest = archive_sources(instance_dir, sources)
    print(f"Installed {final_database}")
    print(f"Archived imported legacy state; manifest: {manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
