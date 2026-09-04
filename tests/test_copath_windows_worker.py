import csv
import datetime as dt
import os
import sys
import tempfile
import unittest
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import copath_queue
import copath_windows_worker


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "queue"
        self.connection_file = Path(self.temporary.name) / "connection.txt"
        self.connection_file.write_text("Trusted_Connection=yes;", encoding="utf-8")
        self.calls = []

        def runner(accessions, output, work_dir):
            self.calls.append((list(accessions), work_dir))
            with output.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=["accession_id", "report"])
                writer.writeheader()
                writer.writerows(
                    {"accession_id": accession, "report": "ok"}
                    for accession in accessions
                )

        self.worker = copath_windows_worker.CoPathWindowsWorker(
            self.root, self.connection_file, runner
        )

    def tearDown(self):
        self.temporary.cleanup()

    def publish(self, request_id, accessions):
        copath_queue.atomic_write_json(
            self.worker.paths["requests"] / f"{request_id}.json",
            {
                "version": 1,
                "request_id": request_id,
                "created_at": copath_queue.format_utc(copath_queue.utc_now()),
                "accessions": accessions,
            },
        )

    def test_processes_initial_batch_and_single_accession_retry(self):
        first = "1" * 32
        retry = "2" * 32
        self.publish(first, ["NP25-100", "NP25-200"])
        self.assertTrue(self.worker.run_once())
        self.publish(retry, ["NP25-300"])
        self.assertTrue(self.worker.run_once())

        self.assertEqual(
            [["NP25-100", "NP25-200"], ["NP25-300"]],
            [call[0] for call in self.calls],
        )
        copath_queue.validate_result_csv(
            self.worker.paths["results"] / f"{first}.csv",
            ["NP25-100", "NP25-200"],
        )
        self.assertFalse((self.worker.paths["work"] / first).exists())

    def test_patient_history_scope_reaches_runner_and_allows_associated_rows(self):
        observed = []
        def runner(accessions, output, _work_dir, scope):
            observed.append((list(accessions), scope))
            with output.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=["accession_id", "mrn"])
                writer.writeheader()
                writer.writerows([
                    {"accession_id": "NP25-100", "mrn": "MRN1"},
                    {"accession_id": "SP20-5", "mrn": "MRN1"},
                ])
        worker = copath_windows_worker.CoPathWindowsWorker(
            self.root, self.connection_file, runner
        )
        request_id = "8" * 32
        copath_queue.atomic_write_json(worker.paths["requests"] / f"{request_id}.json", {
            "version": copath_queue.PROTOCOL_VERSION,
            "request_id": request_id,
            "created_at": copath_queue.format_utc(copath_queue.utc_now()),
            "accessions": ["NP25-100"],
            "scope": "patient_history",
        })
        self.assertTrue(worker.run_once())
        self.assertEqual([(["NP25-100"], "patient_history")], observed)

    def test_invalid_request_publishes_safe_error(self):
        request_id = "3" * 32
        self.publish(request_id, ["  "])
        self.worker.run_once()

        error = copath_queue.read_json(
            self.worker.paths["errors"] / f"{request_id}.json"
        )
        self.assertEqual("invalid_request", error["code"])
        self.assertNotIn("accessions", error["message"])
        self.assertEqual([], self.calls)

    def test_result_must_be_subset_of_request(self):
        def bad_runner(_accessions, output, _work_dir):
            with output.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=["accession_id"])
                writer.writeheader()
                writer.writerow({"accession_id": "NP25-999"})

        worker = copath_windows_worker.CoPathWindowsWorker(
            self.root, self.connection_file, bad_runner
        )
        request_id = "4" * 32
        self.publish(request_id, ["NP25-100"])
        worker.run_once()
        error = copath_queue.read_json(worker.paths["errors"] / f"{request_id}.json")
        self.assertEqual("invalid_result", error["code"])

    def test_atomic_claim_does_not_overwrite_duplicate_processing_file(self):
        request_id = "5" * 32
        self.publish(request_id, ["NP25-100"])
        processing = self.worker.paths["processing"] / f"{request_id}.json"
        processing.write_text("existing", encoding="utf-8")

        self.assertIsNone(self.worker.claim_one())
        self.assertEqual("existing", processing.read_text(encoding="utf-8"))
        self.assertTrue((self.worker.paths["requests"] / f"{request_id}.json").exists())

    def test_recovers_stale_claim_and_removes_old_terminal_artifacts(self):
        request_id = "6" * 32
        claimed = self.worker.paths["processing"] / f"{request_id}.json"
        copath_queue.atomic_write_json(claimed, {
            "version": 1,
            "request_id": request_id,
            "created_at": copath_queue.format_utc(copath_queue.utc_now()),
            "accessions": ["NP25-100"],
        })
        old = (copath_queue.utc_now() - dt.timedelta(minutes=20)).timestamp()
        os.utime(claimed, (old, old))
        self.assertEqual(1, self.worker.recover_stale_processing(max_age_seconds=600))
        self.assertTrue((self.worker.paths["requests"] / claimed.name).exists())

        terminal = self.worker.paths["results"] / f"{'7' * 32}.csv"
        terminal.write_text("accession_id\n", encoding="utf-8")
        os.utime(terminal, (old, old))
        self.assertEqual(1, self.worker.cleanup_old_artifacts(max_age_seconds=600))
        self.assertFalse(terminal.exists())

    def test_heartbeat_is_versioned_and_removed_on_stop(self):
        self.worker.start_heartbeat()
        heartbeat = copath_queue.read_json(self.root / "worker.json")
        self.assertEqual(1, heartbeat["version"])
        self.assertEqual(self.worker.worker_id, heartbeat["worker_id"])
        self.worker.stop()
        self.assertFalse((self.root / "worker.json").exists())


if __name__ == "__main__":
    unittest.main()
