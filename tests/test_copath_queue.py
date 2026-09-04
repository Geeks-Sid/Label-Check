import csv
import datetime as dt
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import copath_queue
import renaming


def write_csv(path, accessions):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["accession_id", "report"])
        writer.writeheader()
        writer.writerows(
            {"accession_id": accession, "report": "result"} for accession in accessions
        )
    os.replace(temporary, path)


class QueueClientTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "queue"
        self.paths = copath_queue.initialize_queue(self.root)
        copath_queue.atomic_write_json(self.root / "worker.json", {
            "version": copath_queue.PROTOCOL_VERSION,
            "updated_at": copath_queue.format_utc(copath_queue.utc_now()),
        })
        self.output = Path(self.temporary.name) / "output.csv"

    def tearDown(self):
        self.temporary.cleanup()

    def run_responder(self, callback):
        def respond():
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                requests = list(self.paths["requests"].glob("*.json"))
                if requests:
                    callback(requests[0])
                    return
                time.sleep(0.005)
        thread = threading.Thread(target=respond)
        thread.start()
        return thread

    def test_publishes_versioned_request_and_consumes_valid_result(self):
        observed = {}

        def respond(request_path):
            payload = copath_queue.read_json(request_path)
            observed.update(payload)
            write_csv(
                self.paths["results"] / f"{payload['request_id']}.csv",
                payload["accessions"],
            )

        thread = self.run_responder(respond)
        copath_queue.submit_query(
            self.root, ["NP25-100", "SP26-2"], self.output, 2, poll_interval=0.005
        )
        thread.join()

        self.assertEqual(1, observed["version"])
        self.assertRegex(observed["request_id"], r"^[0-9a-f]{32}$")
        self.assertEqual(["NP25-100", "SP26-2"], observed["accessions"])
        self.assertEqual([], list(self.paths["requests"].iterdir()))
        self.assertEqual([], list(self.paths["results"].iterdir()))
        fields, rows = copath_queue.validate_result_csv(
            self.output, ["NP25-100", "SP26-2"]
        )
        self.assertEqual(["accession_id", "report"], fields)
        self.assertEqual(2, len(rows))

    def test_rejects_missing_and_stale_heartbeat_before_publication(self):
        (self.root / "worker.json").unlink()
        with self.assertRaisesRegex(copath_queue.QueueProtocolError, "offline"):
            copath_queue.submit_query(self.root, ["NP25-100"], self.output, 1)
        self.assertEqual([], list(self.paths["requests"].iterdir()))

        copath_queue.atomic_write_json(self.root / "worker.json", {
            "version": 1,
            "updated_at": copath_queue.format_utc(
                copath_queue.utc_now() - dt.timedelta(minutes=1)
            ),
        })
        with self.assertRaisesRegex(copath_queue.QueueProtocolError, "offline"):
            copath_queue.submit_query(self.root, ["NP25-100"], self.output, 1)

    def test_surfaces_sanitized_worker_error_and_rejects_mismatch(self):
        def respond(request_path):
            payload = copath_queue.read_json(request_path)
            copath_queue.atomic_write_json(
                self.paths["errors"] / f"{payload['request_id']}.json",
                {
                    "version": 1,
                    "request_id": "0" * 32,
                    "code": "query_failed",
                    "message": "Safe message.",
                },
            )

        thread = self.run_responder(respond)
        with self.assertRaisesRegex(copath_queue.QueueProtocolError, "mismatched"):
            copath_queue.submit_query(
                self.root, ["NP25-100"], self.output, 2, poll_interval=0.005
            )
        thread.join()

    def test_surfaces_valid_worker_error(self):
        def respond(request_path):
            payload = copath_queue.read_json(request_path)
            copath_queue.atomic_write_json(
                self.paths["errors"] / f"{payload['request_id']}.json",
                {
                    "version": 1,
                    "request_id": payload["request_id"],
                    "code": "query_failed",
                    "message": "The database query failed.",
                },
            )

        thread = self.run_responder(respond)
        with self.assertRaisesRegex(
            copath_queue.QueueProtocolError, "query_failed.*database query failed"
        ):
            copath_queue.submit_query(
                self.root, ["NP25-100"], self.output, 2, poll_interval=0.005
            )
        thread.join()

    def test_rejects_stale_result_artifact(self):
        def respond(request_path):
            payload = copath_queue.read_json(request_path)
            result = self.paths["results"] / f"{payload['request_id']}.csv"
            write_csv(result, payload["accessions"])
            old = (copath_queue.utc_now() - dt.timedelta(minutes=1)).timestamp()
            os.utime(result, (old, old))

        thread = self.run_responder(respond)
        with self.assertRaisesRegex(copath_queue.QueueProtocolError, "stale result"):
            copath_queue.submit_query(
                self.root, ["NP25-100"], self.output, 2, poll_interval=0.005
            )
        thread.join()

    def test_rejects_unrequested_accession_and_malformed_csv(self):
        def respond(request_path):
            payload = copath_queue.read_json(request_path)
            write_csv(
                self.paths["results"] / f"{payload['request_id']}.csv", ["NP25-999"]
            )

        thread = self.run_responder(respond)
        with self.assertRaisesRegex(copath_queue.QueueProtocolError, "not requested"):
            copath_queue.submit_query(
                self.root, ["NP25-100"], self.output, 2, poll_interval=0.005
            )
        thread.join()

    def test_patient_history_scope_accepts_valid_associated_accessions(self):
        def respond(request_path):
            payload = copath_queue.read_json(request_path)
            result = self.paths["results"] / f"{payload['request_id']}.csv"
            with result.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=["accession_id", "mrn"])
                writer.writeheader()
                writer.writerows([
                    {"accession_id": "NP25-100", "mrn": "MRN1"},
                    {"accession_id": "SP20-5", "mrn": "MRN1"},
                ])

        thread = self.run_responder(respond)
        copath_queue.submit_query(
            self.root, ["NP25-100"], self.output, 2,
            poll_interval=0.005, scope="patient_history",
        )
        thread.join()
        _, rows = copath_queue.validate_result_csv(
            self.output, ["NP25-100"], "patient_history"
        )
        self.assertEqual({"NP25-100", "SP20-5"}, {row["accession_id"] for row in rows})

    def test_timeout_cancels_unclaimed_request(self):
        with self.assertRaisesRegex(copath_queue.QueueProtocolError, "Timed out"):
            copath_queue.submit_query(
                self.root, ["NP25-100"], self.output, 0.02, poll_interval=0.005
            )
        self.assertEqual([], list(self.paths["requests"].iterdir()))

    def test_timeout_preserves_claim_marker_for_worker_recovery(self):
        claimed = []

        def respond(request_path):
            destination = self.paths["processing"] / request_path.name
            os.replace(request_path, destination)
            claimed.append(destination)

        thread = self.run_responder(respond)
        with self.assertRaisesRegex(copath_queue.QueueProtocolError, "Timed out"):
            copath_queue.submit_query(
                self.root, ["NP25-100"], self.output, 0.03, poll_interval=0.005
            )
        thread.join()
        self.assertEqual(1, len(claimed))
        self.assertTrue(claimed[0].exists())

    def test_accession_validation_allows_arbitrary_values_but_rejects_blanks_and_duplicates(self):
        self.assertEqual(
            ["../worker", "np25-100"],
            copath_queue.validate_accessions([" ../worker ", "np25-100"]),
        )
        for invalid in (["  "], ["NP25-100", "np25-100"]):
            with self.subTest(invalid=invalid), self.assertRaises(
                copath_queue.QueueProtocolError
            ):
                copath_queue.validate_accessions(invalid)
        with self.assertRaisesRegex(copath_queue.QueueProtocolError, "at most"):
            copath_queue.validate_accessions([f"NP25-{index}" for index in range(10001)])

    def test_rejects_symlinked_protocol_directory(self):
        outside = Path(self.temporary.name) / "outside"
        outside.mkdir()
        self.paths["results"].rmdir()
        try:
            self.paths["results"].symlink_to(outside, target_is_directory=True)
        except (OSError, NotImplementedError):
            self.skipTest("directory symlinks are unavailable")
        with self.assertRaisesRegex(copath_queue.QueueProtocolError, "unsafe"):
            copath_queue.initialize_queue(self.root)

    def test_default_query_dispatches_both_modes(self):
        with mock.patch.dict(os.environ, {"COPATH_QUERY_MODE": "direct"}), mock.patch(
            "renaming.direct_query"
        ) as direct:
            renaming.default_query(Path("batch"), ["NP25-100"], self.output)
        direct.assert_called_once()

        with mock.patch.dict(
            os.environ, {"COPATH_QUERY_MODE": "windows_queue"}
        ), mock.patch("renaming.windows_queue_query") as queued:
            renaming.default_query(Path("batch"), ["NP25-100"], self.output)
        queued.assert_called_once()

    def test_windows_queue_mode_reports_offline_without_waiting(self):
        (self.root / "worker.json").unlink()
        with mock.patch.dict(
            os.environ,
            {
                "COPATH_QUERY_QUEUE": str(self.root),
                "COPATH_QUERY_TIMEOUT_SECONDS": "300",
            },
        ):
            started = time.monotonic()
            with self.assertRaisesRegex(renaming.RenamingError, "offline"):
                renaming.windows_queue_query(
                    Path("batch"), ["NP25-100"], self.output
                )
        self.assertLess(time.monotonic() - started, 1)


if __name__ == "__main__":
    unittest.main()
