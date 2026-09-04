import os
import sys
import unittest
from pathlib import Path
from unittest import mock


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from container_paths import runtime_path


class ContainerPathTests(unittest.TestCase):
    def test_maps_unc_prefix_case_insensitively(self):
        environment = {
            "GT450_IMAGES_HOST_PREFIX": (
                r"\\chp.clarian.org\app\Philips_Slide_Images\GT450_images"
            ),
            "GT450_IMAGES_CONTAINER_ROOT": "/data/gt450-images",
        }
        with mock.patch.dict(os.environ, environment, clear=False), mock.patch(
            "container_paths.os.name", "posix"
        ):
            result = runtime_path(
                r"\\CHP.CLARIAN.ORG\app\Philips_Slide_Images\GT450_images\SS12797\2026-07-31"
            )

        self.assertEqual(Path("/data/gt450-images/SS12797/2026-07-31"), result)

    def test_maps_windows_batch_path(self):
        environment = {
            "LABEL_CHECK_BATCHES_HOST_PREFIX": r"D:\label_check_batches",
            "LABEL_CHECK_BATCHES_CONTAINER_ROOT": "/data/label-check-batches",
        }
        with mock.patch.dict(os.environ, environment, clear=False), mock.patch(
            "container_paths.os.name", "posix"
        ):
            result = runtime_path(r"D:\label_check_batches\SS12797\batch")

        self.assertEqual(Path("/data/label-check-batches/SS12797/batch"), result)

    def test_leaves_native_path_unchanged(self):
        with mock.patch("container_paths.os.name", "posix"):
            self.assertEqual(Path("/data/input"), runtime_path("/data/input"))
