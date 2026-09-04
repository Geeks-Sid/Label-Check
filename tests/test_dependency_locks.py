import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCK_FILES = (
    "requirements.txt",
    "requirements-test.txt",
    "requirements-windows-worker.txt",
)


class DependencyLockTests(unittest.TestCase):
    def test_every_locked_requirement_is_exact_and_hashed(self):
        for name in LOCK_FILES:
            lines = (PROJECT_ROOT / name).read_text(encoding="utf-8").splitlines()
            package_count = 0
            index = 0
            while index < len(lines):
                stripped = lines[index].strip()
                if not stripped or stripped.startswith(("#", "--")):
                    index += 1
                    continue

                block = stripped
                while lines[index].rstrip().endswith("\\"):
                    index += 1
                    block += " " + lines[index].strip()
                package_count += 1
                with self.subTest(lock=name, requirement=stripped):
                    self.assertIn("==", stripped)
                    self.assertIn("--hash=sha256:", block)
                index += 1

            self.assertGreater(package_count, 0, name)


if __name__ == "__main__":
    unittest.main()
