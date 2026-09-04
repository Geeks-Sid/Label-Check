import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


UTILITIES_DIR = Path(__file__).resolve().parents[1] / "src" / "copath_utilities"
sys.path.insert(0, str(UTILITIES_DIR))

import copath_connection


WINDOWS_CONNECTION = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=sql.example.org;"
    "DATABASE=COPLIVE;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)


class CoPathConnectionTests(unittest.TestCase):
    def test_accepts_semicolon_delimited_windows_connection(self):
        properties = copath_connection.connection_properties(WINDOWS_CONNECTION)

        self.assertEqual("sql.example.org", properties["SERVER"])
        self.assertEqual("yes", properties["TRUSTED_CONNECTION"])

    def test_rejects_space_delimited_properties(self):
        malformed = (
            "DRIVER={ODBC Driver 18 for SQL Server} SERVER=sql.example.org "
            "DATABASE=COPLIVE Trusted_Connection=yes TrustServerCertificate=yes"
        )

        with self.assertRaisesRegex(
            copath_connection.CoPathConfigurationError, "semicolon"
        ):
            copath_connection.connection_properties(malformed)

    def test_rejects_misspelled_trust_server_certificate(self):
        malformed = WINDOWS_CONNECTION.replace(
            "TrustServerCertificate", "TrustSererCertificate"
        )

        with self.assertRaisesRegex(
            copath_connection.CoPathConfigurationError, "TrustServerCertificate"
        ):
            copath_connection.connection_properties(malformed)

    def test_rejects_empty_configured_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "connection.txt"
            path.write_text("  \n", encoding="utf-8")
            with mock.patch.dict(
                os.environ,
                {"COPATH_CONNECTION_STRING_FILE": str(path)},
                clear=True,
            ):
                with self.assertRaisesRegex(
                    copath_connection.CoPathConfigurationError, "empty"
                ):
                    copath_connection.connection_string()

    def test_windows_connection_requires_valid_ticket(self):
        completed = mock.Mock(returncode=1)
        with mock.patch(
            "copath_connection.shutil.which", return_value="/usr/bin/klist"
        ), mock.patch(
            "copath_connection.subprocess.run", return_value=completed
        ) as run:
            with self.assertRaisesRegex(
                copath_connection.CoPathConfigurationError, "kinit"
            ):
                copath_connection.require_windows_ticket(WINDOWS_CONNECTION)

        run.assert_called_once_with(
            ["klist", "-s"],
            check=False,
            stdout=copath_connection.subprocess.DEVNULL,
            stderr=copath_connection.subprocess.DEVNULL,
        )

    def test_windows_connection_accepts_valid_ticket(self):
        completed = mock.Mock(returncode=0)
        with mock.patch(
            "copath_connection.shutil.which", return_value="/usr/bin/klist"
        ), mock.patch(
            "copath_connection.subprocess.run", return_value=completed
        ):
            copath_connection.require_windows_ticket(WINDOWS_CONNECTION)

    def test_native_windows_connection_does_not_require_mit_klist(self):
        with mock.patch("copath_connection.sys.platform", "win32"), mock.patch(
            "copath_connection.shutil.which"
        ) as which:
            copath_connection.require_windows_ticket(WINDOWS_CONNECTION)

        which.assert_not_called()

    def test_non_windows_connection_does_not_require_ticket(self):
        connection = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=sql.example.org;UID=user;PWD=secret;"
        )
        with mock.patch("copath_connection.shutil.which") as which:
            copath_connection.require_windows_ticket(connection)

        which.assert_not_called()


if __name__ == "__main__":
    unittest.main()
