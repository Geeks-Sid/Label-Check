"""Load and validate CoPath ODBC and Windows authentication settings."""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict


DEFAULT_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=IUHWCPTHDB3980;"
    "DATABASE=COPLIVE;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)


class CoPathConfigurationError(RuntimeError):
    """A safe-to-display CoPath configuration error."""


def _property_name(value: str) -> str:
    return re.sub(r"[ _-]+", "_", value.strip()).upper()


def connection_properties(connection: str) -> Dict[str, str]:
    """Parse the simple semicolon-delimited properties used by this service."""
    if re.search(r"\bTrustSererCertificate\s*=", connection, re.IGNORECASE):
        raise CoPathConfigurationError(
            "The CoPath connection string contains 'TrustSererCertificate'; "
            "use 'TrustServerCertificate'."
        )

    # This catches the common case where each property was separated by a space.
    if re.search(
        r"\s+(?:DRIVER|SERVER|DATABASE|Trusted_Connection|TrustServerCertificate)\s*=",
        connection,
        re.IGNORECASE,
    ):
        raise CoPathConfigurationError(
            "Separate every CoPath connection-string property with a semicolon (;)."
        )

    properties: Dict[str, str] = {}
    for segment in connection.split(";"):
        segment = segment.strip()
        if not segment:
            continue
        if "=" not in segment:
            raise CoPathConfigurationError(
                "Every CoPath connection-string property must use KEY=VALUE syntax "
                "and semicolon separators."
            )
        key, value = segment.split("=", 1)
        properties[_property_name(key)] = value.strip()

    if not properties:
        raise CoPathConfigurationError("The CoPath connection string is empty.")
    if "DSN" not in properties:
        missing = [name for name in ("DRIVER", "SERVER") if not properties.get(name)]
        if missing:
            raise CoPathConfigurationError(
                "The CoPath connection string is missing required property/properties: "
                + ", ".join(missing)
                + "."
            )
    return properties


def connection_string() -> str:
    """Return a validated connection string without ever logging its contents."""
    configured_path = os.environ.get("COPATH_CONNECTION_STRING_FILE", "").strip()
    if configured_path:
        try:
            connection = Path(configured_path).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise CoPathConfigurationError(
                "The configured CoPath connection-string file could not be read."
            ) from exc
    else:
        connection = os.environ.get("COPATH_CONNECTION_STRING", DEFAULT_CONN_STR).strip()

    connection_properties(connection)
    return connection


def require_windows_ticket(connection: str) -> None:
    """Require a valid Kerberos cache when ODBC Windows Authentication is enabled."""
    properties = connection_properties(connection)
    trusted = properties.get("TRUSTED_CONNECTION", "").casefold()
    integrated = properties.get("INTEGRATED_SECURITY", "").casefold()
    if trusted not in {"yes", "true", "1"} and integrated not in {
        "yes",
        "true",
        "1",
        "sspi",
    }:
        return

    # On Windows, ODBC Driver 18 uses the signed-in user's native SSPI token.
    # MIT klist is only a preflight for the Linux/Kerberos direct-mode path.
    if sys.platform == "win32":
        return

    if not shutil.which("klist"):
        raise CoPathConfigurationError(
            "Windows Authentication requires the Kerberos client utility 'klist'."
        )
    result = subprocess.run(
        ["klist", "-s"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if result.returncode:
        raise CoPathConfigurationError(
            "No valid Windows Authentication ticket is available. Run "
            "'docker compose exec label-check kinit YOUR_ACCOUNT@YOUR.AD.REALM', "
            "then retry CoPath preparation."
        )


def prepared_connection_string() -> str:
    """Return the validated string after checking its Windows credential cache."""
    connection = connection_string()
    require_windows_ticket(connection)
    return connection
