"""Translate persisted Windows paths to Linux container mount paths."""

from __future__ import annotations

import os
import re
from pathlib import Path


MAPPINGS = (
    ("GT450_IMAGES_HOST_PREFIX", "GT450_IMAGES_CONTAINER_ROOT"),
    ("LABEL_CHECK_BATCHES_HOST_PREFIX", "LABEL_CHECK_BATCHES_CONTAINER_ROOT"),
)


def runtime_path(value: str | os.PathLike[str]) -> Path:
    """Return a mounted Linux path when value begins with a configured host prefix."""
    raw = os.fspath(value)
    if os.name == "nt":
        return Path(raw)

    normalized = raw.replace("/", "\\").rstrip("\\")
    for prefix_name, root_name in MAPPINGS:
        prefix = os.environ.get(prefix_name, "").replace("/", "\\").rstrip("\\")
        root = os.environ.get(root_name, "").strip()
        if not prefix or not root:
            continue
        if normalized.casefold() == prefix.casefold():
            return Path(root)
        boundary = f"{prefix}\\"
        if normalized.casefold().startswith(boundary.casefold()):
            relative = normalized[len(boundary) :]
            return Path(root).joinpath(*filter(None, re.split(r"[\\/]+", relative)))
    return Path(raw)
