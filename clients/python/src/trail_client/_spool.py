"""Where batches wait while Trail is unreachable.

A spool entry is one request body, exactly as it would be posted. On disk it is a file named
`<uuid7>.json`, so the names sort by age (`spec/client.md`). Without a spool folder the entries
are kept in memory and last only as long as the process.
"""

from __future__ import annotations

import json
import os
from typing import Any, cast

from ._uuid import uuid7

KINDS = ("events", "metrics", "errors", "logs")


def count_records(body: bytes) -> int:
    """How many records a request body holds; 0 when it cannot be read."""
    try:
        parsed: Any = json.loads(body)
    except ValueError:
        return 0
    if not isinstance(parsed, dict):
        return 0
    data = cast("dict[str, Any]", parsed)
    total = 0
    for kind in KINDS:
        items = data.get(kind)
        if isinstance(items, list):
            total += len(cast("list[Any]", items))
    return total


class Spool:
    """The interface both spools share. Methods may raise `OSError`; the client reports it."""

    durable = False

    def names(self) -> list[str]:
        """Entry names, oldest first."""
        raise NotImplementedError

    def read(self, name: str) -> bytes | None:
        """The entry's body, or None when it has gone (another process sent it)."""
        raise NotImplementedError

    def write(self, body: bytes) -> str:
        """Store a body as a new entry and return its name."""
        raise NotImplementedError

    def remove(self, name: str) -> None:
        raise NotImplementedError

    def total_bytes(self) -> int:
        raise NotImplementedError


class MemorySpool(Spool):
    def __init__(self) -> None:
        self._entries: dict[str, bytes] = {}

    def names(self) -> list[str]:
        return sorted(self._entries)

    def read(self, name: str) -> bytes | None:
        return self._entries.get(name)

    def write(self, body: bytes) -> str:
        name = uuid7() + ".json"
        self._entries[name] = body
        return name

    def remove(self, name: str) -> None:
        self._entries.pop(name, None)

    def total_bytes(self) -> int:
        return sum(len(body) for body in self._entries.values())

    def clear(self) -> None:
        self._entries.clear()


class DirSpool(Spool):
    """One file per batch in a folder. The folder may be shared with earlier runs of the service."""

    durable = True

    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(path, exist_ok=True)

    def _entries(self) -> list[os.DirEntry[str]]:
        with os.scandir(self.path) as found:
            return sorted(
                (entry for entry in found if entry.name.endswith(".json") and not entry.name.startswith(".")),
                key=lambda entry: entry.name,
            )

    def names(self) -> list[str]:
        return [entry.name for entry in self._entries()]

    def read(self, name: str) -> bytes | None:
        try:
            with open(os.path.join(self.path, name), "rb") as file:
                return file.read()
        except FileNotFoundError:
            return None

    def write(self, body: bytes) -> str:
        name = uuid7() + ".json"
        # Written under a hidden name and renamed, so a reader never sees half a file.
        temporary = os.path.join(self.path, "." + name + ".tmp")
        with open(temporary, "wb") as file:
            file.write(body)
        os.replace(temporary, os.path.join(self.path, name))
        return name

    def remove(self, name: str) -> None:
        try:
            os.remove(os.path.join(self.path, name))
        except FileNotFoundError:
            pass

    def total_bytes(self) -> int:
        total = 0
        for entry in self._entries():
            try:
                total += entry.stat().st_size
            except FileNotFoundError:
                pass
        return total
