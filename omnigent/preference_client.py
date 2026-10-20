"""Durable local cache and outbox for explicitly connected preference clients.

Callers supply the authenticated HTTP client and choose when to synchronize.
Nothing here imports YAML, changes launch defaults, or starts a background task.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from uuid import UUID, uuid4

import httpx
from filelock import FileLock

from omnigent.preference_sync import (
    MAX_KEYS,
    Name,
    PreferenceMutation,
    PreferenceNamespace,
    PreferenceResult,
    PreferenceSnapshot,
    ProtocolModel,
    SharedPreferenceEntry,
    SharedPreferenceKey,
    Value,
)

_PATH = "/v1/preferences/shared"


class _Binding(ProtocolModel):
    identity: PreferenceNamespace
    installation_id: Name


class PreferenceClient:
    """One identity-bound SQLite cache, shared safely by local processes.

    ``enqueue`` acknowledges a local durable write, not a server save. ``sync``
    flushes a bounded batch and refreshes the cache; failures retain unsent or
    uncertain operations. Use a separate file for each account/workspace.
    """

    def __init__(self, path: Path, *, identity: PreferenceNamespace, installation_id: str) -> None:
        self.path = path
        self._binding = _Binding(identity=identity, installation_id=installation_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._transaction() as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS state "
                "(id INTEGER PRIMARY KEY CHECK (id = 1), binding TEXT NOT NULL, "
                "snapshot TEXT, revision_floor INTEGER NOT NULL DEFAULT 0)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS entries (key TEXT PRIMARY KEY, payload TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS outbox (sequence INTEGER PRIMARY KEY AUTOINCREMENT, "
                "operation TEXT NOT NULL, predecessor TEXT, sent INTEGER NOT NULL DEFAULT 0)"
            )
            connection.execute(
                "INSERT OR IGNORE INTO state (id, binding) VALUES (1, ?)",
                (self._binding.model_dump_json(),),
            )
            binding = connection.execute("SELECT binding FROM state WHERE id = 1").fetchone()[0]
            if _Binding.model_validate_json(binding) != self._binding:
                raise ValueError("Preference cache belongs to another identity or installation")

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=5)
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def cached(self) -> PreferenceSnapshot | None:
        """Return the last complete server snapshot; pending edits stay separate."""
        with self._transaction() as connection:
            raw = connection.execute("SELECT snapshot FROM state WHERE id = 1").fetchone()[0]
        return PreferenceSnapshot.model_validate_json(raw) if raw is not None else None

    def pending(self) -> list[PreferenceMutation]:
        """List durable edits in submission order, including uncertain deliveries."""
        with self._transaction() as connection:
            rows = connection.execute("SELECT operation FROM outbox ORDER BY sequence").fetchall()
        return [PreferenceMutation.model_validate_json(row[0]) for row in rows]

    def discard(self, operation_id: UUID) -> PreferenceMutation:
        """Explicitly abandon the first queued edit, allowing later work to sync.

        Inspect the HTTP error and ``pending()`` first. Discarding never undoes
        a server commit; uncertain deliveries may already have applied. Successors
        retain their versions so they cannot silently overwrite that value.
        """
        with (
            FileLock(str(self.path) + ".sync.lock", timeout=0),
            self._transaction() as connection,
        ):
            row = connection.execute(
                "SELECT sequence, operation FROM outbox ORDER BY sequence LIMIT 1"
            ).fetchone()
            operation = PreferenceMutation.model_validate_json(row[1]) if row else None
            if operation is None or operation.operation_id != operation_id:
                raise ValueError("Only the first queued preference operation can be discarded")
            connection.execute(
                "UPDATE outbox SET predecessor = NULL WHERE predecessor = ?",
                (str(operation_id),),
            )
            connection.execute("DELETE FROM outbox WHERE sequence = ?", (row[0],))
        return operation

    def enqueue(
        self, key: SharedPreferenceKey, value: Value, *, resolves: tuple[UUID, ...] = ()
    ) -> UUID:
        """Save an explicit edit locally, without making a network request.

        Successive offline edits to one key form a causal chain. Only an
        unattempted successor may inherit its predecessor's acknowledged version.
        """
        with self._transaction() as connection:
            if connection.execute("SELECT snapshot FROM state WHERE id = 1").fetchone()[0] is None:
                raise ValueError("Synchronize once to verify identity before queuing preferences")
            rows = connection.execute("SELECT operation FROM outbox ORDER BY sequence").fetchall()
            if len(rows) >= MAX_KEYS:
                raise ValueError("Preference outbox is full; synchronize before adding edits")
            previous = [PreferenceMutation.model_validate_json(row[0]) for row in rows]
            predecessor = next((item for item in reversed(previous) if item.key == key), None)
            raw = connection.execute(
                "SELECT payload FROM entries WHERE key = ?", (key.storage_key(),)
            ).fetchone()
            version = SharedPreferenceEntry.model_validate_json(raw[0]).version if raw else 0
            operation = PreferenceMutation(
                protocol_version=1,
                identity=self._binding.identity,
                installation_id=self._binding.installation_id,
                operation_id=uuid4(),
                key=key,
                expected_version=version,
                value=value,
                resolves=list(resolves),
            )
            connection.execute(
                "INSERT INTO outbox (operation, predecessor) VALUES (?, ?)",
                (
                    operation.model_dump_json(),
                    str(predecessor.operation_id) if predecessor else None,
                ),
            )
        return operation.operation_id

    def _accept_snapshot(self, snapshot: PreferenceSnapshot) -> None:
        if snapshot.identity != self._binding.identity:
            raise ValueError("Server preference identity does not match this cache")
        with self._transaction() as connection:
            floor = connection.execute("SELECT revision_floor FROM state WHERE id = 1").fetchone()[
                0
            ]
            if snapshot.revision < floor:
                raise ValueError("Server preference snapshot is older than acknowledged state")
            incoming = {entry.key.storage_key(): entry for entry in snapshot.entries}
            if len(incoming) != len(snapshot.entries):
                raise ValueError("Server preference snapshot has duplicate keys")
            for key, raw in connection.execute("SELECT key, payload FROM entries"):
                known = SharedPreferenceEntry.model_validate_json(raw)
                entry = incoming.get(key)
                if entry is None or entry.version < known.version:
                    raise ValueError("Server preference snapshot lost an acknowledged version")
                if entry.version == known.version and entry != known:
                    raise ValueError("Server preference value changed without a version")
            connection.executemany(
                "INSERT OR REPLACE INTO entries (key, payload) VALUES (?, ?)",
                [(key, entry.model_dump_json()) for key, entry in incoming.items()],
            )
            connection.execute(
                "UPDATE state SET snapshot = ?, revision_floor = ? WHERE id = 1",
                (snapshot.model_dump_json(), snapshot.revision),
            )

    def _next(self) -> tuple[int, PreferenceMutation] | None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT sequence, operation, predecessor FROM outbox ORDER BY sequence LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            if row[2] is not None:
                raise ValueError("Preference outbox has a missing predecessor")
            connection.execute("UPDATE outbox SET sent = 1 WHERE sequence = ?", (row[0],))
            return row[0], PreferenceMutation.model_validate_json(row[1])

    def _acknowledge(
        self, sequence: int, operation: PreferenceMutation, result: PreferenceResult
    ) -> None:
        if result.operation_id != operation.operation_id or result.entry.key != operation.key:
            raise ValueError("Preference receipt does not match the submitted operation")
        if result.status != "conflict" and result.entry.value != operation.value:
            raise ValueError("Preference receipt did not acknowledge the submitted value")
        with self._transaction() as connection:
            raw = connection.execute(
                "SELECT payload FROM entries WHERE key = ?", (operation.key.storage_key(),)
            ).fetchone()
            known = SharedPreferenceEntry.model_validate_json(raw[0]) if raw else None
            if known is None or result.entry.version > known.version:
                connection.execute(
                    "INSERT OR REPLACE INTO entries (key, payload) VALUES (?, ?)",
                    (operation.key.storage_key(), result.entry.model_dump_json()),
                )
            connection.execute(
                "UPDATE state SET revision_floor = MAX(revision_floor, ?) WHERE id = 1",
                (result.revision,),
            )
            for successor_id, payload, sent in connection.execute(
                "SELECT sequence, operation, sent FROM outbox WHERE predecessor = ?",
                (str(operation.operation_id),),
            ).fetchall():
                if sent:
                    raise ValueError("Cannot rebase an attempted preference operation")
                successor = PreferenceMutation.model_validate_json(payload)
                # Conflict receipts never authorize overwriting the competing value.
                if result.status != "conflict":
                    successor = successor.model_copy(
                        update={"expected_version": result.entry.version}
                    )
                connection.execute(
                    "UPDATE outbox SET operation = ?, predecessor = NULL WHERE sequence = ?",
                    (successor.model_dump_json(), successor_id),
                )
            connection.execute("DELETE FROM outbox WHERE sequence = ?", (sequence,))

    async def sync(
        self, client: httpx.AsyncClient, *, max_operations: int = 100
    ) -> PreferenceSnapshot:
        """Verify identity, deliver a bounded batch, then fetch authoritative state.

        HTTP errors, cancellation, and malformed replies leave uncertain edits
        queued with the same operation ID/body. The caller owns retry scheduling,
        authentication, URL, and HTTP timeouts. No database lock spans HTTP I/O.
        """
        if max_operations < 0:
            raise ValueError("max_operations must be nonnegative")
        # Serialize sync across processes; enqueue remains available during HTTP I/O.
        with FileLock(str(self.path) + ".sync.lock", timeout=0):

            async def refresh() -> PreferenceSnapshot:
                response = await client.get(_PATH)
                response.raise_for_status()
                snapshot = PreferenceSnapshot.model_validate_json(response.content)
                await asyncio.to_thread(self._accept_snapshot, snapshot)
                return snapshot

            snapshot = await refresh()
            delivered = False
            for _ in range(max_operations):
                pending = await asyncio.to_thread(self._next)
                if pending is None:
                    break
                sequence, operation = pending
                response = await client.post(
                    _PATH + "/operations", json=operation.model_dump(mode="json")
                )
                response.raise_for_status()
                result = PreferenceResult.model_validate_json(response.content)
                await asyncio.to_thread(self._acknowledge, sequence, operation, result)
                delivered = True
            return await refresh() if delivered else snapshot
