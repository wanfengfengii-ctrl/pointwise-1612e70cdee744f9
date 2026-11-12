"""Storage port consumed by the application engine.

The port is the only seam between the I/O-free reconciliation rules and a
concrete persistence mechanism.  Adapters keep indexed rows, transactions and
JSON (de)serialisation; they never interpret joins, rollups or revisions.
"""
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional


class Repository:
    """Published storage protocol.

    Tables are ``a``, ``b``, ``c`` (sources), ``j1``, ``j2`` (the two join
    levels) and ``g`` (rollups).  ``id`` maps to each table's primary key;
    sources ``a``/``b`` additionally index ``k``, ``c`` indexes ``route``,
    ``j1`` indexes ``k``/``route`` and ``j2`` indexes ``route``/``group``.
    Index comparisons use SQL ``IS`` semantics, so every ``None`` lands in one
    NULL bucket but NULL never participates in equality matching.
    """

    @contextmanager
    def transaction(self, write: bool = True):
        """Nested transaction context; an exception undoes only this frame."""
        raise NotImplementedError

    def generation(self) -> int:
        raise NotImplementedError

    def set_generation(self, n: int) -> None:
        raise NotImplementedError

    def receipt(self, batch_id: str) -> Optional[Dict[str, Any]]:
        """Return ``None`` or ``{"payload": str, "result": object}``."""
        raise NotImplementedError

    def put_receipt(self, batch_id: str, payload: str, result: Dict[str, Any]) -> None:
        raise NotImplementedError

    def select(self, table: str, index: str, value: Any) -> List[Dict[str, Any]]:
        """Independent copies of full indexed rows, tombstones included."""
        raise NotImplementedError

    def upsert(self, table: str, rows: Iterable[Dict[str, Any]]) -> None:
        """Replace full rows keyed by the table's primary key."""
        raise NotImplementedError

    def delete(self, table: str, ids: Iterable[Any]) -> None:
        """Delete rows by the table's primary key."""
        raise NotImplementedError

    def export(self) -> Dict[str, Any]:
        """Full read snapshot; only the read-only entry point may call it."""
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError
