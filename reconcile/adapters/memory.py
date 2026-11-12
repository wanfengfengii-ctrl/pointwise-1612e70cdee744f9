"""In-memory adapter for the reconciliation storage port.

A plain table/index store with snapshot-based nested transactions, mirroring
the SQLite adapter's commit and rollback semantics without importing it or any
domain rule.  Returned rows are independent deep copies so callers cannot
mutate stored state through a reference.
"""
import copy
from contextlib import contextmanager

from reconcile.ports import Repository

_PRIMARY_KEY = {"a": "id", "b": "id", "c": "id", "j1": "token", "j2": "token", "g": "token"}
_INDEXES = {
    "a": {"id": ("id",), "k": ("k",)},
    "b": {"id": ("id",), "k": ("k",)},
    "c": {"id": ("id",), "route": ("route",)},
    "j1": {"k": ("k",), "route": ("route",)},
    "j2": {"route": ("route",), "group": ("day", "region")},
    "g": {"id": ("token",)},
}
_TABLES = ("a", "b", "c", "j1", "j2", "g")


class MemoryRepository(Repository):
    def __init__(self):
        self._tables = {table: {} for table in _TABLES}
        self._generation = 0
        self._receipts = {}
        self._frames = []

    @contextmanager
    def transaction(self, write=True):
        # Each frame snapshots the full mutable state; releasing drops the
        # snapshot while rolling back restores exactly this frame's view.
        snapshot = (
            copy.deepcopy(self._tables),
            self._generation,
            copy.deepcopy(self._receipts),
        )
        self._frames.append(snapshot)
        try:
            yield
        except BaseException:
            tables, generation, receipts = self._frames.pop()
            self._tables = tables
            self._generation = generation
            self._receipts = receipts
            raise
        else:
            self._frames.pop()

    def generation(self):
        return self._generation

    def set_generation(self, n):
        self._generation = n

    def receipt(self, batch_id):
        record = self._receipts.get(batch_id)
        return None if record is None else copy.deepcopy(record)

    def put_receipt(self, batch_id, payload, result):
        self._receipts[batch_id] = {"payload": payload, "result": copy.deepcopy(result)}

    @staticmethod
    def _same(column_value, wanted):
        # SQL IS semantics: NULL bucket matches NULL only, otherwise equality.
        return column_value is None if wanted is None else column_value == wanted

    def select(self, table, index, value):
        columns = _INDEXES[table][index]
        wanted = (value,) if len(columns) == 1 else tuple(value)
        found = []
        for row in self._tables[table].values():
            if all(self._same(row[column], wanted[pos]) for pos, column in enumerate(columns)):
                found.append(copy.deepcopy(row))
        return found

    def upsert(self, table, rows):
        key = _PRIMARY_KEY[table]
        store = self._tables[table]
        for row in rows:
            store[row[key]] = copy.deepcopy(row)

    def delete(self, table, ids):
        key = _PRIMARY_KEY[table]
        store = self._tables[table]
        for identity in ids:
            for stored_id in [candidate for candidate in store
                              if self._same(candidate, identity)]:
                del store[stored_id]

    def export(self):
        return {
            "generation": self._generation,
            "rows": [copy.deepcopy(row) for row in self._tables["j2"].values()],
            "groups": [copy.deepcopy(row) for row in self._tables["g"].values()],
        }

    def close(self):
        self._frames.clear()
        self._tables = {table: {} for table in _TABLES}
        self._receipts = {}
        self._generation = 0
