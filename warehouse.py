"""Compatibility facade for the reconciliation warehouse.

The original ``Store`` API, CLI, ``Error`` and ``FAULTS`` symbols are kept for
existing callers and diagnostics.  The class only assembles the SQLite adapter
with the application engine and forwards every call; the association and
aggregation rules live in ``reconcile.domain`` and there is no backup runner.
"""
import json
import sys

from reconcile.adapters.sqlite import SQLiteRepository
from reconcile.application import Engine
from reconcile.domain import FAULTS, Error, canonical

__all__ = ["Store", "Error", "FAULTS", "canonical"]


class Store:
    def __init__(self, path=":memory:", *, connection=None):
        self._repository = SQLiteRepository(path, connection=connection)
        self._engine = Engine(self._repository)
        # Retained so existing diagnostics can read the original tables.
        self.conn = self._repository.conn

    def apply(self, batch, fail_at=None):
        return self._engine.apply(batch, fail_at=fail_at)

    def read(self):
        return self._engine.read()

    def transaction(self, write=True):
        return self._engine.transaction(write=write)

    def close(self):
        return self._engine.close()


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    sub = parser.add_subparsers(dest="op", required=True)
    sub.add_parser("read")
    apply_parser = sub.add_parser("apply")
    apply_parser.add_argument("batch")
    args = parser.parse_args(argv)
    store = Store(args.db)
    try:
        if args.op == "apply":
            with open(args.batch, encoding="utf-8") as handle:
                try:
                    batch = json.load(
                        handle,
                        parse_constant=lambda s: (_ for _ in ()).throw(ValueError(s)))
                except ValueError:
                    raise Error("BAD_INPUT") from None
            result = store.apply(batch)
        else:
            result = store.read()
        print(canonical(result))
        return 0
    except Error as error:
        print(canonical({"error": str(error)}))
        return 2
    finally:
        store.close()


if __name__ == "__main__":
    sys.exit(main())
