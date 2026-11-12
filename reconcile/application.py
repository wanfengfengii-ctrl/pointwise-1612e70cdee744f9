"""Application layer: orchestrate pure domain plans through the storage port.

The engine owns transactions, the call order into the repository and the
injected fault positions.  It contains no join, aggregation or revision rule
and never touches an adapter-specific attribute: every rule lives in
``reconcile.domain`` and every row access goes through ``reconcile.ports``.
"""
from contextlib import contextmanager

from reconcile.domain import (
    FAULTS, Error, associate_first, associate_second, build_result, canonical,
    group_key, normalize, ordered, present, revise, rollup,
)
from reconcile.ports import Repository


class Engine:
    def __init__(self, repository: Repository):
        self._repository = repository

    @contextmanager
    def transaction(self, write=True):
        with self._repository.transaction(write):
            yield

    def read(self):
        # export() is the only whole-snapshot read and is restricted to
        # read-only frames.  Wrapping it in a read-only frame keeps it legal
        # even inside a caller's outer write transaction (the same savepoint
        # then sees the not-yet-committed view), while apply() never exports.
        with self._repository.transaction(False):
            return present(self._repository.export())

    def close(self):
        self._repository.close()

    def _source(self, table, identity):
        rows = self._repository.select(table, "id", identity)
        return rows[0] if rows else None

    def apply(self, batch, fail_at=None):
        bid, expected, payload, changes = normalize(batch)
        repository = self._repository

        def fault(point):
            if fail_at == point:
                raise Error("INJECTED:" + point)

        with repository.transaction(True):
            # Replay short-circuit: identical change bytes return the stored
            # receipt; expected_generation, faults and data are all untouched.
            receipt = repository.receipt(bid)
            if receipt is not None:
                if receipt["payload"] != payload:
                    raise Error("BATCH_CONFLICT")
                return receipt["result"]

            generation = repository.generation()
            if generation != expected:
                raise Error("GENERATION_CONFLICT")

            # Revision selection only needs point lookups of the batch's own
            # (source, id) values; domain.revise decides accept/ignore/conflict.
            old_rows = {(row["source"], row["id"]):
                        self._source(row["source"], row["id"])
                        for row in changes}
            writes, keys, routes = revise(changes, old_rows)
            for table in ("a", "b", "c"):
                if writes[table]:
                    repository.upsert(table, writes[table])
            fault("after_sources")

            # First association level, following the baseline's two passes.
            # Pass 1 harvests each affected bucket's existing routes and clears
            # it; this keeps an orphan's old route in the closure when its
            # source moves out of a key, including out of the NULL bucket.
            # Pass 2 recomputes every bucket from the final same-batch sources
            # so key moves and swaps share one end state, then re-harvests.
            # The NULL bucket is rebuilt like any other when touched; its rows
            # are orphans because NULL never participates in equality matching.
            key_order = ordered(keys)
            for key in key_order:
                existing = repository.select("j1", "k", key)
                routes.update(row["route"] for row in existing)
                repository.delete("j1", [row["token"] for row in existing])
            for key in key_order:
                rebuilt = associate_first(repository.select("a", "k", key),
                                          repository.select("b", "k", key))
                if rebuilt:
                    repository.upsert("j1", rebuilt)
                routes.update(row["route"] for row in rebuilt)
            fault("after_first")

            # Second association level: rebuild the route buckets reached from R.
            before, after, groups = {}, {}, set()
            route_order = ordered(routes)
            for route in route_order:
                old_rows = repository.select("j2", "route", route)
                before.update((row["token"], row) for row in old_rows)
                groups.update(group_key(row) for row in old_rows)
            for route in route_order:
                rebuilt = associate_second(
                    repository.select("j1", "route", route),
                    repository.select("c", "route", route))
                repository.delete(
                    "j2", [row["token"]
                           for row in repository.select("j2", "route", route)])
                if rebuilt:
                    repository.upsert("j2", rebuilt)
                after.update((row["token"], row) for row in rebuilt)
                groups.update(group_key(row) for row in rebuilt)
            fault("after_second")

            # Rollups: only the (day, region) buckets touched by R routes;
            # the group index still returns the complete bucket, so rows from
            # untouched routes keep contributing to the same aggregate.
            for day, region in ordered(groups):
                bucket = repository.select("j2", "group", [day, region])
                repository.delete("g", [canonical([day, region])])
                rebuilt_groups = rollup(bucket)
                if rebuilt_groups:
                    repository.upsert("g", rebuilt_groups)
            fault("after_rollup")

            result = build_result(bid, generation + 1,
                                  list(before.values()), list(after.values()),
                                  keys, routes, groups)
            repository.set_generation(generation + 1)
            repository.put_receipt(bid, payload, result)
            fault("after_receipt")
            return result
