"""Extra equivalence checks across the SQLite and memory adapters.

Driven only through the published Engine/Repository seam and cross-checked
against the pure oracle in checks.py; focuses on same-batch key/route moves,
delete-resurrect cycles, duplicate mappings and NULL buckets.
"""
import copy

from checks import oracle, transition
from reconcile.adapters.memory import MemoryRepository
from reconcile.adapters.sqlite import SQLiteRepository
from reconcile.application import Engine
from reconcile.domain import Error


def fresh_engines():
    return {"sqlite": Engine(SQLiteRepository()), "memory": Engine(MemoryRepository())}


BATCHES = [
    {"batch_id": "b0", "expected_generation": 0, "changes": [
        {"source": "a", "id": "a1", "rev": 1, "deleted": False, "k": "m", "day": "d", "amount": 5},
        {"source": "a", "id": "a2", "rev": 1, "deleted": False, "k": None, "day": "d", "amount": 7},
        {"source": "b", "id": "b1", "rev": 1, "deleted": False, "k": "m", "route": "r", "region": "east"},
        {"source": "b", "id": "b2", "rev": 1, "deleted": False, "k": "m", "route": "r", "region": "east"},
        {"source": "c", "id": "c1", "rev": 1, "deleted": False, "route": "r", "factor": 2}]},
    # same-batch swap of keys between two b rows and a c route move
    {"batch_id": "swap", "expected_generation": 1, "changes": [
        {"source": "b", "id": "b1", "rev": 2, "deleted": False, "k": "n", "route": "q", "region": "west"},
        {"source": "b", "id": "b2", "rev": 2, "deleted": False, "k": "m", "route": "q", "region": "west"},
        {"source": "c", "id": "c1", "rev": 2, "deleted": False, "route": "q", "factor": -1}]},
    # delete then resurrect in the same logical identity over batches
    {"batch_id": "kill", "expected_generation": 2, "changes": [
        {"source": "a", "id": "a1", "rev": 2, "deleted": True}]},
    {"batch_id": "revive", "expected_generation": 3, "changes": [
        {"source": "a", "id": "a1", "rev": 3, "deleted": False, "k": "m", "day": "d", "amount": 9}]},
    # empty-string key/route matching, NULL factor/amount
    {"batch_id": "empties", "expected_generation": 4, "changes": [
        {"source": "a", "id": "a3", "rev": 1, "deleted": False, "k": "", "day": "d2", "amount": None},
        {"source": "b", "id": "b3", "rev": 1, "deleted": False, "k": "", "route": "", "region": None},
        {"source": "c", "id": "c3", "rev": 1, "deleted": False, "route": "", "factor": None}]},
    # stale and identical revisions in one batch
    {"batch_id": "mixed", "expected_generation": 5, "changes": [
        {"source": "a", "id": "a3", "rev": 1, "deleted": False, "k": "", "day": "d2", "amount": None},
        {"source": "c", "id": "c3", "rev": 2, "deleted": False, "route": "", "factor": 4}]},
]


def main():
    state = {t: {} for t in "abc"}
    generation = 0
    engines = fresh_engines()
    receipts = {}
    for batch in BATCHES:
        next_state, rows, groups, delta, work = transition(state, batch)
        expected = {"batch_id": batch["batch_id"], "generation": generation + 1,
                    "delta": delta, "work": work}
        for name, engine in engines.items():
            result = engine.apply(copy.deepcopy(batch))
            assert result == expected, (name, result, expected)
            snapshot = engine.read()
            assert snapshot == {"generation": generation + 1, "rows": rows,
                                "groups": groups}, name
            receipts.setdefault(batch["batch_id"], {})[name] = result
        state = next_state
        generation += 1

    # replay is byte-identical on both and ignores expected_generation/faults
    for name, engine in engines.items():
        replay = copy.deepcopy(BATCHES[0])
        replay["expected_generation"] = 999
        assert engine.apply(replay, fail_at="after_receipt") == receipts["b0"][name]
        assert engine.read()["generation"] == generation

    # both adapters expose identical final snapshots
    snaps = {name: engine.read() for name, engine in engines.items()}
    assert snaps["sqlite"] == snaps["memory"]

    # equal-rev different-body conflict rolls back on both
    for name, engine in engines.items():
        bad = copy.deepcopy(BATCHES[-1]["changes"][-1])
        bad["factor"] = 40
        before = engine.read()
        try:
            engine.apply({"batch_id": "conflict-" + name,
                          "expected_generation": generation, "changes": [bad]})
        except Error as error:
            assert str(error) == "REVISION_CONFLICT"
        else:
            raise AssertionError(name)
        assert engine.read() == before
        engine.close()
    print("ADAPTER_EQUIVALENCE_OK")


if __name__ == "__main__":
    main()
