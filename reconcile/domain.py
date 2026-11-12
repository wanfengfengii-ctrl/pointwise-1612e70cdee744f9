"""Pure reconciliation domain.

Every function here is I/O-free: it accepts plain built-in values (stored row
dictionaries shaped like the table columns) and returns plain values or plans
the application layer can interpret through the storage port.  No connection,
repository object or callback ever crosses this boundary.

Responsibilities owned here:
  * batch validation and canonical identity bytes;
  * revision selection (stale / equal / conflict / accepted) and tombstones;
  * the two multiset full outer joins (j1, j2);
  * the locality closure (affected keys, routes and group buckets);
  * NULL-aware grouping aggregates and positive/negative delta rows.
"""
import json

TABLES = ("a", "b", "c", "j1", "j2", "g")
SOURCE_FIELDS = {"a": {"k", "day", "amount"},
                 "b": {"k", "route", "region"},
                 "c": {"route", "factor"}}
COMMON_FIELDS = {"source", "id", "rev", "deleted"}
FAULTS = ("after_sources", "after_first", "after_second", "after_rollup", "after_receipt")

SOURCE_COLUMNS = ("id", "rev", "body", "live", "k", "route", "day", "region", "amount", "factor")
J1_COLUMNS = ("token", "aid", "bid", "k", "route", "day", "region", "amount")
J2_COLUMNS = ("token", "aid", "bid", "cid", "k", "route", "day", "region",
              "amount", "factor", "charge")
G_COLUMNS = ("token", "day", "region", "n", "valued", "total", "samples")


class Error(Exception):
    """Business error; ``str(error)`` is the documented error code."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def normalize(batch):
    """Validate a batch and return identity bytes and id-ordered changes.

    Raises ``Error("BAD_INPUT")`` for any structural or value problem, which
    keeps that precedence ahead of every storage interaction.
    """
    try:
        if not isinstance(batch, dict) or set(batch) != {"batch_id", "expected_generation", "changes"}:
            raise ValueError()
        bid, expected, changes = batch["batch_id"], batch["expected_generation"], batch["changes"]
        if not isinstance(bid, str) or not bid or type(expected) is not int or expected < 0 \
                or not isinstance(changes, list):
            raise ValueError()
        seen = set()
        for row in changes:
            if not isinstance(row, dict):
                raise ValueError()
            source = row.get("source")
            if source not in SOURCE_FIELDS or type(row.get("deleted")) is not bool:
                raise ValueError()
            if set(row) != COMMON_FIELDS | (set() if row["deleted"] else SOURCE_FIELDS[source]):
                raise ValueError()
            if not isinstance(row["id"], str) or not row["id"] \
                    or type(row["rev"]) is not int or row["rev"] < 1:
                raise ValueError()
            identity = (source, row["id"])
            if identity in seen:
                raise ValueError()
            seen.add(identity)
            if not row["deleted"]:
                for key in SOURCE_FIELDS[source]:
                    value = row[key]
                    if key in ("amount", "factor"):
                        if value is not None and (
                                type(value) is not int
                                or abs(value) > (1000000 if key == "amount" else 1000)):
                            raise ValueError()
                    elif value is not None and not isinstance(value, str):
                        raise ValueError()
                if source == "a" and (not isinstance(row["day"], str) or not row["day"]):
                    raise ValueError()
        return bid, expected, canonical(changes), sorted(changes, key=lambda r: (r["source"], r["id"]))
    except (ValueError, TypeError, KeyError):
        raise Error("BAD_INPUT") from None


def stored_source_row(row):
    """Project one validated input line into the source table's row shape."""
    return {"id": row["id"], "rev": row["rev"], "body": canonical(row),
            "live": 0 if row["deleted"] else 1,
            "k": row.get("k"), "route": row.get("route"), "day": row.get("day"),
            "region": row.get("region"), "amount": row.get("amount"),
            "factor": row.get("factor")}


def _live(row):
    return row is not None and row["live"] == 1


def revise(changes, old_rows):
    """Apply per (source, id) revision rules to an id-ordered change list.

    ``old_rows`` maps ``(source, id)`` to the previously stored row or
    ``None``.  Stale and identical revisions are ignored, an equal rev with a
    different body raises REVISION_CONFLICT, and every accepted line yields a
    full replacement row plus its locality closure seeds (old and new live
    rows both seed, so key/route moves and swaps recompute both buckets).
    """
    writes = {"a": [], "b": [], "c": []}
    keys, routes = set(), set()

    def seed(table, candidate):
        if _live(candidate):
            (routes if table == "c" else keys).add(
                candidate["route"] if table == "c" else candidate["k"])

    for row in changes:
        table, identity = row["source"], row["id"]
        old = old_rows[(table, identity)]
        if old is not None:
            if row["rev"] < old["rev"]:
                continue
            if row["rev"] == old["rev"]:
                if old["body"] != canonical(row):
                    raise Error("REVISION_CONFLICT")
                continue
        seed(table, old)
        replacement = stored_source_row(row)
        seed(table, replacement)
        writes[table].append(replacement)
    return writes, keys, routes


def _matches(left, right):
    """Equi-join predicate: NULL never matches, including NULL vs NULL."""
    return left is not None and left == right


def associate_first(a_rows, b_rows):
    """Multiset full outer association of live a/b rows sharing one k bucket."""
    live_a = [r for r in a_rows if r["live"] == 1]
    live_b = [r for r in b_rows if r["live"] == 1]
    result, used_b = [], set()
    for left in live_a:
        matches = [r for r in live_b if _matches(left["k"], r["k"])]
        for right in matches or [None]:
            if right is not None:
                used_b.add(right["id"])
            aid, bid = left["id"], right["id"] if right is not None else None
            result.append({
                "token": canonical([aid, bid]), "aid": aid, "bid": bid,
                "k": left["k"],
                "route": right["route"] if right is not None else None,
                "day": left["day"],
                "region": right["region"] if right is not None else None,
                "amount": left["amount"]})
    for right in live_b:
        if right["id"] not in used_b:
            result.append({
                "token": canonical([None, right["id"]]), "aid": None,
                "bid": right["id"], "k": right["k"], "route": right["route"],
                "day": None, "region": right["region"], "amount": None})
    return result


def associate_second(j1_rows, c_rows):
    """Multiset full outer association of j1 rows and live c rows on one route."""
    live_c = [r for r in c_rows if r["live"] == 1]
    result, used_c = [], set()
    for left in j1_rows:
        matches = [r for r in live_c if _matches(left["route"], r["route"])]
        for right in matches or [None]:
            if right is not None:
                used_c.add(right["id"])
            cid = right["id"] if right is not None else None
            factor = right["factor"] if right is not None else None
            result.append({
                "token": canonical([left["aid"], left["bid"], cid]),
                "aid": left["aid"], "bid": left["bid"], "cid": cid,
                "k": left["k"], "route": left["route"], "day": left["day"],
                "region": left["region"], "amount": left["amount"],
                "factor": factor,
                "charge": None if left["amount"] is None or factor is None
                else left["amount"] * factor})
    for right in live_c:
        if right["id"] not in used_c:
            result.append({
                "token": canonical([None, None, right["id"]]), "aid": None,
                "bid": None, "cid": right["id"], "k": None,
                "route": right["route"], "day": None, "region": None,
                "amount": None, "factor": right["factor"], "charge": None})
    return result


def group_key(row):
    return row["day"], row["region"]


def rollup(j2_rows):
    """Aggregate j2 rows into g rows; NULL groups share one bucket."""
    bins = {}
    for row in j2_rows:
        bins.setdefault(group_key(row), []).append(row)
    groups = []
    for (day, region), rows in bins.items():
        charges = [r["charge"] for r in rows if r["charge"] is not None]
        groups.append({
            "token": canonical([day, region]), "day": day, "region": region,
            "n": len(rows), "valued": len(charges),
            "total": sum(charges) if charges else None,
            "samples": len({r["aid"] for r in rows if r["aid"] is not None})})
    return groups


def delta(before, after):
    """Signed j2 differences; identical full rows cancel, order is canonical."""
    before_map = {r["token"]: r for r in before}
    after_map = {r["token"]: r for r in after}
    changes = []
    for sign, rows, other in ((-1, before_map, after_map), (1, after_map, before_map)):
        for token, row in rows.items():
            if other.get(token) != row:
                changes.append({"weight": sign, "row": row})
    changes.sort(key=lambda item: (canonical(item["row"]), item["weight"]))
    return changes


def ordered(values):
    return sorted(values, key=canonical)


def present(exported):
    """Domain-owned token ordering for the read snapshot."""
    return {"generation": exported["generation"],
            "rows": sorted(exported["rows"], key=lambda r: r["token"]),
            "groups": sorted(exported["groups"], key=lambda r: r["token"])}


def build_result(batch_id, generation, before_rows, after_rows, keys, routes, groups):
    return {
        "batch_id": batch_id,
        "generation": generation,
        "delta": delta(before_rows, after_rows),
        "work": {"keys": ordered(keys),
                 "routes": ordered(routes),
                 "groups": [list(key) for key in ordered(groups)]}}
