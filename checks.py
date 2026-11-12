"""Business oracle. It deliberately does not import baseline SQL or planner helpers."""
import copy
import json
import random
import sqlite3
import sys
from warehouse import Store, Error, FAULTS
from samples import INITIAL, CORRECTION

def enc(x):
    return json.dumps(x,sort_keys=True,separators=(",",":"),ensure_ascii=False,allow_nan=False)

def oracle(state):
    a,b,c = ([r for r in state[t].values() if not r["deleted"]] for t in "abc")
    first, used_b = [],set()
    for left in a:
        matches = [r for r in b if left["k"] is not None and left["k"] == r["k"]]
        for right in matches or [None]:
            if right is not None:
                used_b.add(right["id"])
            aid,bid = left["id"],right["id"] if right else None
            first.append({"token":enc([aid,bid]),"aid":aid,"bid":bid,"k":left["k"],
                          "route":right["route"] if right else None,"day":left["day"],
                          "region":right["region"] if right else None,"amount":left["amount"]})
    for right in b:
        if right["id"] not in used_b:
            first.append({"token":enc([None,right["id"]]),"aid":None,"bid":right["id"],"k":right["k"],
                          "route":right["route"],"day":None,"region":right["region"],"amount":None})
    second,used_c = [],set()
    for left in first:
        matches = [r for r in c if left["route"] is not None and left["route"] == r["route"]]
        for right in matches or [None]:
            if right is not None:
                used_c.add(right["id"])
            cid = right["id"] if right else None
            factor = right["factor"] if right else None
            row = dict(left,token=enc([left["aid"],left["bid"],cid]),cid=cid,factor=factor,
                       charge=None if left["amount"] is None or factor is None else left["amount"]*factor)
            second.append(row)
    for right in c:
        if right["id"] not in used_c:
            second.append({"token":enc([None,None,right["id"]]),"aid":None,"bid":None,"cid":right["id"],
                           "k":None,"route":right["route"],"day":None,"region":None,"amount":None,
                           "factor":right["factor"],"charge":None})
    bins = {}
    for row in second:
        bins.setdefault((row["day"],row["region"]),[]).append(row)
    groups = []
    for (day,region),rows in bins.items():
        numbers = [r["charge"] for r in rows if r["charge"] is not None]
        groups.append({"token":enc([day,region]),"day":day,"region":region,"n":len(rows),
                       "valued":len(numbers),"total":sum(numbers) if numbers else None,
                       "samples":len({r["aid"] for r in rows if r["aid"] is not None})})
    return sorted(first,key=lambda r:r["token"]),sorted(second,key=lambda r:r["token"]),sorted(groups,key=lambda r:r["token"])

def transition(state,batch):
    new = copy.deepcopy(state)
    keys,routes = set(),set()
    for row in batch["changes"]:
        t,identity = row["source"],row["id"]
        old = state[t].get(identity)
        if old is not None and old["rev"] >= row["rev"]:
            continue
        for candidate in (old,row):
            if candidate is not None and not candidate["deleted"]:
                (routes if t == "c" else keys).add(candidate["route"] if t == "c" else candidate["k"])
        new[t][identity] = copy.deepcopy(row)
    first0,second0,_ = oracle(state)
    first1,second1,groups1 = oracle(new)
    routes.update(r["route"] for r in first0+first1 if r["k"] in keys)
    groups = {(r["day"],r["region"]) for r in second0+second1 if r["route"] in routes}
    old,newrows = {r["token"]:r for r in second0},{r["token"]:r for r in second1}
    delta = [{"weight":sign,"row":row}
             for sign,rows,other in ((-1,old,newrows),(1,newrows,old))
             for token,row in rows.items() if row != other.get(token)]
    delta.sort(key=lambda d:(enc(d["row"]),d["weight"]))
    work = {"keys":sorted(keys,key=enc),"routes":sorted(routes,key=enc),
            "groups":[list(g) for g in sorted(groups,key=enc)]}
    return new,second1,groups1,delta,work

def expect_error(store,batch,code,point=None):
    before = tuple(store.conn.iterdump())
    try:
        store.apply(batch,fail_at=point)
    except Error as error:
        assert str(error) == code,(str(error),code)
    else:
        raise AssertionError("expected "+code)
    assert tuple(store.conn.iterdump()) == before,code+" changed persistent state"

def reopen(store):
    image = "\n".join(store.conn.iterdump())
    store.close()
    connection = sqlite3.connect(":memory:",isolation_level=None)
    connection.executescript(image)
    return Store(connection=connection)

def run():
    store = Store()
    state = {t:{} for t in "abc"}
    generation = 0
    history = []
    rng = random.Random(48119)

    def accept(batch):
        nonlocal state,generation
        next_state,rows,groups,delta,work = transition(state,batch)
        receipt = store.apply(batch)
        generation += 1
        assert receipt == {"batch_id":batch["batch_id"],"generation":generation,"delta":delta,"work":work},receipt
        assert store.read() == {"generation":generation,"rows":rows,"groups":groups}
        state = next_state
        history.append((copy.deepcopy(batch),copy.deepcopy(receipt)))

    accept(copy.deepcopy(INITIAL))
    east = next(g for g in store.read()["groups"] if g["region"] == "east")
    assert (east["n"],east["valued"],east["total"],east["samples"]) == (4,4,1000,1)
    accept(copy.deepcopy(CORRECTION))
    assert next(g for g in store.read()["groups"] if g["region"] == "west")["total"] == -100
    assert any(r["cid"] == "c1" and r["aid"] is None for r in store.read()["rows"])
    store = reopen(store)
    for step in range(96):
        changes = []
        for t in "abc":
            for identity in rng.sample(["i0","i1","x:[]","中文"],rng.randrange(3)):
                old = state[t].get(identity)
                rev = old["rev"]+1 if old else 1
                if old and rng.randrange(7) == 0:
                    changes.append(copy.deepcopy(old))
                    continue
                deleted = rng.randrange(5) == 0
                row = {"source":t,"id":identity,"rev":rev,"deleted":deleted}
                if not deleted:
                    if t in "ab":
                        row["k"] = rng.choice([None,"","m","n"])
                    if t == "a":
                        row.update(day=rng.choice(["d0","d1"]),amount=rng.choice([None,0,-5,9]))
                    elif t == "b":
                        row.update(route=rng.choice([None,"","r","q"]),region=rng.choice([None,"east","west"]))
                    else:
                        row.update(route=rng.choice([None,"","r","q"]),factor=rng.choice([None,-1,0,2]))
                changes.append(row)
        batch = {"batch_id":"rnd"+str(step),"expected_generation":generation,"changes":changes}
        if step % 12 == 0:
            for point in FAULTS:
                expect_error(store,batch,"INJECTED:"+point,point)
            store = reopen(store)
        accept(batch)
        replay,receipt = rng.choice(history)
        replay["expected_generation"] = 0
        assert store.apply(replay,fail_at="after_sources") == receipt
        if step % 16 == 0:
            store = reopen(store)
    accept({"batch_id":"empty","expected_generation":generation,"changes":[]})
    replay,_ = history[0]
    bad = copy.deepcopy(replay)
    bad["changes"] = []
    expect_error(store,bad,"BATCH_CONFLICT")
    expect_error(store,{"batch_id":"badgen","expected_generation":0,"changes":[]},"GENERATION_CONFLICT")
    conflict = copy.deepcopy(state["a"]["s1"])
    conflict["amount"] += 1
    expect_error(store,{"batch_id":"conflict","expected_generation":generation,"changes":[conflict]},"REVISION_CONFLICT")
    stale = dict(conflict,rev=1)
    # s1 is rev 1, so use a true stale version after explicitly accepting rev 3.
    fresh = dict(conflict,rev=3)
    accept({"batch_id":"fresh","expected_generation":generation,"changes":[fresh]})
    accept({"batch_id":"stale","expected_generation":generation,"changes":[stale]})
    expect_error(store,{"batch_id":"bad","expected_generation":generation,"changes":[dict(fresh,rev=True)]},"BAD_INPUT")
    assert store.read()["generation"] == generation
    store.close()
    nested = Store()
    original = tuple(nested.conn.iterdump())
    try:
        with nested.transaction():
            nested.apply(INITIAL)
            first = nested.read()
            try:
                with nested.transaction():
                    nested.apply(CORRECTION)
                    raise RuntimeError("cancel inner")
            except RuntimeError:
                pass
            assert nested.read() == first
            expect_error(nested,CORRECTION,"INJECTED:after_receipt","after_receipt")
            nested.apply(CORRECTION)
            assert nested.apply(dict(INITIAL,expected_generation=999))["generation"] == 1
            raise RuntimeError("cancel publish")
    except RuntimeError:
        pass
    assert tuple(nested.conn.iterdump()) == original
    nested.apply(INITIAL)
    nested.apply(CORRECTION)
    nested.close()
    print("BUSINESS_REGRESSION_OK")

def legacy(sql):
    connection = sqlite3.connect(":memory:",isolation_level=None)
    connection.executescript(sql)
    before_schema = list(connection.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY name"))
    store = Store(connection=connection)
    assert [tuple(r) for r in connection.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY name")] == before_schema
    state,rows,groups,_,_ = transition({t:{} for t in "abc"},INITIAL)
    assert store.read() == {"generation":1,"rows":rows,"groups":groups}
    replay = store.apply(dict(INITIAL,expected_generation=999))
    assert replay["generation"] == 1 and store.read()["generation"] == 1
    store.apply(CORRECTION)
    _,rows,groups,_,_ = transition(state,CORRECTION)
    assert store.read() == {"generation":2,"rows":rows,"groups":groups}
    store.close()
    print("LEGACY_DATABASE_OK")

if __name__ == "__main__":
    from pathlib import Path
    run()
    legacy(Path("samples/legacy.sql").read_text(encoding="utf-8"))
    if len(sys.argv) > 1 and sys.argv[1] == "verify":
        import boundary_checks
        boundary_checks.run()
