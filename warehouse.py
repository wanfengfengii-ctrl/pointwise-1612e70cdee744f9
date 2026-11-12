"""Production baseline: incremental reconciliation warehouse, intentionally intertwined."""
import json
from contextlib import contextmanager
import sqlite3
import sys

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)

class Error(Exception):
    pass

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(n INTEGER NOT NULL);
INSERT INTO meta SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM meta);
CREATE TABLE IF NOT EXISTS receipts(id TEXT PRIMARY KEY,payload TEXT NOT NULL,result TEXT NOT NULL);
"""
SOURCE = "(id TEXT PRIMARY KEY,rev INTEGER NOT NULL,body TEXT NOT NULL,live INTEGER NOT NULL,k TEXT,route TEXT,day TEXT,region TEXT,amount INTEGER,factor INTEGER)"
J1 = "(token TEXT PRIMARY KEY,aid TEXT,bid TEXT,k TEXT,route TEXT,day TEXT,region TEXT,amount INTEGER)"
J2 = "(token TEXT PRIMARY KEY,aid TEXT,bid TEXT,cid TEXT,k TEXT,route TEXT,day TEXT,region TEXT,amount INTEGER,factor INTEGER,charge INTEGER)"
G = "(token TEXT PRIMARY KEY,day TEXT,region TEXT,n INTEGER,valued INTEGER,total INTEGER,samples INTEGER)"
FIELDS = {"a": {"k","day","amount"}, "b": {"k","route","region"}, "c": {"route","factor"}}
COMMON = {"source","id","rev","deleted"}
FAULTS = ("after_sources","after_first","after_second","after_rollup","after_receipt")

def normalize(batch):
    try:
        if not isinstance(batch, dict) or set(batch) != {"batch_id","expected_generation","changes"}:
            raise ValueError()
        bid, generation, changes = batch["batch_id"], batch["expected_generation"], batch["changes"]
        if not isinstance(bid,str) or not bid or type(generation) is not int or generation < 0 or not isinstance(changes,list):
            raise ValueError()
        seen = set()
        for row in changes:
            if not isinstance(row,dict):
                raise ValueError()
            source = row.get("source")
            if source not in FIELDS or type(row.get("deleted")) is not bool:
                raise ValueError()
            if set(row) != COMMON | (set() if row["deleted"] else FIELDS[source]):
                raise ValueError()
            if not isinstance(row["id"],str) or not row["id"] or type(row["rev"]) is not int or row["rev"] < 1:
                raise ValueError()
            identity = (source,row["id"])
            if identity in seen:
                raise ValueError()
            seen.add(identity)
            if not row["deleted"]:
                for key in FIELDS[source]:
                    value = row[key]
                    if key in ("amount","factor"):
                        if value is not None and (type(value) is not int or abs(value) > (1000000 if key == "amount" else 1000)):
                            raise ValueError()
                    elif value is not None and not isinstance(value,str):
                        raise ValueError()
                if source == "a" and (not isinstance(row["day"],str) or not row["day"]):
                    raise ValueError()
        return bid, generation, canonical(changes), sorted(changes,key=lambda r:(r["source"],r["id"]))
    except (ValueError,TypeError,KeyError):
        raise Error("BAD_INPUT") from None

class Store:
    def __init__(self, path=":memory:", *, connection=None):
        self.conn = connection if connection is not None else sqlite3.connect(path, timeout=5, isolation_level=None)
        self.conn.isolation_level = None
        self.conn.row_factory = sqlite3.Row
        self._serial = 0
        self.conn.create_function("token", -1, lambda *v: canonical(list(v)), deterministic=True)
        self.conn.executescript(SCHEMA)
        for table in "abc":
            self.conn.execute("CREATE TABLE IF NOT EXISTS " + table + SOURCE)
        for table, ddl in (("j1",J1),("j2",J2),("g",G)):
            self.conn.execute("CREATE TABLE IF NOT EXISTS " + table + ddl)
        for table,index,cols in (("a","k","k"),("b","k","k"),("c","route","route"),("j1","k","k"),("j1","route","route"),("j2","route","route"),("j2","group","day,region")):
            self.conn.execute("CREATE INDEX IF NOT EXISTS ix_"+table+"_"+index+" ON "+table+"("+cols+")")

    def _begin(self, write):
        if self.conn.in_transaction:
            self._serial += 1
            name = "wh_" + str(self._serial)
            self.conn.execute("SAVEPOINT " + name)
            return name
        self.conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        return None

    def _commit(self, frame):
        if frame is None:
            self.conn.commit()
        else:
            self.conn.execute("RELEASE " + frame)

    def _rollback(self, frame):
        if frame is None:
            self.conn.rollback()
        else:
            self.conn.execute("ROLLBACK TO " + frame)
            self.conn.execute("RELEASE " + frame)

    @contextmanager
    def transaction(self, write=True):
        frame = self._begin(write)
        try:
            yield
            self._commit(frame)
        except BaseException:
            self._rollback(frame)
            raise

    def close(self):
        self.conn.close()

    def _rows(self,sql,args=()):
        return [dict(row) for row in self.conn.execute(sql,args)]

    def apply(self,batch,fail_at=None):
        bid,expected,payload,changes = normalize(batch)
        c = self.conn
        frame = self._begin(True)
        def fault(point):
            if fail_at == point:
                raise Error("INJECTED:"+point)
        try:
            receipt = c.execute("SELECT payload,result FROM receipts WHERE id=?",(bid,)).fetchone()
            if receipt is not None:
                if receipt["payload"] != payload:
                    raise Error("BATCH_CONFLICT")
                result = json.loads(receipt["result"])
                self._commit(frame)
                return result
            generation = c.execute("SELECT n FROM meta").fetchone()[0]
            if generation != expected:
                raise Error("GENERATION_CONFLICT")
            keys, routes = set(), set()
            for row in changes:
                table = row["source"]
                old = c.execute("SELECT * FROM "+table+" WHERE id=?",(row["id"],)).fetchone()
                if old is not None:
                    if row["rev"] < old["rev"]:
                        continue
                    if row["rev"] == old["rev"]:
                        if old["body"] != canonical(row):
                            raise Error("REVISION_CONFLICT")
                        continue
                for candidate in (dict(old) if old is not None and old["live"] else None, row if not row["deleted"] else None):
                    if candidate is not None:
                        (routes if table == "c" else keys).add(candidate.get("route") if table == "c" else candidate.get("k"))
                values = [row["id"],row["rev"],canonical(row),int(not row["deleted"])]
                values += [row.get(k) for k in ("k","route","day","region","amount","factor")]
                c.execute("INSERT OR REPLACE INTO "+table+" VALUES(?,?,?,?,?,?,?,?,?,?)",values)
            fault("after_sources")
            key_order = sorted(keys,key=canonical)
            for key in key_order:
                routes.update(r["route"] for r in c.execute("SELECT route FROM j1 WHERE k IS ?",(key,)))
                c.execute("DELETE FROM j1 WHERE k IS ?",(key,))
            for key in key_order:
                c.execute("""
                  INSERT INTO j1
                  SELECT token(a.id,b.id),a.id,b.id,a.k,b.route,a.day,b.region,a.amount
                  FROM a LEFT JOIN b ON a.k=b.k AND b.live=1
                  WHERE a.live=1 AND a.k IS ?
                  UNION ALL
                  SELECT token(NULL,b.id),NULL,b.id,b.k,b.route,NULL,b.region,NULL
                  FROM b LEFT JOIN a ON a.k=b.k AND a.live=1
                  WHERE b.live=1 AND b.k IS ? AND a.id IS NULL
                """,(key,key))
                routes.update(r["route"] for r in c.execute("SELECT route FROM j1 WHERE k IS ?",(key,)))
            fault("after_first")
            before,after,groups = {},{},set()
            route_order = sorted(routes,key=canonical)
            for route in route_order:
                old_rows = self._rows("SELECT * FROM j2 WHERE route IS ?",(route,))
                before.update((r["token"],r) for r in old_rows)
                groups.update((r["day"],r["region"]) for r in old_rows)
                c.execute("DELETE FROM j2 WHERE route IS ?",(route,))
            for route in route_order:
                c.execute("""
                  INSERT INTO j2
                  SELECT token(j1.aid,j1.bid,c.id),j1.aid,j1.bid,c.id,j1.k,j1.route,j1.day,j1.region,j1.amount,c.factor,j1.amount*c.factor
                  FROM j1 LEFT JOIN c ON j1.route=c.route AND c.live=1
                  WHERE j1.route IS ?
                  UNION ALL
                  SELECT token(NULL,NULL,c.id),NULL,NULL,c.id,NULL,c.route,NULL,NULL,NULL,c.factor,NULL
                  FROM c LEFT JOIN j1 ON j1.route=c.route
                  WHERE c.live=1 AND c.route IS ? AND j1.token IS NULL
                """,(route,route))
                new_rows = self._rows("SELECT * FROM j2 WHERE route IS ?",(route,))
                after.update((r["token"],r) for r in new_rows)
                groups.update((r["day"],r["region"]) for r in new_rows)
            fault("after_second")
            group_order = sorted(groups,key=canonical)
            for day,region in group_order:
                c.execute("DELETE FROM g WHERE day IS ? AND region IS ?",(day,region))
                c.execute("""
                  INSERT INTO g SELECT token(day,region),day,region,COUNT(*),COUNT(charge),SUM(charge),COUNT(DISTINCT aid)
                  FROM j2 WHERE day IS ? AND region IS ?
                  GROUP BY day,region
                """,(day,region))
            fault("after_rollup")
            delta = []
            for sign,rows,other in ((-1,before,after),(1,after,before)):
                for token,row in rows.items():
                    if other.get(token) != row:
                        delta.append({"weight":sign,"row":row})
            delta.sort(key=lambda d:(canonical(d["row"]),d["weight"]))
            result = {"batch_id":bid,"generation":generation+1,"delta":delta,
                      "work":{"keys":key_order,"routes":route_order,"groups":[list(g) for g in group_order]}}
            c.execute("UPDATE meta SET n=?",(generation+1,))
            c.execute("INSERT INTO receipts VALUES(?,?,?)",(bid,payload,canonical(result)))
            fault("after_receipt")
            self._commit(frame)
            return result
        except BaseException:
            self._rollback(frame)
            raise

    def read(self):
        c = self.conn
        frame = self._begin(False)
        try:
            result = {"generation":c.execute("SELECT n FROM meta").fetchone()[0],
                      "rows":self._rows("SELECT * FROM j2 ORDER BY token"),
                      "groups":self._rows("SELECT * FROM g ORDER BY token")}
            self._commit(frame)
            return result
        except BaseException:
            self._rollback(frame)
            raise

def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",required=True)
    sub = parser.add_subparsers(dest="op",required=True)
    sub.add_parser("read")
    apply = sub.add_parser("apply")
    apply.add_argument("batch")
    args = parser.parse_args(argv)
    store = Store(args.db)
    try:
        if args.op == "apply":
            with open(args.batch,encoding="utf-8") as handle:
                try:
                    batch = json.load(handle,parse_constant=lambda s: (_ for _ in ()).throw(ValueError(s)))
                except ValueError:
                    raise Error("BAD_INPUT") from None
            result = store.apply(batch)
        else:
            result = store.read()
        print(canonical(result))
        return 0
    except Error as error:
        print(canonical({"error":str(error)}))
        return 2
    finally:
        store.close()

if __name__ == "__main__":
    sys.exit(main())
