"""Refactoring contract checks. The old baseline is not expected to pass this file."""
import ast
import copy
from contextlib import contextmanager
import importlib
from pathlib import Path
import re
import sqlite3
import tempfile
import threading
from checks import enc, oracle, transition
from samples import INITIAL, CORRECTION

PURE = {"__future__","json","typing","dataclasses","collections","decimal","fractions",
        "functools","itertools","math","operator","copy","reconcile.ports"}
ALLOW = {
    "reconcile/domain.py": PURE,
    "reconcile/ports.py": {"__future__","typing","contextlib","dataclasses","collections"},
    "reconcile/application.py": {"__future__","typing","contextlib","reconcile.domain","reconcile.ports"},
    "reconcile/adapters/memory.py": {"__future__","json","copy","contextlib","typing","collections","reconcile.ports"},
    "reconcile/adapters/sqlite.py": {"__future__","sqlite3","json","copy","contextlib","typing","reconcile.ports"},
}
def structure():
    for filename,allowed in ALLOW.items():
        source = Path(filename).read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node,(ast.Import,ast.ImportFrom)):
                assert not isinstance(node,ast.ImportFrom) or not node.level,"use absolute imports"
                modules = [a.name for a in node.names] if isinstance(node,ast.Import) else [node.module]
                assert all(m in allowed for m in modules),(filename,modules)
            if isinstance(node,ast.Call) and isinstance(node.func,ast.Name):
                assert node.func.id not in {"open","eval","exec","__import__","compile"},filename
        if "/adapters/" in filename:
            assert not re.search(r"\b(JOIN|UNION|HAVING)\b|\bGROUP\s+BY\b|\b(COUNT|SUM|AVG|MIN|MAX)\s*\(",source,re.I),filename
    for filename in ("reconcile/__init__.py","reconcile/adapters/__init__.py"):
        if Path(filename).exists():
            assert not ast.parse(Path(filename).read_text(encoding="utf-8")).body,filename+" must be empty"
    facade = ast.parse(Path("warehouse.py").read_text(encoding="utf-8"))
    cls = next(n for n in facade.body if isinstance(n,ast.ClassDef) and n.name == "Store")
    methods = [n for n in cls.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))]
    assert {n.name for n in methods} <= {"__init__","apply","read","close","transaction","conn"}
    assert {"apply","read","close","transaction"} <= {n.name for n in methods}
    for node in methods:
        if node.name != "__init__":
            assert len(node.body) == 1 and isinstance(node.body[0],ast.Return),(node.name,"must delegate")

class Guard:
    """Only the published storage protocol; a runner cannot obtain a connection."""
    __slots__ = ("backend","allowed","reads","writing")
    def __init__(self,backend):
        self.backend,self.allowed,self.reads,self.writing = backend,set(),[],False
    @contextmanager
    def transaction(self,write=True):
        previous = self.writing
        self.writing = write or previous
        try:
            with self.backend.transaction(write):
                yield
        finally:
            self.writing = previous
    def generation(self):
        return self.backend.generation()
    def set_generation(self,n):
        return self.backend.set_generation(n)
    def receipt(self,bid):
        return self.backend.receipt(bid)
    def put_receipt(self,bid,payload,result):
        return self.backend.put_receipt(bid,payload,result)
    def select(self,table,index,value):
        query = (table,index,enc(value))
        assert query in self.allowed,("expanded neighborhood",query,self.allowed)
        self.reads.append(query)
        return self.backend.select(table,index,value)
    def upsert(self,table,rows):
        return self.backend.upsert(table,rows)
    def delete(self,table,ids):
        return self.backend.delete(table,ids)
    def export(self):
        assert not self.writing,"export is forbidden while applying"
        return self.backend.export()
    def close(self):
        return self.backend.close()
    def allow(self,batch,work):
        self.allowed = {(r["source"],"id",enc(r["id"])) for r in batch["changes"]}
        for key in work["keys"]:
            self.allowed.update((t,"k",enc(key)) for t in ("a","b","j1"))
        for route in work["routes"]:
            self.allowed.update((t,"route",enc(route)) for t in ("c","j1","j2"))
        self.allowed.update(("j2","group",enc(group)) for group in work["groups"])
        self.reads = []

def memory_contract():
    Engine = importlib.import_module("reconcile.application").Engine
    Memory = importlib.import_module("reconcile.adapters.memory").MemoryRepository
    import warehouse
    saved_connect,saved_apply = sqlite3.connect,warehouse.Store.apply
    def forbidden(*args,**kwargs):
        raise AssertionError("old runner or SQLite used for a memory application")
    sqlite3.connect,warehouse.Store.apply = forbidden,forbidden
    try:
        port = Guard(Memory())
        engine = Engine(port)
        state,generation = {t:{} for t in "abc"},0
        batches = [INITIAL,CORRECTION,
                   {"batch_id":"swap","expected_generation":2,"changes":[
                       {"source":"b","id":"b1","rev":3,"deleted":False,"k":"n","route":"r","region":None},
                       {"source":"b","id":"b2","rev":3,"deleted":False,"k":"m","route":"q","region":"west"},
                       {"source":"c","id":"c1","rev":2,"deleted":False,"route":"q","factor":0},
                       {"source":"c","id":"c3","rev":2,"deleted":False,"route":"r","factor":None}]}]
        receipts = []
        for batch in batches:
            new,rows,groups,delta,work = transition(state,batch)
            port.allow(batch,work)
            before = engine.read()
            for point in warehouse.FAULTS:
                try:
                    engine.apply(batch,fail_at=point)
                except warehouse.Error as error:
                    assert str(error) == "INJECTED:"+point
                else:
                    raise AssertionError("fault not raised")
                assert engine.read() == before
                with port.transaction(False):
                    assert port.receipt(batch["batch_id"]) is None
            result = engine.apply(batch)
            generation += 1
            assert result == {"batch_id":batch["batch_id"],"generation":generation,"delta":delta,"work":work}
            assert engine.read() == {"generation":generation,"rows":rows,"groups":groups}
            state = new
            receipts.append(result)
        port.allowed = set()
        assert engine.apply(dict(INITIAL,expected_generation=999)) == receipts[0]
        before = engine.read()
        try:
            with engine.transaction():
                empty = {"batch_id":"nested","expected_generation":generation,"changes":[]}
                engine.apply(empty)
                assert engine.read()["generation"] == generation+1
                raise RuntimeError("rollback")
        except RuntimeError:
            pass
        assert engine.read() == before
        engine.close()
    finally:
        sqlite3.connect,warehouse.Store.apply = saved_connect,saved_apply

def competing_writers():
    from warehouse import Store,Error
    with tempfile.TemporaryDirectory() as folder:
        path = str(Path(folder)/"warehouse.sqlite")
        Store(path).close()
        barrier = threading.Barrier(2)
        results,lock = [],threading.Lock()
        def worker(bid):
            store = Store(path)
            try:
                barrier.wait(timeout=10)
                try:
                    outcome = store.apply({"batch_id":bid,"expected_generation":0,"changes":[]})
                except Error as error:
                    outcome = str(error)
                with lock:
                    results.append(outcome)
            finally:
                store.close()
        threads = [threading.Thread(target=worker,args=(bid,)) for bid in ("one","two")]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)
        assert len(results) == 2 and sum(isinstance(r,dict) for r in results) == 1,results
        assert "GENERATION_CONFLICT" in results,results
        store = Store(path)
        assert store.read()["generation"] == 1
        store.close()

def run():
    structure()
    memory_contract()
    competing_writers()
    print("DEPENDENCY_BOUNDARIES_OK")

if __name__ == "__main__":
    run()
