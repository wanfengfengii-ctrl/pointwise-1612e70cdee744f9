"""SQLite adapter for the reconciliation storage port.

It owns the original table/index layout, nested savepoint transactions and
JSON (de)serialisation only.  All rows (including tokens) arrive precomputed
from the domain, so this module only issues single-table index lookups,
primary-key replacements and deletes: no set-oriented SQL operators,
aggregate functions, UDFs, triggers or views, and no domain rule of any kind.
"""
import json
import sqlite3
from contextlib import contextmanager

from reconcile.ports import Repository

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(n INTEGER NOT NULL);
INSERT INTO meta SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM meta);
CREATE TABLE IF NOT EXISTS receipts(id TEXT PRIMARY KEY,payload TEXT NOT NULL,result TEXT NOT NULL);
"""
_SOURCE = "(id TEXT PRIMARY KEY,rev INTEGER NOT NULL,body TEXT NOT NULL,live INTEGER NOT NULL," \
          "k TEXT,route TEXT,day TEXT,region TEXT,amount INTEGER,factor INTEGER)"
_J1 = "(token TEXT PRIMARY KEY,aid TEXT,bid TEXT,k TEXT,route TEXT,day TEXT,region TEXT,amount INTEGER)"
_J2 = "(token TEXT PRIMARY KEY,aid TEXT,bid TEXT,cid TEXT,k TEXT,route TEXT,day TEXT,region TEXT," \
      "amount INTEGER,factor INTEGER,charge INTEGER)"
_G = "(token TEXT PRIMARY KEY,day TEXT,region TEXT,n INTEGER,valued INTEGER,total INTEGER,samples INTEGER)"

_COLUMNS = {
    "a": ("id", "rev", "body", "live", "k", "route", "day", "region", "amount", "factor"),
    "b": ("id", "rev", "body", "live", "k", "route", "day", "region", "amount", "factor"),
    "c": ("id", "rev", "body", "live", "k", "route", "day", "region", "amount", "factor"),
    "j1": ("token", "aid", "bid", "k", "route", "day", "region", "amount"),
    "j2": ("token", "aid", "bid", "cid", "k", "route", "day", "region",
           "amount", "factor", "charge"),
    "g": ("token", "day", "region", "n", "valued", "total", "samples"),
}
_PRIMARY_KEY = {"a": "id", "b": "id", "c": "id", "j1": "token", "j2": "token", "g": "token"}
_INDEXES = {
    "a": {"id": ("id",), "k": ("k",)},
    "b": {"id": ("id",), "k": ("k",)},
    "c": {"id": ("id",), "route": ("route",)},
    "j1": {"k": ("k",), "route": ("route",)},
    "j2": {"route": ("route",), "group": ("day", "region")},
    "g": {"id": ("token",)},
}


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


class SQLiteRepository(Repository):
    def __init__(self, path=":memory:", *, connection=None):
        self.conn = connection if connection is not None else sqlite3.connect(
            path, timeout=5, isolation_level=None)
        self.conn.isolation_level = None
        self.conn.row_factory = sqlite3.Row
        self._serial = 0
        self.conn.executescript(_SCHEMA)
        for table in ("a", "b", "c"):
            self.conn.execute("CREATE TABLE IF NOT EXISTS " + table + _SOURCE)
        for table, ddl in (("j1", _J1), ("j2", _J2), ("g", _G)):
            self.conn.execute("CREATE TABLE IF NOT EXISTS " + table + ddl)
        for table, index, cols in (
                ("a", "k", "k"), ("b", "k", "k"), ("c", "route", "route"),
                ("j1", "k", "k"), ("j1", "route", "route"),
                ("j2", "route", "route"), ("j2", "group", "day,region")):
            self.conn.execute("CREATE INDEX IF NOT EXISTS ix_" + table + "_"
                              + index + " ON " + table + "(" + cols + ")")

    @contextmanager
    def transaction(self, write=True):
        if self.conn.in_transaction:
            self._serial += 1
            frame = "wh_" + str(self._serial)
            self.conn.execute("SAVEPOINT " + frame)
        else:
            self.conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            frame = None
        try:
            yield
            if frame is None:
                self.conn.commit()
            else:
                self.conn.execute("RELEASE " + frame)
        except BaseException:
            if frame is None:
                self.conn.rollback()
            else:
                self.conn.execute("ROLLBACK TO " + frame)
                self.conn.execute("RELEASE " + frame)
            raise

    def generation(self):
        return self.conn.execute("SELECT n FROM meta").fetchone()[0]

    def set_generation(self, n):
        self.conn.execute("UPDATE meta SET n=?", (n,))

    def receipt(self, batch_id):
        row = self.conn.execute(
            "SELECT payload,result FROM receipts WHERE id=?", (batch_id,)).fetchone()
        if row is None:
            return None
        return {"payload": row["payload"], "result": json.loads(row["result"])}

    def put_receipt(self, batch_id, payload, result):
        self.conn.execute(
            "INSERT OR REPLACE INTO receipts VALUES(?,?,?)",
            (batch_id, payload, _canonical(result)))

    def select(self, table, index, value):
        columns = _INDEXES[table][index]
        if len(columns) == 1:
            sql = "SELECT * FROM " + table + " WHERE " + columns[0] + " IS ?"
            args = (value,)
        else:
            sql = "SELECT * FROM " + table + " WHERE "
            for position, column in enumerate(columns):
                if position:
                    sql += " AND "
                sql += column + " IS ?"
            args = tuple(value)
        return [dict(row) for row in self.conn.execute(sql, args)]

    def upsert(self, table, rows):
        columns = _COLUMNS[table]
        placeholders = ""
        column_list = ""
        for position, column in enumerate(columns):
            if position:
                placeholders += ","
                column_list += ","
            placeholders += "?"
            column_list += column
        sql = "INSERT OR REPLACE INTO " + table + " (" + column_list + \
              ") VALUES(" + placeholders + ")"
        for row in rows:
            self.conn.execute(sql, tuple(row[column] for column in columns))

    def delete(self, table, ids):
        key = _PRIMARY_KEY[table]
        sql = "DELETE FROM " + table + " WHERE " + key + " IS ?"
        for identity in ids:
            self.conn.execute(sql, (identity,))

    def export(self):
        rows = [dict(row) for row in self.conn.execute("SELECT * FROM j2")]
        groups = [dict(row) for row in self.conn.execute("SELECT * FROM g")]
        return {"generation": self.generation(), "rows": rows, "groups": groups}

    def close(self):
        self.conn.close()
