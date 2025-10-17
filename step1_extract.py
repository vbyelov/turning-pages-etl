"""
Turning Pages – ETL
Script 2/5: step1_extract.py
Purpose:
  - Connect to the OLTP (TurningPages_Business)
  - Read core source tables (Customer, Book, PaymentMethod, Order, OrderItem)
  - Save raw extracts to ./data/stage/<timestamp>/*.csv for reproducibility

Notes:
  - We don't assume exact table names (singular/plural). We try common variants.
  - No filtering/joins yet — full dump; filtering happens in transform step.
  - Requires pandas (pip install pandas).
Usage:
  python step1_extract.py
"""

from __future__ import annotations
import os
import sys
import time
import pandas as pd
import pyodbc

from config_and_connect import connect, DB_SRC  # re-use our tested connection + db name

# --- candidates for table names (singular/plural) ---
TABLE_CANDIDATES = {
    "Customer":      ["Customer", "Customers"],
    "Book":          ["Book", "Books"],
    "PaymentMethod": ["PaymentMethod", "PaymentMethods"],
    "Order":         ["Order", "Orders"],
    "OrderItem":     ["OrderItem", "OrderItems", "Order_Detail", "Order_Details"],
}

def find_first_existing_table(cur: pyodbc.Cursor, candidates: list[str]) -> str | None:
    """
    Return the first table from candidates that exists in dbo schema (case-insensitive).
    """
    q = """
    SELECT LOWER(t.name) AS tname
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = 'dbo';
    """
    cur.execute(q)
    existing = {row.tname for row in cur.fetchall()}
    for cand in candidates:
        if cand and cand.lower() in existing:
            return cand
    return None

def read_table(conn: pyodbc.Connection, table_name: str) -> pd.DataFrame:
    """
    SELECT * from dbo.[table_name] into a DataFrame.
    """
    sql = f"SELECT * FROM dbo.[{table_name}];"
    return pd.read_sql(sql, conn)

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def main() -> int:
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join("data", "stage", ts)
    ensure_dir(out_dir)

    print(f"[Extract] Source DB   : {DB_SRC}")
    print(f"[Extract] Output dir  : {out_dir}")

    # connect once
    with connect(DB_SRC) as conn, conn.cursor() as cur:
        # resolve actual table names
        resolved: dict[str, str] = {}
        for logical, candidates in TABLE_CANDIDATES.items():
            found = find_first_existing_table(cur, candidates)
            if found is None:
                print(f"[WARN] {logical}: no matching table among {candidates}", file=sys.stderr)
            else:
                resolved[logical] = found

        if not {"Customer","Book","PaymentMethod","Order","OrderItem"}.issubset(resolved.keys()):
            missing = {"Customer","Book","PaymentMethod","Order","OrderItem"} - set(resolved.keys())
            print(f"[WARN] Missing required tables in source: {sorted(missing)}", file=sys.stderr)

        # extract each resolved table
        total_rows = 0
        for logical in ["Customer","Book","PaymentMethod","Order","OrderItem"]:
            phys = resolved.get(logical)
            if not phys:
                continue
            df = read_table(conn, phys)
            rows = len(df)
            total_rows += rows
            out_path = os.path.join(out_dir, f"{logical}.csv")
            df.to_csv(out_path, index=False, encoding="utf-8")
            print(f"[OK] {logical:<14} ← dbo.[{phys}] : {rows:>7} rows → {out_path}")

        if total_rows == 0:
            print("[FAIL] No rows extracted. Check source tables.", file=sys.stderr)
            return 2

    print(f"[DONE] Extract complete. Total rows: {total_rows}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
