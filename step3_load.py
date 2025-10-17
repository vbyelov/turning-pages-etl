# step3_load.py
from __future__ import annotations
import os, sys, argparse
from pathlib import Path
import pandas as pd
import pyodbc

# Reuse your connection helper
from config_and_connect import connect, DB_DWH  # expects a context manager returning a pyodbc connection

# ---------- config ----------
SCHEMA = os.getenv("TP_DWH_SCHEMA", "dwh")   # our DWH schema is 'dwh'
OPEN_END = None  # For SCD2 current rows ValidTo is NULL per DDL
TRANSFORM_DIR = "data/transform"  # step2 output root

# ---------- small helpers ----------
def latest_transform_dir(base: str = TRANSFORM_DIR) -> Path:
    p = Path(base)
    subs = [d for d in p.iterdir() if d.is_dir()] if p.exists() else []
    if not subs:
        raise FileNotFoundError(f"No transform folders under {base}")
    return sorted(subs)[-1]

def read_stage(tdir: Path) -> dict[str, pd.DataFrame]:
    files = {
        "customer": tdir / "customer_stage.csv",
        "book":     tdir / "book_stage.csv",
        "paym":     tdir / "paymentmethod_stage.csv",
        "fact":     tdir / "fact_orderitem_stage.csv",
    }
    for f in files.values():
        if not f.exists():
            raise FileNotFoundError(f"Missing file: {f}")
    return {k: pd.read_csv(v) for k, v in files.items()}

def q(name: str) -> str:
    return f"[{SCHEMA}].[{name}]"

def fetch_set(cur, sql: str) -> set:
    cur.execute(sql); return set(r[0] for r in cur.fetchall())

def fetch_map(cur, sql: str) -> dict:
    cur.execute(sql); return {r[0]: r[1] for r in cur.fetchall()}

def to_int(v):
    try:
        return int(v) if pd.notna(v) else None
    except Exception:
        return None

def to_dec(v):
    try:
        return float(v) if pd.notna(v) else None
    except Exception:
        return None

def to_str(v):
    if pd.isna(v): return None
    s = str(v).strip()
    return s if s != "" else None

def to_varbinary_from_hex(hex_str):
    if hex_str is None: return None
    try:
        h = str(hex_str).strip().lower()
        if h.startswith("0x"): h = h[2:]
        return bytes.fromhex(h) if h else None
    except Exception:
        return None

# ---------- stages ----------
def stage_prechecks():
    print("[PRE] Connecting and checking files …")
    with connect(DB_DWH) as cn:
        with cn.cursor() as cur:
            cur.execute("SELECT DB_NAME(), SUSER_SNAME();")
            db, user = cur.fetchone()
    tdir = latest_transform_dir()
    dfs = read_stage(tdir)
    print(f"[PRE] DWH={db} user={user} schema={SCHEMA}")
    print(f"[PRE] Transform dir: {tdir}")
    for k, df in dfs.items():
        print(f"[PRE] {k}: rows={len(df)}")

def stage_paymentmethod_insert_only():
    """
    SCD0: Insert Code/DisplayName into dwh.Dim_PaymentMethod if Code doesn't exist.
    No updates (Type 0 = fixed attributes).
    """
    print("[PM ] Loading Dim_PaymentMethod (Type 0, insert-only) …")
    tdir = latest_transform_dir()
    paym = read_stage(tdir)["paym"]

    # Column mapping from stage
    col_code = "PaymentMethodNK" if "PaymentMethodNK" in paym.columns else ("Code" if "Code" in paym.columns else None)
    col_name = "PaymentMethodName" if "PaymentMethodName" in paym.columns else ("DisplayName" if "DisplayName" in paym.columns else None)
    if not col_code or not col_name:
        raise KeyError("Stage paymentmethod CSV must have PaymentMethodNK/Code and PaymentMethodName/DisplayName columns.")

    with connect(DB_DWH) as cn:
        with cn.cursor() as cur:
            existing = fetch_set(cur, f"SELECT Code FROM {q('Dim_PaymentMethod')}")
            to_insert = 0
            for _, r in paym.iterrows():
                code = to_str(r[col_code])
                name = to_str(r[col_name])
                if not code or not name:
                    continue
                # normalize NK to lower() to avoid mismatches
                code_lower = code.lower()
                if code_lower in {c.lower() for c in existing}:
                    continue
                cur.execute(
                    f"INSERT INTO {q('Dim_PaymentMethod')} (Code, DisplayName) VALUES (?,?)",
                    code, name
                )
                existing.add(code)  # track new code
                to_insert += 1
    print(f"[PM ] read={len(paym)} inserted={to_insert} skipped={len(paym)-to_insert}")

def stage_book_upsert_type1():
    """
    SCD1: Upsert by ISBN into dwh.Dim_Book.
    Updates fields present in DWH (Title, Language, PublishYear, Pages, ListPrice) and sets UpdatedAt=SYSUTCDATETIME().
    """
    print("[BK ] Loading Dim_Book (Type 1 upsert) …")
    tdir = latest_transform_dir()
    book = read_stage(tdir)["book"]

    # Stage columns (flexible): expect at least BookNK/ISBN, Title, ListPrice; optional Language/PublishYear/Pages
    isbn_col = "BookNK" if "BookNK" in book.columns else ("ISBN" if "ISBN" in book.columns else None)
    if not isbn_col or "Title" not in book.columns:
        raise KeyError("Stage book CSV must have BookNK/ISBN and Title (optionally Language, PublishYear, Pages, ListPrice).")

    with connect(DB_DWH) as cn:
        with cn.cursor() as cur:
            updated = inserted = 0
            for _, r in book.iterrows():
                isbn = to_str(r[isbn_col])
                if not isbn:
                    continue
                title = to_str(r.get("Title"))
                language = to_str(r.get("Language"))
                publish_year = to_int(r.get("PublishYear"))
                pages = to_int(r.get("Pages"))
                list_price = to_dec(r.get("ListPrice"))

                # Try update
                cur.execute(
                    f"""
                    UPDATE {q('Dim_Book')}
                    SET Title = COALESCE(?, Title),
                        [Language] = ?,
                        PublishYear = ?,
                        Pages = ?,
                        ListPrice = COALESCE(?, ListPrice),
                        UpdatedAt = SYSUTCDATETIME()
                    WHERE ISBN = ?;
                    """,
                    title, language, publish_year, pages, list_price, isbn
                )
                if cur.rowcount and cur.rowcount > 0:
                    updated += 1
                else:
                    # Insert minimal required fields per DDL: ISBN, Title, ListPrice NOT NULL
                    if not title:
                        continue
                    if list_price is None:
                        list_price = 0.00
                    cur.execute(
                        f"""
                        INSERT INTO {q('Dim_Book')} (ISBN, Title, [Language], PublishYear, Pages, ListPrice, UpdatedAt)
                        VALUES (?,?,?,?,?,?,SYSUTCDATETIME());
                        """,
                        isbn, title, language, publish_year, pages, list_price
                    )
                    inserted += 1
    print(f"[BK ] read={len(book)} updated={updated} inserted={inserted} unchanged={len(book)-updated-inserted}")

def stage_customer_scd2():
    """
    SCD2 on dwh.Dim_Customer:
    - NK: CustomerNK_Email
    - CLOSE current version when HashDiff changed
    - INSERT new current version with ValidFrom=SYSUTCDATETIME(), ValidTo=NULL, IsCurrent=1
    """
    print("[CUS] Loading Dim_Customer (Type 2) …")
    tdir = latest_transform_dir()
    cust = read_stage(tdir)["customer"]

    # Stage mapping: need NK and attributes + HashDiff (hex)
    nk_col = "CustomerNK" if "CustomerNK" in cust.columns else ("CustomerNK_Email" if "CustomerNK_Email" in cust.columns else ("Email" if "Email" in cust.columns else None))
    if not nk_col:
        raise KeyError("Stage customer CSV must have CustomerNK (normalized email) or CustomerNK_Email/Email.")
    # Optional columns
    disp_col = "DisplayName" if "DisplayName" in cust.columns else None
    phone_col = "Phone" if "Phone" in cust.columns else None
    notes_col = "Notes" if "Notes" in cust.columns else None
    hash_col = "HashDiff" if "HashDiff" in cust.columns else None

    with connect(DB_DWH) as cn:
        with cn.cursor() as cur:
            inserted_new = scd2_changed = unchanged = closed_old = 0
            for _, r in cust.iterrows():
                nk = to_str(r[nk_col])
                if not nk:
                    continue
                nk_norm = nk.lower()
                disp = to_str(r[disp_col]) if disp_col else None
                phone = to_str(r[phone_col]) if phone_col else None
                notes = to_str(r[notes_col]) if notes_col else None
                hd = to_varbinary_from_hex(to_str(r[hash_col])) if hash_col else None

                # Read current version (IsCurrent=1)
                cur.execute(
                    f"""
                    SELECT TOP (1) CustomerSK, HashDiff
                    FROM {q('Dim_Customer')}
                    WHERE CustomerNK_Email = ? AND IsCurrent = 1
                    ORDER BY CustomerSK DESC;
                    """,
                    nk_norm
                )
                row = cur.fetchone()
                if row is None:
                    # First version
                    cur.execute(
                        f"""
                        INSERT INTO {q('Dim_Customer')}
                        (CustomerNK_Email, DisplayName, Phone, Notes, ValidFrom, ValidTo, IsCurrent, HashDiff)
                        VALUES (?, ?, ?, ?, SYSUTCDATETIME(), NULL, 1, ?);
                        """,
                        nk_norm, disp, phone, notes, hd
                    )
                    inserted_new += 1
                else:
                    current_sk, current_hash = row
                    # Compare varbinary by bytes
                    # Normalize types: pyodbc returns 'bytes' for varbinary
                    equal_hash = (current_hash == hd) if (hash_col is not None) else True
                    if not equal_hash:
                        # Close old
                        cur.execute(
                            f"UPDATE {q('Dim_Customer')} SET ValidTo = SYSUTCDATETIME(), IsCurrent = 0 WHERE CustomerSK = ?;",
                            current_sk
                        )
                        closed_old += 1
                        # Insert new
                        cur.execute(
                            f"""
                            INSERT INTO {q('Dim_Customer')}
                            (CustomerNK_Email, DisplayName, Phone, Notes, ValidFrom, ValidTo, IsCurrent, HashDiff)
                            VALUES (?, ?, ?, ?, SYSUTCDATETIME(), NULL, 1, ?);
                            """,
                            nk_norm, disp, phone, notes, hd
                        )
                        scd2_changed += 1
                    else:
                        unchanged += 1
    print(f"[CUS] read={len(cust)} inserted_new={inserted_new} scd2_changed={scd2_changed} "
          f"closed_old={closed_old} unchanged={unchanged}")

def stage_build_lookups() -> dict[str, dict]:
    print("[LKP] Building lookups …")
    with connect(DB_DWH) as cn:
        with cn.cursor() as cur:
            pm = fetch_map(cur, f"SELECT LOWER(Code), PaymentMethodSK FROM {q('Dim_PaymentMethod')}")
            bk = fetch_map(cur, f"SELECT LOWER(ISBN), BookSK FROM {q('Dim_Book')}")
            cu = fetch_map(cur, f"SELECT LOWER(CustomerNK_Email), CustomerSK FROM {q('Dim_Customer')} WHERE IsCurrent=1")
    print(f"[LKP] sizes: PM={len(pm)} Book={len(bk)} Cust={len(cu)}")
    return {"pm": pm, "bk": bk, "cu": cu}

def fetch_valid_dates_for(keys: set[int]) -> set[int]:
    if not keys:
        return set()
    placeholders = ",".join(str(int(k)) for k in keys if k)
    sql = f"SELECT DateSK FROM {q('Dim_Date')} WHERE DateSK IN ({placeholders})"
    with connect(DB_DWH) as cn:
        with cn.cursor() as cur:
            return fetch_set(cur, sql)

def stage_fact_reload():
    """
    Full reload of dwh.Fact_Sales:
    - TRUNCATE then INSERT
    - Required NOT NULL: BookSK, CustomerSK, PaymentMethodSK, DateSK, Quantity (>0), UnitPriceAtSale, OrderNumber, ShippingAddress
    - LineAmount is computed column in DWH
    """
    print("[FCT] Loading Fact_Sales (truncate+insert) …")
    tdir = latest_transform_dir()
    fact = read_stage(tdir)["fact"]

    # Stage mapping
    bk_col  = "BookNK" if "BookNK" in fact.columns else ("ISBN" if "ISBN" in fact.columns else None)
    cu_col  = "CustomerNK" if "CustomerNK" in fact.columns else ("CustomerNK_Email" if "CustomerNK_Email" in fact.columns else ("Email" if "Email" in fact.columns else None))
    pm_col  = "PaymentMethodNK" if "PaymentMethodNK" in fact.columns else ("Code" if "Code" in fact.columns else None)
    datecol = "DateKey" if "DateKey" in fact.columns else ("DateSK" if "DateSK" in fact.columns else None)
    qty_col = "Quantity" if "Quantity" in fact.columns else None
    price_c = "UnitPrice" if "UnitPrice" in fact.columns else ("UnitPriceAtSale" if "UnitPriceAtSale" in fact.columns else None)
    ord_col = "OrderNumber" if "OrderNumber" in fact.columns else None
    ship_c  = "ShippingAddress" if "ShippingAddress" in fact.columns else None

    for req, nm in [(bk_col,"BookNK/ISBN"), (cu_col,"CustomerNK/Email"), (pm_col,"PaymentMethodNK/Code"),
                    (datecol,"DateKey/DateSK"), (qty_col,"Quantity"), (price_c,"UnitPrice/UnitPriceAtSale"),
                    (ord_col,"OrderNumber"), (ship_c,"ShippingAddress")]:
        if req is None:
            raise KeyError(f"Fact stage is missing required column: {nm}")

    # Build lookups and valid dates
    lk = stage_build_lookups()
    distinct_dates = set(int(x) for x in pd.unique(fact[datecol].dropna()))
    valid_dates = fetch_valid_dates_for(distinct_dates)

    inserted = fk_missing = invalid_req = invalid_date = qty_zero = 0
    miss = {"pm":0, "bk":0, "cu":0}

    with connect(DB_DWH) as cn:
        with cn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {q('Fact_Sales')};")
            for _, r in fact.iterrows():
                isbn = to_str(r[bk_col]); cust = to_str(r[cu_col]); code = to_str(r[pm_col])
                dkey = to_int(r[datecol])
                qty  = to_dec(r[qty_col])
                price= to_dec(r[price_c])
                order= to_str(r[ord_col])
                ship = to_str(r[ship_c])

                # Required fields check
                if not all([isbn, cust, code, dkey, qty, price, order, ship]):
                    invalid_req += 1; continue
                if dkey not in valid_dates:
                    invalid_date += 1; continue
                if qty is None or qty <= 0:
                    qty_zero += 1; continue

                bk = lk["bk"].get(isbn.lower()); cu = lk["cu"].get(cust.lower()); pm = lk["pm"].get(code.lower())
                if not bk or not cu or not pm:
                    fk_missing += 1
                    if not pm: miss["pm"] += 1
                    if not bk: miss["bk"] += 1
                    if not cu: miss["cu"] += 1
                    continue

                cur.execute(
                    f"""
                    INSERT INTO {q('Fact_Sales')}
                    (BookSK, CustomerSK, PaymentMethodSK, DateSK, Quantity, UnitPriceAtSale, OrderNumber, ShippingAddress)
                    VALUES (?,?,?,?,?,?,?,?);
                    """,
                    int(bk), int(cu), int(pm), int(dkey), float(qty), float(price), order, ship
                )
                inserted += 1

    print(f"[FCT] read={len(fact)} inserted={inserted} "
          f"fk_missing={fk_missing} (pm={miss['pm']}, book={miss['bk']}, cust={miss['cu']}) "
          f"invalid_req={invalid_req} invalid_date={invalid_date} qty_leq0={qty_zero}")

def stage_checks():
    print("[CHK] Quick counts …")
    with connect(DB_DWH) as cn:
        with cn.cursor() as cur:
            for t in ("Dim_PaymentMethod", "Dim_Book", "Dim_Customer", "Dim_Date", "Fact_Sales"):
                cur.execute(f"SELECT COUNT(*) FROM {q(t)};")
                cnt = cur.fetchone()[0]
                print(f"[CHK] {t} = {cnt}")
            cur.execute(f"""
                SELECT TOP (5) DateSK, Quantity, UnitPriceAtSale, OrderNumber
                FROM {q('Fact_Sales')} ORDER BY SalesID DESC;
            """)
            rows = cur.fetchall()
            for r in rows:
                print(f"[CHK] Fact TOP -> DateSK={r[0]} Qty={r[1]} Price={r[2]} Order={r[3]}")

# ---------- CLI ----------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["prechecks","payment","book","customer","fact","checks"])
    args = ap.parse_args()

    if args.only == "prechecks": stage_prechecks(); return 0
    if args.only == "payment":   stage_paymentmethod_insert_only(); return 0
    if args.only == "book":      stage_book_upsert_type1(); return 0
    if args.only == "customer":  stage_customer_scd2(); return 0
    if args.only == "fact":      stage_fact_reload(); return 0
    if args.only == "checks":    stage_checks(); return 0

    # Full path
    stage_prechecks()
    stage_paymentmethod_insert_only()
    stage_book_upsert_type1()
    stage_customer_scd2()
    stage_fact_reload()
    stage_checks()
    print("[DONE] step3_load complete.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
