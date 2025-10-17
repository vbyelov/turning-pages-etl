"""
Turning Pages – ETL
Script 3/5: step2_transform.py
Purpose:
  - Read the latest ./data/stage/<timestamp>/*.csv
  - Build normalized stage datasets ready for loading into DWH:
      * customer_stage (SCD2 candidate with HashDiff)
      * book_stage (Type 1)
      * paymentmethod_stage (Type 0)
      * fact_orderitem_stage (incl. DateKey=YYYYMMDD, OrderNumber, ShippingAddress)
"""

from __future__ import annotations
import os, sys, re, time, hashlib
from pathlib import Path
import pandas as pd


# ---------- helpers ----------
def latest_stage_dir(base: str = "data/stage") -> Path:
    p = Path(base)
    subs = [d for d in p.iterdir() if d.is_dir()] if p.exists() else []
    if not subs:
        raise FileNotFoundError(f"No stage folders under {base}")
    return sorted(subs)[-1]

def ensure_dir(path: str | Path) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)

def norm_text(s: pd.Series) -> pd.Series:
    def _clean(x):
        if pd.isna(x): return x
        x = str(x).strip()
        x = re.sub(r"\s+", " ", x)
        return x.lower()
    return s.map(_clean)

def to_int_yyyymmdd(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce")
    return (dt.dt.year * 10000 + dt.dt.month * 100 + dt.dt.day).astype("Int64")

def make_hashdiff(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    def _row_hash(row):
        vals = [str(row.get(c)) if pd.notna(row.get(c)) else "<NULL>" for c in cols]
        return hashlib.sha256("|".join(vals).encode("utf-8")).hexdigest()
    return df.apply(_row_hash, axis=1)


# ---------- main ----------
def main() -> int:
    stage_dir = latest_stage_dir()
    out_dir = Path("data/transform") / time.strftime("%Y%m%d_%H%M%S")
    ensure_dir(out_dir)

    print(f"[Transform] Stage dir : {stage_dir}")
    print(f"[Transform] Output dir: {out_dir}")

    # Load source CSVs
    cust = pd.read_csv(stage_dir / "Customer.csv")
    book = pd.read_csv(stage_dir / "Book.csv")
    paym = pd.read_csv(stage_dir / "PaymentMethod.csv")
    order = pd.read_csv(stage_dir / "Order.csv")
    item = pd.read_csv(stage_dir / "OrderItem.csv")

    # ---------- CUSTOMER ----------
    customer_stage = pd.DataFrame({
        "CustomerNK":  norm_text(cust["Email"].astype("string")),
        "DisplayName": cust["DisplayName"].astype("string"),
        "Phone":       cust["Phone"].astype("string")
    })
    customer_stage["HashDiff"] = make_hashdiff(customer_stage, ["DisplayName", "Phone"])

    # ---------- BOOK ----------
    book_stage = pd.DataFrame({
        "BookNK":    norm_text(book["ISBN"].astype("string")),
        "Title":     book["Title"].astype("string"),
        "Author":    book.get("Author", pd.Series([None]*len(book))),
        "ListPrice": book["ListPrice"]
    })

    # ---------- PAYMENT METHOD ----------
    payment_stage = pd.DataFrame({
        "PaymentMethodNK":   norm_text(paym["Code"].astype("string")),
        "PaymentMethodName": paym["DisplayName"].astype("string")
    })

    # ---------- FACT ----------
    # Merge Item + Book
    fact = item.merge(book[["BookID", "ISBN"]], on="BookID", how="left")

    # Merge with Order for CustomerID, PaymentMethodID, OrderDate, OrderNumber, ShippingAddress
    fact = fact.merge(
        order[["OrderID", "CustomerID", "PaymentMethodID", "OrderDate", "OrderNumber", "ShippingAddress"]],
        on="OrderID",
        how="left"
    )

    # Join Customer (Email) and PaymentMethod (Code)
    fact = fact.merge(cust[["CustomerID", "Email"]], on="CustomerID", how="left")
    fact = fact.merge(paym[["PaymentMethodID", "Code"]], on="PaymentMethodID", how="left")

    # Final shape
    fact_orderitem_stage = pd.DataFrame({
        "OrderID":          fact["OrderID"].astype("Int64"),
        "BookNK":           norm_text(fact["ISBN"].astype("string")),
        "CustomerNK":       norm_text(fact["Email"].astype("string")),
        "PaymentMethodNK":  norm_text(fact["Code"].astype("string")),
        "DateKey":          to_int_yyyymmdd(fact["OrderDate"]),
        "Quantity":         pd.to_numeric(fact["Quantity"], errors="coerce").astype("Float64"),
        "UnitPrice":        pd.to_numeric(fact["UnitPriceAtSale"], errors="coerce").astype("Float64"),
        "OrderNumber":      fact["OrderNumber"].astype("string"),
        "ShippingAddress":  fact["ShippingAddress"].astype("string")
    })
    fact_orderitem_stage["Revenue"] = (
        fact_orderitem_stage["Quantity"] * fact_orderitem_stage["UnitPrice"]
    ).astype("Float64")

    # ---------- write outputs ----------
    out_customer = out_dir / "customer_stage.csv"
    out_book     = out_dir / "book_stage.csv"
    out_payment  = out_dir / "paymentmethod_stage.csv"
    out_fact     = out_dir / "fact_orderitem_stage.csv"

    customer_stage.to_csv(out_customer, index=False, encoding="utf-8")
    book_stage.to_csv(out_book, index=False, encoding="utf-8")
    payment_stage.to_csv(out_payment, index=False, encoding="utf-8")
    fact_orderitem_stage.to_csv(out_fact, index=False, encoding="utf-8")

    print(f"[OK] customer_stage       → {out_customer}")
    print(f"[OK] book_stage           → {out_book}")
    print(f"[OK] paymentmethod_stage  → {out_payment}")
    print(f"[OK] fact_orderitem_stage → {out_fact}")
    print("[DONE] Transform complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
