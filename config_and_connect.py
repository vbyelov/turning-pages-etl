"""
Turning Pages – ETL
Script 1/5: config_and_connect.py
Purpose: Centralize config, read .env (optional), build pyodbc connection strings for
         Windows Authentication, and run a quick connectivity self-test.

Usage:
    python config_and_connect.py
Files/Env:
    Optional .env keys:
      TP_SQL_SERVER=localhost
      TP_SQL_DRIVER=ODBC Driver 18 for SQL Server
      TP_DB_SRC=TurningPages_Business
      TP_DB_DWH=TurningPages_DWH
Notes:
    - Windows Authentication (Trusted_Connection=yes)
    - Requires: pyodbc (pip install pyodbc). python-dotenv is optional.
"""

from __future__ import annotations
import os
import sys
import pyodbc

# --- Load .env dynamically if available (optional dependency) ---
def _try_load_dotenv() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        # Silent: .env loading is optional
        pass

_try_load_dotenv()

# --- Driver resolution ---
COMMON_DRIVERS = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "ODBC Driver 13 for SQL Server",
    "SQL Server"
]

def pick_driver(preferred: str | None = None) -> str:
    """
    Return an installed driver name. If `preferred` is provided and installed, use it.
    Otherwise pick the first match from COMMON_DRIVERS. Raise if none found.
    """
    installed = [d for d in pyodbc.drivers()]
    if preferred and preferred in installed:
        return preferred
    for d in COMMON_DRIVERS:
        if d in installed:
            return d
    raise RuntimeError(
        f"No suitable SQL Server ODBC driver found. Installed: {installed}"
    )

# --- Config (env with sane defaults) ---
SQL_SERVER = os.getenv("TP_SQL_SERVER", "localhost")
SQL_DRIVER = pick_driver(os.getenv("TP_SQL_DRIVER"))
DB_SRC    = os.getenv("TP_DB_SRC", "TurningPages_Business")   # from masterskript_bd.sql
DB_DWH    = os.getenv("TP_DB_DWH", "TurningPages_DWH")        # from DWH_CREATE.sql

def build_conn_str(database: str) -> str:
    """
    Build a Windows Authentication connection string for the given database.
    """
    return (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
        f"Encrypt=no;"
    ).replace("SQL_DRIVER", SQL_DRIVER)

def connect(database: str) -> pyodbc.Connection:
    """
    Connect to the specified database using pyodbc and return an open connection.
    """
    return pyodbc.connect(build_conn_str(database), autocommit=True)

def quick_ping(cnx: pyodbc.Connection) -> dict:
    """
    Simple sanity query: return DB_NAME, Login (SUSER_SNAME), and @@VERSION (first line).
    """
    with cnx.cursor() as cur:
        cur.execute("SELECT DB_NAME() AS db, SUSER_SNAME() AS login;")
        row = cur.fetchone()
        cur.execute("SELECT CAST(@@VERSION AS NVARCHAR(4000));")
        ver = cur.fetchone()[0]
    first_line = ver.splitlines()[0] if ver else ""
    return {"db": row.db, "login": row.login, "version": first_line}

def self_test() -> int:
    """
    Try connecting to both Source and DWH; print status and return exit code.
    """
    print("[SelfTest] Using driver:", SQL_DRIVER)
    print("[SelfTest] Server      :", SQL_SERVER)
    # Source
    try:
        with connect(DB_SRC) as c1:
            info1 = quick_ping(c1)
        print(f"[OK] Source: {info1['db']} | User: {info1['login']} | {info1['version']}")
    except Exception as ex:
        print(f"[FAIL] Source ({DB_SRC}) → {ex}", file=sys.stderr)
        return 1
    # DWH
    try:
        with connect(DB_DWH) as c2:
            info2 = quick_ping(c2)
        print(f"[OK] DWH   : {info2['db']} | User: {info2['login']} | {info2['version']}")
    except Exception as ex:
        print(f"[FAIL] DWH ({DB_DWH}) → {ex}", file=sys.stderr)
        return 2
    return 0

if __name__ == "__main__":
    sys.exit(self_test())
