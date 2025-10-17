# etl_main.py
from __future__ import annotations
import os
import sys
import argparse
import subprocess

# --- Defaults / env ---

os.environ.setdefault("TP_DWH_SCHEMA", "dwh")

# --- Helpers ---
def run_step(title: str, cmd: list[str]) -> None:
    print(f"\n=== {title} ===")
    print(f"$ {' '.join(cmd)}")
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")

    if r.stdout:
        print(r.stdout.strip())
    if r.returncode != 0:
        if r.stderr:
            print(r.stderr.strip(), file=sys.stderr)
        raise SystemExit(f"[FAIL] {title} (exit={r.returncode})")
    print(f"[OK] {title}")

def main() -> int:
    ap = argparse.ArgumentParser(description="Turning Pages — Minimal ETL Orchestrator")
    ap.add_argument("--step",
                    choices=["step1", "step2", "step3", "full"],
                    default="full",
                    help="Which part to run (default: full)")
    ap.add_argument("--only",
                    choices=["prechecks","payment","book","customer","fact","checks"],
                    help="Pass-through to step3_load.py for focused runs")
    args = ap.parse_args()

    if args.step in ("step1", "full"):
        run_step("STEP 1 — Extract",
                 [sys.executable, "step1_extract.py"])

    if args.step in ("step2", "full"):
        run_step("STEP 2 — Transform",
                 [sys.executable, "step2_transform.py"])

    if args.step in ("step3", "full"):
        cmd = [sys.executable, "step3_load.py"]
        if args.only:
            cmd.extend(["--only", args.only])
        run_step("STEP 3 — Load", cmd)

    print("\n[DONE] ETL pipeline finished.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
