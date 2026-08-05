#!/usr/bin/env python3
"""Reorganizes flat mock-generator output into the nested folder structure
needed for a one-shot S3 Console folder upload.

Why this exists: the S3 Console's Upload dialog has no field to type a
custom destination key path for loose files -- "Upload" always lands files
at whatever prefix you're currently browsing. The only way to recreate a
nested key path (raw/<source>/<yyyy>/<mm>/<dd>/<file>.csv) via the Console
without manually clicking "Create folder" at every level is to upload a
*local folder* using Upload > "Add folder" (or drag-and-drop) -- the
Console preserves that folder's name and everything nested under it as the
S3 key prefix. This script builds that local folder tree for you.

Usage:
    python3 scripts/stage_for_s3_upload.py --generated-dir data/mock/full --as-of-date 2026-08-04

Writes to:
    dist/raw_upload/<source>/<yyyy>/<mm>/<dd>/<source>.csv
for each of the 7 source files found in --generated-dir.

Then in the S3 Console:
  1. Open the raw bucket (retail-mdp-<env>-raw-<account-id>) -- stay at the
     bucket root, don't navigate into any prefix.
  2. Click Upload.
  3. Click "Add folder" (or drag-and-drop from Finder/Explorer) and select
     ALL 7 folders inside dist/raw_upload/ at once: product, store,
     promotion, orders, returns, web_sessions, inventory_snapshot.
  4. Click Upload.

That one action creates every raw/<source>/<yyyy>/<mm>/<dd>/<file>.csv key
in a single step -- no manual "Create folder" clicking needed.
"""
from __future__ import annotations

import argparse
import pathlib
import shutil
from datetime import datetime

SOURCES = [
    "product", "store", "promotion", "orders",
    "returns", "web_sessions", "inventory_snapshot",
]

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_STAGING_DIR = REPO_ROOT / "dist" / "raw_upload"


def stage(generated_dir: pathlib.Path, as_of_date: str, staging_dir: pathlib.Path) -> list[str]:
    d = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    yyyy, mm, dd = f"{d.year:04d}", f"{d.month:02d}", f"{d.day:02d}"

    staged, missing = [], []
    for source in SOURCES:
        src_file = generated_dir / f"{source}.csv"
        if not src_file.is_file():
            missing.append(source)
            continue
        dest_dir = staging_dir / source / yyyy / mm / dd
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_file = dest_dir / f"{source}.csv"
        shutil.copy2(src_file, dest_file)
        staged.append(f"{source}/{yyyy}/{mm}/{dd}/{source}.csv")

    if missing:
        print(f"WARNING: not found in {generated_dir}, skipped: {missing}")
    return staged


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--generated-dir", required=True, help="Output dir passed to generate_mock_data.py's --out-dir")
    p.add_argument("--as-of-date", required=True, help="Same date passed to generate_mock_data.py's --as-of-date (YYYY-MM-DD)")
    p.add_argument("--staging-dir", default=str(DEFAULT_STAGING_DIR), help=f"Default: {DEFAULT_STAGING_DIR}")
    args = p.parse_args()

    generated_dir = pathlib.Path(args.generated_dir).resolve()
    staging_dir = pathlib.Path(args.staging_dir).resolve()
    if not generated_dir.is_dir():
        raise SystemExit(f"--generated-dir {generated_dir} does not exist -- run generate_mock_data.py first")

    staged = stage(generated_dir, args.as_of_date, staging_dir)
    if not staged:
        raise SystemExit(f"No source CSVs found in {generated_dir} -- nothing staged")

    print(f"Staged {len(staged)} file(s) under {staging_dir}:")
    for key in staged:
        print(f"  {key}")

    top_level = sorted({key.split('/')[0] for key in staged})
    print("\nNext: in the S3 Console, open the raw bucket at its ROOT, click Upload,")
    print(f"then 'Add folder' (or drag-and-drop) and select these {len(top_level)} folders from")
    print(f"{staging_dir} all at once: {', '.join(top_level)}")


if __name__ == "__main__":
    main()
