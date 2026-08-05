#!/usr/bin/env python3
"""Builds dist/common.zip from jobs/common/ for Glue's --extra-py-files.

Glue's --extra-py-files argument accepts individual .py files or a .zip
archive -- it does NOT accept a bare S3 "folder" of loose .py files. This
script zips jobs/common/ with `common/` itself as the zip root (i.e. the
archive contains common/__init__.py, common/iceberg_utils.py, ...), which is
exactly the layout `from common import iceberg_utils` (used by every job
script) expects once Glue extracts it onto the job's sys.path.

Usage:
    python3 scripts/build_common_zip.py
    # -> writes dist/common.zip

Then upload dist/common.zip to s3://<scripts-bucket>/common/common.zip via
the S3 Console (see DEPLOYMENT.md). Re-run this script and re-upload any
time jobs/common/*.py changes -- Glue does not auto-detect changes to a zip
already sitting in S3.
"""
from __future__ import annotations

import pathlib
import zipfile

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
COMMON_DIR = REPO_ROOT / "jobs" / "common"
OUTPUT_DIR = REPO_ROOT / "dist"
OUTPUT_PATH = OUTPUT_DIR / "common.zip"

EXCLUDE_SUFFIXES = {".pyc"}
EXCLUDE_DIR_NAMES = {"__pycache__"}


def iter_source_files():
    for path in sorted(COMMON_DIR.rglob("*")):
        if path.is_dir():
            continue
        if path.suffix in EXCLUDE_SUFFIXES:
            continue
        if any(part in EXCLUDE_DIR_NAMES for part in path.parts):
            continue
        yield path


def main() -> None:
    if not COMMON_DIR.is_dir():
        raise SystemExit(f"Expected {COMMON_DIR} to exist -- run this from the repo root context.")

    OUTPUT_DIR.mkdir(exist_ok=True)
    files = list(iter_source_files())
    if not files:
        raise SystemExit(f"No source files found under {COMMON_DIR} -- nothing to package.")

    with zipfile.ZipFile(OUTPUT_PATH, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in files:
            arcname = pathlib.Path("common") / path.relative_to(COMMON_DIR)
            zf.write(path, arcname=str(arcname))

    print(f"Wrote {OUTPUT_PATH} ({len(files)} files):")
    for path in files:
        print(f"  common/{path.relative_to(COMMON_DIR)}")
    print(f"\nNext: upload {OUTPUT_PATH} to s3://<scripts-bucket>/common/common.zip (see DEPLOYMENT.md).")


if __name__ == "__main__":
    main()
