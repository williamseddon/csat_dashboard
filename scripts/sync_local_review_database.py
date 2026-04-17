from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from review_analyst.local_database import sync_local_review_database


def _progress_callback(**kwargs):
    progress = float(kwargs.get("progress") or 0.0)
    title = str(kwargs.get("title") or "")
    detail = str(kwargs.get("detail") or "")
    pct = f"{progress * 100:.0f}%"
    print(f"[{pct}] {title} - {detail}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync the local central review database from the incoming review and SKU mapping folders.")
    parser.add_argument("--root", default="", help="Optional local database root. Defaults to the app's standard local database folder.")
    parser.add_argument("--force", action="store_true", help="Rebuild even when the current database already matches the latest input files.")
    parser.add_argument("--chunk-size", type=int, default=50000, help="Review rows per import chunk. Default: 50000")
    parser.add_argument("--export-snapshot", action="store_true", help="Also export the Excel snapshot workbook after sync completes.")
    args = parser.parse_args()

    result = sync_local_review_database(
        root=args.root or None,
        force=bool(args.force),
        chunk_size=max(int(args.chunk_size or 50000), 1000),
        progress_callback=_progress_callback,
        export_snapshot=bool(args.export_snapshot),
    )
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
