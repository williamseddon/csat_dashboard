from __future__ import annotations

import gzip
import io
import json
import os
import sqlite3
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from .analytics import locale_to_region_label


_NON_VALUES = {"", "<NA>", "NA", "N/A", "NONE", "NAN", "NULL", "NOT MENTIONED", "-"}


_WORKSPACE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS saved_workspaces (
    workspace_id TEXT PRIMARY KEY,
    workspace_name TEXT NOT NULL,
    source_type TEXT,
    source_label TEXT,
    source_key TEXT,
    search_text TEXT,
    review_count INTEGER NOT NULL DEFAULT 0,
    avg_rating REAL,
    product_id TEXT,
    product_url TEXT,
    latest_review_date TEXT,
    symptomized INTEGER NOT NULL DEFAULT 0,
    symptomized_count INTEGER NOT NULL DEFAULT 0,
    symptomized_pct REAL NOT NULL DEFAULT 0,
    region_count INTEGER NOT NULL DEFAULT 0,
    symptom_engine TEXT,
    symptomized_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_loaded_at TEXT,
    metadata_json TEXT NOT NULL,
    dataset_blob BLOB NOT NULL,
    reviews_blob BLOB NOT NULL,
    state_blob BLOB
)
"""


_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_saved_workspaces_updated_at ON saved_workspaces(updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_saved_workspaces_workspace_name ON saved_workspaces(workspace_name)",
    "CREATE INDEX IF NOT EXISTS idx_saved_workspaces_source_type ON saved_workspaces(source_type)",
    "CREATE INDEX IF NOT EXISTS idx_saved_workspaces_symptomized ON saved_workspaces(symptomized)",
    "CREATE INDEX IF NOT EXISTS idx_saved_workspaces_source_key ON saved_workspaces(source_key)",
]


_ALTER_TABLE_SQL = {
    "avg_rating": "ALTER TABLE saved_workspaces ADD COLUMN avg_rating REAL",
    "symptomized_count": "ALTER TABLE saved_workspaces ADD COLUMN symptomized_count INTEGER NOT NULL DEFAULT 0",
    "symptomized_pct": "ALTER TABLE saved_workspaces ADD COLUMN symptomized_pct REAL NOT NULL DEFAULT 0",
    "region_count": "ALTER TABLE saved_workspaces ADD COLUMN region_count INTEGER NOT NULL DEFAULT 0",
}


class WorkspaceStoreError(Exception):
    pass


# Public helpers

def default_db_path() -> str:
    raw = os.environ.get("STARWALK_DB_PATH", "").strip()
    if raw:
        path = Path(raw).expanduser().resolve()
    else:
        root = Path(__file__).resolve().parents[1]
        path = (root / ".starwalk_data" / "starwalk_workspaces.sqlite3").resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def ensure_workspace_store(db_path: Optional[str] = None) -> str:
    path = str(Path(db_path or default_db_path()).expanduser().resolve())
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute(_WORKSPACE_TABLE_SQL)
        cols = {
            str(row[1]).strip().lower()
            for row in conn.execute("PRAGMA table_info(saved_workspaces)").fetchall()
        }
        needs_region_backfill = "region_count" not in cols
        for col_name, alter_sql in _ALTER_TABLE_SQL.items():
            if col_name.lower() not in cols:
                conn.execute(alter_sql)
        if needs_region_backfill:
            _backfill_region_counts(conn)
        _refresh_stale_workspace_metrics(conn)
        for sql in _INDEX_SQL:
            conn.execute(sql)
    return path


def count_workspace_records(db_path: Optional[str] = None) -> int:
    path = ensure_workspace_store(db_path)
    with sqlite3.connect(path) as conn:
        value = conn.execute("SELECT COUNT(*) FROM saved_workspaces").fetchone()[0]
    return int(value or 0)


def save_workspace_record(
    *,
    workspace_name: str,
    source_type: str,
    source_label: str,
    reviews_df: pd.DataFrame,
    dataset_payload: Dict[str, Any],
    state_payload: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    source_key: str = "",
    review_count: Optional[int] = None,
    avg_rating: Optional[float] = None,
    product_id: str = "",
    product_url: str = "",
    latest_review_date: str = "",
    symptomized: bool = False,
    symptomized_count: Optional[int] = None,
    symptomized_pct: Optional[float] = None,
    region_count: Optional[int] = None,
    symptom_engine: str = "",
    symptomized_at: str = "",
    workspace_id: Optional[str] = None,
    allow_source_upsert: bool = False,
    db_path: Optional[str] = None,
) -> str:
    path = ensure_workspace_store(db_path)
    now = _utcnow_iso()
    frame = reviews_df.copy() if isinstance(reviews_df, pd.DataFrame) else pd.DataFrame()
    safe_name = str(workspace_name or "Untitled workspace").strip() or "Untitled workspace"
    safe_source_type = str(source_type or "unknown").strip()
    safe_source_label = str(source_label or "").strip()
    safe_source_key = str(source_key or "").strip()
    safe_product_id = str(product_id or "").strip()
    safe_product_url = str(product_url or "").strip()
    safe_latest_review_date = str(latest_review_date or "").strip()
    safe_symptom_engine = str(symptom_engine or "").strip()
    safe_symptomized_at = str(symptomized_at or "").strip()
    safe_review_count = int(review_count if review_count is not None else len(frame))
    safe_avg_rating = _safe_float(avg_rating)
    if safe_avg_rating is None and "rating" in frame.columns:
        safe_avg_rating = _safe_float(pd.to_numeric(frame["rating"], errors="coerce").mean())
    inferred_symptom_count, inferred_symptom_pct = _infer_symptom_stats(frame, state_payload=state_payload)
    inferred_region_count, inferred_region_labels = _infer_region_stats(frame)
    safe_symptomized_count = int(symptomized_count) if symptomized_count is not None else inferred_symptom_count
    safe_symptomized_pct = float(symptomized_pct) if symptomized_pct is not None else inferred_symptom_pct
    safe_region_count = int(region_count) if region_count is not None else inferred_region_count
    metadata = dict(metadata or {})
    metadata.setdefault("region_count", safe_region_count)
    metadata.setdefault("region_labels", list(inferred_region_labels))
    dataset_payload = dict(dataset_payload or {})
    search_text_value = _build_search_text(
        workspace_name=safe_name,
        source_type=safe_source_type,
        source_label=safe_source_label,
        product_id=safe_product_id,
        product_url=safe_product_url,
        source_key=safe_source_key,
        latest_review_date=safe_latest_review_date,
        symptom_engine=safe_symptom_engine,
        review_count=safe_review_count,
        symptomized_count=safe_symptomized_count,
        region_count=safe_region_count,
    )
    metadata_json = json.dumps(metadata, ensure_ascii=False, sort_keys=True, default=_json_default)
    dataset_blob = sqlite3.Binary(_compress_json(dataset_payload) or b"")
    reviews_blob = sqlite3.Binary(_compress_df(frame))
    state_blob = sqlite3.Binary(_compress_json(state_payload) or b"") if state_payload is not None else None

    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        existing_row = None
        resolved_id = str(workspace_id or "").strip()
        if not resolved_id and allow_source_upsert and safe_source_key:
            existing_row = conn.execute(
                "SELECT workspace_id, created_at FROM saved_workspaces WHERE source_key = ? ORDER BY updated_at DESC LIMIT 1",
                (safe_source_key,),
            ).fetchone()
            if existing_row is not None:
                resolved_id = str(existing_row["workspace_id"])
        if resolved_id:
            existing_row = existing_row or conn.execute(
                "SELECT workspace_id, created_at FROM saved_workspaces WHERE workspace_id = ?",
                (resolved_id,),
            ).fetchone()
        if existing_row is None:
            resolved_id = resolved_id or uuid.uuid4().hex
            created_at = now
        else:
            created_at = str(existing_row["created_at"] or now)
        conn.execute(
            """
            INSERT INTO saved_workspaces (
                workspace_id,
                workspace_name,
                source_type,
                source_label,
                source_key,
                search_text,
                review_count,
                avg_rating,
                product_id,
                product_url,
                latest_review_date,
                symptomized,
                symptomized_count,
                symptomized_pct,
                region_count,
                symptom_engine,
                symptomized_at,
                created_at,
                updated_at,
                last_loaded_at,
                metadata_json,
                dataset_blob,
                reviews_blob,
                state_blob
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(workspace_id) DO UPDATE SET
                workspace_name = excluded.workspace_name,
                source_type = excluded.source_type,
                source_label = excluded.source_label,
                source_key = excluded.source_key,
                search_text = excluded.search_text,
                review_count = excluded.review_count,
                avg_rating = excluded.avg_rating,
                product_id = excluded.product_id,
                product_url = excluded.product_url,
                latest_review_date = excluded.latest_review_date,
                symptomized = excluded.symptomized,
                symptomized_count = excluded.symptomized_count,
                symptomized_pct = excluded.symptomized_pct,
                region_count = excluded.region_count,
                symptom_engine = excluded.symptom_engine,
                symptomized_at = excluded.symptomized_at,
                updated_at = excluded.updated_at,
                metadata_json = excluded.metadata_json,
                dataset_blob = excluded.dataset_blob,
                reviews_blob = excluded.reviews_blob,
                state_blob = excluded.state_blob
            """,
            (
                resolved_id,
                safe_name,
                safe_source_type,
                safe_source_label,
                safe_source_key,
                search_text_value,
                safe_review_count,
                safe_avg_rating,
                safe_product_id,
                safe_product_url,
                safe_latest_review_date,
                1 if bool(symptomized) else 0,
                safe_symptomized_count,
                safe_symptomized_pct,
                safe_region_count,
                safe_symptom_engine,
                safe_symptomized_at,
                created_at,
                now,
                None,
                metadata_json,
                dataset_blob,
                reviews_blob,
                state_blob,
            ),
        )
    return resolved_id


def list_workspace_records(
    *,
    search: str = "",
    symptom_filter: str = "All",
    source_filter: str = "All",
    sort_by: str = "Newest",
    limit: int = 200,
    db_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    path = ensure_workspace_store(db_path)
    where = ["1=1"]
    params: List[Any] = []
    search_text = str(search or "").strip().lower()
    if search_text:
        needle = f"%{search_text}%"
        where.append("(lower(search_text) LIKE ? OR lower(coalesce(metadata_json, '')) LIKE ?)")
        params.extend([needle, needle])
    symptom_mode = str(symptom_filter or "All").strip().lower()
    if symptom_mode in {"symptomized", "only symptomized"}:
        where.append("symptomized = 1")
    elif symptom_mode in {"needs symptomizer", "not symptomized", "without symptoms"}:
        where.append("symptomized = 0")
    safe_source_filter = str(source_filter or "All").strip()
    if safe_source_filter and safe_source_filter != "All":
        where.append("source_type = ?")
        params.append(safe_source_filter)
    safe_sort = str(sort_by or "Newest").strip()
    if safe_sort == "A–Z":
        order_sql = "workspace_name COLLATE NOCASE ASC, updated_at DESC"
    elif safe_sort == "Largest":
        order_sql = "review_count DESC, updated_at DESC"
    elif safe_sort == "Highest rating":
        order_sql = "coalesce(avg_rating, -1) DESC, review_count DESC, updated_at DESC"
    elif safe_sort == "Lowest rating":
        order_sql = "coalesce(avg_rating, 99) ASC, review_count DESC, updated_at DESC"
    elif safe_sort == "Most symptomized":
        order_sql = "coalesce(symptomized_count, 0) DESC, review_count DESC, updated_at DESC"
    elif safe_sort == "Oldest":
        order_sql = "updated_at ASC"
    else:
        order_sql = "updated_at DESC"
    params.append(int(max(1, limit)))
    sql = (
        "SELECT workspace_id, workspace_name, source_type, source_label, source_key, review_count, avg_rating, product_id, product_url, "
        "latest_review_date, symptomized, symptomized_count, symptomized_pct, region_count, symptom_engine, symptomized_at, created_at, updated_at, last_loaded_at, metadata_json "
        "FROM saved_workspaces WHERE "
        + " AND ".join(where)
        + f" ORDER BY {order_sql} LIMIT ?"
    )
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(row) for row in rows]


def load_workspace_record(workspace_id: str, db_path: Optional[str] = None) -> Dict[str, Any]:
    path = ensure_workspace_store(db_path)
    safe_id = str(workspace_id or "").strip()
    if not safe_id:
        raise WorkspaceStoreError("Workspace ID is required.")
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM saved_workspaces WHERE workspace_id = ?", (safe_id,)).fetchone()
    if row is None:
        raise WorkspaceStoreError(f"Saved workspace not found: {safe_id}")
    base = _row_to_dict(row)
    base["dataset_payload"] = _decompress_json(row["dataset_blob"]) or {}
    base["state_payload"] = _decompress_json(row["state_blob"]) or {}
    base["reviews_df"] = _decompress_df(row["reviews_blob"])
    return base


def delete_workspace_record(workspace_id: str, db_path: Optional[str] = None) -> None:
    path = ensure_workspace_store(db_path)
    safe_id = str(workspace_id or "").strip()
    if not safe_id:
        return
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM saved_workspaces WHERE workspace_id = ?", (safe_id,))


def rename_workspace_record(workspace_id: str, workspace_name: str, db_path: Optional[str] = None) -> None:
    path = ensure_workspace_store(db_path)
    safe_id = str(workspace_id or "").strip()
    safe_name = str(workspace_name or "").strip()
    if not safe_id:
        raise WorkspaceStoreError("Workspace ID is required.")
    if not safe_name:
        raise WorkspaceStoreError("Workspace name cannot be blank.")
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT source_type, source_label, source_key, product_id, product_url, latest_review_date, symptom_engine, review_count, symptomized_count, region_count FROM saved_workspaces WHERE workspace_id = ?",
            (safe_id,),
        ).fetchone()
        if row is None:
            raise WorkspaceStoreError(f"Saved workspace not found: {safe_id}")
        search_text_value = _build_search_text(
            workspace_name=safe_name,
            source_type=row["source_type"],
            source_label=row["source_label"],
            product_id=row["product_id"],
            product_url=row["product_url"],
            source_key=row["source_key"],
            latest_review_date=row["latest_review_date"],
            symptom_engine=row["symptom_engine"],
            review_count=row["review_count"],
            symptomized_count=row["symptomized_count"],
            region_count=row["region_count"],
        )
        conn.execute(
            "UPDATE saved_workspaces SET workspace_name = ?, search_text = ?, updated_at = ? WHERE workspace_id = ?",
            (safe_name, search_text_value, _utcnow_iso(), safe_id),
        )


def touch_workspace_loaded(workspace_id: str, db_path: Optional[str] = None) -> None:
    path = ensure_workspace_store(db_path)
    safe_id = str(workspace_id or "").strip()
    if not safe_id:
        return
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE saved_workspaces SET last_loaded_at = ?, updated_at = updated_at WHERE workspace_id = ?",
            (_utcnow_iso(), safe_id),
        )


# Internal helpers

def _utcnow_iso() -> str:
    return pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


def _build_search_text(
    *,
    workspace_name: Any,
    source_type: Any,
    source_label: Any,
    product_id: Any,
    product_url: Any,
    source_key: Any,
    latest_review_date: Any,
    symptom_engine: Any,
    review_count: Any,
    symptomized_count: Any,
    region_count: Any,
) -> str:
    search_bits = [
        str(workspace_name or "").strip(),
        str(source_type or "").strip(),
        str(source_label or "").strip(),
        str(product_id or "").strip(),
        str(product_url or "").strip(),
        str(source_key or "").strip(),
        str(latest_review_date or "").strip(),
        str(symptom_engine or "").strip(),
        str(review_count or "").strip(),
        str(symptomized_count or "").strip(),
        str(region_count or "").strip(),
    ]
    return " | ".join(bit for bit in search_bits if bit)


def _json_default(value: Any):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, (set, tuple)):
        return list(value)
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def _compress_json(payload: Optional[Dict[str, Any]]) -> Optional[bytes]:
    if payload is None:
        return None
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=_json_default).encode("utf-8")
    return gzip.compress(raw)


def _decompress_json(blob: Optional[bytes]) -> Optional[Dict[str, Any]]:
    if not blob:
        return None
    try:
        text = gzip.decompress(blob).decode("utf-8")
    except Exception as exc:
        raise WorkspaceStoreError(f"Could not decode stored workspace JSON payload: {exc}") from exc
    if not text.strip():
        return None
    data = json.loads(text)
    return data if isinstance(data, dict) else {"value": data}


def _compress_df(df: pd.DataFrame) -> bytes:
    payload = df.to_json(
        orient="split",
        index=True,
        date_format="iso",
        date_unit="us",
        default_handler=str,
    )
    return gzip.compress(payload.encode("utf-8"))


def _decompress_df(blob: bytes) -> pd.DataFrame:
    if not blob:
        return pd.DataFrame()
    try:
        text = gzip.decompress(blob).decode("utf-8")
    except Exception as exc:
        raise WorkspaceStoreError(f"Could not decode stored workspace dataframe payload: {exc}") from exc
    if not text.strip():
        return pd.DataFrame()
    return pd.read_json(io.StringIO(text), orient="split")


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    out = dict(row)
    out["symptomized"] = bool(out.get("symptomized", 0))
    if out.get("avg_rating") is not None:
        out["avg_rating"] = _safe_float(out.get("avg_rating"))
    out["symptomized_count"] = int(out.get("symptomized_count") or 0)
    out["symptomized_pct"] = float(out.get("symptomized_pct") or 0.0)
    out["region_count"] = int(out.get("region_count") or 0)
    metadata_raw = out.pop("metadata_json", None)
    out["metadata"] = json.loads(metadata_raw) if metadata_raw else {}
    return out


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return None
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def _infer_symptom_stats_from_state(state_payload: Optional[Dict[str, Any]]) -> int:
    if not isinstance(state_payload, dict):
        return 0
    tagged_ids = set()
    for raw in state_payload.get("sym_processed_rows") or []:
        if not isinstance(raw, dict):
            continue
        dets = [str(value).strip() for value in (raw.get("wrote_dets") or []) if str(value).strip()]
        dels = [str(value).strip() for value in (raw.get("wrote_dels") or []) if str(value).strip()]
        if not dets and not dels:
            continue
        idx = raw.get("idx")
        key = str(idx).strip() if idx is not None else ""
        tagged_ids.add(key or f"row_{len(tagged_ids)}")
    return len(tagged_ids)


def _infer_symptom_stats(frame: pd.DataFrame, state_payload: Optional[Dict[str, Any]] = None) -> tuple[int, float]:
    if frame is None or frame.empty:
        state_count = _infer_symptom_stats_from_state(state_payload)
        return state_count, 0.0
    cols = [
        col for col in frame.columns
        if str(col).strip().lower().startswith("ai symptom detractor")
        or str(col).strip().lower().startswith("ai symptom delighter")
        or str(col).strip().lower() in {f"symptom {idx}" for idx in range(1, 21)}
    ]
    mask = pd.Series([False] * len(frame), index=frame.index, dtype=bool)
    for col in cols:
        series = frame[col].astype("string").fillna("").str.strip()
        valid = (series != "") & (~series.str.upper().isin(_NON_VALUES)) & (~series.str.startswith("<"))
        mask |= valid
    count = int(mask.sum()) if not mask.empty else 0
    state_count = _infer_symptom_stats_from_state(state_payload)
    if state_count > count:
        count = state_count
    pct = float(count / len(frame)) if len(frame) else 0.0
    return count, pct


def _infer_region_stats(frame: pd.DataFrame) -> tuple[int, List[str]]:
    if frame is None or frame.empty:
        return 0, []
    candidate_columns = [
        col for col in frame.columns
        if str(col).strip().lower() in {"content_locale", "locale", "country", "location"}
    ]
    if not candidate_columns:
        return 0, []
    labels: List[str] = []
    seen = set()
    for col in candidate_columns:
        try:
            series = frame[col].astype("string").fillna("").str.strip()
        except Exception:
            continue
        for raw in series.tolist():
            if not raw:
                continue
            region = str(locale_to_region_label(raw) or "").strip()
            if not region or region.upper() in {"UNKNOWN", "NAN"}:
                continue
            if region not in seen:
                seen.add(region)
                labels.append(region)
    labels = sorted(labels)
    return len(labels), labels


def _refresh_stale_workspace_metrics(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT workspace_id, workspace_name, source_type, source_label, source_key, product_id, product_url, latest_review_date,
               symptomized, symptomized_count, symptomized_pct, region_count, symptom_engine, review_count, avg_rating,
               metadata_json, reviews_blob, state_blob
        FROM saved_workspaces
        WHERE coalesce(symptomized_count, 0) = 0
           OR coalesce(region_count, 0) = 0
           OR coalesce(review_count, 0) = 0
           OR avg_rating IS NULL
        """
    ).fetchall()
    for row in rows:
        try:
            frame = _decompress_df(row["reviews_blob"]) if row["reviews_blob"] else pd.DataFrame()
            state_payload = _decompress_json(row["state_blob"]) if row["state_blob"] else None
            review_count = int(row["review_count"] or 0) or len(frame)
            avg_rating = _safe_float(row["avg_rating"])
            if avg_rating is None and not frame.empty and "rating" in frame.columns:
                avg_rating = _safe_float(pd.to_numeric(frame["rating"], errors="coerce").mean())
            symptomized_count, symptomized_pct = _infer_symptom_stats(frame, state_payload=state_payload)
            region_count, region_labels = _infer_region_stats(frame)
            metadata = json.loads(row["metadata_json"]) if row["metadata_json"] else {}
            if not isinstance(metadata, dict):
                metadata = {}
            metadata["region_count"] = int(region_count or 0)
            metadata["region_labels"] = list(region_labels)
            symptomized_flag = 1 if bool(row["symptomized"] or symptomized_count > 0) else 0
            search_text_value = _build_search_text(
                workspace_name=row["workspace_name"],
                source_type=row["source_type"],
                source_label=row["source_label"],
                product_id=row["product_id"],
                product_url=row["product_url"],
                source_key=row["source_key"],
                latest_review_date=row["latest_review_date"],
                symptom_engine=row["symptom_engine"],
                review_count=review_count,
                symptomized_count=symptomized_count,
                region_count=region_count,
            )
            conn.execute(
                """
                UPDATE saved_workspaces
                SET review_count = ?,
                    avg_rating = ?,
                    symptomized = ?,
                    symptomized_count = ?,
                    symptomized_pct = ?,
                    region_count = ?,
                    metadata_json = ?,
                    search_text = ?
                WHERE workspace_id = ?
                """,
                (
                    int(review_count or 0),
                    avg_rating,
                    symptomized_flag,
                    int(symptomized_count or 0),
                    float(symptomized_pct or 0.0),
                    int(region_count or 0),
                    json.dumps(metadata, ensure_ascii=False, sort_keys=True, default=_json_default),
                    search_text_value,
                    str(row["workspace_id"]),
                ),
            )
        except Exception:
            continue


def _backfill_region_counts(conn: sqlite3.Connection) -> None:
    rows = conn.execute("SELECT workspace_id, reviews_blob, metadata_json FROM saved_workspaces").fetchall()
    for workspace_id, reviews_blob, metadata_json in rows:
        try:
            frame = _decompress_df(reviews_blob) if reviews_blob else pd.DataFrame()
            region_count, region_labels = _infer_region_stats(frame)
            meta = json.loads(metadata_json) if metadata_json else {}
            if not isinstance(meta, dict):
                meta = {}
            meta.setdefault("region_count", region_count)
            meta.setdefault("region_labels", list(region_labels))
            conn.execute(
                "UPDATE saved_workspaces SET region_count = ?, metadata_json = ? WHERE workspace_id = ?",
                (int(region_count or 0), json.dumps(meta, ensure_ascii=False, sort_keys=True, default=_json_default), str(workspace_id)),
            )
        except Exception:
            continue
