"""
Cleanup valuations safely.

Default behavior (safe):
- Only remove weekend rows (Sat/Sun) that are exact duplicates of the most
  recent prior row on metrics. If weekend metrics differ, keep them.
- Weekday rows are always kept.

Optional strict mode (set CLEANUP_STRICT=1):
- Drop any valuation row whose Date is not present in the corresponding price
  CSV Date column. Use with caution — valuations are not backfillable.

Summary CSV is written with KST timestamp.
"""

from __future__ import annotations

from pathlib import Path
import os
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parent
VAL_DIR = BASE_DIR / "data" / "valuations"
PRICE_DIR = BASE_DIR / "data" / "daily"
SUMMARY_PATH = BASE_DIR / "data" / "cleanup_valuations_summary.csv"


def _read_dates(path: Path) -> pd.Series | None:
    try:
        df = pd.read_csv(path)
        if "Date" not in df.columns or df.empty:
            return None
        return pd.to_datetime(df["Date"], errors="coerce", utc=True).dt.tz_localize(None).dt.date.astype(str)
    except Exception:
        return None


def cleanup_valuations() -> pd.DataFrame:
    VAL_DIR.mkdir(parents=True, exist_ok=True)
    PRICE_DIR.mkdir(parents=True, exist_ok=True)
    rows: list[dict] = []
    run_at = datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    strict = os.environ.get("CLEANUP_STRICT", "0").lower() in {"1","true","yes","on"}
    for vf in sorted(VAL_DIR.glob("*.csv")):
        symbol_file = vf.name
        pf = PRICE_DIR / symbol_file  # same basename mapping
        status = "ok"
        reason = None
        removed = 0
        before = 0
        after = 0
        changed = False
        vdf = None
        try:
            vdf = pd.read_csv(vf)
        except Exception:
            status = "error"
            reason = "read_valuation_failed"
        if vdf is not None and not vdf.empty and "Date" in vdf.columns:
            before = len(vdf)
            vdf["Date"] = pd.to_datetime(vdf["Date"], errors="coerce", utc=True).dt.tz_localize(None).dt.date.astype(str)
            # Weekday helper
            wd = pd.to_datetime(vdf["Date"]).dt.weekday  # 0=Mon ... 6=Sun
            if strict:
                price_dates = _read_dates(pf) if pf.exists() else pd.Series(dtype=str)
                keep_mask = vdf["Date"].isin(set(price_dates or []))
                reason = "strict_match_price_dates"
            else:
                # Safe mode: drop only weekend rows that are duplicates of previous metrics
                meta_cols = {"Date","symbol","currency","quoteType"}
                comp_cols = [c for c in vdf.columns if c not in meta_cols]
                # Forward fill to compare consecutive duplicates robustly
                cmp_df = vdf[comp_cols].copy()
                # build previous row values for comparison
                prev_cmp = cmp_df.shift(1)
                # equal flags across all comp cols
                eq = (cmp_df == prev_cmp) | (cmp_df.isna() & prev_cmp.isna())
                eq_all = eq.all(axis=1)
                is_weekend = wd >= 5
                keep_mask = ~is_weekend | ~eq_all
                reason = "dropped_weekend_duplicates_only"
            cleaned = vdf[keep_mask].copy()
            cleaned = cleaned.drop_duplicates(subset=["Date"], keep="last").sort_values("Date")
            after = len(cleaned)
            removed = before - after
            if removed > 0 or after != before:
                tmp = vf.with_suffix(".csv.tmp")
                cleaned.to_csv(tmp, index=False)
                tmp.replace(vf)
                changed = True
        rows.append({
            "file": str(vf),
            "symbol_file": symbol_file,
            "price_file": str(pf),
            "rows_before": before,
            "rows_after": after,
            "removed": removed,
            "changed": changed,
            "status": status,
            "reason": reason,
            "run_at": run_at,
        })
    df = pd.DataFrame(rows)
    df.to_csv(SUMMARY_PATH, index=False)
    return df


if __name__ == "__main__":
    out = cleanup_valuations()
    print("Cleanup complete. Summary:")
    try:
        print(out.head())
    except Exception:
        pass
