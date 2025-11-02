"""
Automation helpers for running data update pipelines.

Usage
-----
    python -m global_universe.automation --mode full
    python -m global_universe.automation --mode smoke --symbols 5
    python -m global_universe.automation --mode check
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from typing import Dict, Iterable, Tuple

import pandas as pd

from . import world_indices as wi


DATA_DIR = Path(__file__).resolve().parent / "data"

PRICE_OK_STATUSES = {"ok", "skip", "noop"}
VALUATION_OK_STATUSES = {"ok", "no_data", "no_change", "noop"}
KRX_OK_STATUSES = {"ok"}


def ensure_dependencies() -> list[str]:
    """Return list of missing dependencies required for full updates."""
    missing: list[str] = []
    try:
        import pkg_resources  # noqa: F401
    except ImportError:
        missing.append("setuptools (pkg_resources)")
    try:
        import pykrx  # noqa: F401
    except ImportError:
        missing.append("pykrx")
    if shutil.which("curl") is None:
        missing.append("curl (system)")
    return missing


def sanitize_daily_csvs(verbose: bool = True) -> int:
    """Sanitize all daily CSVs and return number of files touched."""
    df = wi.sanitize_all_daily_csvs()
    touched = 0
    if not df.empty and "changed" in df.columns:
        touched = int(df[df["changed"] == True].shape[0])  # noqa: E712
    if verbose:
        print(f"Sanitized daily CSVs: {touched} files updated.")
    return touched


def run_smoke_test(max_symbols: int = 3, lookback_days: int = 1, pause: float = 0.1) -> pd.DataFrame:
    """Run a lightweight update on a subset of symbols."""
    primaries = wi.list_primary_symbols(wi.investment_universe)
    subset = primaries[:max(1, max_symbols)]
    print(f"Running smoke test for {len(subset)} symbols (lookback={lookback_days}, pause={pause})...")
    summary = wi.update_all_daily_data(
        wi.investment_universe,
        pause=pause,
        symbols=subset,
        lookback_days=lookback_days,
    )
    _print_status_counts(summary, label="Smoke price update")
    return summary


def run_full_update(
    lookback_days: int = 7,
    pause: float = 0.3,
    valuation_pause: float = 0.2,
    include_krx: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame | None, pd.DataFrame | None]:
    """Run the full universe update and return summaries."""
    print(f"Running full price update (lookback={lookback_days}, pause={pause}) for primary universe...")
    wi.build_symbols_catalog(wi.investment_universe, primary_only=True)
    price_summary = wi.update_all_daily_data(
        wi.investment_universe,
        pause=pause,
        symbols=None,
        lookback_days=lookback_days,
    )
    _print_status_counts(price_summary, label="Price update")

    print("Updating valuation snapshots for primary symbols...")
    valuation_summary = wi.update_all_valuations(
        wi.investment_universe,
        pause=valuation_pause,
        symbols=None,
    )
    _print_status_counts(valuation_summary, label="Valuation update")

    krx_summary = None
    if include_krx:
        print("Updating KRX indices via pykrx...")
        try:
            krx_summary = wi.update_krx_indices()
            if krx_summary is None:
                print("KRX update returned no summary (possibly skipped).")
            else:
                _print_status_counts(krx_summary, label="KRX update")
        except Exception as exc:  # noqa: BLE001
            print(f"KRX update failed: {exc}", file=sys.stderr)
            raise
    else:
        print("Skipping KRX update (per configuration).")

    return price_summary, valuation_summary, krx_summary


def validate_summaries() -> Dict[str, pd.DataFrame]:
    """Inspect summary CSV files for non-OK statuses."""
    issues: Dict[str, pd.DataFrame] = {}

    def _flag(path: Path, ok_statuses: Iterable[str]) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        df = pd.read_csv(path)
        if "status" not in df.columns:
            return pd.DataFrame()
        bad = df[~df["status"].fillna("").isin(ok_statuses)]
        if "reason" in df.columns:
            reason_bad = df[df["reason"].fillna("").str.contains("error", case=False, na=False)]
            if not reason_bad.empty:
                bad = pd.concat([bad, reason_bad]).drop_duplicates()
        return bad

    price_issues = _flag(DATA_DIR / "update_summary.csv", PRICE_OK_STATUSES)
    if not price_issues.empty:
        issues["price"] = price_issues

    valuation_issues = _flag(DATA_DIR / "valuations_update_summary.csv", VALUATION_OK_STATUSES)
    if not valuation_issues.empty:
        issues["valuations"] = valuation_issues

    krx_path = DATA_DIR / "krx_batch_summary_from_world_indices.csv"
    if krx_path.exists():
        krx_issues = _flag(krx_path, KRX_OK_STATUSES)
        if not krx_issues.empty:
            issues["krx"] = krx_issues

    return issues


def _print_status_counts(summary: pd.DataFrame | None, *, label: str) -> None:
    if summary is None or summary.empty:
        print(f"{label}: no rows.")
        return
    if "status" not in summary.columns:
        print(f"{label}: summary does not contain status column.")
        return
    counts = summary["status"].value_counts()
    compact = ", ".join(f"{k}={v}" for k, v in counts.items())
    print(f"{label}: {compact}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Automation helpers for global_universe updates.")
    parser.add_argument("--mode", choices=["full", "smoke", "check"], default="full")
    parser.add_argument("--symbols", type=int, default=3, help="Number of symbols for smoke tests.")
    parser.add_argument("--lookback", type=int, default=7, help="Lookback days for price update.")
    parser.add_argument("--pause", type=float, default=0.3, help="Pause between Yahoo requests.")
    parser.add_argument("--valuation-pause", type=float, default=0.2, help="Pause between valuation requests.")
    parser.add_argument("--skip-krx", action="store_true", help="Skip KRX updates during full run.")
    parser.add_argument("--no-sanitize", action="store_true", help="Do not run CSV sanitization before updates.")
    parser.add_argument("--skip-smoke", action="store_true", help="Skip smoke test before full run.")
    parser.add_argument("--allow-issues", action="store_true", help="Do not fail even if summaries report issues.")
    args = parser.parse_args(argv)

    if args.mode in {"full", "smoke"}:
        missing = ensure_dependencies()
        if missing:
            msg = "Missing dependencies: " + ", ".join(missing)
            print(msg, file=sys.stderr)
            return 1

    if args.mode in {"full", "smoke"} and not args.no_sanitize:
        sanitize_daily_csvs(verbose=True)

    if args.mode == "smoke":
        run_smoke_test(max_symbols=args.symbols, lookback_days=min(args.lookback, 2), pause=args.pause)
        return 0

    if args.mode == "full":
        if not args.skip_smoke:
            run_smoke_test(max_symbols=args.symbols, lookback_days=min(2, args.lookback), pause=min(args.pause, 0.2))

        try:
            run_full_update(
                lookback_days=args.lookback,
                pause=args.pause,
                valuation_pause=args.valuation_pause,
                include_krx=not args.skip_krx,
            )
        except Exception:
            return 1

    issues = validate_summaries()
    if issues:
        print("Detected issues in summary files:")
        for key, df in issues.items():
            print(f" - {key}: {len(df)} problematic rows")
            with pd.option_context("display.max_rows", None, "display.max_columns", None):
                print(df)
        if not args.allow_issues:
            return 1

    print("All checks completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
