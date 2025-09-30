"""
Fix currency fields in world_indices.py based on the latest audit results.

Rules:
- Only adjust entries where the primary field ('etf' or 'index') mismatched.
- Ignore 'alternative' field mismatches since a single entry cannot satisfy
  multiple currencies across alt listings.

Usage:
  python -m global_universe.fix_universe_currencies [--dry-run]

Writes a concise change log to stdout.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple


THIS_DIR = Path(__file__).resolve().parent
WORLD_FILE = THIS_DIR / "world_indices.py"
AUDIT_CSV = THIS_DIR / "audit_results.csv"


@dataclass
class Change:
    country: str
    category: str
    name: str
    field: str  # 'etf' | 'index'
    symbol: str
    from_curr: str
    to_curr: str
    line_no: int
    line_before: str
    line_after: str


def load_audit_primary_mismatches(audit_csv: Path) -> List[Tuple[str, str, str, str, str, str]]:
    """Return deduped tuples (country, category, name, field, symbol, yahoo_currency)
    where field in {'etf','index'} and currency mismatched and exists==True.

    If both 'etf' and 'index' for the same (country, category, name) mismatch with
    different currencies, prefer 'etf' (since valuation_data typically follows ETF).
    """
    rows: List[Tuple[str, str, str, str, str, str]] = []
    with audit_csv.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            field = row.get("field")
            if field not in {"etf", "index"}:
                continue
            if row.get("exists") not in {"True", True}:  # csv serialized boolean
                continue
            if row.get("currency_match") in {"True", True}:
                continue
            yahoo_curr = (row.get("currency") or "").strip()
            if not yahoo_curr:
                continue
            rows.append(
                (
                    row.get("country") or "",
                    row.get("category") or "",
                    row.get("name") or "",
                    field,
                    row.get("symbol") or "",
                    yahoo_curr,
                )
            )

    # Deduplicate with preference: etf over index
    chosen: dict[Tuple[str, str, str], Tuple[str, str, str]] = {}
    for country, category, name, field, symbol, yahoo_curr in rows:
        key = (country, category, name)
        if key not in chosen:
            chosen[key] = (field, symbol, yahoo_curr)
        else:
            existing_field, _, _ = chosen[key]
            if existing_field != "etf" and field == "etf":
                chosen[key] = (field, symbol, yahoo_curr)

    out: List[Tuple[str, str, str, str, str, str]] = []
    for (country, category, name), (field, symbol, yahoo_curr) in chosen.items():
        out.append((country, category, name, field, symbol, yahoo_curr))
    return out


def replace_currency_in_lines(
    lines: List[str], name: str, field: str, symbol: str, new_curr: str
) -> Change | None:
    """Find the universe entry line matching name and field-symbol, replace currency.

    Strategy: entries are formatted on a single line like
      'Technology': {'index': None, 'etf': 'IXN', 'currency': 'USD', ...},

    We find a line that contains both "'<name>': {" and "'<field>': '<symbol>'".
    Then we regex-substitute the currency value.
    """
    name_token = f"'{name}':"
    field_token = f"'{field}': '{symbol}'"

    for i, line in enumerate(lines):
        if name_token in line and field_token in line:
            # Extract current currency
            m = re.search(r"('currency'\s*:\s*')([A-Z]{3})(')", line)
            if not m:
                # Currency not present as expected on this line
                continue
            old_curr = m.group(2)
            if old_curr == new_curr:
                return None
            new_line = line[: m.start(2)] + new_curr + line[m.end(2) :]
            change = Change(
                country="",
                category="",
                name=name,
                field=field,
                symbol=symbol,
                from_curr=old_curr,
                to_curr=new_curr,
                line_no=i + 1,
                line_before=line.rstrip("\n"),
                line_after=new_line.rstrip("\n"),
            )
            lines[i] = new_line
            return change
    return None


def main(argv: List[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    if not AUDIT_CSV.exists():
        print(f"ERROR: Audit CSV not found: {AUDIT_CSV}. Run audit first.")
        return 2

    targets = load_audit_primary_mismatches(AUDIT_CSV)
    if not targets:
        print("No primary currency mismatches found. Nothing to change.")
        return 0

    text = WORLD_FILE.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    changes: List[Change] = []
    for country, category, name, field, symbol, yahoo_curr in targets:
        ch = replace_currency_in_lines(lines, name=name, field=field, symbol=symbol, new_curr=yahoo_curr)
        if ch:
            ch.country = country
            ch.category = category
            changes.append(ch)

    if not changes:
        print("No applicable lines found to update. Ensure file formatting matches expectations.")
        return 0

    print("Proposed currency updates (primary only):")
    for c in changes:
        print(
            f"- {c.country} | {c.category}:{c.name} | {c.field} {c.symbol} | {c.from_curr} -> {c.to_curr} @ line {c.line_no}"
        )

    if args.dry_run:
        print("Dry-run: no changes written.")
        return 0

    WORLD_FILE.write_text("".join(lines), encoding="utf-8")
    print(f"Updated {WORLD_FILE} with {len(changes)} currency fixes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
