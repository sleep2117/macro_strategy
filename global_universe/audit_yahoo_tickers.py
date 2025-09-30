"""
Audit Yahoo Finance existence and semantic match for symbols
extracted from `investment_universe` in `world_indices copy.py`.

Outputs:
- audit_results.csv: per-symbol existence and match details
- audit_summary.txt: aggregated issues summary
"""

# Note: file moved under global_universe/ for project organization

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any
import subprocess

import requests


THIS_DIR = Path(__file__).resolve().parent
# Use the updated filename
UNIVERSE_FILE = THIS_DIR / "world_indices.py"


def load_investment_universe(path: Path) -> Dict[str, Any]:
    """Load the investment_universe literal from the file without executing imports."""
    import ast

    text = path.read_text(encoding="utf-8")
    # Find the start of the assignment
    marker = "investment_universe ="
    idx = text.find(marker)
    if idx == -1:
        raise KeyError("investment_universe assignment not found")
    # Extract from the first '{' after the marker
    brace_idx = text.find("{", idx)
    if brace_idx == -1:
        raise ValueError("Could not find opening '{' for investment_universe")

    # Simple brace matching to extract the dict literal
    depth = 0
    end_idx = None
    for i in range(brace_idx, len(text)):
        ch = text[i]
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                end_idx = i + 1
                break
    if end_idx is None:
        raise ValueError("Failed to match braces for investment_universe")

    literal = text[brace_idx:end_idx]
    try:
        data = ast.literal_eval(literal)
    except Exception as e:
        # For debugging help
        snippet = literal[:500]
        raise ValueError(f"Failed to parse investment_universe: {e}\nSnippet: {snippet}")
    return data


# Build heuristics
COUNTRY_TOKENS = {
    "US": ["US", "U.S.", "United States", "USA", "S&P", "America"],
    "Europe": ["Europe", "European", "STOXX", "Eurozone", "Euro Stoxx"],
    "Germany": ["Germany", "German", "Deutschland", "DAX", "MDAX", "SDAX"],
    "Japan": ["Japan", "Japanese", "Nikkei", "TOPIX"],
    "China": ["China", "Chinese", "Mainland", "A-Share", "A Shares", "MSCI China"],
    "Southeast_Asia": ["ASEAN", "South East Asia", "Southeast Asia", "Asia ex Japan", "AXJ"],
    "India": ["India", "Indian", "Nifty", "MSCI India"],
    "UK": ["UK", "United Kingdom", "FTSE", "Britain", "British"],
    "France": ["France", "French", "CAC"],
    "Italy": ["Italy", "Italian", "FTSE MIB", "MIB"],
    "Spain": ["Spain", "Spanish", "IBEX"],
    "Taiwan": ["Taiwan", "Taipei", "TAIEX", "TWSE"],
    "Hong_Kong": ["Hong Kong", "Hang Seng", "H.K.", "HK"],
}

# If tokens fail, also consider exchange location hints as a proxy for country match
COUNTRY_EXCHANGES = {
    "Europe": [
        "Zurich",
        "XETRA",
        "Frankfurt",
        "Paris",
        "Euronext",
        "Milan",
        "Madrid",
        "Amsterdam",
        "Brussels",
        "Lisbon",
        "Dublin",
        "Vienna",
        "Oslo",
        "Stockholm",
        "Copenhagen",
        "Helsinki",
        "LSE",
        "London",
    ],
    "US": ["NYSE", "NYSEArca", "Nasdaq", "Cboe US", "AMEX", "PCX"],
    "UK": ["LSE"],
    "France": ["Paris"],
    "Germany": ["XETRA", "XETR", "Frankfurt"],
    "Italy": ["Milan", "MIL", "Borsa Italiana"],
    "Spain": ["MCE", "BME", "Madrid"],
    "Japan": ["Tokyo"],
    "Hong_Kong": ["HKSE", "Hong Kong"],
    "Taiwan": ["Taiwan"],
}

SECTOR_TOKENS = {
    "Broad_Market": ["Broad", "Total Market", "All Country", "All Cap", "Market"],
    "Technology": ["Technology", "Tech", "Information Technology"],
    "Healthcare": ["Healthcare", "Health Care"],
    "Financials": ["Financials", "Financial", "Finance", "Banks"],
    "Cons_Discr.": ["Consumer Discretionary"],
    "Cons_Staples": ["Consumer Staples"],
    "Industrials": ["Industrials", "Industrial"],
    "Energy": ["Energy"],
    "Materials": ["Materials"],
    "Utilities": ["Utilities", "Utility"],
    "Comm_Services": ["Communication Services", "Communications", "Telecom", "Telecommunications"],
    "Real_Estate": ["Real Estate", "REIT"],
    "Telecom": ["Telecom", "Telecommunications", "Communication"],
    "Automobiles": ["Automobiles", "Autos", "Automotive"],
    "Basic_Resrcs": ["Basic Resources", "Resources", "Metals", "Mining"],
    "Chemicals": ["Chemicals"],
    "Banks": ["Banks", "Banking"],
    "Insurance": ["Insurance", "Insurer"],
    "Construction": ["Construction", "Construction & Materials"],
    "Technology(Proxy)": ["Semiconductor", "Technology", "Tech"],
    "Real_Estate_AXJ": ["REIT", "Real Estate", "Asia ex Japan"],
    "ASEAN": ["ASEAN"],
}

FACTOR_TOKENS = {
    "Equal_Weight": ["Equal Weight", "Equal-Weight", "Equal Weighted"],
    "Low_Vol": ["Low Volatility", "Minimum Volatility", "Min Vol"],
    "High_Beta": ["High Beta"],
    "Low_Beta": ["Low Beta", "Low Volatility"],
    "High_Dividend": ["High Dividend", "Dividend"],
    "Dividend_Growth": ["Dividend Growth", "Dividend Appreciation"],
    "Value": ["Value"],
    "Growth": ["Growth"],
    "Momentum": ["Momentum"],
    "Quality": ["Quality"],
    "High_Quality": ["Quality", "High Quality"],
    "Profitability": ["Profit", "Cash Cows"],
    "Multi_Factor": ["Multifactor", "Multi-Factor", "Diversified Factors"],
    "Large_Cap": ["Large Cap", "Large-Cap", "Large"],
    "Mid_Cap": ["Mid Cap", "Mid-Cap", "Mid"],
    "Small_Cap": ["Small Cap", "Small-Cap", "Small"],
    "A_Shares": ["A-Share", "A Shares", "China A"],
    "ASEAN_LargeMid": ["ASEAN", "Large", "Mid"],
    "Value(Proxy)": ["Value"],
}


def to_tokens(s: str) -> List[str]:
    if not s:
        return []
    # Split by non-alphanumeric while preserving words
    return re.findall(r"[A-Za-z0-9\-\+]+", s)


def any_token_in(text: str, candidates: List[str]) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(c.lower() in t for c in candidates)


def get_chart_meta(symbol: str) -> Dict[str, Any]:
    """Use the chart endpoint (less restricted) to fetch symbol meta.
    Returns an object mimicking fields used downstream.
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": "1d", "interval": "1d"}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"https://finance.yahoo.com/quote/{symbol}",
        "Connection": "keep-alive",
    }
    # Use curl to avoid Yahoo's aggressive blocking of Python clients
    try:
        cmd = [
            "curl",
            "-s",
            "-H",
            headers["User-Agent"],
            "-H",
            f"Accept: {headers['Accept']}",
            "-H",
            f"Accept-Language: {headers['Accept-Language']}",
            "-H",
            f"Referer: {headers['Referer']}",
            f"{url}?range={params['range']}&interval={params['interval']}",
        ]
        # Prepend header name for UA
        cmd[3] = f"User-Agent: {cmd[3]}"
        out = subprocess.check_output(cmd, timeout=15)
        if not out:
            return {}
        data = json.loads(out.decode("utf-8", errors="ignore"))
        result = (data or {}).get("chart", {}).get("result")
        if not result:
            return {}
        meta = result[0].get("meta", {})
        norm = {
            "symbol": meta.get("symbol"),
            "currency": meta.get("currency"),
            "fullExchangeName": meta.get("fullExchangeName"),
            "exchangeName": meta.get("exchangeName"),
            "quoteType": meta.get("instrumentType"),
            "shortName": meta.get("shortName"),
            "longName": meta.get("longName"),
        }
        # Normalize currency for STOXX Zurich (.Z) indices to EUR
        if symbol.endswith(".Z"):
            norm["currency"] = "EUR"
        return norm
    except Exception:
        return {}


def expected_type_for_field(field: str) -> str | None:
    if field == "index":
        return "INDEX"
    if field in ("etf", "alternative"):
        return "ETF"
    return None


def build_symbol_list(universe: Dict[str, Any]) -> List[Tuple[str, str, str, str, str]]:
    """Return tuples of (country, category_kind, category_name, field, symbol)."""
    items: List[Tuple[str, str, str, str, str]] = []
    for country, cdata in universe.items():
        for kind in ("sectors", "factors", "themes"):
            for name, entry in cdata.get(kind, {}).items():
                # index
                if entry.get("index"):
                    items.append((country, kind, name, "index", str(entry["index"])) )
                # etf
                if entry.get("etf"):
                    items.append((country, kind, name, "etf", str(entry["etf"])) )
                # alternatives
                for alt in entry.get("alternatives", []) or []:
                    if alt:
                        items.append((country, kind, name, "alternative", str(alt)))
    # Deduplicate preserving order
    seen = set()
    out: List[Tuple[str, str, str, str, str]] = []
    for t in items:
        key = (t[0], t[1], t[2], t[3], t[4])
        if key not in seen:
            seen.add(key)
            out.append(t)
    return out


def evaluate_match(country: str, kind: str, name: str, meta: Dict[str, Any]) -> Tuple[bool, bool, List[str]]:
    """Return (country_match, topic_match, notes)."""
    name_fields = [
        meta.get("longName"),
        meta.get("shortName"),
        meta.get("displayName"),
        meta.get("quoteType"),
    ]
    combined = " | ".join([s for s in name_fields if s])

    notes: List[str] = []

    # Country/region match
    expected_country_tokens = COUNTRY_TOKENS.get(country, [])
    country_match = any_token_in(combined, expected_country_tokens) if expected_country_tokens else True
    if not expected_country_tokens:
        notes.append("No country tokens configured")
    if not country_match:
        # Fallback via exchange hint
        exch = (meta.get("fullExchangeName") or meta.get("exchangeName") or "")
        hints = COUNTRY_EXCHANGES.get(country, [])
        if any(h in (exch or "") for h in hints):
            country_match = True
            notes.append("Country matched via exchange hint")
        else:
            notes.append("Country tokens not found in name")

    # Topic match: sector or factor
    tokens_map = SECTOR_TOKENS if kind == "sectors" else FACTOR_TOKENS
    expected_topic_tokens = tokens_map.get(name, [])
    topic_match = any_token_in(combined, expected_topic_tokens) if expected_topic_tokens else True
    if not expected_topic_tokens:
        notes.append("No topic tokens configured")
    if not topic_match:
        notes.append("Topic tokens not found in name")

    return country_match, topic_match, notes


def audit(universe: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    items = build_symbol_list(universe)
    symbols = [sym for (_, _, _, _, sym) in items]

    for country, kind, name, field, symbol in items:
        quoted = get_chart_meta(symbol)
        exists = bool(quoted)
        quoteType = quoted.get("quoteType") if exists else None
        exchange = quoted.get("fullExchangeName") if exists else None
        currency = quoted.get("currency") if exists else None
        shortName = quoted.get("shortName") if exists else None
        longName = quoted.get("longName") if exists else None

        expected_type = expected_type_for_field(field)
        type_match = True
        if expected_type and exists:
            type_match = (quoteType or "").upper() == expected_type

        # currency match vs entry's currency
        entry = universe[country][kind][name]
        expected_currency = entry.get("currency")
        currency_match = True if (expected_currency is None or not exists) else (currency == expected_currency)

        country_match, topic_match, notes = evaluate_match(country, kind, name, quoted if exists else {})

        reason_notes: List[str] = []
        if not exists:
            reason_notes.append("Symbol not found on Yahoo")
        if exists and expected_type and not type_match:
            reason_notes.append(f"Type mismatch: expected {expected_type}, got {quoteType}")
        if exists and not currency_match:
            reason_notes.append(f"Currency mismatch: expected {expected_currency}, got {currency}")
        reason = "; ".join(notes + reason_notes)

        records.append(
            {
                "country": country,
                "category": kind,
                "name": name,
                "field": field,
                "symbol": symbol,
                "exists": exists,
                "quoteType": quoteType,
                "exchange": exchange,
                "currency": currency,
                "shortName": shortName,
                "longName": longName,
                "expected_type": expected_type,
                "type_match": type_match,
                "currency_match": currency_match,
                "country_match": country_match,
                "topic_match": topic_match,
                "reason": reason,
            }
        )

    # Summaries
    summary = {
        "total": len(records),
        "not_found": [r for r in records if not r["exists"]],
        "type_mismatch": [r for r in records if r["exists"] and not r["type_match"]],
        "currency_mismatch": [r for r in records if r["exists"] and not r["currency_match"]],
        "country_mismatch": [r for r in records if r["exists"] and not r["country_match"]],
        "topic_mismatch": [r for r in records if r["exists"] and not r["topic_match"]],
    }

    return records, summary


def write_csv(records: List[Dict[str, Any]], path: Path) -> None:
    import csv

    fields = [
        "country",
        "category",
        "name",
        "field",
        "symbol",
        "exists",
        "quoteType",
        "exchange",
        "currency",
        "shortName",
        "longName",
        "expected_type",
        "type_match",
        "currency_match",
        "country_match",
        "topic_match",
        "reason",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in records:
            writer.writerow({k: r.get(k) for k in fields})


def write_summary(summary: Dict[str, Any], path: Path) -> None:
    lines: List[str] = []
    lines.append(f"Total symbols: {summary['total']}")
    lines.append("")
    for key in [
        ("not_found", "Not found on Yahoo"),
        ("type_mismatch", "Expected type mismatch"),
        ("currency_mismatch", "Currency mismatch"),
        ("country_mismatch", "Country/region tokens not found"),
        ("topic_mismatch", "Sector/Factor tokens not found"),
    ]:
        id_, title = key
        rows = summary[id_]
        lines.append(f"{title}: {len(rows)}")
        for r in rows:
            lines.append(
                f"  - {r['country']} | {r['category']}:{r['name']} | {r['field']} {r['symbol']} | type={r.get('quoteType')} exch={r.get('exchange')} name={r.get('shortName') or r.get('longName') or ''}"
            )
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    try:
        universe = load_investment_universe(UNIVERSE_FILE)
    except Exception as e:
        print(f"Failed to load investment_universe: {e}")
        return 1

    records, summary = audit(universe)

    out_csv = THIS_DIR / "audit_results.csv"
    out_txt = THIS_DIR / "audit_summary.txt"
    write_csv(records, out_csv)
    write_summary(summary, out_txt)

    print(f"Wrote: {out_csv}")
    print(f"Wrote: {out_txt}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
