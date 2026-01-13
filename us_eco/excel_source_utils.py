"""Helpers for Excel-based data sources."""

from __future__ import annotations

from io import BytesIO
from typing import Callable, Iterable, Optional
from urllib.request import urlopen

import pandas as pd

DateParser = Callable[[object], Optional[pd.Timestamp]]


def download_excel_bytes(url: str, timeout: int = 30) -> bytes:
    with urlopen(url, timeout=timeout) as response:
        return response.read()


def load_excel_sheets(
    url: str,
    sheet_names: Iterable[str] | None = None,
    timeout: int = 30,
) -> dict[str, pd.DataFrame]:
    content = download_excel_bytes(url, timeout=timeout)
    workbook = pd.ExcelFile(BytesIO(content))
    target_sheets = list(sheet_names) if sheet_names else workbook.sheet_names
    return {name: pd.read_excel(workbook, sheet_name=name) for name in target_sheets}


def parse_time_month(value: object) -> pd.Timestamp | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.normalize()
    text = str(value).strip().lower()
    if not text:
        return None
    if "m" in text:
        parts = text.split("m", 1)
        try:
            year = int(parts[0])
            month = int(parts[1])
            return pd.Timestamp(year=year, month=month, day=1)
        except ValueError:
            return None
    parsed = pd.to_datetime(text, errors="coerce")
    if isinstance(parsed, pd.Timestamp) and not pd.isna(parsed):
        return parsed.normalize()
    return None


def tidy_excel_frame(
    df: pd.DataFrame,
    date_column: str = "date",
    rename_map: dict[str, str] | None = None,
    keep_columns: Iterable[str] | None = None,
    drop_columns: Iterable[str] | None = None,
    date_parser: DateParser | None = None,
) -> pd.DataFrame:
    if date_column not in df.columns:
        raise ValueError(f"Missing {date_column} in source data.")

    working = df.copy()

    if rename_map:
        working = working.rename(columns=rename_map)
    if drop_columns:
        working = working.drop(columns=list(drop_columns), errors="ignore")

    if date_parser:
        parsed_dates = working[date_column].apply(date_parser)
    else:
        parsed_dates = pd.to_datetime(working[date_column], errors="coerce")
    working["date"] = parsed_dates
    working = working.dropna(subset=["date"]).set_index("date")
    working = working.drop(columns=[date_column], errors="ignore")

    if keep_columns:
        keep_list = [col for col in keep_columns if col in working.columns]
        working = working[keep_list]

    working = working.apply(pd.to_numeric, errors="coerce")
    working = working.sort_index()
    working.index.name = "date"
    return working
