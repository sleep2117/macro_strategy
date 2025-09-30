"""기타 FRED 물가 지표 (Median CPI 등) 통합 모듈."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import pandas as pd

try:
    from .us_eco_utils import load_economic_data  # type: ignore
except ImportError:
    from us_eco_utils import load_economic_data  # type: ignore


CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), "data", "misc_fred_series_data.csv")

MISC_FRED_SERIES: dict[str, str] = {
    "median_cpi": "MEDCPIM094SFRBCLE",
    "trimmed_mean_cpi": "TRMMEANCPIM094SFRBCLE",
}

MISC_FRED_KOREAN_NAMES: dict[str, str] = {
    "median_cpi": "중간값 CPI",
    "trimmed_mean_cpi": "16% 트림 평균 CPI",
}

MISC_FRED_DATA: dict[str, Any] = {
    "raw_data": pd.DataFrame(),
    "mom_data": pd.DataFrame(),
    "mom_change": pd.DataFrame(),
    "yoy_data": pd.DataFrame(),
    "yoy_change": pd.DataFrame(),
    "latest_values": {},
    "load_info": {
        "loaded": False,
        "load_time": None,
        "start_date": None,
        "series_count": 0,
        "data_points": 0,
        "source": None,
    },
}


def _build_latest_values(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    if df is None or df.empty:
        return latest
    for column in df.columns:
        series = pd.to_numeric(df[column], errors="coerce").dropna()
        if series.empty:
            continue
        last_timestamp = series.index[-1]
        latest[column] = {
            "value": float(series.iloc[-1]),
            "date": last_timestamp.strftime("%Y-%m-%d"),
        }
    return latest


def load_misc_fred_data(
    start_date: str = "1990-01-01",
    smart_update: bool = True,
    force_reload: bool = False,
) -> dict[str, Any] | None:
    """Load Median/Trimmed CPI series from FRED."""

    result = load_economic_data(
        MISC_FRED_SERIES,
        data_source="FRED",
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
    )

    if not result:
        return None

    for key in ("raw_data", "mom_data", "mom_change", "yoy_data", "yoy_change"):
        value = result.get(key)
        MISC_FRED_DATA[key] = value.copy() if isinstance(value, pd.DataFrame) else pd.DataFrame()

    load_info = result.get("load_info", {}).copy()
    MISC_FRED_DATA["load_info"] = load_info

    raw_df = MISC_FRED_DATA.get("raw_data", pd.DataFrame())
    latest_values = _build_latest_values(raw_df)
    MISC_FRED_DATA["latest_values"] = latest_values
    MISC_FRED_DATA["korean_names"] = dict(MISC_FRED_KOREAN_NAMES)
    MISC_FRED_DATA["load_info"].update(
        {
            "loaded": True,
            "load_time": datetime.now(),
            "start_date": start_date,
            "series_count": raw_df.shape[1] if isinstance(raw_df, pd.DataFrame) else 0,
            "data_points": raw_df.shape[0] if isinstance(raw_df, pd.DataFrame) else 0,
        }
    )

    return MISC_FRED_DATA


# 초기 로드는 지연 평가 (대시보드에서 필요 시 호출)
