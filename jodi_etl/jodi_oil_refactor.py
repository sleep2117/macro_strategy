"""
JODI Oil Visualization (refactored like CPS_employ_refactor)

- Reads the official JODI world CSV files saved by jodi_etl/cli.py
- Lets you define flexible series via filters (country/product/flow/unit)
- Builds a time-series DataFrame and uses us_eco_utils plot/export helpers

CSV columns (from JODI website):
  REF_AREA, TIME_PERIOD, ENERGY_PRODUCT, FLOW_BREAKDOWN, UNIT_MEASURE,
  OBS_VALUE, ASSESSMENT_CODE

Examples of codes:
  ENERGY_PRODUCT: CRUDEOIL, NGL, OTHERCRUDE, TOTCRUDE, GASOLINE, GASDIES, ...
  FLOW_BREAKDOWN (PRIMARY): INDPROD, REFINOBS, OSOURCES, TRANSBAK, ...
  FLOW_BREAKDOWN (SECONDARY): REFGROUT, RECEIPTS, TOTDEMO, IPTRANSF, ...
  UNIT_MEASURE: KBD, KBBL, KL, KTONS, CONVBBL
"""

import os
import sys
import json
from functools import lru_cache
from io import StringIO
import socket
import pandas as pd
import numpy as np
from datetime import datetime, date
import warnings
from typing import Optional, Any
import plotly.graph_objects as go
from pathlib import Path
from types import SimpleNamespace

import dash
from dash import dcc, html, dash_table, Input, Output, State, no_update

try:
    from jodi_etl import cli as jodi_cli
except ImportError:  # pragma: no cover - fallback when package path missing
    jodi_cli = None

warnings.filterwarnings("ignore")

# Use common plotting helpers (KPDS styling) via us_eco_utils
# Robust import regardless of working directory
try:
    from us_eco_utils import (
        calculate_mom_percent,
        calculate_mom_change,
        calculate_yoy_percent,
        calculate_yoy_change,
        plot_economic_series,
        export_economic_data,
    )
    from macro_strategy.kpds_fig_format_enhanced import (
        get_kpds_color,
        format_date_ticks,
        FONT_SIZE_GENERAL,
        FONT_SIZE_LEGEND,
        FONT_SIZE_ANNOTATION,
        calculate_title_position,
        create_five_year_comparison_chart,
    )
except ImportError:
    base_path = Path(__file__).resolve().parent
    repo_root = base_path.parent
    project_root_parent = repo_root.parent

    sys.path.append(str(repo_root / "us_eco"))
    from us_eco_utils import (
        calculate_mom_percent,
        calculate_mom_change,
        calculate_yoy_percent,
        calculate_yoy_change,
        plot_economic_series,
        export_economic_data,
    )
    sys.path.append(str(project_root_parent))
    from macro_strategy.kpds_fig_format_enhanced import (
        get_kpds_color,
        format_date_ticks,
        FONT_SIZE_GENERAL,
        FONT_SIZE_LEGEND,
        FONT_SIZE_ANNOTATION,
        calculate_title_position,
        create_five_year_comparison_chart,
    )

# -----------------------------------------------------------------------------
# Paths and constants
# -----------------------------------------------------------------------------

BASE_DIR = os.path.dirname(__file__)
JODI_DATA_DIR = os.path.join(BASE_DIR, "data")
PRIMARY_CSV = os.path.join(JODI_DATA_DIR, "NewProcedure_Primary_CSV.csv")
SECONDARY_CSV = os.path.join(JODI_DATA_DIR, "NewProcedure_Secondary_CSV.csv")
CACHE_DIR = os.path.join(JODI_DATA_DIR, "_cache")
CACHE_VERSION = "v2"
CACHE_COLUMNS = [
    "REF_AREA",
    "TIME_PERIOD",
    "ENERGY_PRODUCT",
    "FLOW_BREAKDOWN",
    "UNIT_MEASURE",
    "OBS_VALUE",
]

try:  # prefer feather/arrow cache when available
    import pyarrow  # type: ignore  # noqa: F401

    CACHE_BACKEND = "feather"
    CACHE_EXT = ".feather"
except ModuleNotFoundError:
    CACHE_BACKEND = "pickle"
    CACHE_EXT = ".pkl"

DASH_CACHE_VERSION = "v2"
DASH_CACHE_DIR = os.path.join(JODI_DATA_DIR, "_dash_cache")
DASH_CACHE_BACKEND = "feather" if CACHE_BACKEND == "feather" else "pickle"
DASH_CACHE_EXT = ".feather" if DASH_CACHE_BACKEND == "feather" else ".pkl"
DASH_CACHE_META_FILE = os.path.join(DASH_CACHE_DIR, f"jodi_dash_meta_{DASH_CACHE_VERSION}.pkl")
DASH_CACHE_WIDE_FILE = os.path.join(DASH_CACHE_DIR, f"jodi_dash_wide_{DASH_CACHE_VERSION}{DASH_CACHE_EXT}")
DASH_CACHE_COMBOS_FILE = os.path.join(DASH_CACHE_DIR, f"jodi_dash_combos_{DASH_CACHE_VERSION}{DASH_CACHE_EXT}")
LEGACY_DASH_CACHE_FILE = os.path.join(DASH_CACHE_DIR, "jodi_dash_cache_v1.pkl")
DASH_CACHE_FILE = LEGACY_DASH_CACHE_FILE  # Backward compat for older cache checks.

try:
    from babel import Locale

    KO_LOCALE = Locale("ko")
except Exception:
    KO_LOCALE = None


# Some lightweight aliases for common country inputs
COUNTRY_ALIASES = {
    "USA": "US",
    "KOR": "KR",
    "GBR": "GB",
    "ARE": "AE",
    "DEU": "DE",
    "FRA": "FR",
    "JPN": "JP",
    "CHN": "CN",
    "CAN": "CA",
    "AUS": "AU",
    "IND": "IN",
    "BRA": "BR",
    "RUS": "RU",
    "SAU": "SA",
    "MEX": "MX",
    "ITA": "IT",
    "ESP": "ES",
    "NLD": "NL",
    "NOR": "NO",
    "SWE": "SE",
    "CHE": "CH",
    "SGP": "SG",
}


COUNTRY_DISPLAY_NAMES = {
    "US": "미국",
    "KR": "한국",
    "GB": "영국",
    "AE": "아랍에미리트",
    "DE": "독일",
    "FR": "프랑스",
    "JP": "일본",
    "CN": "중국",
    "CA": "캐나다",
    "AU": "호주",
    "IN": "인도",
    "BR": "브라질",
    "RU": "러시아",
    "SA": "사우디아라비아",
    "MX": "멕시코",
    "IT": "이탈리아",
    "ES": "스페인",
    "NL": "네덜란드",
    "NO": "노르웨이",
    "SE": "스웨덴",
    "CH": "스위스",
    "SG": "싱가포르",
    "EU": "유럽연합",
    "OA": "중동",
    "OAS": "동남아시아",
    "AF": "아프리카",
    "WORLD": "세계",
    "TOTAL": "합계",
}


SECTION_LABELS = {
    "PRIMARY": "1차 공급 (Primary)",
    "SECONDARY": "2차 제품 (Secondary)",
}


PRODUCT_LABELS = {
    # primary
    "CRUDEOIL": "원유",
    "NGL": "천연가스액",
    "OTHERCRUDE": "기타 원료",
    "TOTCRUDE": "1차 제품 합계",
    # secondary
    "LPG": "액화석유가스(LPG)",
    "NAPHTHA": "나프타",
    "GASOLINE": "휘발유",
    "KEROSENE": "등유",
    "JETKERO": "항공유",
    "GASDIES": "경유",
    "RESFUEL": "중유",
    "ONONSPEC": "기타 석유제품",
    "TOTPRODS": "석유제품 합계",
}


FLOW_LABELS = {
    # primary
    "INDPROD": "생산",
    "OSOURCES": "기타 공급원",
    "TOTIMPSB": "수입",
    "TOTEXPSB": "수출",
    "TRANSBAK": "제품 이동/역류",
    "DIRECUSE": "직접 사용",
    "STOCKCH": "재고 변화",
    "STATDIFF": "통계 차이",
    "REFINOBS": "정유 투입",
    "CLOSTLV": "기말 재고",
    # secondary
    "REFGROUT": "정유 생산",
    "RECEIPTS": "수령",
    "PTRANSF": "제품 이전",
    "IPTRANSF": "제품 간 이전",
    "TOTDEMO": "수요",
}


UNIT_LABELS = {
    "KBD": "천 bpd",
    "KBBL": "천 배럴",
    "KL": "천 kL",
    "KTONS": "천 톤",
    "CONVBBL": "톤당 배럴",
}

PX_PER_CM = 37.7952755906
PRESETS_FILE_PATH = Path(__file__).with_name("jodi_dashboard_presets.json")

CHART_TYPE_LABELS: dict[str, str] = {
    "멀티 라인": "multi_line",
    "단일 라인": "single_line",
    "이중 축": "dual_axis",
    "가로 막대": "horizontal_bar",
    "세로 막대": "vertical_bar",
    "5년 비교": "five_year",
}

STANDARD_DATA_KEYS = [
    ("raw_data", "수준"),
    ("mom_data", "전월 대비 %"),
    ("mom_change", "전월 대비 변화량"),
    ("yoy_data", "전년동월 대비 %"),
    ("yoy_change", "전년동월 대비 변화량"),
]


def _ensure_presets_dir() -> None:
    try:
        PRESETS_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        print(f"⚠️ 프리셋 디렉터리 생성 실패: {exc}")


def load_jodi_presets() -> dict[str, Any]:
    if not PRESETS_FILE_PATH.exists():
        return {}
    try:
        with PRESETS_FILE_PATH.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"⚠️ 프리셋 파일 로드 실패: {exc}")
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def save_jodi_presets(presets: dict[str, Any]) -> None:
    _ensure_presets_dir()
    try:
        with PRESETS_FILE_PATH.open("w", encoding="utf-8") as handle:
            json.dump(presets, handle, ensure_ascii=False, indent=2)
    except OSError as exc:
        print(f"⚠️ 프리셋 저장 실패: {exc}")


def _update_jodi_data(outdir: str) -> tuple[bool, str]:
    if jodi_cli is None:
        return False, "데이터 업데이트 모듈(jodi_etl.cli)을 불러올 수 없습니다."

    args = SimpleNamespace(
        outdir=outdir,
        primary_url=jodi_cli.WORLD_PRIMARY_ZIP_URL,
        secondary_url=jodi_cli.WORLD_SECONDARY_ZIP_URL,
        split_format="csv",
        quiet=True,
    )

    try:
        jodi_cli.cmd_fetch(args)
    except Exception as exc:  # pragma: no cover - network/IO heavy path
        return False, f"데이터 업데이트 실패: {exc}"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    return True, f"데이터 업데이트가 완료되었습니다. ({timestamp})"


def _country_label(code: str) -> str:
    if not code:
        return code
    normalized = _normalize_country(code)
    if normalized in COUNTRY_DISPLAY_NAMES:
        return COUNTRY_DISPLAY_NAMES[normalized]
    if KO_LOCALE is not None:
        try:
            label = KO_LOCALE.territories.get(normalized)
            if label:
                return label
        except Exception:
            pass
    return normalized


def _product_label(code: str) -> str:
    return PRODUCT_LABELS.get(code, code)


def _flow_label(code: str) -> str:
    return FLOW_LABELS.get(code, code)


def _unit_label(code: str) -> str:
    return UNIT_LABELS.get(code, code)


def _section_label(code: str) -> str:
    key = (code or "").upper()
    return SECTION_LABELS.get(key, key.title())


def _series_simple_label(country: str, product: str, flow: str) -> str:
    cc = _country_label(country)
    pc = _product_label(product)
    fc = _flow_label(flow)
    parts = [p for p in [cc, pc, fc] if p]
    return " ".join(parts)


def _sanitize_plotly_figure(fig):
    if fig is None or not hasattr(fig, "layout"):
        return fig

    if hasattr(fig.layout, "title"):
        title_text = getattr(fig.layout.title, "text", None)
        if title_text is None or str(title_text).strip().lower() == "undefined":
            fig.layout.title.text = ""

    if hasattr(fig.layout, "yaxis") and hasattr(fig.layout.yaxis, "title"):
        y_text = getattr(fig.layout.yaxis.title, "text", None)
        if y_text is None or str(y_text).strip().lower() == "undefined":
            fig.layout.yaxis.title.text = ""

    if hasattr(fig.layout, "yaxis2") and hasattr(fig.layout.yaxis2, "title"):
        y2_text = getattr(fig.layout.yaxis2.title, "text", None)
        if y2_text is None or str(y2_text).strip().lower() == "undefined":
            fig.layout.yaxis2.title.text = ""

    annotations = []
    for ann in getattr(fig.layout, "annotations", []):
        text = getattr(ann, "text", None)
        if text is None:
            annotations.append(ann)
        elif str(text).strip().lower() != "undefined":
            annotations.append(ann)
    if annotations or getattr(fig.layout, "annotations", None):
        fig.update_layout(annotations=annotations)

    return fig


def _localized_combo_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    display = df.copy()
    display["SECTION"] = display["SECTION"].astype(str).map(_section_label)
    display["REF_AREA"] = display["REF_AREA"].astype(str).map(_country_label)
    display["ENERGY_PRODUCT"] = display["ENERGY_PRODUCT"].astype(str).map(
        lambda x: _product_label(x.upper())
    )
    display["FLOW_BREAKDOWN"] = display["FLOW_BREAKDOWN"].astype(str).map(
        lambda x: _flow_label(x.upper())
    )
    display["UNIT_MEASURE"] = display["UNIT_MEASURE"].astype(str).map(
        lambda x: _unit_label(x.upper())
    )
    display["DISPLAY_LABEL"] = display.apply(
        lambda row: _series_simple_label(row["REF_AREA"], row["ENERGY_PRODUCT"], row["FLOW_BREAKDOWN"]),
        axis=1,
    )
    return display.rename(
        columns={
            "SECTION": "섹션",
            "REF_AREA": "국가",
            "ENERGY_PRODUCT": "제품",
            "FLOW_BREAKDOWN": "흐름",
            "UNIT_MEASURE": "단위",
            "DISPLAY_LABEL": "표시명",
        }
    )


def _filter_chart_dataframe(data_pack: dict, data_type: str, selected_keys: list[str], periods: Optional[int], target_date: Optional[str]) -> tuple[Optional[pd.DataFrame], Optional[str]]:
    data_key_map = {
        "raw": "raw_data",
        "mom": "mom_data",
        "mom_change": "mom_change",
        "yoy": "yoy_data",
        "yoy_change": "yoy_change",
    }

    target = data_pack[data_key_map[data_type]][selected_keys]

    if target_date:
        try:
            parsed = pd.to_datetime(target_date)
            target = target[target.index <= parsed]
            if target.empty:
                return None, f"{target_date} 이전의 데이터가 없습니다."
        except Exception:
            return None, f"잘못된 날짜 형식입니다: {target_date}. 'YYYY-MM-DD' 형식을 사용하세요."

    if periods:
        target = target.tail(periods)

    if target.empty:
        return None, "선택한 조건에서 표시할 데이터가 없습니다."

    target = target.sort_index()

    if target.replace(0, np.nan).dropna(how="all").empty:
        return target, "선택한 시리즈는 모두 0 혹은 결측치입니다."

    return target, None


def _axis_title_for(data_type: str, keys: list[str], series_defs: dict) -> str:
    if not keys:
        return ""
    if data_type in {"mom", "yoy"}:
        return "%"

    units = []
    for key in keys:
        spec = series_defs.get(key, {})
        unit_code = str(spec.get("unit", "")).upper().strip()
        label = _unit_label(unit_code)
        if label and label not in units:
            units.append(label)

    return ", ".join(units) if units else ""


def _compute_axis_range(series_list: list[pd.Series]) -> Optional[list[float]]:
    combined = pd.concat([s.dropna() for s in series_list], axis=0) if series_list else pd.Series(dtype=float)
    if combined.empty:
        return None

    min_val = combined.min()
    max_val = combined.max()
    if pd.isna(min_val) or pd.isna(max_val):
        return None

    if min_val == max_val:
        pad = abs(min_val) * 0.05 if min_val != 0 else 1.0
        return [min_val - pad, max_val + pad]

    span = max_val - min_val
    pad = span * 0.05
    return [min_val - pad, max_val + pad]


def _normalize_series(series: pd.Series) -> pd.Series:
    series = pd.to_numeric(series, errors="coerce")
    return series.dropna()


def _infer_series_period(series: pd.Series) -> str:
    if series is None or series.empty:
        return "month"
    if len(series) < 3:
        return "month"
    normalized_series = series.copy()
    if not isinstance(normalized_series.index, pd.DatetimeIndex):
        try:
            normalized_series.index = pd.to_datetime(normalized_series.index)
        except Exception:
            return "month"
    diffs = normalized_series.index.to_series().diff().dropna()
    if diffs.empty:
        return "month"
    median_delta = diffs.median()
    if pd.isna(median_delta):
        return "month"
    delta_days = median_delta / pd.Timedelta(days=1)
    if delta_days <= 9:
        return "week"
    return "month"


def _build_combined_dataframe_jodi(selected_infos: list[dict[str, Any]], dtype_map: dict[str, str], data_pack: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, dict[str, str]]:
    frames: list[pd.Series] = []
    label_dtype: dict[str, str] = {}
    for info in selected_infos:
        key = info["key"]
        dtype_choice = dtype_map.get(key)
        if not dtype_choice:
            continue
        source_df = data_pack.get(dtype_choice)
        if not isinstance(source_df, pd.DataFrame) or key not in source_df.columns:
            continue
        label = info.get("leaf_label", key)
        series = pd.to_numeric(source_df[key], errors="coerce").rename(label)
        frames.append(series)
        label_dtype[label] = dtype_choice
    if not frames:
        return pd.DataFrame(), label_dtype
    combined = pd.concat(frames, axis=1, join="outer").sort_index()
    return combined, label_dtype


def build_jodi_series_registry(combos: pd.DataFrame) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], list[str]]:
    registry: dict[str, dict[str, Any]] = {}
    tree_sections: dict[str, dict[str, Any]] = {}

    for _, row in combos.iterrows():
        section = str(row["SECTION"]).upper()
        country = str(row["REF_AREA"]).upper()
        product = str(row["ENERGY_PRODUCT"]).upper()
        flow = str(row["FLOW_BREAKDOWN"]).upper()
        unit = str(row["UNIT_MEASURE"]).upper()
        display_label = _series_simple_label(country, product, flow)
        unit_label = _unit_label(unit)
        leaf_label = f"{display_label} · {unit_label}" if unit_label else display_label
        key = sectioned_series_key(section, country, product, flow, unit)

        registry[key] = {
            "key": key,
            "section": section,
            "country": country,
            "product": product,
            "flow": flow,
            "unit": unit,
            "display_label": display_label,
            "leaf_label": leaf_label,
            "series_label": display_label,
            "unit_label": unit_label,
            "column_tuple": (section, country, product, flow, unit),
            "series_spec": {
                "section": section,
                "country": country,
                "product": product,
                "flow": flow,
                "unit": unit,
            },
            "available_types": [key for key, _ in STANDARD_DATA_KEYS],
        }

        section_node = tree_sections.setdefault(section, {"label": _section_label(section), "value": f"section::{section}", "children": {}})
        country_nodes = section_node["children"]
        country_node = country_nodes.setdefault(country, {"label": _country_label(country), "value": f"country::{section}::{country}", "children": {}})
        product_nodes = country_node["children"]
        product_node = product_nodes.setdefault(product, {"label": _product_label(product), "value": f"product::{section}::{country}::{product}", "children": {}})
        flow_nodes = product_node["children"]
        flow_node = flow_nodes.setdefault(flow, {"label": _flow_label(flow), "value": f"flow::{section}::{country}::{product}::{flow}", "children": []})
        flow_children = flow_node["children"]
        flow_children.append({"label": leaf_label, "value": key})

    tree_nodes: list[dict[str, Any]] = []
    for section_key in sorted(tree_sections.keys()):
        section_node = tree_sections[section_key]
        country_nodes = []
        for country_key in sorted(section_node["children"].keys()):
            country_node = section_node["children"][country_key]
            product_nodes = []
            for product_key in sorted(country_node["children"].keys()):
                product_node = country_node["children"][product_key]
                flow_nodes = []
                for flow_key in sorted(product_node["children"].keys()):
                    flow_node = product_node["children"][flow_key]
                    flow_node_struct = {
                        "label": flow_node["label"],
                        "value": flow_node["value"],
                        "children": flow_node["children"],
                    }
                    flow_nodes.append(flow_node_struct)
                product_nodes.append({
                    "label": product_node["label"],
                    "value": product_node["value"],
                    "children": flow_nodes,
                })
            country_nodes.append({
                "label": country_node["label"],
                "value": country_node["value"],
                "children": product_nodes,
            })
        tree_nodes.append({
            "label": section_node["label"],
            "value": section_node["value"],
            "children": country_nodes,
        })

    default_checked = [key for key in list(registry.keys())[:2]]
    return registry, tree_nodes, default_checked


def _create_single_axis_line_chart(
    df: pd.DataFrame,
    axis_title: str,
    chart_width: int,
    chart_height: int,
    zero_line: bool,
    frequency_map: Optional[dict[str, str]] = None,
) -> Optional[go.Figure]:
    if df is None or df.empty:
        return None

    fig = go.Figure()
    for idx, column in enumerate(df.columns):
        connect_flag = True
        if frequency_map is not None and frequency_map.get(column) == "week":
            connect_flag = False
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df[column],
                name=column,
                line=dict(color=get_kpds_color(idx), dash="solid"),
                connectgaps=connect_flag,
            )
        )

    fig.update_layout(
        paper_bgcolor="white",
        plot_bgcolor="white",
        width=chart_width,
        height=chart_height,
        font=dict(family="NanumGothic", size=FONT_SIZE_GENERAL, color="black"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            font=dict(family="NanumGothic", size=FONT_SIZE_LEGEND),
            borderwidth=0,
            bordercolor="rgba(0,0,0,0)",
        ),
        margin=dict(l=60, r=40, t=40, b=60),
    )

    fig.update_xaxes(
        title_text="",
        showline=True,
        linewidth=1.3,
        linecolor="lightgrey",
        tickwidth=1.3,
        tickcolor="lightgrey",
        ticks="outside",
        showgrid=False,
    )

    fig.update_yaxes(
        title_text="",
        tickformat=",",
        showline=False,
        tickcolor="white",
        showgrid=False,
    )

    fig = format_date_ticks(fig, "%b-%y", "auto", df.index)

    if zero_line:
        fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)

    cleaned_title = (axis_title or "").strip()
    if cleaned_title:
        fig.add_annotation(
            text=cleaned_title,
            xref="paper",
            yref="paper",
            x=calculate_title_position(cleaned_title, "left"),
            y=1.1,
            showarrow=False,
            font=dict(family="NanumGothic", size=FONT_SIZE_ANNOTATION, color="black"),
            align="left",
        )

    return _sanitize_plotly_figure(fig)


def _create_dual_axis_chart(
    df: pd.DataFrame,
    axis_allocation: dict[str, list[str]],
    label_map: dict,
    data_type: str,
    series_defs: dict,
    chart_width: Optional[int] = None,
    chart_height: Optional[int] = None,
    left_title_offset: float = 0.0,
    right_title_offset: float = 0.0,
    left_axis_data_type: Optional[str] = None,
    right_axis_data_type: Optional[str] = None,
    left_title_override: Optional[str] = None,
    right_title_override: Optional[str] = None,
    left_axis_range_override: Optional[list[float]] = None,
    right_axis_range_override: Optional[list[float]] = None,
    connect_map: Optional[dict[str, str]] = None,
) -> Optional[object]:
    left_cols = [col for col in axis_allocation.get("left", []) if col in df.columns]
    right_cols = [col for col in axis_allocation.get("right", []) if col in df.columns and col not in left_cols]

    if not left_cols or not right_cols:
        return None

    working_df = df[left_cols + right_cols].copy()
    working_df = working_df.apply(pd.to_numeric, errors="coerce")
    working_df = working_df.dropna(how="all")
    if working_df.empty:
        return None

    left_dtype = left_axis_data_type or data_type
    right_dtype = right_axis_data_type or data_type
    left_title_default = _axis_title_for(left_dtype, left_cols, series_defs)
    right_title_default = _axis_title_for(right_dtype, right_cols, series_defs)
    left_title = left_title_default if left_title_override is None else left_title_override
    right_title = right_title_default if right_title_override is None else right_title_override

    left_range = _compute_axis_range([working_df[col] for col in left_cols])
    right_range = _compute_axis_range([working_df[col] for col in right_cols])

    if left_axis_range_override and len(left_axis_range_override) == 2:
        if left_axis_range_override[0] < left_axis_range_override[1]:
            left_range = list(left_axis_range_override)
    if right_axis_range_override and len(right_axis_range_override) == 2:
        if right_axis_range_override[0] < right_axis_range_override[1]:
            right_range = list(right_axis_range_override)

    zero_line_needed = False
    if left_range and left_range[0] <= 0 <= left_range[1]:
        zero_line_needed = True
    if right_range and right_range[0] <= 0 <= right_range[1]:
        zero_line_needed = True

    change_types = {"mom", "yoy", "mom_change", "yoy_change"}
    if left_dtype in change_types or right_dtype in change_types or data_type in change_types:
        zero_line_needed = True

    font_family = "NanumGothic"
    fig = go.Figure()

    for idx, col in enumerate(left_cols):
        label = label_map.get(col, col)
        connect_flag = True
        if connect_map is not None and connect_map.get(label) == "week":
            connect_flag = False
        fig.add_trace(
            go.Scatter(
                x=working_df.index,
                y=working_df[col],
                name=label,
                line=dict(color=get_kpds_color(idx)),
                yaxis="y",
                connectgaps=connect_flag,
            )
        )

    for idx, col in enumerate(right_cols):
        color_idx = len(left_cols) + idx
        label = label_map.get(col, col)
        connect_flag = True
        if connect_map is not None and connect_map.get(label) == "week":
            connect_flag = False
        fig.add_trace(
            go.Scatter(
                x=working_df.index,
                y=working_df[col],
                name=label,
                line=dict(color=get_kpds_color(color_idx)),
                yaxis="y2",
                connectgaps=connect_flag,
            )
        )

    fig.update_layout(
        paper_bgcolor="white",
        plot_bgcolor="white",
        width=chart_width or 686,
        height=chart_height or 400,
        font=dict(family=font_family, size=FONT_SIZE_GENERAL, color="black"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            font=dict(family=font_family, size=FONT_SIZE_LEGEND),
            borderwidth=0,
            bordercolor="rgba(0,0,0,0)",
        ),
        margin=dict(l=60, r=60, t=40, b=60),
        title=None,
        showlegend=True,
    )

    fig.update_xaxes(
        title_text="",
        showline=True,
        linewidth=1.3,
        linecolor="lightgrey",
        tickwidth=1.3,
        tickcolor="lightgrey",
        ticks="outside",
        showgrid=False,
    )

    fig.update_yaxes(
        range=left_range,
        tickformat=",",
        showline=False,
        tickcolor="white",
        showgrid=False,
        title_text="",
        zeroline=False,
    )

    fig.update_layout(
        yaxis2=dict(
            range=right_range,
            tickformat=",",
            showline=False,
            tickcolor="white",
            anchor="x",
            overlaying="y",
            side="right",
            showgrid=False,
            title_text="",
            zeroline=False,
        )
    )

    fig = format_date_ticks(fig, "%b-%y", "auto", working_df.index)

    if zero_line_needed:
        fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)

    if left_title:
        left_pos = (calculate_title_position(left_title, "left") or -0.03) + left_title_offset
        fig.add_annotation(
            text=left_title,
            xref="paper",
            yref="paper",
            x=left_pos,
            y=1.1,
            showarrow=False,
            xanchor="left",
            yanchor="bottom",
            font=dict(family=font_family, size=FONT_SIZE_ANNOTATION, color="black"),
            align="left",
        )

    if right_title:
        right_pos = (calculate_title_position(right_title, "right") or 1.03) + right_title_offset
        fig.add_annotation(
            text=right_title,
            xref="paper",
            yref="paper",
            x=right_pos,
            y=1.1,
            showarrow=False,
            xanchor="right",
            yanchor="bottom",
            font=dict(family=font_family, size=FONT_SIZE_ANNOTATION, color="black"),
            align="right",
        )

    fig.update_layout(title=None)

    # Remove any remaining annotations with text "undefined"
    fig.update_layout(
        annotations=[
            a
            for a in fig.layout.annotations
            if getattr(a, "text", None) not in ("undefined", None)
        ]
    )

    return _sanitize_plotly_figure(fig)


def _make_monthly_five_year_format(series: pd.Series, history_years: int = 5) -> Optional[pd.DataFrame]:
    if series is None:
        return None

    monthly = pd.to_numeric(series, errors="coerce").dropna()
    if monthly.empty:
        return None

    monthly.index = pd.to_datetime(monthly.index)
    monthly = monthly[~monthly.index.duplicated(keep="last")].sort_index()

    last_year = int(monthly.index.year.max())
    earliest = int(monthly.index.year.min())
    start_year = max(earliest, last_year - history_years + 1)
    selected = monthly[monthly.index.year >= start_year]
    if selected.empty:
        return None

    df_temp = selected.to_frame("value")
    df_temp["year"] = df_temp.index.year
    df_temp["month"] = df_temp.index.month
    pivot = df_temp.pivot(index="month", columns="year", values="value").sort_index()
    pivot = pivot.reindex(range(1, 13))
    pivot.columns = [str(col) for col in pivot.columns]

    numeric = pivot.astype(float)
    stats = pd.DataFrame(index=pivot.index)
    stats["평균"] = numeric.mean(axis=1, skipna=True)
    stats["Min"] = numeric.min(axis=1, skipna=True)
    stats["Min~Max"] = numeric.max(axis=1, skipna=True)
    result = pd.concat([numeric, stats], axis=1)
    result.index.name = "month"
    return result


def _create_monthly_five_year_chart(
    series: pd.Series,
    series_name: str,
    unit_label: str,
    recent_years: int,
    chart_width: int,
    chart_height: int,
) -> tuple[Optional[object], Optional[pd.DataFrame]]:
    history_years = max(recent_years, 5)
    formatted = _make_monthly_five_year_format(series, history_years=history_years)
    if formatted is None or formatted.dropna(how="all").empty:
        return None, None

    fig = create_five_year_comparison_chart(
        formatted,
        title=series_name,
        y_title=unit_label,
        x_axis_type="month",
        recent_years=recent_years,
    )
    if fig is not None:
        fig.update_layout(width=chart_width, height=chart_height)
    return _sanitize_plotly_figure(fig), formatted


def _normalize_country(code: str) -> str:
    if not code:
        return code
    c = code.strip().upper()
    if len(c) == 3 and c in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[c]
    return c


def _ensure_cache_dir() -> None:
    if not os.path.isdir(CACHE_DIR):
        os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_path_for_csv(csv_path: str) -> str:
    """Return a deterministic pickle cache path for a CSV."""
    fname = os.path.basename(csv_path)
    safe_name, _ = os.path.splitext(fname)
    return os.path.join(CACHE_DIR, f"{safe_name}.{CACHE_VERSION}{CACHE_EXT}")


def _load_cached_frame(cache_path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(cache_path):
        return None
    try:
        if CACHE_BACKEND == "feather":
            return pd.read_feather(cache_path)
        return pd.read_pickle(cache_path)
    except Exception:
        return None


def _store_cached_frame(df: pd.DataFrame, cache_path: str) -> None:
    try:
        _ensure_cache_dir()
        if CACHE_BACKEND == "feather":
            df.to_feather(cache_path, version=2)
        else:
            df.to_pickle(cache_path)
    except Exception:
        pass  # caching failure should not break execution


def _ensure_dash_cache_dir() -> None:
    try:
        os.makedirs(DASH_CACHE_DIR, exist_ok=True)
    except OSError:
        pass


def _latest_jodi_data_mtime() -> float:
    latest = 0.0
    for path in (PRIMARY_CSV, SECONDARY_CSV):
        if os.path.exists(path):
            try:
                latest = max(latest, os.path.getmtime(path))
            except OSError:
                pass

    split_dir = os.path.join(JODI_DATA_DIR, "split")
    if os.path.isdir(split_dir):
        for root, _, files in os.walk(split_dir):
            for fname in files:
                ext = os.path.splitext(fname)[1].lower()
                if ext not in {".csv", ".parquet"}:
                    continue
                fpath = os.path.join(root, fname)
                try:
                    latest = max(latest, os.path.getmtime(fpath))
                except OSError:
                    continue
    return latest


def _read_dash_frame(path: str, index_name: Optional[str] = None) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    try:
        if DASH_CACHE_BACKEND == "feather":
            df = pd.read_feather(path)
            if index_name and index_name in df.columns:
                df = df.set_index(index_name)
            return df
        return pd.read_pickle(path)
    except Exception:
        return None


def _write_dash_frame(df: pd.DataFrame, path: str, index_name: Optional[str] = None) -> None:
    _ensure_dash_cache_dir()
    try:
        if DASH_CACHE_BACKEND == "feather":
            if index_name:
                df_to_save = df.reset_index()
            else:
                df_to_save = df.reset_index(drop=True)
            df_to_save.to_feather(path, version=2)
        else:
            df.to_pickle(path)
    except Exception:
        pass


def _load_dash_cache_meta() -> Optional[dict[str, Any]]:
    if not os.path.exists(DASH_CACHE_META_FILE):
        return None
    try:
        meta = pd.read_pickle(DASH_CACHE_META_FILE)
    except Exception:
        return None
    if not isinstance(meta, dict):
        return None
    if meta.get("version") != DASH_CACHE_VERSION:
        return None
    source_mtime = meta.get("source_mtime")
    if not isinstance(source_mtime, (int, float)):
        return None
    if _latest_jodi_data_mtime() > float(source_mtime):
        return None
    return meta


def _load_dash_cache() -> Optional[dict[str, Any]]:
    meta = _load_dash_cache_meta()
    if not meta:
        return None
    wide_df = _read_dash_frame(DASH_CACHE_WIDE_FILE, meta.get("wide_index_name"))
    combos = _read_dash_frame(DASH_CACHE_COMBOS_FILE)
    options = meta.get("options")
    units_by_series = meta.get("units_by_series")
    if not isinstance(wide_df, pd.DataFrame) or not isinstance(combos, pd.DataFrame):
        return None
    if not isinstance(options, dict) or not isinstance(units_by_series, dict):
        return None
    return {
        "wide_df": wide_df,
        "combos": combos,
        "options": options,
        "units_by_series": units_by_series,
    }


def _save_dash_cache(
    wide_df: pd.DataFrame,
    combos: pd.DataFrame,
    options: dict[str, Any],
    units_by_series: dict[str, set[str]],
) -> None:
    index_name = wide_df.index.name or "index"
    _write_dash_frame(wide_df, DASH_CACHE_WIDE_FILE, index_name=index_name)
    _write_dash_frame(combos, DASH_CACHE_COMBOS_FILE)
    meta = {
        "version": DASH_CACHE_VERSION,
        "source_mtime": _latest_jodi_data_mtime(),
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "wide_index_name": index_name,
        "options": options,
        "units_by_series": units_by_series,
    }
    _ensure_dash_cache_dir()
    try:
        pd.to_pickle(meta, DASH_CACHE_META_FILE)
    except Exception:
        pass


def _normalize_category(series: pd.Series, uppercase: bool = True) -> pd.Series:
    """Return a categorical Series with trimmed/uppercased categories."""

    if series.dtype.name == "category":
        new_categories = [
            (str(cat).strip().upper() if uppercase else str(cat).strip())
            for cat in series.cat.categories
        ]
        series = series.cat.rename_categories(new_categories)
        return series

    string_series = series.astype("string").str.strip()
    if uppercase:
        string_series = string_series.str.upper()
    return string_series.astype("category")


def _finalize_jodi_frame(df: pd.DataFrame, section: str) -> pd.DataFrame:
    """Normalize dtypes and ensure canonical section label."""

    df = df.copy()
    canonical_section = section.upper()

    if "OBS_VALUE" in df.columns:
        df = df.drop(columns=["OBS_VALUE"])

    if "SECTION" not in df.columns:
        df["SECTION"] = canonical_section
    df["SECTION"] = _normalize_category(df["SECTION"], uppercase=True)

    if "TIME_PERIOD" in df.columns:
        if np.issubdtype(df["TIME_PERIOD"].dtype, np.datetime64):
            df["TIME_PERIOD"] = df["TIME_PERIOD"].dt.to_period("M").dt.to_timestamp()
        else:
            parsed = pd.to_datetime(df["TIME_PERIOD"], errors="coerce")
            df["TIME_PERIOD"] = parsed.dt.to_period("M").dt.to_timestamp()

    if "VALUE_NUM" in df.columns:
        df["VALUE_NUM"] = pd.to_numeric(df["VALUE_NUM"], errors="coerce").astype("float32")

    for cat_col in ("REF_AREA", "ENERGY_PRODUCT", "FLOW_BREAKDOWN", "UNIT_MEASURE"):
        if cat_col in df.columns:
            df[cat_col] = _normalize_category(df[cat_col], uppercase=True)

    df = df[df["TIME_PERIOD"].notna()]
    if canonical_section not in df["SECTION"].cat.categories:
        df["SECTION"] = df["SECTION"].cat.add_categories([canonical_section])
    df["SECTION"] = df["SECTION"].fillna(canonical_section)
    df["SECTION"] = df["SECTION"].cat.set_categories(["PRIMARY", "SECONDARY"], ordered=False)

    return df


def _read_jodi_csv(path: str, section: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found: {path}")

    canonical_section = section.upper()
    cache_path = _cache_path_for_csv(path)
    csv_mtime = os.path.getmtime(path)
    cache_df = _load_cached_frame(cache_path)
    if cache_df is not None and "VALUE_NUM" in cache_df.columns:
        cache_mtime = os.path.getmtime(cache_path)
        if cache_mtime >= csv_mtime:
            if cache_df.empty:
                return cache_df
            if "SECTION" in cache_df.columns:
                cached_section = str(cache_df["SECTION"].iloc[0]).upper()
                if cached_section == canonical_section:
                    return _finalize_jodi_frame(cache_df, canonical_section)

    df = pd.read_csv(path, dtype=str, usecols=CACHE_COLUMNS)
    val = df["OBS_VALUE"].astype(str).str.strip().replace({"-": np.nan, "x": np.nan, "N/A": np.nan, "..": np.nan})
    df["VALUE_NUM"] = val

    df = _finalize_jodi_frame(df, canonical_section)

    _store_cached_frame(df, cache_path)
    return df


def _load_split_section(section: str) -> Optional[pd.DataFrame]:
    split_dir = os.path.join(JODI_DATA_DIR, "split", section.lower())
    if not os.path.isdir(split_dir):
        return None

    frames: list[pd.DataFrame] = []
    for root, _, files in os.walk(split_dir):
        chosen: dict[str, tuple[str, str]] = {}
        for fname in files:
            base, ext = os.path.splitext(fname)
            ext_lower = ext.lower()
            if ext_lower not in {".csv", ".parquet"}:
                continue
            fpath = os.path.join(root, fname)
            current = chosen.get(base)
            if current is None:
                chosen[base] = (ext_lower, fpath)
                continue
            current_ext, current_path = current
            current_mtime = os.path.getmtime(current_path)
            new_mtime = os.path.getmtime(fpath)
            if new_mtime > current_mtime:
                chosen[base] = (ext_lower, fpath)
            elif new_mtime == current_mtime and current_ext != ".parquet" and ext_lower == ".parquet":
                chosen[base] = (ext_lower, fpath)

        for ext_lower, fpath in chosen.values():
            try:
                if ext_lower == ".csv":
                    df = pd.read_csv(fpath, dtype=str)
                else:
                    df = pd.read_parquet(fpath)
                    df = df.astype(str)
            except Exception as exc:
                print(f"⚠️ {fpath} 로드에 실패했습니다: {exc}")
                continue

            if "OBS_VALUE" in df.columns and "VALUE_NUM" not in df.columns:
                val = df["OBS_VALUE"].astype(str).str.strip().replace({"-": np.nan, "x": np.nan, "N/A": np.nan, "..": np.nan})
                df["VALUE_NUM"] = val

            frames.append(_finalize_jodi_frame(df, section))

    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def load_jodi_base() -> pd.DataFrame:
    """Load and concatenate primary + secondary with a SECTION column."""
    primary = _load_split_section("PRIMARY")
    if primary is None:
        primary = _read_jodi_csv(PRIMARY_CSV, section="PRIMARY")

    secondary = _load_split_section("SECONDARY")
    if secondary is None:
        secondary = _read_jodi_csv(SECONDARY_CSV, section="SECONDARY")
    df = pd.concat([primary, secondary], ignore_index=True)
    return df


def build_series_dataframe_from_df(
    df: pd.DataFrame,
    series_defs: dict,
    start_date: str = "2002-01-01",
    sections: Optional[list] = None,
) -> pd.DataFrame:
    """Build a wide DataFrame from a provided base df with optional SECTION filter.

    Args:
        df: combined primary+secondary DataFrame from load_jodi_base()
        series_defs: {name: {country, product, flow, unit}}
        start_date: earliest date string
        sections: optional list, subset of ["PRIMARY","SECONDARY"] to include
    """
    if sections:
        df = df[df["SECTION"].isin(sections)]
    out = {}
    start_ts = pd.to_datetime(start_date)
    for name, spec in series_defs.items():
        country = _normalize_country(spec.get("country", ""))
        product = spec.get("product", "").upper().strip()
        flow = spec.get("flow", "").upper().strip()
        unit = spec.get("unit", "").upper().strip()
        section = spec.get("section", "").upper().strip()
        sel = (
            (df["REF_AREA"] == country)
            & (df["ENERGY_PRODUCT"] == product)
            & (df["FLOW_BREAKDOWN"] == flow)
            & (df["UNIT_MEASURE"] == unit)
        )
        if section:
            sel = sel & (df["SECTION"] == section)
        s = (
            df.loc[sel, ["TIME_PERIOD", "VALUE_NUM"]]
            .groupby("TIME_PERIOD")
            .sum()["VALUE_NUM"]
            .sort_index()
        )
        s = s[s.index >= start_ts]
        out[name] = s
    if not out:
        return pd.DataFrame()
    return pd.DataFrame(out)


def list_value_options(df: pd.DataFrame) -> dict:
    """Return sorted unique options for selectors."""
    return {
        "countries": sorted(df["REF_AREA"].dropna().astype(str).unique().tolist()),
        "products": sorted(df["ENERGY_PRODUCT"].dropna().astype(str).unique().tolist()),
        "flows": sorted(df["FLOW_BREAKDOWN"].dropna().astype(str).unique().tolist()),
        "units": sorted(df["UNIT_MEASURE"].dropna().astype(str).unique().tolist()),
        "sections": [sec for sec in ["PRIMARY", "SECONDARY"] if sec in df["SECTION"].astype(str).unique().tolist()],
    }


def series_key(country: str, product: str, flow: str, unit: str) -> str:
    return f"{country}_{product}_{flow}_{unit}"


def sectioned_series_key(section: str, country: str, product: str, flow: str, unit: str) -> str:
    base = series_key(country, product, flow, unit)
    section = (section or "").upper().strip()
    return f"{section}_{base}" if section else base


def series_label(country: str, product: str, flow: str, unit: str) -> str:
    return _series_simple_label(country, product, flow)


def build_series_dataframe(series_defs: dict, start_date: str = "2002-01-01") -> pd.DataFrame:
    """
    Build a wide DataFrame from JODI CSVs where each column is a series.

    series_defs example:
        {
          'US_CRUDE_PROD_KBD': {
              'country': 'US',
              'product': 'CRUDEOIL',
              'flow': 'INDPROD',
              'unit': 'KBD'
          },
          'KR_GASOLINE_DEMAND_KBD': {
              'country': 'KR', 'product': 'GASOLINE', 'flow': 'TOTDEMO', 'unit': 'KBD'
          }
        }
    """
    df = load_jodi_base()
    out = {}
    start_ts = pd.to_datetime(start_date)

    for name, spec in series_defs.items():
        country = _normalize_country(spec.get("country", ""))
        product = spec.get("product", "").upper().strip()
        flow = spec.get("flow", "").upper().strip()
        unit = spec.get("unit", "").upper().strip()
        section = spec.get("section", "").upper().strip()

        sel = (
            (df["REF_AREA"] == country)
            & (df["ENERGY_PRODUCT"] == product)
            & (df["FLOW_BREAKDOWN"] == flow)
            & (df["UNIT_MEASURE"] == unit)
        )
        if section:
            sel = sel & (df["SECTION"] == section)
        s = (
            df.loc[sel, ["TIME_PERIOD", "VALUE_NUM"]]
            .groupby("TIME_PERIOD")
            .sum()["VALUE_NUM"]
            .sort_index()
        )
        s = s[s.index >= start_ts]
        out[name] = s

    if not out:
        return pd.DataFrame()
    wide = pd.DataFrame(out)
    return wide


def make_korean_names(series_defs: dict) -> dict:
    labels = {}
    for name, spec in series_defs.items():
        c = _normalize_country(spec.get("country", ""))
        p = spec.get("product", "").upper().strip()
        f = spec.get("flow", "").upper().strip()
        u = spec.get("unit", "").upper().strip()
        _ = u  # unit unused but kept for future extensions
        labels[name] = series_label(c, p, f, u)
    return labels


# Global cache similar to CPS script
JODI_DATA = {}


def load_jodi_data(series_defs: dict, start_date: str = "2002-01-01") -> bool:
    """Load selected JODI series to global dict like CPS loader does."""
    global JODI_DATA
    raw = build_series_dataframe(series_defs, start_date=start_date)
    if raw is None or raw.empty:
        print("❌ 선택된 시리즈에서 데이터가 생성되지 않았습니다.")
        return False
    mom = calculate_mom_percent(raw)
    mom_ch = calculate_mom_change(raw)
    yoy = calculate_yoy_percent(raw)
    yoy_ch = calculate_yoy_change(raw)

    JODI_DATA = {
        "raw_data": raw,
        "mom_data": mom,
        "mom_change": mom_ch,
        "yoy_data": yoy,
        "yoy_change": yoy_ch,
        "load_info": {
            "loaded": True,
            "load_time": datetime.now(),
            "start_date": start_date,
            "series_count": raw.shape[1],
            "data_points": raw.shape[0],
            "source": "JODI CSV (local)",
        },
    }
    print("✅ JODI 데이터 로드 완료")
    return True


def plot_jodi_series(series_list, chart_type="multi_line", data_type="raw", periods=None, target_date=None, labels=None):
    if not JODI_DATA:
        print("⚠️ 먼저 load_jodi_data()를 실행하세요.")
        return None
    korean_names = labels if labels else {}
    return plot_economic_series(
        data_dict=JODI_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=korean_names,
    )


def export_jodi_data(series_list, data_type="raw", periods=None, target_date=None, labels=None, export_path=None, file_format="excel"):
    if not JODI_DATA:
        print("⚠️ 먼저 load_jodi_data()를 실행하세요.")
        return None
    korean_names = labels if labels else {}
    return export_economic_data(
        data_dict=JODI_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=korean_names,
        export_path=export_path,
        file_format=file_format,
    )


# -----------------------------------------------------------------------------
# Example presets to get started quickly
# -----------------------------------------------------------------------------

JODI_SERIES_EXAMPLE = {
    # US crude production (primary) in KBD
    "US_CRUDE_PROD_KBD": {"country": "US", "product": "CRUDEOIL", "flow": "INDPROD", "unit": "KBD"},
    # US gasoline demand (secondary) in KBD
    "US_GASOLINE_DEMAND_KBD": {"country": "US", "product": "GASOLINE", "flow": "TOTDEMO", "unit": "KBD"},
    # KR diesel demand (secondary) in KBD
    "KR_DIESEL_DEMAND_KBD": {"country": "KR", "product": "GASDIES", "flow": "TOTDEMO", "unit": "KBD"},
}

JODI_KOREAN_NAMES = make_korean_names(JODI_SERIES_EXAMPLE)


# -----------------------------------------------------------------------------
# Dash app
# -----------------------------------------------------------------------------

DEFAULT_CHART_WIDTH_CM = 24.0
DEFAULT_CHART_HEIGHT_CM = 12.0

DATA_TYPE_LABELS = {key: label for key, label in STANDARD_DATA_KEYS}
CHART_TYPE_OPTIONS = [{"label": label, "value": code} for label, code in CHART_TYPE_LABELS.items()]


def _make_empty_figure(message: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        paper_bgcolor="white",
        plot_bgcolor="white",
        width=int(DEFAULT_CHART_WIDTH_CM * PX_PER_CM),
        height=int(DEFAULT_CHART_HEIGHT_CM * PX_PER_CM),
        margin=dict(l=40, r=40, t=40, b=40),
    )
    fig.add_annotation(
        text=message,
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.5,
        showarrow=False,
        font=dict(size=14, color="#444"),
    )
    return fig


def _get_preset_options(presets: dict[str, Any]) -> list[dict[str, str]]:
    return [{"label": name, "value": name} for name in sorted(presets.keys())]


def _default_dtype_for_series(available_types: list[str]) -> str:
    return available_types[0] if available_types else ""


def _collect_selected_infos(
    selected_keys: Optional[list[str]],
    registry: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    infos: list[dict[str, Any]] = []
    for key in selected_keys or []:
        info = registry.get(key)
        if info:
            infos.append(dict(info))
    return infos


def _build_table_rows(
    selected_infos: list[dict[str, Any]],
    settings: Optional[dict[str, dict[str, str]]],
    global_dtype: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for info in selected_infos:
        key = info["key"]
        stored = settings.get(key, {}) if settings else {}
        base_label = info.get("leaf_label", info.get("display_label", key))
        label = stored.get("label") or base_label
        stored_dtype = stored.get("dtype") or ""
        override_flag = stored.get("dtype_override")
        if override_flag is None:
            override_flag = bool(stored_dtype and stored_dtype != global_dtype)
        if override_flag and stored_dtype:
            dtype = stored_dtype
        else:
            dtype = global_dtype
        if dtype not in info.get("available_types", []):
            dtype = _default_dtype_for_series(info.get("available_types", []))
        axis = stored.get("axis") or "left"
        rows.append(
            {
                "key": key,
                "series": base_label,
                "label": str(label) if label is not None else base_label,
                "dtype": dtype,
                "axis": axis,
            }
        )
    return rows


def _settings_from_rows(
    rows: Optional[list[dict[str, Any]]],
    existing: Optional[dict[str, dict[str, str]]] = None,
    global_dtype: Optional[str] = None,
) -> dict[str, dict[str, str]]:
    settings = dict(existing or {})
    normalized_global = str(global_dtype).strip() if global_dtype else None
    for row in rows or []:
        key = row.get("key")
        if not key:
            continue
        label = str(row.get("label") or "").strip()
        dtype = str(row.get("dtype") or "").strip()
        dtype_override = False
        if normalized_global:
            dtype_override = dtype != "" and dtype != normalized_global
            if not dtype_override:
                dtype = ""
        axis = str(row.get("axis") or "left").strip() or "left"
        settings[key] = {
            "label": label,
            "dtype": dtype,
            "axis": axis,
            "dtype_override": dtype_override,
        }
    return settings


def _prepare_data_table(
    df: Optional[pd.DataFrame],
    max_rows: int = 200,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    if df is None or df.empty:
        return [], []
    display_df = df.copy()
    if isinstance(display_df.index, pd.DatetimeIndex):
        display_df = display_df.sort_index()
    if isinstance(display_df.index, pd.DatetimeIndex):
        display_df.insert(0, "Date", display_df.index.strftime("%Y-%m-%d"))
    else:
        display_df.insert(0, "Index", display_df.index.astype(str))
    display_df = display_df.tail(max_rows).reset_index(drop=True)
    columns = [{"name": col, "id": col} for col in display_df.columns]
    return display_df.to_dict("records"), columns


def _extract_axis_range(
    enabled: bool,
    min_val: Any,
    max_val: Any,
) -> Optional[list[float]]:
    if not enabled:
        return None
    if min_val is None or max_val is None:
        return None
    try:
        min_f = float(min_val)
        max_f = float(max_val)
    except (TypeError, ValueError):
        return None
    if min_f >= max_f:
        return None
    return [min_f, max_f]


def _dtype_key_to_axis(dtype_key: Optional[str]) -> str:
    if not dtype_key:
        return "raw"
    dtype_key = dtype_key.strip()
    if dtype_key.endswith("_data"):
        return dtype_key.replace("_data", "")
    return dtype_key


@lru_cache(maxsize=1)
def _load_processed_views_cached() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, list[str]], dict[str, set[str]]]:
    cached = _load_dash_cache()
    if cached:
        wide_cached = cached.get("wide_df")
        combos_cached = cached.get("combos")
        options_cached = cached.get("options")
        units_cached = cached.get("units_by_series")
        if isinstance(wide_cached, pd.DataFrame) and isinstance(combos_cached, pd.DataFrame):
            if isinstance(options_cached, dict) and isinstance(units_cached, dict):
                return wide_cached, combos_cached, options_cached, units_cached
    wide_df, combos, options, units_by_series = _compute_processed_views()
    _save_dash_cache(wide_df, combos, options, units_by_series)
    return wide_df, combos, options, units_by_series


def _compute_processed_views() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, list[str]], dict[str, set[str]]]:
    base = load_jodi_base()
    grouped = (
        base.groupby(
            [
                "TIME_PERIOD",
                "SECTION",
                "REF_AREA",
                "ENERGY_PRODUCT",
                "FLOW_BREAKDOWN",
                "UNIT_MEASURE",
            ],
            observed=True,
        )["VALUE_NUM"]
        .sum()
        .astype("float32")
    )
    wide = grouped.unstack(
        ["SECTION", "REF_AREA", "ENERGY_PRODUCT", "FLOW_BREAKDOWN", "UNIT_MEASURE"]
    ).sort_index()

    def _valid_series(series: pd.Series) -> bool:
        ser = series.dropna()
        if ser.empty:
            return False
        if (ser.abs() > 1e-6).any():
            return True
        recent_window = 36
        recent = ser.tail(recent_window)
        if recent.empty:
            recent = ser
        return (recent.abs() > 1e-6).any()

    if not wide.empty:
        valid_mask = wide.apply(_valid_series, axis=0)
        wide = wide.loc[:, valid_mask]

    combos = wide.columns.to_frame(index=False).reset_index(drop=True)
    combos.columns = [
        "SECTION",
        "REF_AREA",
        "ENERGY_PRODUCT",
        "FLOW_BREAKDOWN",
        "UNIT_MEASURE",
    ]
    combos["DISPLAY_LABEL"] = combos.apply(
        lambda row: _series_simple_label(
            row["REF_AREA"], row["ENERGY_PRODUCT"], row["FLOW_BREAKDOWN"]
        ),
        axis=1,
    )

    units_by_series: dict[str, set[str]] = {}
    for tup, label in zip(wide.columns, combos["DISPLAY_LABEL"]):
        units_by_series[label] = units_by_series.get(label, set()) | {tup[4]}

    options = {
        "sections": sorted(combos["SECTION"].dropna().astype(str).unique().tolist()),
        "countries": sorted(combos["REF_AREA"].dropna().astype(str).unique().tolist()),
        "products": sorted(combos["ENERGY_PRODUCT"].dropna().astype(str).unique().tolist()),
        "flows": sorted(combos["FLOW_BREAKDOWN"].dropna().astype(str).unique().tolist()),
        "units": sorted(combos["UNIT_MEASURE"].dropna().astype(str).unique().tolist()),
    }

    return wide.astype("float32"), combos, options, units_by_series


@lru_cache(maxsize=1)
def _build_registry_bundle():
    wide_df, combos_all, options, units_by_series = _load_processed_views_cached()
    registry, _, default_checked = build_jodi_series_registry(combos_all)
    return wide_df, combos_all, options, units_by_series, registry, default_checked


def _clear_jodi_dash_cache() -> None:
    _load_processed_views_cached.cache_clear()
    _build_registry_bundle.cache_clear()
    for path in (
        DASH_CACHE_META_FILE,
        DASH_CACHE_WIDE_FILE,
        DASH_CACHE_COMBOS_FILE,
        LEGACY_DASH_CACHE_FILE,
    ):
        if os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass


def _filter_combos(
    combos: pd.DataFrame,
    sections: Optional[list[str]],
    countries: Optional[list[str]],
    products: Optional[list[str]],
    flows: Optional[list[str]],
    units: Optional[list[str]],
) -> pd.DataFrame:
    filtered = combos
    if sections:
        filtered = filtered[filtered["SECTION"].isin(sections)]
    if countries:
        filtered = filtered[filtered["REF_AREA"].isin(countries)]
    if products:
        filtered = filtered[filtered["ENERGY_PRODUCT"].isin(products)]
    if flows:
        filtered = filtered[filtered["FLOW_BREAKDOWN"].isin(flows)]
    if units:
        filtered = filtered[filtered["UNIT_MEASURE"].isin(units)]
    return filtered


def _series_options_from_combos(
    combos: pd.DataFrame,
    registry: dict[str, dict[str, Any]],
    selected_keys: Optional[list[str]] = None,
) -> list[dict[str, str]]:
    seen: set[str] = set()
    options: list[dict[str, str]] = []

    for _, row in combos.iterrows():
        key = sectioned_series_key(
            str(row["SECTION"]),
            str(row["REF_AREA"]),
            str(row["ENERGY_PRODUCT"]),
            str(row["FLOW_BREAKDOWN"]),
            str(row["UNIT_MEASURE"]),
        )
        if key in seen:
            continue
        info = registry.get(key)
        if not info:
            continue
        leaf_label = info.get("leaf_label", key)
        section_label = _section_label(info.get("section", ""))
        option_label = f"{section_label} · {leaf_label}" if section_label else leaf_label
        options.append({"label": option_label, "value": key})
        seen.add(key)

    for key in selected_keys or []:
        if key in seen:
            continue
        info = registry.get(key)
        if not info:
            continue
        leaf_label = info.get("leaf_label", key)
        section_label = _section_label(info.get("section", ""))
        option_label = f"{section_label} · {leaf_label}" if section_label else leaf_label
        options.append({"label": option_label, "value": key})
        seen.add(key)

    return options


def _build_data_pack(
    wide_df: pd.DataFrame,
    selected_infos: list[dict[str, Any]],
    start_date: Optional[str],
) -> tuple[Optional[dict[str, pd.DataFrame]], Optional[str]]:
    if not selected_infos:
        return None, "시리즈를 선택하세요."

    selected_tuples = [info["column_tuple"] for info in selected_infos if "column_tuple" in info]
    if not selected_tuples:
        return None, "선택한 시리즈를 불러올 수 없습니다."

    start_ts = None
    if start_date:
        try:
            start_ts = pd.to_datetime(start_date)
        except Exception:
            return None, f"잘못된 날짜 형식입니다: {start_date}"

    try:
        if start_ts is not None:
            series_df = wide_df.loc[wide_df.index >= start_ts, selected_tuples]
        else:
            series_df = wide_df.loc[:, selected_tuples]
    except KeyError:
        return None, "선택한 시리즈에서 데이터를 찾을 수 없습니다."

    if series_df is None or series_df.empty:
        return None, "선택한 조건에 데이터가 없습니다."

    if isinstance(series_df.columns, pd.MultiIndex):
        series_df.columns = [info["key"] for info in selected_infos]
    else:
        series_df.columns = [selected_infos[0]["key"]]
    series_df = series_df.astype("float32").sort_index()

    data_pack = {
        "raw_data": series_df,
        "mom_data": calculate_mom_percent(series_df),
        "mom_change": calculate_mom_change(series_df),
        "yoy_data": calculate_yoy_percent(series_df),
        "yoy_change": calculate_yoy_change(series_df),
    }
    return data_pack, None


def _build_figure_jodi(
    selected_infos: list[dict[str, Any]],
    settings: Optional[dict[str, dict[str, str]]],
    chart_type: str,
    global_dtype: str,
    start_date: Optional[str],
    chart_width_cm: Optional[float],
    chart_height_cm: Optional[float],
    axis_titles: dict[str, str],
    axis_offsets: dict[str, float],
    manual_ranges: dict[str, Optional[list[float]]],
    five_year_recent: Optional[int],
    zero_line: bool,
) -> tuple[go.Figure, Optional[pd.DataFrame], list[str]]:
    messages: list[str] = []
    settings = settings or {}

    if not selected_infos:
        return _make_empty_figure("시리즈를 선택하세요."), None, messages

    wide_df, _, _, _, _, _ = _build_registry_bundle()
    data_pack, error_message = _build_data_pack(wide_df, selected_infos, start_date)
    if error_message:
        return _make_empty_figure(error_message), None, messages
    if data_pack is None:
        return _make_empty_figure("데이터를 불러올 수 없습니다."), None, messages

    dtype_map: dict[str, str] = {}
    for info in selected_infos:
        key = info["key"]
        available = info.get("available_types", [])
        dtype_choice = settings.get(key, {}).get("dtype") or ""
        if not dtype_choice:
            dtype_choice = global_dtype
        if dtype_choice not in available:
            dtype_choice = _default_dtype_for_series(available)
        if not dtype_choice:
            continue
        dtype_map[key] = dtype_choice

    if not dtype_map:
        return _make_empty_figure("데이터 타입을 선택하세요."), None, messages

    combined_df, _ = _build_combined_dataframe_jodi(selected_infos, dtype_map, data_pack)
    if combined_df.empty:
        return _make_empty_figure("선택한 시리즈에 데이터가 없습니다."), None, messages

    label_map: dict[str, str] = {}
    label_map_for_chart: dict[str, str] = {}
    for info in selected_infos:
        key = info["key"]
        base_label = info.get("leaf_label", info.get("display_label", key))
        custom_label = settings.get(key, {}).get("label")
        if isinstance(custom_label, str):
            custom_label = custom_label.strip()
        if not custom_label:
            custom_label = base_label
        label_map[base_label] = custom_label
        label_map_for_chart[key] = custom_label
        info["effective_label"] = custom_label

    combined_df = combined_df.rename(columns=label_map)
    combined_df = combined_df.dropna(how="all").dropna(axis=1, how="all")
    if combined_df.empty:
        return _make_empty_figure("선택한 시리즈에 유효한 데이터가 없습니다."), None, messages

    frequency_map: dict[str, str] = {}
    for label in combined_df.columns:
        series = combined_df[label]
        if series is None:
            continue
        frequency_map[label] = _infer_series_period(series.dropna())

    dtype_set = {dtype_map.get(info["key"]) for info in selected_infos if dtype_map.get(info["key"])}
    dtype_uniform = len(dtype_set) == 1

    series_defs = {info["key"]: info.get("series_spec", {}) for info in selected_infos}
    axis_label_default = _axis_title_for(
        _dtype_key_to_axis(global_dtype),
        [info["key"] for info in selected_infos],
        series_defs,
    )
    if not axis_label_default and dtype_uniform:
        axis_label_default = DATA_TYPE_LABELS.get(next(iter(dtype_set)), "")

    chart_width = int((chart_width_cm or DEFAULT_CHART_WIDTH_CM) * PX_PER_CM)
    chart_height = int((chart_height_cm or DEFAULT_CHART_HEIGHT_CM) * PX_PER_CM)

    table_df: Optional[pd.DataFrame] = combined_df.copy()
    fig: Optional[go.Figure] = None

    single_axis_title = (axis_titles.get("single") or axis_label_default or "").strip()
    left_axis_title = (axis_titles.get("left") or "").strip()
    right_axis_title = (axis_titles.get("right") or "").strip()

    if chart_type == "single_line":
        if combined_df.shape[1] > 1:
            messages.append("단일 라인 차트는 첫 번째 시리즈만 표시합니다.")
        fig = _create_single_axis_line_chart(
            combined_df[[combined_df.columns[0]]],
            single_axis_title,
            chart_width,
            chart_height,
            zero_line,
            frequency_map,
        )
    elif chart_type == "multi_line":
        fig = _create_single_axis_line_chart(
            combined_df,
            single_axis_title,
            chart_width,
            chart_height,
            zero_line,
            frequency_map,
        )
    elif chart_type == "dual_axis":
        labels = list(combined_df.columns)
        axis_allocation: dict[str, list[str]] = {"left": [], "right": []}
        for info in selected_infos:
            label = info.get("effective_label", info.get("leaf_label", info["key"]))
            axis = settings.get(info["key"], {}).get("axis", "left")
            target = "right" if axis == "right" else "left"
            axis_allocation[target].append(label)
        if not axis_allocation["left"] or not axis_allocation["right"]:
            if len(labels) >= 2:
                axis_allocation["left"] = labels[:-1]
                axis_allocation["right"] = labels[-1:]
            else:
                return _make_empty_figure("이중 축은 2개 이상의 시리즈가 필요합니다."), table_df, messages

        series_type_map = {
            info.get("effective_label", info.get("leaf_label", info["key"])): dtype_map.get(info["key"], "")
            for info in selected_infos
        }
        left_dtype_keys = {series_type_map.get(label) for label in axis_allocation["left"] if series_type_map.get(label)}
        right_dtype_keys = {series_type_map.get(label) for label in axis_allocation["right"] if series_type_map.get(label)}
        left_axis_dtype = _dtype_key_to_axis(next(iter(left_dtype_keys))) if len(left_dtype_keys) == 1 else None
        right_axis_dtype = _dtype_key_to_axis(next(iter(right_dtype_keys))) if len(right_dtype_keys) == 1 else None
        base_dtype = _dtype_key_to_axis(next(iter(dtype_set))) if dtype_uniform else _dtype_key_to_axis(global_dtype)

        label_series_defs = {
            info.get("effective_label", info.get("leaf_label", info["key"])): {"unit": info.get("unit", "")}
            for info in selected_infos
        }

        fig = _create_dual_axis_chart(
            df=combined_df,
            axis_allocation=axis_allocation,
            label_map={label: label for label in labels},
            data_type=base_dtype,
            series_defs=label_series_defs,
            chart_width=chart_width,
            chart_height=chart_height,
            left_title_offset=axis_offsets.get("left", 0.0),
            right_title_offset=axis_offsets.get("right", 0.0),
            left_axis_data_type=left_axis_dtype,
            right_axis_data_type=right_axis_dtype,
            left_title_override=(left_axis_title or axis_label_default or None),
            right_title_override=(right_axis_title or axis_label_default or None),
            left_axis_range_override=manual_ranges.get("left"),
            right_axis_range_override=manual_ranges.get("right"),
            connect_map=frequency_map,
        )
    elif chart_type == "five_year":
        if len(selected_infos) > 1:
            messages.append("5년 비교 차트는 첫 번째 시리즈만 사용합니다.")
        info = selected_infos[0]
        raw_df = data_pack.get("raw_data") if isinstance(data_pack, dict) else None
        if not isinstance(raw_df, pd.DataFrame) or info["key"] not in raw_df.columns:
            return _make_empty_figure("5년 비교 차트용 데이터가 없습니다."), table_df, messages
        series = raw_df[info["key"]]
        if series.dropna().empty:
            return _make_empty_figure("5년 비교 차트용 데이터가 없습니다."), table_df, messages
        unit_label = _axis_title_for("raw", [info["key"]], series_defs)
        recent_years = five_year_recent or 5
        fig, formatted_df = _create_monthly_five_year_chart(
            series=series,
            series_name=info.get("effective_label", info.get("leaf_label", info["key"])),
            unit_label=unit_label,
            recent_years=recent_years,
            chart_width=chart_width,
            chart_height=chart_height,
        )
        if formatted_df is not None:
            table_df = formatted_df
    elif chart_type in {"horizontal_bar", "vertical_bar"}:
        if not dtype_uniform:
            return _make_empty_figure("막대 차트는 동일한 데이터 타입이 필요합니다."), table_df, messages
        dtype_value = next(iter(dtype_set)) if dtype_set else global_dtype
        data_type = _dtype_key_to_axis(dtype_value)
        fig = plot_economic_series(
            data_dict=data_pack,
            series_list=[info["key"] for info in selected_infos],
            chart_type=chart_type,
            data_type=data_type,
            labels=label_map_for_chart,
            korean_names=label_map_for_chart,
            left_ytitle=single_axis_title or axis_label_default or None,
        )
        if fig is not None:
            fig.update_layout(width=chart_width, height=chart_height)
    else:
        return _make_empty_figure("지원하지 않는 차트 유형입니다."), table_df, messages

    if fig is None:
        return _make_empty_figure("차트를 생성할 수 없습니다."), table_df, messages

    single_axis_range = manual_ranges.get("single")
    if single_axis_range and chart_type != "dual_axis":
        if chart_type == "horizontal_bar":
            fig.update_xaxes(range=single_axis_range)
        else:
            fig.update_yaxes(range=single_axis_range)

    return _sanitize_plotly_figure(fig), table_df, messages

WIDE_DF, COMBOS_ALL, FILTER_OPTIONS, UNITS_BY_SERIES, SERIES_REGISTRY, DEFAULT_SERIES_SELECTION = _build_registry_bundle()

SECTION_OPTIONS = [{"label": _section_label(code), "value": code} for code in FILTER_OPTIONS.get("sections", [])]
COUNTRY_OPTIONS = [{"label": _country_label(code), "value": code} for code in FILTER_OPTIONS.get("countries", [])]
PRODUCT_OPTIONS = [{"label": _product_label(code), "value": code} for code in FILTER_OPTIONS.get("products", [])]
FLOW_OPTIONS = [{"label": _flow_label(code), "value": code} for code in FILTER_OPTIONS.get("flows", [])]
UNIT_OPTIONS = [{"label": _unit_label(code), "value": code} for code in FILTER_OPTIONS.get("units", [])]

SERIES_OPTIONS = _series_options_from_combos(COMBOS_ALL, SERIES_REGISTRY, DEFAULT_SERIES_SELECTION)
PRESET_CACHE: dict[str, Any] = load_jodi_presets()

ASSETS_PATH = Path(__file__).resolve().parent.parent / "us_eco" / "assets"
app = dash.Dash(__name__, assets_folder=str(ASSETS_PATH))
app.title = "JODI Oil Dashboard (Dash)"

app.layout = html.Div(
    className="app-shell",
    children=[
        html.Div(
            className="app-header",
            children=[
                html.Div(
                    children=[
                        html.Div("KPDS Macro Lab", className="brand-kicker"),
                        html.H2("JODI 석유 데이터", className="app-title"),
                        html.P(
                            "국가·품목·흐름 선택과 프리셋으로 빠르게 비교하세요.",
                            className="app-subtitle",
                        ),
                    ]
                )
            ],
        ),
        html.Div(id="status-message", className="status-message"),
        html.Div(
            className="app-body",
            children=[
                html.Div(
                    className="side-panel card",
                    children=[
                        html.Div("컨트롤", className="card-title"),
                        html.Div("데이터 관리", className="card-title"),
                        html.Button("데이터 업데이트", id="data-update-btn", className="btn btn-secondary"),
                        html.Div(id="data-update-message", className="preset-message"),
                        html.Hr(className="divider"),
                        html.Div("필터", className="card-title"),
                        html.Label("섹션"),
                        dcc.Dropdown(
                            id="section-filter",
                            options=SECTION_OPTIONS,
                            value=[],
                            multi=True,
                            placeholder="섹션 선택",
                        ),
                        html.Label("국가"),
                        dcc.Dropdown(
                            id="country-filter",
                            options=COUNTRY_OPTIONS,
                            value=[],
                            multi=True,
                            placeholder="국가 선택",
                        ),
                        html.Label("제품"),
                        dcc.Dropdown(
                            id="product-filter",
                            options=PRODUCT_OPTIONS,
                            value=[],
                            multi=True,
                            placeholder="제품 선택",
                        ),
                        html.Label("흐름"),
                        dcc.Dropdown(
                            id="flow-filter",
                            options=FLOW_OPTIONS,
                            value=[],
                            multi=True,
                            placeholder="흐름 선택",
                        ),
                        html.Label("단위"),
                        dcc.Dropdown(
                            id="unit-filter",
                            options=UNIT_OPTIONS,
                            value=[],
                            multi=True,
                            placeholder="단위 선택",
                        ),
                        html.Label("시작일"),
                        dcc.DatePickerSingle(
                            id="start-date",
                            date=datetime(2002, 1, 1),
                            display_format="YYYY-MM-DD",
                        ),
                        html.Hr(className="divider"),
                        html.Label("기본 데이터 타입"),
                        dcc.Dropdown(
                            id="global-dtype",
                            options=[{"label": label, "value": key} for key, label in STANDARD_DATA_KEYS],
                            value="raw_data",
                            clearable=False,
                        ),
                        html.Div("개별 시리즈 타입은 테이블에서 조정합니다.", className="helper-text"),
                        html.Label("차트 유형", style={"marginTop": "10px"}),
                        dcc.Dropdown(
                            id="chart-type",
                            options=CHART_TYPE_OPTIONS,
                            value="multi_line",
                            clearable=False,
                        ),
                        html.Label("차트 너비 (cm)", style={"marginTop": "10px"}),
                        dcc.Input(
                            id="chart-width",
                            type="number",
                            value=DEFAULT_CHART_WIDTH_CM,
                            min=15,
                            max=45,
                            step=0.5,
                        ),
                        html.Label("차트 높이 (cm)", style={"marginTop": "10px"}),
                        dcc.Input(
                            id="chart-height",
                            type="number",
                            value=DEFAULT_CHART_HEIGHT_CM,
                            min=10,
                            max=25,
                            step=0.5,
                        ),
                        html.Details(
                            [
                                html.Summary("5년 비교 차트"),
                                dcc.Slider(
                                    id="five-year-recent",
                                    min=3,
                                    max=6,
                                    step=1,
                                    value=5,
                                    marks={i: str(i) for i in range(3, 7)},
                                ),
                            ],
                            open=False,
                            className="axis-box",
                        ),
                        html.Details(
                            [
                                html.Summary("차트 보조 옵션"),
                                dcc.Checklist(
                                    id="zero-line",
                                    options=[{"label": "제로 라인 표시", "value": "on"}],
                                    value=[],
                                ),
                            ],
                            open=False,
                            className="axis-box",
                        ),
                        html.Hr(className="divider"),
                        html.Div("프리셋", className="card-title"),
                        dcc.Dropdown(
                            id="preset-load",
                            options=_get_preset_options(PRESET_CACHE),
                            placeholder="프리셋 선택",
                        ),
                        html.Div(
                            [
                                html.Button("불러오기", id="preset-load-btn", className="btn btn-secondary"),
                                html.Button("삭제", id="preset-delete-btn", className="btn btn-ghost"),
                            ],
                            className="button-row",
                        ),
                        html.Label("프리셋 이름", style={"marginTop": "10px"}),
                        dcc.Input(id="preset-name-input", type="text", value=""),
                        dcc.Checklist(
                            id="preset-overwrite",
                            options=[{"label": "덮어쓰기 허용", "value": "on"}],
                            value=[],
                            className="inline-check",
                            labelStyle={"display": "flex", "alignItems": "center", "gap": "6px"},
                            inputStyle={"marginRight": "6px"},
                            style={"marginTop": "6px"},
                        ),
                        html.Button("저장", id="preset-save-btn", className="btn btn-primary"),
                        html.Div(id="preset-message", className="preset-message"),
                        html.Hr(className="divider"),
                        html.Div("시리즈 선택", className="card-title"),
                        html.Button("선택 초기화", id="series-clear-all", className="btn btn-ghost btn-mini"),
                        dcc.Dropdown(
                            id="series-select",
                            options=SERIES_OPTIONS,
                            value=DEFAULT_SERIES_SELECTION,
                            multi=True,
                            placeholder="시리즈 선택",
                            className="series-select",
                        ),
                    ],
                ),
                html.Div(
                    className="main-panel",
                    children=[
                        html.Div(
                            className="card chart-card",
                            children=[
                                dcc.Graph(
                                    id="chart",
                                    config={
                                        "displaylogo": False,
                                        "modeBarButtonsToAdd": [],
                                        "toImageButtonOptions": {
                                            "format": "png",
                                            "scale": 2,
                                        },
                                    },
                                ),
                                html.Div(
                                    [
                                        html.Button(
                                            "차트 클립보드 복사",
                                            id="copy-chart-btn",
                                            className="btn btn-primary",
                                        ),
                                        html.Div(id="copy-status", className="copy-status"),
                                    ],
                                    className="chart-actions",
                                ),
                            ],
                        ),
                        html.Div(
                            className="card axis-card",
                            children=[
                                html.Div("축 설정", className="card-title"),
                                html.Div(
                                    className="axis-panel",
                                    children=[
                                        html.Details(
                                            [
                                                html.Summary("축 제목"),
                                                html.Div(
                                                    className="axis-fields",
                                                    children=[
                                                        html.Div(
                                                            [
                                                                html.Label("단일 축 제목"),
                                                                dcc.Input(
                                                                    id="single-axis-title",
                                                                    type="text",
                                                                    value="",
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("왼쪽 축 제목"),
                                                                dcc.Input(
                                                                    id="left-axis-title",
                                                                    type="text",
                                                                    value="",
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("오른쪽 축 제목"),
                                                                dcc.Input(
                                                                    id="right-axis-title",
                                                                    type="text",
                                                                    value="",
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("왼쪽 제목 위치 보정"),
                                                                dcc.Input(
                                                                    id="left-title-offset",
                                                                    type="number",
                                                                    value=0.0,
                                                                    step=0.01,
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("오른쪽 제목 위치 보정"),
                                                                dcc.Input(
                                                                    id="right-title-offset",
                                                                    type="number",
                                                                    value=0.0,
                                                                    step=0.01,
                                                                ),
                                                            ]
                                                        ),
                                                    ],
                                                ),
                                            ],
                                            open=True,
                                            className="axis-box",
                                        ),
                                        html.Details(
                                            [
                                                html.Summary("축 범위"),
                                                html.Div(
                                                    className="range-grid",
                                                    children=[
                                                        html.Div(
                                                            [
                                                                dcc.Checklist(
                                                                    id="single-axis-manual",
                                                                    options=[{"label": "단일 축 수동", "value": "on"}],
                                                                    value=[],
                                                                )
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("단일 축 최소"),
                                                                dcc.Input(
                                                                    id="single-axis-min",
                                                                    type="number",
                                                                    step=0.1,
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("단일 축 최대"),
                                                                dcc.Input(
                                                                    id="single-axis-max",
                                                                    type="number",
                                                                    step=0.1,
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                dcc.Checklist(
                                                                    id="left-axis-manual",
                                                                    options=[{"label": "왼쪽 축 수동", "value": "on"}],
                                                                    value=[],
                                                                )
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("왼쪽 축 최소"),
                                                                dcc.Input(
                                                                    id="left-axis-min",
                                                                    type="number",
                                                                    step=0.1,
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("왼쪽 축 최대"),
                                                                dcc.Input(
                                                                    id="left-axis-max",
                                                                    type="number",
                                                                    step=0.1,
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                dcc.Checklist(
                                                                    id="right-axis-manual",
                                                                    options=[{"label": "오른쪽 축 수동", "value": "on"}],
                                                                    value=[],
                                                                )
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("오른쪽 축 최소"),
                                                                dcc.Input(
                                                                    id="right-axis-min",
                                                                    type="number",
                                                                    step=0.1,
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("오른쪽 축 최대"),
                                                                dcc.Input(
                                                                    id="right-axis-max",
                                                                    type="number",
                                                                    step=0.1,
                                                                ),
                                                            ]
                                                        ),
                                                    ],
                                                ),
                                            ],
                                            open=True,
                                            className="axis-box",
                                        ),
                                    ],
                                ),
                            ],
                        ),
                        html.Div(
                            className="card table-card",
                            children=[
                                html.Div("선택된 시리즈", className="card-title"),
                                dash_table.DataTable(
                                    id="series-table",
                                    data=[],
                                    columns=[],
                                    editable=True,
                                    hidden_columns=["key"],
                                    dropdown={},
                                    dropdown_conditional=[],
                                    tooltip_data=[],
                                    tooltip_delay=0,
                                    tooltip_duration=None,
                                    style_table={
                                        "overflowX": "auto",
                                        "height": "240px",
                                        "overflowY": "auto",
                                        "tableLayout": "fixed",
                                    },
                                    style_cell={
                                        "textAlign": "left",
                                        "padding": "6px",
                                        "whiteSpace": "normal",
                                        "wordBreak": "break-word",
                                        "lineHeight": "1.2",
                                        "width": "120px",
                                        "minWidth": "120px",
                                        "maxWidth": "120px",
                                    },
                                    style_cell_conditional=[
                                        {
                                            "if": {"column_id": "series"},
                                            "width": "260px",
                                            "minWidth": "260px",
                                            "maxWidth": "260px",
                                        },
                                        {
                                            "if": {"column_id": "label"},
                                            "width": "260px",
                                            "minWidth": "260px",
                                            "maxWidth": "260px",
                                        },
                                        {
                                            "if": {"column_id": "dtype"},
                                            "width": "120px",
                                            "minWidth": "120px",
                                            "maxWidth": "120px",
                                        },
                                        {
                                            "if": {"column_id": "axis"},
                                            "width": "100px",
                                            "minWidth": "100px",
                                            "maxWidth": "100px",
                                        },
                                    ],
                                    style_data={
                                        "whiteSpace": "normal",
                                        "height": "auto",
                                        "lineHeight": "1.2",
                                    },
                                    style_data_conditional=[
                                        {"if": {"row_index": "odd"}, "backgroundColor": "#f6f2ea"}
                                    ],
                                    style_header={
                                        "fontWeight": "600",
                                        "backgroundColor": "#efe9dd",
                                    },
                                    css=[
                                        {
                                            "selector": ".dash-spreadsheet-container table",
                                            "rule": "table-layout: fixed; width: 100%;",
                                        },
                                    ],
                                ),
                            ],
                        ),
                        html.Div(
                            className="card table-card",
                            children=[
                                html.Details(
                                    open=False,
                                    className="table-details",
                                    children=[
                                        html.Summary("데이터 미리보기", className="card-title"),
                                        html.Div(
                                            [
                                                html.Button(
                                                    "CSV 다운로드",
                                                    id="download-csv-btn",
                                                    className="btn btn-secondary",
                                                )
                                            ],
                                            className="button-row",
                                        ),
                                        dash_table.DataTable(
                                            id="data-table",
                                            data=[],
                                            columns=[],
                                            page_size=10,
                                            style_table={
                                                "overflowX": "auto",
                                                "height": "240px",
                                                "overflowY": "auto",
                                            },
                                            style_cell={
                                                "textAlign": "left",
                                                "padding": "6px",
                                                "minWidth": "90px",
                                            },
                                            style_data_conditional=[
                                                {"if": {"row_index": "odd"}, "backgroundColor": "#f6f2ea"}
                                            ],
                                            style_header={
                                                "fontWeight": "600",
                                                "backgroundColor": "#efe9dd",
                                            },
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        ),
        dcc.Store(id="series-settings-store", data={}),
        dcc.Store(id="full-data-store", data=""),
        dcc.Store(id="data-refresh-store", data=""),
        dcc.Download(id="download-data"),
    ],
)

@app.callback(
    Output("section-filter", "options"),
    Output("country-filter", "options"),
    Output("product-filter", "options"),
    Output("flow-filter", "options"),
    Output("unit-filter", "options"),
    Input("data-refresh-store", "data"),
)
def refresh_filter_options(_):
    _, _, options, _, _, _ = _build_registry_bundle()
    section_options = [{"label": _section_label(code), "value": code} for code in options.get("sections", [])]
    country_options = [{"label": _country_label(code), "value": code} for code in options.get("countries", [])]
    product_options = [{"label": _product_label(code), "value": code} for code in options.get("products", [])]
    flow_options = [{"label": _flow_label(code), "value": code} for code in options.get("flows", [])]
    unit_options = [{"label": _unit_label(code), "value": code} for code in options.get("units", [])]
    return section_options, country_options, product_options, flow_options, unit_options


@app.callback(
    Output("series-select", "options"),
    Output("series-select", "value"),
    Input("section-filter", "value"),
    Input("country-filter", "value"),
    Input("product-filter", "value"),
    Input("flow-filter", "value"),
    Input("unit-filter", "value"),
    Input("data-refresh-store", "data"),
    State("series-select", "value"),
)
def update_series_options(
    sections,
    countries,
    products,
    flows,
    units,
    _,
    current_selection,
):
    _, combos_all, _, _, registry, default_checked = _build_registry_bundle()
    filtered = _filter_combos(combos_all, sections, countries, products, flows, units)
    options = _series_options_from_combos(filtered, registry, current_selection)

    available_keys = [opt["value"] for opt in options]
    selected = [key for key in (current_selection or []) if key in available_keys]
    if not selected:
        fallback = [key for key in default_checked if key in available_keys]
        selected = fallback if fallback else available_keys[:2]
    return options, selected


@app.callback(
    Output("series-select", "value", allow_duplicate=True),
    Input("series-clear-all", "n_clicks"),
    prevent_initial_call=True,
)
def clear_series_selection(n_clicks):
    if not n_clicks:
        return no_update
    return []


@app.callback(
    Output("series-table", "data"),
    Output("series-table", "columns"),
    Output("series-table", "dropdown"),
    Output("series-table", "dropdown_conditional"),
    Output("series-table", "tooltip_data"),
    Input("series-select", "value"),
    Input("global-dtype", "value"),
    State("series-settings-store", "data"),
)
def update_series_table(selected_keys, global_dtype, settings):
    _, _, _, _, registry, _ = _build_registry_bundle()
    selected_infos = _collect_selected_infos(selected_keys, registry)
    rows = _build_table_rows(selected_infos, settings, global_dtype or "raw_data")

    columns = [
        {"name": "Key", "id": "key"},
        {"name": "Series", "id": "series", "editable": False},
        {"name": "Label", "id": "label", "editable": True},
        {"name": "Data type", "id": "dtype", "editable": True, "presentation": "dropdown"},
        {"name": "Axis", "id": "axis", "editable": True, "presentation": "dropdown"},
    ]

    dropdown = {
        "axis": {
            "options": [
                {"label": "Left", "value": "left"},
                {"label": "Right", "value": "right"},
            ]
        },
    }

    dropdown_conditional = []
    for info in selected_infos:
        dtype_options = [
            {"label": DATA_TYPE_LABELS.get(key, key), "value": key}
            for key in info.get("available_types", [])
        ]
        if dtype_options:
            dropdown_conditional.append(
                {
                    "if": {"column_id": "dtype", "filter_query": f'{{key}} eq "{info["key"]}"'},
                    "options": dtype_options,
                }
            )

    tooltips = []
    for row in rows:
        tooltips.append(
            {
                "series": {"value": str(row.get("series") or ""), "type": "text"},
                "label": {"value": str(row.get("label") or ""), "type": "text"},
            }
        )

    return rows, columns, dropdown, dropdown_conditional, tooltips


@app.callback(
    Output("series-settings-store", "data", allow_duplicate=True),
    Input("series-table", "data"),
    State("series-settings-store", "data"),
    State("global-dtype", "value"),
    prevent_initial_call=True,
)
def update_series_settings(table_data, existing, global_dtype):
    return _settings_from_rows(table_data, existing, global_dtype)


@app.callback(
    Output("chart", "figure"),
    Output("data-table", "data"),
    Output("data-table", "columns"),
    Output("full-data-store", "data"),
    Output("status-message", "children"),
    Input("series-select", "value"),
    Input("series-table", "data"),
    Input("chart-type", "value"),
    Input("global-dtype", "value"),
    Input("start-date", "date"),
    Input("chart-width", "value"),
    Input("chart-height", "value"),
    Input("single-axis-title", "value"),
    Input("left-axis-title", "value"),
    Input("right-axis-title", "value"),
    Input("left-title-offset", "value"),
    Input("right-title-offset", "value"),
    Input("single-axis-manual", "value"),
    Input("single-axis-min", "value"),
    Input("single-axis-max", "value"),
    Input("left-axis-manual", "value"),
    Input("left-axis-min", "value"),
    Input("left-axis-max", "value"),
    Input("right-axis-manual", "value"),
    Input("right-axis-min", "value"),
    Input("right-axis-max", "value"),
    Input("five-year-recent", "value"),
    Input("zero-line", "value"),
)
def update_chart(
    selected_keys,
    table_data,
    chart_type,
    global_dtype,
    start_date,
    chart_width_cm,
    chart_height_cm,
    single_axis_title,
    left_axis_title,
    right_axis_title,
    left_title_offset,
    right_title_offset,
    single_axis_manual,
    single_axis_min,
    single_axis_max,
    left_axis_manual,
    left_axis_min,
    left_axis_max,
    right_axis_manual,
    right_axis_min,
    right_axis_max,
    five_year_recent,
    zero_line_value,
):
    _, _, _, _, registry, _ = _build_registry_bundle()
    selected_infos = _collect_selected_infos(selected_keys, registry)
    settings = _settings_from_rows(table_data, global_dtype=global_dtype)

    axis_titles = {
        "single": single_axis_title or "",
        "left": left_axis_title or "",
        "right": right_axis_title or "",
    }
    axis_offsets = {
        "left": float(left_title_offset) if left_title_offset is not None else 0.0,
        "right": float(right_title_offset) if right_title_offset is not None else 0.0,
    }
    manual_ranges = {
        "single": _extract_axis_range("on" in (single_axis_manual or []), single_axis_min, single_axis_max),
        "left": _extract_axis_range("on" in (left_axis_manual or []), left_axis_min, left_axis_max),
        "right": _extract_axis_range("on" in (right_axis_manual or []), right_axis_min, right_axis_max),
    }
    zero_line = "on" in (zero_line_value or [])

    fig, table_df, messages = _build_figure_jodi(
        selected_infos=selected_infos,
        settings=settings,
        chart_type=chart_type or "multi_line",
        global_dtype=global_dtype or "raw_data",
        start_date=start_date,
        chart_width_cm=chart_width_cm,
        chart_height_cm=chart_height_cm,
        axis_titles=axis_titles,
        axis_offsets=axis_offsets,
        manual_ranges=manual_ranges,
        five_year_recent=five_year_recent,
        zero_line=zero_line,
    )

    data_records, data_columns = _prepare_data_table(table_df)
    full_data_json = ""
    if isinstance(table_df, pd.DataFrame) and not table_df.empty:
        full_data_json = table_df.to_json(orient="split", date_format="iso")

    status = ""
    if messages:
        status = html.Ul([html.Li(msg) for msg in messages])

    return fig, data_records, data_columns, full_data_json, status


@app.callback(
    Output("download-data", "data"),
    Input("download-csv-btn", "n_clicks"),
    State("full-data-store", "data"),
    prevent_initial_call=True,
)
def download_csv(n_clicks, json_payload):
    if not n_clicks or not json_payload:
        return no_update
    try:
        df = pd.read_json(StringIO(json_payload), orient="split")
    except ValueError:
        return no_update
    filename = f"jodi_dashboard_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    return dcc.send_data_frame(df.to_csv, filename, index=True, encoding="utf-8-sig")


@app.callback(
    Output("preset-load", "options"),
    Output("preset-load", "value"),
    Output("series-select", "value", allow_duplicate=True),
    Output("global-dtype", "value"),
    Output("chart-type", "value"),
    Output("chart-width", "value"),
    Output("chart-height", "value"),
    Output("single-axis-title", "value"),
    Output("left-axis-title", "value"),
    Output("right-axis-title", "value"),
    Output("left-title-offset", "value"),
    Output("right-title-offset", "value"),
    Output("five-year-recent", "value"),
    Output("single-axis-manual", "value"),
    Output("single-axis-min", "value"),
    Output("single-axis-max", "value"),
    Output("left-axis-manual", "value"),
    Output("left-axis-min", "value"),
    Output("left-axis-max", "value"),
    Output("right-axis-manual", "value"),
    Output("right-axis-min", "value"),
    Output("right-axis-max", "value"),
    Output("zero-line", "value"),
    Output("series-settings-store", "data"),
    Output("preset-message", "children"),
    Output("preset-name-input", "value"),
    Input("preset-load-btn", "n_clicks"),
    Input("preset-save-btn", "n_clicks"),
    Input("preset-delete-btn", "n_clicks"),
    State("preset-load", "value"),
    State("preset-name-input", "value"),
    State("preset-overwrite", "value"),
    State("series-select", "value"),
    State("series-table", "data"),
    State("chart-type", "value"),
    State("global-dtype", "value"),
    State("chart-width", "value"),
    State("chart-height", "value"),
    State("single-axis-title", "value"),
    State("left-axis-title", "value"),
    State("right-axis-title", "value"),
    State("left-title-offset", "value"),
    State("right-title-offset", "value"),
    State("five-year-recent", "value"),
    State("single-axis-manual", "value"),
    State("single-axis-min", "value"),
    State("single-axis-max", "value"),
    State("left-axis-manual", "value"),
    State("left-axis-min", "value"),
    State("left-axis-max", "value"),
    State("right-axis-manual", "value"),
    State("right-axis-min", "value"),
    State("right-axis-max", "value"),
    State("zero-line", "value"),
    prevent_initial_call=True,
)
def handle_presets(
    load_clicks,
    save_clicks,
    delete_clicks,
    preset_value,
    preset_name,
    overwrite_value,
    selected_keys,
    table_data,
    chart_type,
    global_dtype,
    chart_width_cm,
    chart_height_cm,
    single_axis_title,
    left_axis_title,
    right_axis_title,
    left_title_offset,
    right_title_offset,
    five_year_recent,
    single_axis_manual,
    single_axis_min,
    single_axis_max,
    left_axis_manual,
    left_axis_min,
    left_axis_max,
    right_axis_manual,
    right_axis_min,
    right_axis_max,
    zero_line_value,
):
    ctx = dash.callback_context
    triggered = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else None

    options = _get_preset_options(PRESET_CACHE)

    defaults = [
        options,
        preset_value,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
    ]

    if triggered == "preset-delete-btn":
        if not preset_value:
            defaults[24] = "삭제할 프리셋을 선택하세요."
            return defaults
        if preset_value in PRESET_CACHE:
            del PRESET_CACHE[preset_value]
            save_jodi_presets(PRESET_CACHE)
            options = _get_preset_options(PRESET_CACHE)
            defaults[0] = options
            defaults[1] = None
            defaults[24] = f"삭제 완료: {preset_value}"
            return defaults
        defaults[24] = "프리셋을 찾을 수 없습니다."
        return defaults

    if triggered == "preset-save-btn":
        if not preset_name:
            defaults[24] = "프리셋 이름을 입력하세요."
            return defaults
        allow_overwrite = "on" in (overwrite_value or [])
        if preset_name in PRESET_CACHE and not allow_overwrite:
            defaults[24] = "이미 존재하는 프리셋입니다. 덮어쓰기를 허용하세요."
            return defaults

        _, _, _, _, registry, _ = _build_registry_bundle()
        selected_infos = _collect_selected_infos(selected_keys, registry)
        if not selected_infos:
            defaults[24] = "시리즈를 선택한 후 저장하세요."
            return defaults

        settings = _settings_from_rows(table_data, global_dtype=global_dtype)
        dtype_map: dict[str, str] = {}
        custom_labels: dict[str, str] = {}
        series_labels: dict[str, str] = {}
        axis_allocation = {"left": [], "right": []}

        for info in selected_infos:
            key = info["key"]
            base_label = info.get("leaf_label", info.get("display_label", key))
            series_labels[key] = base_label
            label = settings.get(key, {}).get("label") or base_label
            label = str(label).strip() if label else base_label
            custom_labels[key] = label
            axis = settings.get(key, {}).get("axis", "left")
            axis_allocation["right" if axis == "right" else "left"].append(label)

            dtype_choice = settings.get(key, {}).get("dtype") or global_dtype
            if dtype_choice not in info.get("available_types", []):
                dtype_choice = _default_dtype_for_series(info.get("available_types", []))
            dtype_map[key] = dtype_choice

        display_labels = [custom_labels.get(info["key"], info.get("leaf_label", "")) for info in selected_infos]
        axis_label_default = _axis_title_for(
            _dtype_key_to_axis(global_dtype),
            [info["key"] for info in selected_infos],
            {info["key"]: info.get("series_spec", {}) for info in selected_infos},
        )

        single_range = _extract_axis_range("on" in (single_axis_manual or []), single_axis_min, single_axis_max)
        left_range = _extract_axis_range("on" in (left_axis_manual or []), left_axis_min, left_axis_max)
        right_range = _extract_axis_range("on" in (right_axis_manual or []), right_axis_min, right_axis_max)

        snapshot = {
            "series_keys": list(selected_keys or []),
            "series_labels": series_labels,
            "custom_labels": custom_labels,
            "global_dtype_key": global_dtype,
            "dtype_map": dtype_map,
            "chart_type": chart_type,
            "chart_type_label": next(
                (label for label, code in CHART_TYPE_LABELS.items() if code == chart_type),
                chart_type,
            ),
            "chart_width_cm": float(chart_width_cm) if chart_width_cm is not None else DEFAULT_CHART_WIDTH_CM,
            "chart_height_cm": float(chart_height_cm) if chart_height_cm is not None else DEFAULT_CHART_HEIGHT_CM,
            "single_axis_title": single_axis_title or "",
            "left_axis_title": left_axis_title or "",
            "right_axis_title": right_axis_title or "",
            "left_title_offset": float(left_title_offset) if left_title_offset is not None else 0.0,
            "right_title_offset": float(right_title_offset) if right_title_offset is not None else 0.0,
            "dual_axis_left": list(axis_allocation.get("left", [])),
            "dual_axis_right": list(axis_allocation.get("right", [])),
            "five_year_recent_years": five_year_recent,
            "active_series_keys": list(selected_keys or []),
            "display_labels": display_labels,
            "axis_label_default": axis_label_default,
            "single_axis_manual_range": single_range is not None,
            "single_axis_range": single_range,
            "left_axis_manual_range": left_range is not None,
            "left_axis_range": left_range,
            "right_axis_manual_range": right_range is not None,
            "right_axis_range": right_range,
            "zero_line": "on" in (zero_line_value or []),
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "name": preset_name,
        }

        PRESET_CACHE[preset_name] = snapshot
        save_jodi_presets(PRESET_CACHE)
        options = _get_preset_options(PRESET_CACHE)
        defaults[0] = options
        defaults[1] = preset_name
        defaults[24] = f"저장 완료: {preset_name}"
        defaults[25] = preset_name
        return defaults

    if triggered == "preset-load-btn":
        if not preset_value:
            defaults[24] = "불러올 프리셋을 선택하세요."
            return defaults
        preset = PRESET_CACHE.get(preset_value)
        if not preset:
            defaults[24] = "프리셋을 찾을 수 없습니다."
            return defaults

        _, _, _, _, registry, _ = _build_registry_bundle()
        series_keys = preset.get("active_series_keys") or preset.get("series_keys") or []
        available_keys = [key for key in series_keys if key in registry]
        if not available_keys:
            defaults[24] = "프리셋에서 사용할 수 있는 시리즈가 없습니다."
            return defaults

        global_dtype_key = preset.get("global_dtype_key") or "raw_data"
        chart_type_value = preset.get("chart_type") or "multi_line"
        custom_labels = preset.get("custom_labels") or {}
        series_labels = preset.get("series_labels") or {}
        dtype_map = preset.get("dtype_map") or {}
        left_labels = set(preset.get("dual_axis_left") or [])
        right_labels = set(preset.get("dual_axis_right") or [])

        settings: dict[str, dict[str, str]] = {}
        for key in available_keys:
            base_label = series_labels.get(key) or registry[key].get("leaf_label", key)
            label = custom_labels.get(key) or base_label
            axis = "left"
            if label in right_labels:
                axis = "right"
            elif label in left_labels:
                axis = "left"
            dtype_value = dtype_map.get(key, "")
            dtype_override = bool(dtype_value and dtype_value != global_dtype_key)
            settings[key] = {
                "label": label,
                "dtype": dtype_value if dtype_override else "",
                "axis": axis,
                "dtype_override": dtype_override,
            }

        defaults = [
            options,
            preset_value,
            available_keys,
            global_dtype_key,
            chart_type_value,
            preset.get("chart_width_cm", DEFAULT_CHART_WIDTH_CM),
            preset.get("chart_height_cm", DEFAULT_CHART_HEIGHT_CM),
            preset.get("single_axis_title", ""),
            preset.get("left_axis_title", ""),
            preset.get("right_axis_title", ""),
            preset.get("left_title_offset", 0.0),
            preset.get("right_title_offset", 0.0),
            preset.get("five_year_recent_years", 5),
            ["on"] if preset.get("single_axis_manual_range") else [],
            (preset.get("single_axis_range") or [None, None])[0],
            (preset.get("single_axis_range") or [None, None])[1],
            ["on"] if preset.get("left_axis_manual_range") else [],
            (preset.get("left_axis_range") or [None, None])[0],
            (preset.get("left_axis_range") or [None, None])[1],
            ["on"] if preset.get("right_axis_manual_range") else [],
            (preset.get("right_axis_range") or [None, None])[0],
            (preset.get("right_axis_range") or [None, None])[1],
            ["on"] if preset.get("zero_line") else [],
            settings,
            f"불러오기 완료: {preset_value}",
            preset_value,
        ]
        return defaults

    return defaults


@app.callback(
    Output("data-update-message", "children"),
    Output("data-refresh-store", "data"),
    Input("data-update-btn", "n_clicks"),
    prevent_initial_call=True,
)
def update_jodi_data_callback(n_clicks):
    if not n_clicks:
        return no_update, no_update
    success, message = _update_jodi_data(JODI_DATA_DIR)
    if success:
        _clear_jodi_dash_cache()
        try:
            wide_df, combos, options, units_by_series = _compute_processed_views()
            _save_dash_cache(wide_df, combos, options, units_by_series)
        except Exception:
            pass
        return message, datetime.now().isoformat(timespec="seconds")
    return message, no_update


app.clientside_callback(
    """
    async function(n_clicks) {
        if (!n_clicks) {
            return '';
        }
        const container = document.getElementById('chart');
        if (!container) {
            return 'Chart not found.';
        }
        const plotlyDiv = container.querySelector('.js-plotly-plot');
        if (!plotlyDiv) {
            return 'Chart not ready.';
        }
        try {
            if (!navigator.clipboard || !window.ClipboardItem) {
                return 'Clipboard API not available. Use the download button.';
            }
            const dataUrl = await Plotly.toImage(plotlyDiv, {format: 'png', scale: 2});
            const response = await fetch(dataUrl);
            const blob = await response.blob();
            await navigator.clipboard.write([new ClipboardItem({[blob.type]: blob})]);
            return '차트 이미지를 클립보드에 복사했습니다.';
        } catch (err) {
            return '복사 실패: ' + err;
        }
    }
    """,
    Output("copy-status", "children"),
    Input("copy-chart-btn", "n_clicks"),
)

DEFAULT_DASH_PORT = 8051


def _parse_port_flag(args: list[str]) -> Optional[int]:
    for idx, arg in enumerate(args):
        if arg.startswith("--port="):
            candidate = arg.split("=", 1)[1].strip()
        elif arg == "--port" and idx + 1 < len(args):
            candidate = args[idx + 1].strip()
        else:
            continue
        try:
            return int(candidate)
        except ValueError:
            return None
    return None


def _pick_free_port(preferred: int, scan: int = 20) -> int:
    for port in range(preferred, preferred + max(scan, 1)):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    return preferred


def _resolve_dash_port() -> int:
    env_value = os.environ.get("DASH_PORT")
    port = None
    if env_value:
        try:
            port = int(env_value)
        except ValueError:
            port = None
    if port is None:
        port = _parse_port_flag(sys.argv)
    if port is None or port < 1 or port > 65535:
        port = DEFAULT_DASH_PORT
    chosen = _pick_free_port(port)
    if chosen != port:
        print(f"Port {port} is in use. Using {chosen} instead.")
    return chosen


def run_dash_app(debug: bool = True, port: Optional[int] = None) -> None:
    chosen_port = port if port is not None else _resolve_dash_port()
    app.run_server(debug=debug, port=chosen_port)

def _run_cli_demo() -> None:
    print("=== JODI 시각화 도구 (CLI 데모) ===")
    print(f"CSV 위치: {PRIMARY_CSV}\n        {SECONDARY_CSV}")
    ok = load_jodi_data(JODI_SERIES_EXAMPLE, start_date="2002-01-01")
    if ok:
        plot_jodi_series(
            ["US_CRUDE_PROD_KBD", "US_GASOLINE_DEMAND_KBD"],
            chart_type="multi_line",
            data_type="mom",
            labels=JODI_KOREAN_NAMES,
        )


if __name__ == "__main__":
    if "--cli" in sys.argv:
        _run_cli_demo()
    else:
        try:
            run_dash_app()
        except ModuleNotFoundError as exc:  # Graceful fallback when dash is missing
            if getattr(exc, "name", "") == "dash":
                print("dash module not installed. pip install dash and retry.")
                _run_cli_demo()
            else:
                raise
