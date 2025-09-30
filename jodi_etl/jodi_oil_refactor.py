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
import pandas as pd
import numpy as np
from datetime import datetime, date
import warnings
from typing import Optional, Any
import plotly.graph_objects as go
from pathlib import Path
from types import SimpleNamespace

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
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'us_eco'))
    from us_eco_utils import (
        calculate_mom_percent,
        calculate_mom_change,
        calculate_yoy_percent,
        calculate_yoy_change,
        plot_economic_series,
        export_economic_data,
    )
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from macro_strategy.kpds_fig_format_enhanced import (
        get_kpds_color,
        format_date_ticks,
        FONT_SIZE_GENERAL,
        FONT_SIZE_LEGEND,
        FONT_SIZE_ANNOTATION,
        calculate_title_position,
        create_five_year_comparison_chart,
    )

try:
    from streamlit_tree_select import tree_select  # type: ignore
    TREE_AVAILABLE = True
except Exception:
    TREE_AVAILABLE = False


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


def _trigger_rerun() -> None:
    try:
        import streamlit as st

        rerun_candidate = getattr(st, "experimental_rerun", None)
        if callable(rerun_candidate):
            rerun_candidate()
            return
        rerun_candidate = getattr(st, "rerun", None)
        if callable(rerun_candidate):
            rerun_candidate()
            return
        from streamlit.runtime.scriptrunner import RerunException, RerunData  # type: ignore

        raise RerunException(RerunData(None))
    except Exception as exc:
        print(f"⚠️ rerun 호출 실패: {exc}")


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


def apply_jodi_preset(preset: dict[str, Any], registry: dict[str, dict[str, Any]]) -> bool:
    import streamlit as st

    if not preset:
        st.session_state["jodi_preset_error"] = "프리셋 데이터가 비어 있습니다."
        return False

    series_keys = [key for key in preset.get("series_keys", []) if isinstance(key, str)]
    if not series_keys:
        st.session_state["jodi_preset_error"] = "프리셋에 저장된 시리즈가 없습니다."
        return False

    available_keys = [key for key in series_keys if key in registry]
    missing_keys = [key for key in series_keys if key not in registry]
    if not available_keys:
        st.session_state["jodi_preset_error"] = "프리셋에 포함된 시리즈를 찾을 수 없습니다."
        return False

    if missing_keys:
        st.session_state["jodi_preset_warning"] = {
            "name": preset.get("name"),
            "missing": missing_keys,
        }

    st.session_state.pop("jodi_preset_error", None)

    st.session_state["jodi_selection"] = list(available_keys)
    st.session_state["jodi_series_selection"] = list(available_keys)
    st.session_state["jodi_selection_version"] = st.session_state.get("jodi_selection_version", 0) + 1
    st.session_state["jodi_selection_source"] = "preset"

    custom_labels = {
        key: value
        for key, value in (preset.get("custom_labels") or {}).items()
        if isinstance(key, str) and isinstance(value, str)
    }
    st.session_state["jodi_custom_labels"] = dict(custom_labels)

    for session_key in list(st.session_state.keys()):
        if session_key.startswith("jodi_custom_label_input_"):
            base_key = session_key.replace("jodi_custom_label_input_", "", 1)
            if base_key not in custom_labels:
                st.session_state.pop(session_key, None)
    for key, label in custom_labels.items():
        st.session_state[f"jodi_custom_label_input_{key}"] = label

    dtype_key = preset.get("global_dtype_key")
    if dtype_key:
        st.session_state["jodi_dtype_key"] = dtype_key

    per_override = bool(preset.get("per_series_override"))
    st.session_state["jodi_dtype_override"] = per_override

    dtype_map = preset.get("dtype_map", {}) or {}
    for session_key in list(st.session_state.keys()):
        if session_key.startswith("jodi_dtype_override_"):
            base_key = session_key.replace("jodi_dtype_override_", "", 1)
            if base_key not in dtype_map:
                st.session_state.pop(session_key, None)
    for key, value in dtype_map.items():
        if isinstance(key, str) and isinstance(value, str):
            st.session_state[f"jodi_dtype_override_{key}"] = value

    chart_type_code = preset.get("chart_type")
    if chart_type_code:
        st.session_state["jodi_chart_type"] = chart_type_code

    chart_width = preset.get("chart_width_cm")
    chart_height = preset.get("chart_height_cm")
    if chart_width is not None:
        st.session_state["jodi_chart_width"] = float(chart_width)
    if chart_height is not None:
        st.session_state["jodi_chart_height"] = float(chart_height)

    st.session_state["jodi_single_axis_title"] = preset.get("single_axis_title", "")
    st.session_state["jodi_left_axis_title"] = preset.get("left_axis_title", "")
    st.session_state["jodi_right_axis_title"] = preset.get("right_axis_title", "")

    st.session_state["jodi_left_title_offset"] = float(preset.get("left_title_offset", 0.0))
    st.session_state["jodi_right_title_offset"] = float(preset.get("right_title_offset", 0.0))

    st.session_state["jodi_dual_left"] = list(preset.get("dual_axis_left", []) or [])
    st.session_state["jodi_dual_right"] = list(preset.get("dual_axis_right", []) or [])

    single_manual = bool(preset.get("single_axis_manual_range"))
    st.session_state["jodi_single_axis_manual_range"] = single_manual
    if single_manual:
        rng = preset.get("single_axis_range")
        if isinstance(rng, (list, tuple)) and len(rng) == 2:
            try:
                st.session_state["jodi_single_axis_min"] = float(rng[0])
                st.session_state["jodi_single_axis_max"] = float(rng[1])
            except (TypeError, ValueError):
                pass

    left_manual = bool(preset.get("left_axis_manual_range"))
    st.session_state["jodi_left_axis_manual_range"] = left_manual
    if left_manual:
        rng = preset.get("left_axis_range")
        if isinstance(rng, (list, tuple)) and len(rng) == 2:
            try:
                st.session_state["jodi_left_axis_min"] = float(rng[0])
                st.session_state["jodi_left_axis_max"] = float(rng[1])
            except (TypeError, ValueError):
                pass

    right_manual = bool(preset.get("right_axis_manual_range"))
    st.session_state["jodi_right_axis_manual_range"] = right_manual
    if right_manual:
        rng = preset.get("right_axis_range")
        if isinstance(rng, (list, tuple)) and len(rng) == 2:
            try:
                st.session_state["jodi_right_axis_min"] = float(rng[0])
                st.session_state["jodi_right_axis_max"] = float(rng[1])
            except (TypeError, ValueError):
                pass

    st.session_state["jodi_active_series_override"] = list(preset.get("active_series_keys", []) or [])

    st.session_state["jodi_preset_loaded"] = preset.get("name")

    _trigger_rerun()
    return True


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
    axis_allocation: dict,
    label_map: dict,
    data_type: str,
    series_defs: dict,
    chart_width: int | None = None,
    chart_height: int | None = None,
    left_title_offset: float = 0.0,
    right_title_offset: float = 0.0,
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

    left_title = _axis_title_for(data_type, left_cols, series_defs)
    right_title = _axis_title_for(data_type, right_cols, series_defs)

    left_range = _compute_axis_range([working_df[col] for col in left_cols])
    right_range = _compute_axis_range([working_df[col] for col in right_cols])

    font_family = "NanumGothic"
    fig = go.Figure()

    for idx, col in enumerate(left_cols):
        fig.add_trace(
            go.Scatter(
                x=working_df.index,
                y=working_df[col],
                name=label_map.get(col, col),
                line=dict(color=get_kpds_color(idx)),
                yaxis="y",
            )
        )

    for idx, col in enumerate(right_cols):
        color_idx = len(left_cols) + idx
        fig.add_trace(
            go.Scatter(
                x=working_df.index,
                y=working_df[col],
                name=label_map.get(col, col),
                line=dict(color=get_kpds_color(color_idx)),
                yaxis="y2",
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
        )
    )

    fig = format_date_ticks(fig, "%b-%y", "auto", working_df.index)

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

    if data_type in {"mom", "yoy", "mom_change", "yoy_change"}:
        fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)

    fig.update_layout(title=None)

    # Remove any remaining annotations with text "undefined"
    fig.update_layout(annotations=[a for a in fig.layout.annotations if getattr(a, "text", None) not in ("undefined", None)])

    return _sanitize_plotly_figure(fig)


def _make_monthly_five_year_format(series: pd.Series, history_years: int = 5) -> pd.DataFrame | None:
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


def _load_split_section(section: str) -> pd.DataFrame | None:
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


def build_series_dataframe_from_df(df: pd.DataFrame, series_defs: dict, start_date: str = "2002-01-01", sections: list | None = None) -> pd.DataFrame:
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


def run_streamlit_app():
    """Launch an interactive Streamlit app for JODI oil data."""

    import streamlit as st

    st.set_page_config(page_title="JODI 석유 데이터 시각화", layout="wide")
    st.title("JODI 석유 데이터 시각화")
    st.caption("원하는 국가/품목/흐름을 선택하고 다양한 데이터 변환으로 시각화하세요.")

    update_notice = st.session_state.pop("jodi_data_update_notice", None)
    if isinstance(update_notice, dict):
        status = update_notice.get("status")
        message = update_notice.get("message", "데이터 업데이트 상태를 확인할 수 없습니다.")
        if status == "success":
            st.success(message)
        elif status == "error":
            st.error(message)
        else:
            st.info(message)

    @st.cache_data(show_spinner=False)
    def _load_processed_views():
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
            )["VALUE_NUM"].sum().astype("float32")
        )
        wide = grouped.unstack([
            "SECTION",
            "REF_AREA",
            "ENERGY_PRODUCT",
            "FLOW_BREAKDOWN",
            "UNIT_MEASURE",
        ])
        wide = wide.sort_index()

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
            if (recent.abs() > 1e-6).any():
                return True
            return False

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

        units_by_series = {}
        for tup, label in zip(wide.columns, combos["DISPLAY_LABEL"]):
            units_by_series[label] = units_by_series.get(label, set()) | {tup[4]}

        options = {
            "sections": sorted(combos["SECTION"].dropna().astype(str).unique().tolist()),
            "countries": sorted(combos["REF_AREA"].dropna().astype(str).unique().tolist()),
            "products": sorted(combos["ENERGY_PRODUCT"].dropna().astype(str).unique().tolist()),
            "flows": sorted(combos["FLOW_BREAKDOWN"].dropna().astype(str).unique().tolist()),
        }

        return wide.astype("float32"), combos, options, units_by_series

    wide_df, combos_all, options, units_by_series = _load_processed_views()
    combos_filtered = combos_all.copy()
    registry: dict[str, dict[str, Any]] = {}
    tree_nodes: list[dict[str, Any]] = []
    default_checked: list[str] = []

    with st.sidebar:
        st.header("데이터 관리")
        if st.button("데이터 업데이트", key="jodi_data_update_button"):
            with st.spinner("JODI 데이터를 업데이트하는 중입니다..."):
                success, message = _update_jodi_data(JODI_DATA_DIR)
            if success:
                st.session_state["jodi_data_update_notice"] = {
                    "status": "success",
                    "message": message or "JODI 데이터 업데이트가 완료되었습니다.",
                }
                try:
                    _load_processed_views.clear()
                except Exception:
                    pass
                _trigger_rerun()
            else:
                st.error(message or "데이터 업데이트에 실패했습니다.")

        st.header("시각화 옵션")

        chart_type = st.selectbox(
            "차트 유형",
            (
                ("multi_line", "멀티 라인"),
                ("single_line", "단일 라인"),
                ("dual_axis", "이중 축"),
                ("horizontal_bar", "가로 바"),
                ("vertical_bar", "세로 바"),
                ("five_year", "5년 비교 (월간)"),
            ),
            format_func=lambda x: x[1],
            key="jodi_chart_type_select",
        )[0]

        chart_width_cm = st.slider("차트 너비 (cm)", min_value=15.0, max_value=45.0, value=24.0, step=0.5)
        chart_height_cm = st.slider("차트 높이 (cm)", min_value=10.0, max_value=25.0, value=12.0, step=0.5)
        chart_width = int(chart_width_cm * PX_PER_CM)
        chart_height = int(chart_height_cm * PX_PER_CM)
        left_title_offset = st.slider("왼쪽 축 제목 위치 보정", min_value=-0.2, max_value=0.2, value=0.0, step=0.01)
        right_title_offset = st.slider("오른쪽 축 제목 위치 보정", min_value=-0.2, max_value=0.2, value=0.0, step=0.01)
        five_year_recent_years = None
        if chart_type == "five_year":
            five_year_recent_years = st.slider("최근 비교 연도 수", min_value=3, max_value=6, value=5, step=1)
        st.session_state["jodi_chart_type"] = chart_type
        st.session_state["jodi_chart_width_cm"] = chart_width_cm
        st.session_state["jodi_chart_height_cm"] = chart_height_cm
        st.session_state["jodi_left_title_offset"] = left_title_offset
        st.session_state["jodi_right_title_offset"] = right_title_offset

        custom_single_axis_title = ""
        custom_left_axis_title = ""
        custom_right_axis_title = ""
        if chart_type in {"multi_line", "single_line", "horizontal_bar", "vertical_bar", "five_year"}:
            custom_single_axis_title = st.text_input(
                "축 제목 (단위 Annotation)",
                value=st.session_state.get("jodi_single_axis_title", ""),
                key="jodi_single_axis_title",
            )
        if chart_type == "dual_axis":
            custom_left_axis_title = st.text_input(
                "왼쪽 축 제목",
                value=st.session_state.get("jodi_left_axis_title", ""),
                key="jodi_left_axis_title",
            )
            custom_right_axis_title = st.text_input(
                "오른쪽 축 제목",
                value=st.session_state.get("jodi_right_axis_title", ""),
                key="jodi_right_axis_title",
            )

        st.markdown("---")
        st.header("시리즈 선택")

        default_country = ["US"] if "US" in options["countries"] else []
        selected_countries = st.multiselect(
            "국가 (ISO 2자리)",
            options["countries"],
            default=default_country,
            format_func=_country_label,
        )

        start_date_input = st.date_input(
            "시작일",
            value=datetime(2002, 1, 1),
            format="YYYY-MM-DD",
        )
        start_date_str = start_date_input.strftime("%Y-%m-%d")
        target_date: Optional[str] = None
        periods: Optional[int] = None

        if selected_countries:
            combos_filtered = combos_filtered[combos_filtered["REF_AREA"].isin(selected_countries)]

        combos_sorted = combos_filtered.sort_values(
            ["SECTION", "REF_AREA", "ENERGY_PRODUCT", "FLOW_BREAKDOWN", "UNIT_MEASURE"]
        ).reset_index(drop=True)

        registry, tree_nodes, default_checked = build_jodi_series_registry(combos_sorted)

        if not registry:
            st.info("선택한 조건에 해당하는 시리즈가 없습니다.")
        else:
            session_store_key = "jodi_series_selection"
            if session_store_key not in st.session_state:
                st.session_state[session_store_key] = list(default_checked)

            if "jodi_selection" not in st.session_state or not st.session_state["jodi_selection"]:
                st.session_state["jodi_selection"] = list(st.session_state[session_store_key])
            if "jodi_selection_version" not in st.session_state:
                st.session_state["jodi_selection_version"] = 0
            if "jodi_selection_source" not in st.session_state:
                st.session_state["jodi_selection_source"] = "init"

            selected_series_keys = list(st.session_state["jodi_selection"])

            tree_widget_key = "jodi_series_tree"
            tree_version_key = "jodi_tree_version"
            if tree_version_key not in st.session_state:
                st.session_state[tree_version_key] = 0

            if st.button("선택 초기화", key="jodi_selection_reset"):
                st.session_state["jodi_selection"] = []
                st.session_state[session_store_key] = []
                st.session_state["jodi_selection_version"] += 1
                st.session_state["jodi_selection_source"] = "reset"
                st.session_state.pop(tree_widget_key, None)
                st.session_state[tree_version_key] += 1
                selected_series_keys = []

            if TREE_AVAILABLE:
                if st.session_state.get("jodi_selection_source") != "tree":
                    st.session_state.pop(tree_widget_key, None)
                tree_result = tree_select(
                    nodes=tree_nodes,
                    check_model="leaf",
                    only_leaf_checkboxes=True,
                    show_expand_all=True,
                    checked=selected_series_keys,
                    key=f"{tree_widget_key}_{st.session_state[tree_version_key]}",
                )
                selected_from_tree = tree_result.get("checked", []) if isinstance(tree_result, dict) else []
                if set(selected_from_tree) != set(st.session_state["jodi_selection"]):
                    st.session_state["jodi_selection"] = selected_from_tree
                    st.session_state["jodi_selection_version"] += 1
                st.session_state["jodi_selection_source"] = "tree"
                selected_series_keys = list(st.session_state["jodi_selection"])
            else:
                st.info("streamlit-tree-select 미설치: 기본 멀티 선택 UI 사용")
                label_to_key = {info["leaf_label"]: key for key, info in registry.items()}
                default_labels = [registry[key]["leaf_label"] for key in selected_series_keys if key in registry]
                selected_labels = st.multiselect(
                    "시리즈",
                    options=sorted(label_to_key.keys()),
                    default=default_labels or [registry[key]["leaf_label"] for key in default_checked if key in registry],
                    key="jodi_series_fallback",
                )
                selected_manual = [label_to_key[label] for label in selected_labels]
                if set(selected_manual) != set(st.session_state["jodi_selection"]):
                    st.session_state["jodi_selection"] = selected_manual
                    st.session_state["jodi_selection_version"] += 1
                st.session_state["jodi_selection_source"] = "sidebar"
                selected_series_keys = list(st.session_state["jodi_selection"])

            st.session_state[session_store_key] = list(selected_series_keys)

    combos_display = combos_filtered.sort_values(
        ["SECTION", "REF_AREA", "ENERGY_PRODUCT", "FLOW_BREAKDOWN", "UNIT_MEASURE"]
    ).reset_index(drop=True)
    localized_table = _localized_combo_dataframe(combos_display)

    if not registry:
        st.warning("선택한 조건에 사용할 수 있는 시리즈가 없습니다.")
        st.dataframe(localized_table)
        return

    presets = load_jodi_presets()
    preset_error = st.session_state.pop("jodi_preset_error", None)
    if preset_error:
        st.error(preset_error)

    preset_warning_payload = st.session_state.pop("jodi_preset_warning", None)
    if isinstance(preset_warning_payload, dict):
        missing_list = preset_warning_payload.get("missing", [])
        if missing_list:
            name = preset_warning_payload.get("name") or "프리셋"
            st.warning(
                f"'{name}' 프리셋에서 일부 시리즈를 찾지 못했습니다: "
                + ", ".join(missing_list)
            )

    tab_labels = ["현재 설정"] + [f"프리셋 · {name}" for name in sorted(presets.keys())] + ["프리셋 관리"]
    tab_refs = st.tabs(tab_labels)
    current_tab = tab_refs[0]
    preset_management_tab = tab_refs[-1]
    preset_tab_map = {
        name: tab_refs[idx + 1]
        for idx, name in enumerate(sorted(presets.keys()))
    }

    for preset_name, tab in preset_tab_map.items():
        payload = presets.get(preset_name) or {}
        with tab:
            st.subheader(preset_name)
            saved_at = payload.get("saved_at")
            if saved_at:
                st.caption(f"저장 시각: {saved_at}")
            st.write(f"- 시리즈: {len(payload.get('series_keys', []))}개")
            if st.button("이 프리셋 불러오기", key=f"apply_jodi_preset_{preset_name}"):
                payload_copy = dict(payload)
                payload_copy["name"] = preset_name
                apply_jodi_preset(payload_copy, registry)

    selected_series_keys = list(st.session_state.get("jodi_selection", []))
    selected_infos = [registry[key] for key in selected_series_keys if key in registry]

    if not selected_infos:
        with current_tab:
            st.info("시각화할 시리즈를 선택하세요.")
            st.dataframe(localized_table)
        return

    label_order = [info.get("leaf_label", info["key"]) for info in selected_infos]
    label_to_info = {info.get("leaf_label", info["key"]): info for info in selected_infos}

    custom_label_state = st.session_state.setdefault("jodi_custom_labels", {})
    active_keys_set = {info["key"] for info in selected_infos}
    for stale_key in list(custom_label_state.keys()):
        if stale_key not in active_keys_set:
            custom_label_state.pop(stale_key, None)
            st.session_state.pop(f"jodi_custom_label_input_{stale_key}", None)

    with st.sidebar.expander("시리즈 레이블 편집", expanded=False):
        for info in selected_infos:
            base_label = info.get("leaf_label", info["key"])
            storage_key = info["key"]
            input_key = f"jodi_custom_label_input_{storage_key}"
            default_label = custom_label_state.get(storage_key, base_label)
            if input_key not in st.session_state:
                st.session_state[input_key] = default_label
            current_value = st.text_input(base_label, key=input_key)
            clean_value = current_value.strip()
            if clean_value and clean_value != base_label:
                custom_label_state[storage_key] = clean_value
            else:
                custom_label_state.pop(storage_key, None)

    effective_label_map: dict[str, str] = {}
    for info in selected_infos:
        base_label = info.get("leaf_label", info["key"])
        chosen = custom_label_state.get(info["key"], "").strip()
        final_label = chosen if chosen else base_label
        effective_label_map[base_label] = final_label
        info["effective_label"] = final_label

    sidebar_key = f"jodi_active_labels_{st.session_state.get('jodi_selection_version', 0)}"
    override_keys = st.session_state.pop("jodi_active_series_override", None)
    if override_keys:
        override_labels = []
        key_to_info = {info["key"]: info for info in selected_infos}
        for key in override_keys:
            info = key_to_info.get(key)
            if info:
                override_labels.append(info.get("effective_label", info.get("leaf_label", key)))
        if override_labels:
            st.session_state[sidebar_key] = override_labels

    active_labels = st.sidebar.multiselect(
        "현재 선택된 시리즈",
        options=label_order,
        default=label_order,
        key=sidebar_key,
        help="체크 해제하면 해당 시리즈가 제외됩니다.",
    )

    if not active_labels:
        st.sidebar.warning("최소 한 개의 시리즈를 선택하세요.")
        active_labels = label_order

    active_keys = [label_to_info[label]["key"] for label in active_labels if label in label_to_info]
    if set(active_keys) != set(selected_series_keys):
        st.session_state["jodi_selection"] = active_keys
        st.session_state["jodi_selection_version"] = st.session_state.get("jodi_selection_version", 0) + 1
        st.session_state["jodi_selection_source"] = "sidebar"
        st.session_state["jodi_series_selection"] = list(active_keys)
        selected_series_keys = list(active_keys)
        selected_infos = [registry[key] for key in selected_series_keys if key in registry]
        if not selected_infos:
            with current_tab:
                st.warning("선택된 시리즈가 없습니다.")
                st.dataframe(localized_table)
            return
        label_order = [info.get("leaf_label", info["key"]) for info in selected_infos]
        label_to_info = {info.get("leaf_label", info["key"]): info for info in selected_infos}

    data_type_labels = {key: label for key, label in STANDARD_DATA_KEYS}
    available_union = [
        key
        for key, _ in STANDARD_DATA_KEYS
        if any(key in info.get("available_types", []) for info in selected_infos)
    ]
    if not available_union:
        with current_tab:
            st.warning("선택한 시리즈에서 사용할 수 있는 데이터 타입이 없습니다.")
        return

    default_dtype_key = "raw_data" if "raw_data" in available_union else available_union[0]
    global_dtype_key = st.sidebar.selectbox(
        "데이터 변환",
        options=available_union,
        index=available_union.index(default_dtype_key),
        format_func=lambda key: data_type_labels.get(key, key),
        key="jodi_dtype_key",
        help="선택한 모든 시리즈에 적용할 기본 데이터 변환입니다.",
    )

    per_series_override = False
    if len(available_union) > 1 and len(selected_infos) > 1:
        per_series_override = st.sidebar.checkbox(
            "시리즈별 데이터 타입 지정",
            value=st.session_state.get("jodi_dtype_override", False),
            key="jodi_dtype_override",
        )

    dtype_map: dict[str, str] = {}
    fallback_messages: list[str] = []
    if per_series_override:
        st.sidebar.markdown("#### 시리즈 데이터 타입")
        for info in selected_infos:
            allowed_options = [key for key, _ in STANDARD_DATA_KEYS if key in info.get("available_types", [])]
            if not allowed_options:
                continue
            default_index = allowed_options.index(global_dtype_key) if global_dtype_key in allowed_options else 0
            dtype_choice = st.sidebar.selectbox(
                info.get("effective_label", info.get("leaf_label", info["key"])),
                options=allowed_options,
                index=default_index,
                format_func=lambda key, labels=data_type_labels: labels.get(key, key),
                key=f"jodi_dtype_override_{info['key']}",
            )
            dtype_map[info["key"]] = dtype_choice
    else:
        for info in selected_infos:
            allowed = info.get("available_types", [])
            dtype_choice = global_dtype_key if global_dtype_key in allowed else (allowed[0] if allowed else "")
            if not dtype_choice:
                continue
            if dtype_choice != global_dtype_key:
                fallback_messages.append(
                    f"{info.get('leaf_label', info['key'])} → {data_type_labels.get(dtype_choice, dtype_choice)}"
                )
            dtype_map[info["key"]] = dtype_choice

    if not dtype_map:
        with current_tab:
            st.warning("선택한 시리즈에 대한 데이터 타입을 결정할 수 없습니다.")
        return

    series_defs = {info["key"]: info["series_spec"] for info in selected_infos}
    selected_tuples = [info["column_tuple"] for info in selected_infos]

    start_ts = pd.to_datetime(start_date_str)
    try:
        series_df = wide_df.loc[wide_df.index >= start_ts, selected_tuples]
    except KeyError:
        with current_tab:
            st.warning("선택한 시리즈에서 데이터를 찾을 수 없습니다.")
            st.dataframe(localized_table)
        return

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

    combined_df, label_dtype = _build_combined_dataframe_jodi(selected_infos, dtype_map, data_pack)
    if effective_label_map:
        combined_df = combined_df.rename(columns=effective_label_map)
    if combined_df.empty:
        with current_tab:
            st.warning("선택한 시리즈 조합에 대한 데이터가 없습니다.")
        return

    combined_df = combined_df.dropna(how="all").dropna(axis=1, how="all")
    if combined_df.empty:
        with current_tab:
            st.warning("선택한 시리즈의 유효한 데이터가 없습니다.")
        return

    for info in selected_infos:
        label = info.get("effective_label", info.get("leaf_label", info["key"]))
        if label in combined_df.columns:
            continue
        base_label = info.get("leaf_label", info["key"])
        if base_label in combined_df.columns and label != base_label:
            combined_df = combined_df.rename(columns={base_label: label})

    valid_labels = set(combined_df.columns)
    selected_infos = [info for info in selected_infos if info.get("effective_label", info.get("leaf_label", info["key"])) in valid_labels]
    if not selected_infos:
        with current_tab:
            st.warning("선택한 시리즈의 데이터가 모두 비어 있습니다.")
        return

    if fallback_messages:
        st.sidebar.caption(
            "기본 타입을 지원하지 않는 시리즈는 다음 타입으로 대체되었습니다:\n- "
            + "\n- ".join(fallback_messages)
        )

    display_labels = list(combined_df.columns)
    frequency_map: dict[str, str] = {}
    for info in selected_infos:
        label = info.get("effective_label", info.get("leaf_label", info["key"]))
        series = combined_df.get(label)
        if series is None:
            continue
        frequency_map[label] = _infer_series_period(series.dropna())

    series_type_map = {info.get("effective_label", info.get("leaf_label", info["key"])): dtype_map.get(info["key"], "") for info in selected_infos}
    dtype_set = {series_type_map[label] for label in display_labels if series_type_map.get(label)}

    axis_label_default = _axis_title_for(
        data_type=global_dtype_key[:-5] if global_dtype_key.endswith("_data") else global_dtype_key,
        keys=[info["key"] for info in selected_infos],
        series_defs=series_defs,
    )
    if not axis_label_default and len(dtype_set) == 1:
        axis_label_default = data_type_labels.get(next(iter(dtype_set)), "")

    axis_allocation: Optional[dict[str, list[str]]] = None
    if chart_type == "dual_axis":
        st.sidebar.subheader("이중축 배치")
        default_left_labels = st.session_state.get("jodi_dual_left") or display_labels[: max(1, len(display_labels) // 2)]
        left_selected_labels = st.sidebar.multiselect(
            "왼쪽 축",
            display_labels,
            default=default_left_labels,
            key="jodi_dual_left",
        )
        right_default_labels = st.session_state.get("jodi_dual_right") or [label for label in display_labels if label not in left_selected_labels]
        if not right_default_labels and display_labels:
            right_default_labels = display_labels[-1:]
        right_selected_labels = st.sidebar.multiselect(
            "오른쪽 축",
            display_labels,
            default=right_default_labels,
            key="jodi_dual_right",
        )

        left_clean = [label for label in left_selected_labels if label in display_labels]
        if not left_clean and display_labels:
            left_clean = [display_labels[0]]
        right_clean = [label for label in right_selected_labels if label in display_labels and label not in left_clean]
        leftovers = [label for label in display_labels if label not in left_clean and label not in right_clean]
        if not right_clean:
            if leftovers:
                right_clean = leftovers
            elif len(left_clean) > 1:
                right_clean = [left_clean.pop()]

        axis_allocation = {"left": left_clean, "right": right_clean}

    custom_single_axis_title = st.session_state.get("jodi_single_axis_title", "")
    custom_left_axis_title = st.session_state.get("jodi_left_axis_title", "")
    custom_right_axis_title = st.session_state.get("jodi_right_axis_title", "")

    single_axis_range: Optional[list[float]] = None
    left_axis_range_override: Optional[list[float]] = None
    right_axis_range_override: Optional[list[float]] = None

    if chart_type == "dual_axis" and axis_allocation:
        left_auto_range = _compute_axis_range([combined_df[col] for col in axis_allocation.get("left", []) if col in combined_df.columns]) or [0.0, 1.0]
        right_auto_range = _compute_axis_range([combined_df[col] for col in axis_allocation.get("right", []) if col in combined_df.columns]) or [0.0, 1.0]

        left_manual = st.sidebar.checkbox(
            "왼쪽 축 범위 직접 설정",
            value=st.session_state.get("jodi_left_axis_manual_range", False),
            key="jodi_left_axis_manual_range",
        )
        if left_manual:
            left_min = st.sidebar.number_input(
                "왼쪽 축 최소값",
                value=float(st.session_state.get("jodi_left_axis_min", left_auto_range[0])),
                step=0.1,
                key="jodi_left_axis_min",
            )
            left_max = st.sidebar.number_input(
                "왼쪽 축 최대값",
                value=float(st.session_state.get("jodi_left_axis_max", left_auto_range[1])),
                step=0.1,
                key="jodi_left_axis_max",
            )
            if left_min < left_max:
                left_axis_range_override = [float(left_min), float(left_max)]
            else:
                st.sidebar.warning("왼쪽 축 최소/최대값을 확인하세요.")

        right_manual = st.sidebar.checkbox(
            "오른쪽 축 범위 직접 설정",
            value=st.session_state.get("jodi_right_axis_manual_range", False),
            key="jodi_right_axis_manual_range",
        )
        if right_manual:
            right_min = st.sidebar.number_input(
                "오른쪽 축 최소값",
                value=float(st.session_state.get("jodi_right_axis_min", right_auto_range[0])),
                step=0.1,
                key="jodi_right_axis_min",
            )
            right_max = st.sidebar.number_input(
                "오른쪽 축 최대값",
                value=float(st.session_state.get("jodi_right_axis_max", right_auto_range[1])),
                step=0.1,
                key="jodi_right_axis_max",
            )
            if right_min < right_max:
                right_axis_range_override = [float(right_min), float(right_max)]
            else:
                st.sidebar.warning("오른쪽 축 최소/최대값을 확인하세요.")
    else:
        auto_range = _compute_axis_range([combined_df[col] for col in combined_df.columns]) or [0.0, 1.0]
        manual_enabled = st.sidebar.checkbox(
            "Y축 범위 직접 설정",
            value=st.session_state.get("jodi_single_axis_manual_range", False),
            key="jodi_single_axis_manual_range",
        )
        if manual_enabled:
            axis_min_val = st.sidebar.number_input(
                "Y축 최소값",
                value=float(st.session_state.get("jodi_single_axis_min", auto_range[0])),
                step=0.1,
                key="jodi_single_axis_min",
            )
            axis_max_val = st.sidebar.number_input(
                "Y축 최대값",
                value=float(st.session_state.get("jodi_single_axis_max", auto_range[1])),
                step=0.1,
                key="jodi_single_axis_max",
            )
            if axis_min_val < axis_max_val:
                single_axis_range = [float(axis_min_val), float(axis_max_val)]
            else:
                st.sidebar.warning("Y축 최소/최대값을 확인하세요.")
        else:
            st.session_state.pop("jodi_single_axis_min", None)
            st.session_state.pop("jodi_single_axis_max", None)

    if target_date:
        try:
            parsed_date = pd.to_datetime(target_date)
            combined_df = combined_df.loc[combined_df.index <= parsed_date]
        except Exception:
            with current_tab:
                st.warning(f"잘못된 날짜 형식입니다: {target_date}. 'YYYY-MM-DD' 형식을 사용하세요.")
            return
        if combined_df.empty:
            with current_tab:
                st.warning(f"{target_date} 이전의 데이터가 없습니다.")
            return

    if periods:
        combined_df = combined_df.tail(periods)
        if combined_df.empty:
            with current_tab:
                st.warning("선택한 기간에서 표시할 데이터가 없습니다.")
            return

    label_map_keys = {info["key"]: info.get("effective_label", info.get("leaf_label", info["key"])) for info in selected_infos}

    summary_rows = []
    for info in selected_infos:
        dtype_key = dtype_map.get(info["key"], "")
        summary_rows.append(
            {
                "섹션": _section_label(info.get("section", "")),
                "국가": _country_label(info.get("country", "")),
                "제품": _product_label(info.get("product", "")),
                "흐름": _flow_label(info.get("flow", "")),
                "단위": _unit_label(info.get("unit", "")),
                "표시 이름": label_map_keys[info["key"]],
                "데이터 타입": data_type_labels.get(dtype_key, dtype_key),
            }
        )

    snapshot = {
        "series_keys": list(selected_series_keys),
        "custom_labels": dict(custom_label_state),
        "series_info": summary_rows,
        "global_dtype_key": global_dtype_key,
        "per_series_override": per_series_override,
        "dtype_map": {key: dtype_map.get(key, "") for key in label_map_keys},
        "chart_type": chart_type,
        "chart_type_label": next((label for label, code in CHART_TYPE_LABELS.items() if code == chart_type), chart_type),
        "chart_width_cm": chart_width_cm,
        "chart_height_cm": chart_height_cm,
        "single_axis_title": custom_single_axis_title,
        "left_axis_title": custom_left_axis_title,
        "right_axis_title": custom_right_axis_title,
        "left_title_offset": left_title_offset,
        "right_title_offset": right_title_offset,
        "dual_axis_left": axis_allocation.get("left", []) if axis_allocation else [],
        "dual_axis_right": axis_allocation.get("right", []) if axis_allocation else [],
        "single_axis_manual_range": bool(st.session_state.get("jodi_single_axis_manual_range")),
        "single_axis_range": single_axis_range,
        "left_axis_manual_range": bool(st.session_state.get("jodi_left_axis_manual_range")) if axis_allocation else False,
        "left_axis_range": left_axis_range_override,
        "right_axis_manual_range": bool(st.session_state.get("jodi_right_axis_manual_range")) if axis_allocation else False,
        "right_axis_range": right_axis_range_override,
        "active_series_keys": list(selected_series_keys),
        "display_labels": display_labels,
        "axis_label_default": axis_label_default,
        "frequency_map": frequency_map,
    }

    label_map_for_chart = {info["key"]: label_map_keys[info["key"]] for info in selected_infos}

    zero_line_types = {"mom_data", "yoy_data", "mom_change", "yoy_change"}
    zero_line = bool(display_labels) and all(label_dtype.get(label) in zero_line_types for label in display_labels)
    dtype_values_set = {dtype_map.get(info["key"]) for info in selected_infos if dtype_map.get(info["key"])}
    dtype_uniform = len(dtype_values_set) == 1

    with current_tab:
        st.subheader("선택된 시리즈")
        if summary_rows:
            summary_df = pd.DataFrame(summary_rows)
            st.dataframe(summary_df)

        fig = None
        table_df: Optional[pd.DataFrame] = combined_df.copy()

        if chart_type == "multi_line":
            fig = _create_single_axis_line_chart(
                combined_df,
                custom_single_axis_title or axis_label_default,
                chart_width,
                chart_height,
                zero_line,
                frequency_map,
            )
        elif chart_type == "single_line":
            if len(display_labels) > 1:
                st.warning("단일 라인 차트는 한 개 시리즈만 표시합니다. 첫 번째 시리즈만 사용합니다.")
            single_df = combined_df[[display_labels[0]]] if display_labels else combined_df
            fig = _create_single_axis_line_chart(
                single_df,
                custom_single_axis_title or axis_label_default,
                chart_width,
                chart_height,
                zero_line,
                frequency_map,
            )
        elif chart_type == "dual_axis":
            if not axis_allocation:
                st.warning("이중 축 차트를 위해 왼쪽/오른쪽 축을 설정하세요.")
                return
            left_dtype_keys = {series_type_map.get(label) for label in axis_allocation.get("left", []) if series_type_map.get(label)}
            right_dtype_keys = {series_type_map.get(label) for label in axis_allocation.get("right", []) if series_type_map.get(label)}
            left_axis_dtype = next(iter(left_dtype_keys)).replace("_data", "") if len(left_dtype_keys) == 1 else ""
            right_axis_dtype = next(iter(right_dtype_keys)).replace("_data", "") if len(right_dtype_keys) == 1 else ""
            base_data_type = global_dtype_key.replace("_data", "")
            fig = _create_dual_axis_chart(
                df=combined_df,
                axis_allocation=axis_allocation,
                label_map={label: label for label in display_labels},
                data_type=base_data_type,
                series_defs={info["key"]: {"unit": info.get("unit", "")} for info in selected_infos},
                chart_width=chart_width,
                chart_height=chart_height,
                left_title_offset=left_title_offset,
                right_title_offset=right_title_offset,
                left_axis_data_type=left_axis_dtype,
                right_axis_data_type=right_axis_dtype,
                left_title_override=(custom_left_axis_title or axis_label_default or None),
                right_title_override=(custom_right_axis_title or axis_label_default or None),
                left_axis_range_override=left_axis_range_override,
                right_axis_range_override=right_axis_range_override,
                connect_map=frequency_map,
            )
        elif chart_type == "five_year":
            if len(selected_infos) != 1:
                st.warning("5년 비교 차트는 하나의 시리즈만 선택하세요.")
                return
            info = selected_infos[0]
            unit_label = _axis_title_for("raw", [info["key"]], series_defs)
            series_raw = series_df[info["key"]]
            recent_years = five_year_recent_years or 5
            fig, formatted_df = _create_monthly_five_year_chart(
                series=series_raw,
                series_name=label_map_keys[info["key"]],
                unit_label=unit_label,
                recent_years=recent_years,
                chart_width=chart_width,
                chart_height=chart_height,
            )
            if fig is None or formatted_df is None:
                st.warning("5년 비교 차트를 생성할 수 있는 데이터가 부족합니다.")
                return
            month_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            formatted_df = formatted_df.copy()
            formatted_df.index = [month_labels[i - 1] if 1 <= i <= 12 else str(i) for i in formatted_df.index]
            table_df = formatted_df
        else:
            if not dtype_uniform:
                st.warning("해당 차트 유형은 동일한 데이터 타입을 사용해야 합니다. 기본 데이터 타입을 사용하세요.")
                return
            dtype_value = next(iter(dtype_values_set)) if dtype_values_set else global_dtype_key
            dtype_value = dtype_value[:-5] if dtype_value.endswith("_data") else dtype_value
            fig = plot_economic_series(
                data_dict=data_pack,
                series_list=[info["key"] for info in selected_infos],
                chart_type=chart_type,
                data_type=dtype_value.replace("_data", ""),
                periods=periods,
                target_date=target_date,
                labels=label_map_for_chart,
                korean_names=label_map_for_chart,
                left_ytitle=custom_single_axis_title or axis_label_default or None,
            )
            if fig is not None:
                fig.update_layout(width=chart_width, height=chart_height)

        if fig is not None:
            fig = _sanitize_plotly_figure(fig)
            if chart_type != "dual_axis" and single_axis_range:
                fig.update_yaxes(range=single_axis_range)
            st.plotly_chart(fig, use_container_width=False)
        else:
            st.info("차트를 생성할 수 없습니다. 설정을 확인하세요.")

        st.markdown("## 데이터 테이블")

        if chart_type == "five_year":
            display_df = table_df
            display_df.index.name = "월"
        else:
            if table_df is None or table_df.empty:
                st.warning("표시할 데이터가 없습니다.")
                return
            display_df = table_df
            display_df.index.name = "날짜"

        st.dataframe(display_df)

        csv_data = display_df.to_csv(index=True).encode("utf-8-sig")
        st.download_button(
            label="CSV 다운로드",
            data=csv_data,
            file_name=f"jodi_{global_dtype_key}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )

    with preset_management_tab:
        st.markdown("#### 프리셋 저장")
        if not snapshot.get("series_keys"):
            st.info("저장할 설정이 없습니다. 시리즈를 먼저 선택하세요.")
        else:
            default_name = st.session_state.get("jodi_preset_save_name", "")
            preset_name_input = st.text_input("프리셋 이름", value=default_name, key="jodi_preset_save_name")
            overwrite_allowed = st.checkbox(
                "동일 이름 덮어쓰기 허용",
                value=True,
                key="jodi_preset_overwrite_allowed",
            )
            if st.button("현재 설정 저장", key="jodi_preset_save_button"):
                preset_name = preset_name_input.strip()
                if not preset_name:
                    st.warning("프리셋 이름을 입력하세요.")
                elif not overwrite_allowed and preset_name in presets:
                    st.warning("이미 존재하는 이름입니다. 다른 이름을 사용하거나 덮어쓰기를 허용하세요.")
                else:
                    payload_to_save = dict(snapshot)
                    payload_to_save["saved_at"] = datetime.now().isoformat(timespec="seconds")
                    payload_to_save["name"] = preset_name
                    presets[preset_name] = payload_to_save
                    save_jodi_presets(presets)
                    st.success(f"'{preset_name}' 프리셋을 저장했습니다.")
                    _trigger_rerun()

        st.markdown("#### 프리셋 삭제")
        if presets:
            delete_choice = st.selectbox(
                "삭제할 프리셋",
                sorted(presets.keys()),
                key="jodi_preset_delete_choice",
            )
            if st.button("선택한 프리셋 삭제", key="jodi_preset_delete_button"):
                presets.pop(delete_choice, None)
                save_jodi_presets(presets)
                st.success(f"'{delete_choice}' 프리셋을 삭제했습니다.")
                _trigger_rerun()
        else:
            st.info("저장된 프리셋이 없습니다.")



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
            run_streamlit_app()
        except ModuleNotFoundError as exc:  # Graceful fallback when streamlit is missing
            if getattr(exc, "name", "") == "streamlit":
                print("⚠️ streamlit 모듈이 설치되어 있지 않습니다. pip install streamlit 후 다시 시도하세요.")
                _run_cli_demo()
            else:
                raise
