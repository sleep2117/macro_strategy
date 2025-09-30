"""
OECD Composite Leading Indicator (CLI) utilities

This module fetches OECD CLI monthly series via SDMX, classifies
business-cycle phases (Recovery, Recession, Expansion, Slowdown), and
computes a diffusion index (% of members with MoM>0) for country groups.

Key rules (as provided):
- CLI < 100 and MoM < 0 -> Recession (침체기)
- CLI < 100 and MoM > 0 -> Recovery (회복기)
- CLI >= 100 and MoM > 0 -> Expansion (확장기)
- CLI >= 100 and MoM < 0 -> Slowdown (둔화기)

Outputs include helpers to plot phase bands and diffusion crossings.
"""

from __future__ import annotations

import os
import warnings
from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np
import pandas as pd
import matplotlib
# Use a non-interactive backend for headless environments
try:
    matplotlib.use("Agg", force=True)
except Exception:
    pass
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib import font_manager as _fm
from matplotlib.font_manager import FontProperties as _FontProperties

try:
    import sdmxthon
except Exception as e:  # pragma: no cover
    sdmxthon = None

warnings.filterwarnings("ignore")

def _use_local_nanumgothic():
    """Register and set NanumGothic from local 'font' folder next to this file."""
    base_dir = os.path.dirname(__file__)
    font_dir = os.path.join(base_dir, "font")
    if not os.path.isdir(font_dir):
        return False
    # Candidate file names (common names)
    candidates = []
    for fn in os.listdir(font_dir):
        if fn.lower().startswith("nanumgothic") and (fn.lower().endswith(".ttf") or fn.lower().endswith(".otf")):
            candidates.append(os.path.join(font_dir, fn))
    if not candidates:
        return False
    # Register all variants for proper weight selection
    for path in candidates:
        try:
            _fm.fontManager.addfont(path)
        except Exception:
            pass
    # Use the first file to extract the family name
    try:
        fam = _FontProperties(fname=candidates[0]).get_name()
    except Exception:
        fam = "NanumGothic"
    plt.rcParams["font.family"] = fam
    plt.rcParams["font.sans-serif"] = [fam]
    plt.rcParams["axes.unicode_minus"] = False
    return True


# Force using local NanumGothic provided by the user
_use_local_nanumgothic()


# --------------------
# Configuration
# --------------------

# Default REF_AREA selection (groups + key countries) kept small for speed
DEFAULT_AREAS: list[str] = [
    # Regions/aggregates available in OECD SDMX for CLI
    "G20", "G7", "G4E", "NAFTA", "A5M",
    # Major countries
    "USA", "GBR", "DEU", "FRA", "ITA", "ESP", "CAN", "AUS", "JPN", "KOR", "MEX",
    "CHN", "IND", "IDN", "BRA", "TUR", "ZAF",
]

# Country groups for diffusion calculations (members filtered by availability)
G20_MEMBERS: list[str] = [
    "ARG", "AUS", "BRA", "CAN", "CHN", "DEU", "FRA", "GBR", "IND", "IDN",
    "ITA", "JPN", "KOR", "MEX", "RUS", "SAU", "ZAF", "TUR", "USA",
]
G7_MEMBERS: list[str] = ["USA", "CAN", "GBR", "DEU", "FRA", "ITA", "JPN"]
NAFTA_MEMBERS: list[str] = ["USA", "CAN", "MEX"]

# Aggregated codes present in dataset – exclude from default country list
AGGREGATE_CODES: set[str] = {"G20", "G7", "G4E", "NAFTA", "A5M"}

PHASE_EN_TO_KO = {
    "Recession": "침체기",
    "Recovery": "회복기",
    "Expansion": "확장기",
    "Slowdown": "둔화기",
    "Neutral": "중립",
}

# Area code -> Korean display name
AREA_KO_NAMES: dict[str, str] = {
    # Aggregates / regions (keep code if unclear)
    "G20": "G20",
    "G7": "G7",
    "NAFTA": "NAFTA",
    "G4E": "G4E",
    "A5M": "A5M",
    # Countries
    "USA": "미국",
    "GBR": "영국",
    "DEU": "독일",
    "FRA": "프랑스",
    "ITA": "이탈리아",
    "ESP": "스페인",
    "CAN": "캐나다",
    "AUS": "호주",
    "JPN": "일본",
    "KOR": "한국",
    "MEX": "멕시코",
    "CHN": "중국",
    "IND": "인도",
    "IDN": "인도네시아",
    "BRA": "브라질",
    "TUR": "터키",
    "ZAF": "남아프리카공화국",
    "ARG": "아르헨티나",
    "RUS": "러시아",
    "SAU": "사우디아라비아",
}

def korean_area_name(area: str) -> str:
    return AREA_KO_NAMES.get(area, area)

# Simple Korean descriptions for aggregate regions
REGION_DESC_KO: dict[str, str] = {
    "G20": "주요 20개국",
    "G7": "주요 7개국",
    "NAFTA": "북미자유무역협정",
    "G4E": "유럽 4개국",
    "A5M": "아시아 5개국",
}


# --------------------
# Data loading
# --------------------

def _build_sdmx_url(ref_areas: Sequence[str], start_period: str = "1990-01") -> str:
    areas = "+".join(ref_areas)
    # Structure: {provider,dataflow}/{REF_AREA}.{FREQ}.{SUBJECT?}... querystring
    # For CLI: frequency M, subject LI, measure AA, subject detail H (headline)
    return (
        "https://sdmx.oecd.org/public/rest/data/"
        "OECD.SDD.STES,DSD_STES@DF_CLI,4.1/"
        f"{areas}.M.LI...AA...H?startPeriod={start_period}"
    )


def fetch_cli(ref_areas: Sequence[str] | None = None, start_period: str = "1990-01") -> pd.DataFrame:
    """Fetch OECD CLI series for given ref areas; return pivoted wide DataFrame.

    - Columns: REF_AREA codes
    - Index: Timestamp (month)
    - Values: float CLI index (long-term average=100)
    """
    if ref_areas is None:
        ref_areas = DEFAULT_AREAS
    if sdmxthon is None:
        raise RuntimeError("sdmxthon is not installed. Please install sdmxthon.")
    url = _build_sdmx_url(ref_areas, start_period=start_period)
    msg = sdmxthon.read_sdmx(url)
    dataset_key = next((k for k in msg.content.keys() if "DF_CLI" in k), None)
    if dataset_key is None:
        raise RuntimeError("CLI dataset not found in SDMX response")
    df = msg.content[dataset_key].data
    keep = ["REF_AREA", "TIME_PERIOD", "OBS_VALUE"]
    if not set(keep).issubset(df.columns):
        raise RuntimeError("Unexpected SDMX columns: " + ",".join(df.columns))
    out = (
        df[keep]
        .rename(columns={"REF_AREA": "area", "TIME_PERIOD": "date", "OBS_VALUE": "value"})
    )
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"]).sort_values("date")
    wide = out.pivot(index="date", columns="area", values="value").sort_index()
    # Numeric cleanup
    wide = wide.apply(pd.to_numeric, errors="coerce")
    return wide


# --------------------
# Phase classification
# --------------------

def classify_phase(cli_value: float | pd.Series, mom: float | pd.Series, long_avg: float = 100.0):
    """Return phase name(s) given value and MoM. Vectorized for Series.

    If MoM==0 or any input is NaN -> 'Neutral'.
    """
    v = pd.Series(cli_value)
    d = pd.Series(mom)
    conditions = [
        (v < long_avg) & (d < 0),
        (v < long_avg) & (d > 0),
        (v >= long_avg) & (d > 0),
        (v >= long_avg) & (d < 0),
    ]
    choices = ["Recession", "Recovery", "Expansion", "Slowdown"]
    # Use string default to avoid dtype conflict across numpy versions
    vals = np.select(conditions, choices, default="Neutral")
    phase = pd.Series(vals, index=v.index, dtype="object")
    # Keep previous phase when MoM==0 (flat month) to avoid grey gaps
    flat = d.isna() | d.eq(0)
    phase = phase.mask(flat)
    phase = phase.ffill().fillna("Neutral")
    return phase


def compute_phases(wide: pd.DataFrame, long_avg: float = 100.0) -> pd.DataFrame:
    """Compute MoM and phase for each area; return long DataFrame.

    Columns: [date, area, value, mom, phase_en, phase_ko]
    """
    if wide is None or wide.empty:
        return pd.DataFrame(columns=["date", "area", "value", "mom", "phase_en", "phase_ko"])
    long_rows: list[pd.DataFrame] = []
    for area in wide.columns:
        ser = wide[area].copy()
        mom = ser.diff()
        phase = classify_phase(ser, mom, long_avg)
        df_area = pd.DataFrame({
            "date": ser.index,
            "area": area,
            "value": ser.values,
            "mom": mom.values,
            "phase_en": phase.values,
        })
        df_area["phase_ko"] = df_area["phase_en"].map(PHASE_EN_TO_KO)
        long_rows.append(df_area)
    return pd.concat(long_rows, ignore_index=True)


# --------------------
# Diffusion index
# --------------------

def diffusion_index(wide: pd.DataFrame, members: Iterable[str] | None = None) -> pd.Series:
    """Compute fraction of members with MoM > 0 for each month.

    - If members is None: use all non-aggregate columns
    Returns a Series in [0,1] indexed by date.
    """
    if wide is None or wide.empty:
        return pd.Series(dtype=float)
    if members is None:
        members = [c for c in wide.columns if c not in AGGREGATE_CODES]
    members = [m for m in members if m in wide.columns]
    if not members:
        return pd.Series(index=wide.index, dtype=float)
    mom = wide[members].diff()
    pos = (mom > 0).sum(axis=1)
    count = mom.notna().sum(axis=1).replace(0, np.nan)
    di = (pos / count).fillna(method="ffill")  # keep continuity when early NaNs
    di.name = "diffusion"
    return di


def diffusion_crossings(di: pd.Series, threshold: float = 0.5) -> pd.DataFrame:
    """Return DataFrame with boolean cross_up/cross_down at threshold."""
    if di is None or di.empty:
        return pd.DataFrame(columns=["cross_up", "cross_down"]).astype(bool)
    prev = di.shift(1)
    return pd.DataFrame({
        "cross_up": (prev < threshold) & (di >= threshold),
        "cross_down": (prev > threshold) & (di <= threshold),
    })


# --------------------
# Plotting
# --------------------

PHASE_COLORS = {
    # Stronger, clearer colors for lines/areas
    "Recession": "#e74c3c",  # red
    "Recovery": "#f39c12",   # orange
    "Expansion": "#2ecc71",  # green
    "Slowdown": "#3498db",   # blue
    "Neutral": "#bdbdbd",    # gray
}


def plot_phase_bands(
    series: pd.Series,
    phase: pd.Series,
    title: str = "CLI",
    long_avg: float = 100.0,
    ylim: tuple[float, float] | None = None,
    ax=None,
    draw_bands: bool = True,
    colored_line: bool = False,
):
    """Plot a single series with phase-colored line and optional background bands.

    - draw_bands=True: shade background by phase.
    - colored_line=True: color the line per phase segment for clarity.
    """
    if ax is None:
        fig, ax = plt.subplots(figsize=(14, 6))

    # Determine y-range
    if ylim is None:
        ypad = 2.0
        ymin = float(np.nanmin(series.values)) - ypad
        ymax = float(np.nanmax(series.values)) + ypad
    else:
        ymin, ymax = ylim

    # Optional background shading per phase
    if draw_bands:
        # More robust vertical band drawing using contiguous segments
        idx = series.index
        phs = phase.fillna("Neutral").values
        # Pre-compute right edges per point
        edges = list(idx)  # left edges
        # Estimate typical step for the last segment
        if len(idx) >= 2:
            step = idx[-1] - idx[-2]
        else:
            step = pd.Timedelta(days=30)
        right_edges = list(idx[1:]) + [idx[-1] + step]
        current = phs[0] if len(phs) else None
        seg_start = edges[0] if len(edges) else None
        for i in range(1, len(phs) + 1):
            is_break = (i == len(phs)) or (phs[i] != current)
            if is_break and current is not None:
                left = seg_start
                right = right_edges[i - 1]
                color = PHASE_COLORS.get(current, "#bdbdbd")
                ax.axvspan(left, right, color=color, alpha=0.22, linewidth=0)
                if i < len(phs):
                    current = phs[i]
                    seg_start = edges[i]
            elif not is_break:
                continue

    # Draw line: color-coded by phase segments for visibility
    if colored_line:
        dfp = pd.DataFrame({"v": series, "p": phase.fillna("Neutral")}).copy()
        seg_id = (dfp["p"] != dfp["p"].shift(1)).cumsum()
        first_label_done = False
        for _, seg in dfp.groupby(seg_id):
            p = seg["p"].iloc[0]
            color = PHASE_COLORS.get(p, "#666666")
            label = "CLI" if not first_label_done else None
            ax.plot(seg.index, seg["v"].values, color=color, linewidth=2.0, label=label)
            first_label_done = True
    else:
        ax.plot(series.index, series.values, color="black", linewidth=1.8, label="CLI")

    # Reference line and decorations
    ax.axhline(long_avg, color="gray", linestyle="--", linewidth=1.2, label="장기 평균")
    ax.set_ylim(ymin, ymax)
    ax.set_title(title)
    ax.set_xlabel("연도")
    ax.set_ylabel("OECD 경기선행지수")

    # Build legend: phase color patches + CLI + average line
    phase_handles = [
        Patch(facecolor=PHASE_COLORS["Recession"], edgecolor="none", alpha=0.6, label="침체기"),
        Patch(facecolor=PHASE_COLORS["Recovery"],  edgecolor="none", alpha=0.6, label="회복기"),
        Patch(facecolor=PHASE_COLORS["Expansion"], edgecolor="none", alpha=0.6, label="확장기"),
        Patch(facecolor=PHASE_COLORS["Slowdown"],  edgecolor="none", alpha=0.6, label="둔화기"),
    ]
    # Existing line and hline labels are already added
    ax.legend(handles=phase_handles + ax.get_legend_handles_labels()[0], loc="best")
    return ax


def plot_g20_with_diffusion(wide: pd.DataFrame, phases_long: pd.DataFrame, outdir: str | None = None):
    """Plot G20 CLI with phase bands and mark diffusion 50% crossings (G20 members)."""
    if "G20" not in wide.columns:
        return None
    # Series + phase for G20 (align by date index!)
    g20 = wide["G20"].copy()
    ph = phases_long.loc[phases_long["area"] == "G20", ["date", "phase_en"]].set_index("date")["phase_en"].reindex(g20.index)
    di = diffusion_index(wide, members=[m for m in G20_MEMBERS if m in wide.columns])
    crosses = diffusion_crossings(di, threshold=0.5)

    fig, ax = plt.subplots(figsize=(15, 7))
    # Draw black line with background phase bands to match requested style
    desc = REGION_DESC_KO.get('G20')
    suffix = f" ({desc})" if desc else ""
    title_text = f"{korean_area_name('G20')} 경기선행지수{suffix}"
    plot_phase_bands(g20, ph, title=title_text, ax=ax, colored_line=False, draw_bands=True)

    # Mark crossings on the CLI line (use markers at CLI value)
    up_dates = crosses.index[crosses["cross_up"]]
    dn_dates = crosses.index[crosses["cross_down"]]
    ax.scatter(up_dates, g20.reindex(up_dates), color="green", s=35, label="50% 상회")
    ax.scatter(dn_dates, g20.reindex(dn_dates), color="red", s=35, label="50% 하회")
    ax.legend(loc="best")
    fig.tight_layout()

    if outdir:
        os.makedirs(outdir, exist_ok=True)
        path = os.path.join(outdir, "g20_cli_phases_diffusion.png")
        fig.savefig(path, dpi=150)
        return path
    return fig


def plot_multi_areas(wide: pd.DataFrame, phases_long: pd.DataFrame, areas: Sequence[str], ncols: int = 3, outdir: str | None = None, prefix: str = "cli_phases_"):
    """Small multiples for selected areas with phase bands."""
    areas = [a for a in areas if a in wide.columns]
    if not areas:
        return None
    n = len(areas)
    ncols = min(ncols, n)
    nrows = int(np.ceil(n / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols + 2, 3.6 * nrows + 1.5), sharex=True)
    axes = np.array(axes).reshape(-1)
    for i, a in enumerate(areas):
        ax = axes[i]
        ser = wide[a]
        ph = phases_long.loc[phases_long["area"] == a, ["date", "phase_en"]].set_index("date")["phase_en"].reindex(ser.index)
        # Add brief Korean region description for aggregates
        desc = REGION_DESC_KO.get(a)
        suffix = f" ({desc})" if desc else ""
        title_txt = f"{korean_area_name(a)} 경기선행지수{suffix}"
        plot_phase_bands(ser, ph, title=title_txt, ax=ax, colored_line=False, draw_bands=True)
    for j in range(i + 1, len(axes)):
        axes[j].axis("off")
    fig.tight_layout()
    if outdir:
        os.makedirs(outdir, exist_ok=True)
        path = os.path.join(outdir, f"{prefix}{len(areas)}.png")
        fig.savefig(path, dpi=150)
        return path
    return fig


# --------------------
# CLI entrypoint
# --------------------

def run_analysis(start_period: str = "1990-01", ref_areas: Sequence[str] | None = None, outdir: str | None = None) -> dict:
    """Fetch data, compute phases and diffusion metrics, and produce key plots.

    Returns a dict with DataFrames/paths: {
      'wide': DataFrame, 'phases': DataFrame, 'diffusion_g20': Series,
      'g20_plot': path_or_fig, 'countries_plot': path_or_fig, 'regions_plot': path_or_fig
    }
    """
    wide = fetch_cli(ref_areas, start_period=start_period)
    phases = compute_phases(wide)

    # Diffusion examples
    di_g20 = diffusion_index(wide, members=[m for m in G20_MEMBERS if m in wide.columns])

    # Plots
    out = {}
    out["g20_plot"] = plot_g20_with_diffusion(wide, phases, outdir=outdir)
    # Countries = non-aggregates present
    countries = [c for c in wide.columns if c not in AGGREGATE_CODES]
    out["countries_plot"] = plot_multi_areas(wide, phases, countries, outdir=outdir, prefix="cli_phases_countries_")
    # Regions = aggregates only (that we fetched)
    regions = [r for r in wide.columns if r in AGGREGATE_CODES]
    out["regions_plot"] = plot_multi_areas(wide, phases, regions, outdir=outdir, prefix="cli_phases_regions_")

    result = {
        "wide": wide,
        "phases": phases,
        "diffusion_g20": di_g20,
        **out,
    }
    return result


if __name__ == "__main__":
    # Run quick analysis and save figures under global_universe/figs
    OUT_DIR = os.path.join(os.path.dirname(__file__), "figs")
    try:
        res = run_analysis(start_period=os.environ.get("OECD_CLI_START", "1990-01"), outdir=OUT_DIR)
        print("OECD CLI analysis complete.")
        for k in ["g20_plot", "countries_plot", "regions_plot"]:
            v = res.get(k)
            if isinstance(v, str):
                print(f"Saved: {v}")
    except Exception as e:
        print(f"Error: {e}")
