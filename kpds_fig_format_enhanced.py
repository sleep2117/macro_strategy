"""
KPDS 그래프 포맷팅 라이브러리 - Enhanced Version
===============================================

회사 표준 그래프 포맷에 맞춘 plotly 차트 생성 함수들

색상 사용 우선순위:
1. deepred_pds (첫 번째)
2. deepblue_pds (두 번째) 
3. beige_pds (세 번째)
4. blue_pds (네 번째)
5. grey_pds (다섯 번째)

특수 색상:
- lightbeige_pds: 5년 평균 min/max 영역

left_ytitle, right_ytitle에는 단위가 들어감
예시로 MoM, YoY면 "%"가 들어가고
구인건수라고 하면 "명"이나 "천 명" 이런 식으로
"""

import warnings
import plotly.express as px
warnings.filterwarnings("ignore")

from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

# 한글폰트 설치 #
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
from matplotlib import colors as mcolors
import pandas as pd
import numpy as np
import tempfile
import os
import datetime
import colorsys

# 폰트 설정
font_list = [font.name for font in fm.fontManager.ttflist]
plt.rcParams['font.family'] = 'Malgun Gothic'

# 폰트 크기 설정 변수들 (전역으로 조정 가능)
# 사용법: 원하는 크기로 변경 후 라이브러리 임포트
# 예시: FONT_SIZE_TITLE = 16  # 제목을 더 크게
#       FONT_SIZE_LEGEND = 10  # 범례를 더 작게
total_font_size = 12  # 전체 폰트 크기 (기본값)
FONT_SIZE_TITLE = total_font_size       # 차트 제목
FONT_SIZE_AXIS_TITLE = total_font_size  # 축 제목 (X축, Y축 라벨)
FONT_SIZE_TICK = total_font_size        # 축 눈금 텍스트 (현재 미사용, 추후 확장용)
FONT_SIZE_LEGEND = total_font_size      # 범례 텍스트
FONT_SIZE_ANNOTATION = total_font_size  # 주석 텍스트 (Y축 단위 등)
FONT_SIZE_GENERAL = total_font_size     # 일반 텍스트 (기본 폰트)

# 나눔고딕 폰트 등록
try:
    fe = fm.FontEntry(
        fname='C:/Users/USRP/AppData/Local/Microsoft/Windows/Fonts/NanumGothic.ttf',
        name='NanumGothic')
    fm.fontManager.ttflist.insert(0, fe)
except:
    print("나눔고딕 폰트를 찾을 수 없습니다. 기본 폰트를 사용합니다.")

plt.rcParams.update({'font.size': FONT_SIZE_GENERAL, 'font.family': 'NanumGothic'})
plt.rcParams['axes.unicode_minus'] = False

#### KPDS Color Palette #### 
deepred_pds = "rgb(242,27,45)"
deepblue_pds = "rgb(1,30,81)"
beige_pds = "rgb(188,157,126)"
lightbeige_pds = "rgb(242,233,218)"
red_pds = "rgb(255,34,71)"
blue_pds = "rgb(39,101,255)"
grey_pds = "rgb(114,113,113)"

# 색상 순서 리스트
KPDS_COLORS = [deepred_pds, deepblue_pds, beige_pds, blue_pds, grey_pds]


def _parse_color_to_rgb_tuple(color: str | None):
    if not color:
        return None
    value = str(color).strip()
    def _component_to_int(raw_value: float) -> int:
        if 0.0 <= raw_value <= 1.0:
            return int(round(raw_value * 255))
        return int(round(raw_value))
    try:
        if value.lower().startswith("rgba"):
            raw = value[value.find("(") + 1 : value.find(")")].split(",")
            r, g, b = [_component_to_int(float(v.strip())) for v in raw[:3]]
            return r, g, b
        if value.lower().startswith("rgb"):
            raw = value[value.find("(") + 1 : value.find(")")].split(",")
            r, g, b = [_component_to_int(float(v.strip())) for v in raw[:3]]
            return r, g, b
        if value.startswith("#") or value.lower() in mcolors.get_named_colors_mapping():
            r_f, g_f, b_f = mcolors.to_rgb(value)
            return int(round(r_f * 255)), int(round(g_f * 255)), int(round(b_f * 255))
    except Exception:
        return None
    return None


def _rgb_tuple_to_string(rgb: tuple[int, int, int]) -> str:
    r = max(0, min(255, int(round(rgb[0]))))
    g = max(0, min(255, int(round(rgb[1]))))
    b = max(0, min(255, int(round(rgb[2]))))
    return f"rgb({r},{g},{b})"


def _generate_fallback_color(seed: int) -> tuple[int, int, int]:
    golden_ratio = 0.61803398875
    hue = (seed * golden_ratio) % 1.0
    saturation = 0.55 + 0.25 * ((seed * 0.37) % 1.0)
    value = 0.65 + 0.25 * ((seed * 0.53) % 1.0)
    s_clamped = min(max(saturation, 0.35), 0.95)
    v_clamped = min(max(value, 0.4), 0.98)
    r_f, g_f, b_f = colorsys.hsv_to_rgb(hue, s_clamped, v_clamped)
    return int(round(r_f * 255)), int(round(g_f * 255)), int(round(b_f * 255))


KPDS_EXTENDED_COLORS: list[str] = list(KPDS_COLORS)
_KPDS_COLOR_TUPLES: set[tuple[int, int, int]] = set()

for base_color in KPDS_COLORS:
    parsed = _parse_color_to_rgb_tuple(base_color)
    if parsed:
        _KPDS_COLOR_TUPLES.add(parsed)


def _add_colors_from_palette(palette: list[str] | tuple[str, ...]) -> None:
    for colour in palette:
        rgb_tuple = _parse_color_to_rgb_tuple(colour)
        if rgb_tuple and rgb_tuple not in _KPDS_COLOR_TUPLES:
            KPDS_EXTENDED_COLORS.append(_rgb_tuple_to_string(rgb_tuple))
            _KPDS_COLOR_TUPLES.add(rgb_tuple)


for attribute in dir(px.colors.qualitative):
    if attribute.startswith("_"):
        continue
    palette_candidate = getattr(px.colors.qualitative, attribute)
    if isinstance(palette_candidate, (list, tuple)):
        _add_colors_from_palette(palette_candidate)


def _ensure_color_capacity(index: int) -> None:
    if index < len(KPDS_EXTENDED_COLORS):
        return
    seed = len(KPDS_EXTENDED_COLORS)
    attempt = 0
    while len(KPDS_EXTENDED_COLORS) <= index:
        rgb_tuple = _generate_fallback_color(seed + attempt)
        attempt += 1
        if rgb_tuple in _KPDS_COLOR_TUPLES:
            continue
        KPDS_EXTENDED_COLORS.append(_rgb_tuple_to_string(rgb_tuple))
        _KPDS_COLOR_TUPLES.add(rgb_tuple)


import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Jupyter notebook 호환성을 위한 plotly 설정
try:
    import plotly.io as pio
    # 기본 렌더링 설정 (웹 브라우저 대신 기본값 사용)
    # pio.renderers.default = "browser"  # 주석 처리
    print("Plotly 기본 렌더링 설정 완료")
except Exception as e:
    print(f"Plotly 설정 실패: {e}")

# 기본 폰트 설정
font_dict = dict(
    family='NanumGothic',
    size=FONT_SIZE_GENERAL,
    color="black"
)

def calculate_optimal_date_interval(data_length, data_span_years=None):
    """
    데이터 길이에 따라 최적의 날짜 간격을 자동 계산
    
    Args:
        data_length: 데이터 포인트 개수
        data_span_years: 데이터 기간 (연 단위, 선택사항)
    
    Returns:
        str: plotly dtick 형식 (예: "M1", "M3", "M6", "M12")
    """
    if data_span_years is None:
        # 데이터 길이로부터 대략적인 기간 추정 (월단위 가정)
        data_span_years = data_length / 12
    
    # 화면에 표시할 적정 tick 개수 (6-12개 정도가 적당)
    target_ticks = 10
    
    if data_span_years <= 0.5:  # 6개월 이하
        return "M1"  # 매월
    elif data_span_years <= 1:  # 1년 이하
        return "M2"  # 2개월
    elif data_span_years <= 2:  # 2년 이하
        return "M3"  # 3개월
    elif data_span_years <= 4:  # 4년 이하
        return "M6"  # 6개월
    elif data_span_years <= 8:  # 8년 이하
        return "M12"  # 1년
    else:  # 8년 초과
        return "M24"  # 2년

def format_date_ticks(fig, date_format='%b-%y', tick_interval="auto", xdata=None):
    """
    plotly figure의 날짜 축을 원하는 형식으로 포맷팅 (확대 시 자동 조정 포함)
    
    Args:
        fig: plotly figure 객체
        date_format: 날짜 형식 ('%b-%y' = Jan-25 형식)
        tick_interval: 날짜 간격 ("auto"=자동, "M1"=매월, "M3"=3개월, "M6"=6개월)
        xdata: X축 데이터 (자동 간격 계산용, 선택사항)
    """
    if tick_interval == "auto" and xdata is not None:
        try:
            data_length = len(xdata)
            # 날짜 데이터인 경우 실제 기간 계산
            try:
                if hasattr(xdata, 'min') and hasattr(xdata, 'max'):
                    date_range = xdata.max() - xdata.min()
                    if hasattr(date_range, 'days'):
                        data_span_years = date_range.days / 365.25
                    else:
                        data_span_years = None
                else:
                    data_span_years = None
            except:
                data_span_years = None
            
            tick_interval = calculate_optimal_date_interval(data_length, data_span_years)
        except:
            tick_interval = "M6"
    elif tick_interval == "auto":
        tick_interval = "M6"
    
    # 멀티레벨 tick 설정으로 확대 시 더 세밀한 표시
    fig.update_xaxes(
        tickformat=date_format,
        dtick=tick_interval,
        tickangle=0,
        # 확대 시 자동으로 tick 밀도 증가
        nticks=15,  # 기본 tick 개수
        # tickmode를 'auto'로 설정하여 zoom에 따라 자동 조정
        tickmode='auto',
        # 추가: zoom 레벨에 따른 동적 조정 활성화
        automargin=True
    )
    
    # JavaScript 콜백을 통한 동적 tick 조정 (plotly의 relayout 이벤트 활용)
    # 이는 Jupyter 환경에서 작동
    fig.update_layout(
        xaxis=dict(
            # zoom 시 자동으로 tick 간격 재계산
            autorange=True,
            # 기본 tick 설정 유지하면서 zoom 시 더 세밀하게
            rangeslider=dict(visible=False),  # range slider 비활성화로 성능 개선
            # fixedrange=False를 명시적으로 설정하여 zoom 허용
            fixedrange=False
        )
    )
    
    return fig

def get_minor_tick_interval(major_interval):
    """
    주요 tick 간격에 대응하는 보조 tick 간격 반환
    """
    interval_map = {
        "M1": "D15",    # 월간 -> 15일 간격
        "M2": "M1",     # 2개월 -> 월간
        "M3": "M1",     # 3개월 -> 월간  
        "M6": "M2",     # 6개월 -> 2개월
        "M12": "M3",    # 연간 -> 3개월
        "M24": "M6"     # 2년 -> 6개월
    }
    return interval_map.get(major_interval)


def get_kpds_color(index):
    """KPDS 색상 순서에 따라 색상 반환"""
    if index < 0:
        index = 0
    _ensure_color_capacity(index)
    return KPDS_EXTENDED_COLORS[index]

def calculate_title_position(title_text, position='left'):
    """
    제목 텍스트 길이에 따라 위치를 자동 계산
    """
    if not title_text:
        return None
    
    text_length = len(str(title_text))
    
    if position == 'left':
        # 왼쪽 제목: 텍스트가 길수록 더 왼쪽으로
        if text_length <= 10:
            return -0.08
        elif text_length <= 15:
            return -0.10
        else:
            return -0.12
    else:  # right
        # 오른쪽 제목: 텍스트가 길수록 더 왼쪽으로 이동
        if text_length <= 10:
            return 1.02
        elif text_length <= 15:
            return 0.98
        else:
            return 0.94
    
def get_dynamic_margins(ytitle1=None, ytitle2=None, title=None):
    """
    Y축 제목 길이에 따라 동적으로 margin 계산
    """
    left_margin = 60  # 기본값
    right_margin = 60  # 기본값
    top_margin = 60 if title else 40
    bottom_margin = 60
    
    # 왼쪽 Y축 제목이 있으면 왼쪽 margin 증가
    if ytitle1:
        left_margin = max(80, 60 + len(str(ytitle1)) * 2)
    
    # 오른쪽 Y축 제목이 있으면 오른쪽 margin 증가
    if ytitle2:
        right_margin = max(80, 60 + len(str(ytitle2)) * 2)
    
    return dict(l=left_margin, r=right_margin, t=top_margin, b=bottom_margin)


def format_date_axis(fig, date_format='monthly'):
    """
    날짜 축 포맷팅
    
    Args:
        fig: plotly figure 객체
        date_format: 'monthly' (Jan, Feb) 또는 'yearly' (Jan-11, Nov-12)
    """
    if date_format == 'monthly':
        fig.update_xaxes(
            tickformat='%b',  # Jan, Feb, Mar
            dtick='M1'
        )
    elif date_format == 'yearly':
        fig.update_xaxes(
            tickformat='%b-%y',  # Jan-11, Nov-12
            dtick='M12'
        )
    
    return fig

def df2xa(df, dim_list, coordinates):
    """DataFrame을 xarray로 변환"""
    import xarray as xr
    data = xr.DataArray(data=df, dims=dim_list, coords=coordinates)
    return data 

# 데이터프레임 기반 입력을 지원하는 새로운 함수들

def df_line_chart(df, column=None, title=None, xtitle=None, ytitle=None, label=None, 
                 width_cm=9.5, height_cm=6.5):
    """
    데이터프레임에서 단일 시리즈 라인 차트 생성
    
    Args:
        df: 데이터프레임 (index는 date, column은 값)
        column: 그릴 열 이름 (None이면 첫 번째 열 사용)
        title: 차트 제목
        xtitle: X축 제목
        ytitle: Y축 제목 (None이면 column명 사용)
        label: 시리즈 라벨 (None이면 column명 사용)
        width_cm: 차트 너비 (cm, 기본값: 9.5)
        height_cm: 차트 높이 (cm, 기본값: 6.5)
    """
    # column 자동 선택
    if column is None:
        column = df.columns[0]
    
    # 라벨 자동 설정 (column명 사용)
    if label is None:
        label = column
        
    # Y축 제목 자동 설정 (column명 사용)
    if ytitle is None:
        ytitle = column
    
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df.index, 
            y=df[column], 
            name=label, 
            line_color=deepred_pds, 
            yaxis='y'
        )
    )
    
    # 폰트 설정
    font_family = 'NanumGothic'
    chart_font = dict(
        family=font_family,
        size=FONT_SIZE_GENERAL,
        color="black"
    )
    
    fig.update_layout(
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686, 
        height=400, 
        font=chart_font,
        xaxis=dict(
            title=dict(text=xtitle, font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE)),
            showline=True, linewidth=1.3, linecolor='lightgrey', 
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            showgrid=False
        ),
        yaxis=dict(
            title=dict(text=None, font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE)),
            showline=False, 
            tickcolor='white',
            tickformat=',',
            showgrid=False
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.05,
            xanchor="center", x=0.5,
            font=dict(family=font_family, size=FONT_SIZE_LEGEND),
            borderwidth=0,
            bordercolor="rgba(0,0,0,0)"
        ),
        showlegend=True
    )
    
    # 날짜 포맷 적용 (자동 간격 계산, zoom 지원)
    fig = format_date_ticks(fig, '%b-%y', "auto", df.index)

    # 0선 추가
    fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)

    # Y축 단위 annotation (자동 위치 계산)
    if ytitle:
        x_pos = calculate_title_position(ytitle, 'left')
        fig.add_annotation(
            text=ytitle,
            xref="paper", yref="paper",
            x=x_pos, y=1.1,
            showarrow=False,
            font=dict(size=FONT_SIZE_ANNOTATION, family=font_family, color="black"),
            align='left'
        )

    # 동적 margin 적용
    margins = get_dynamic_margins(ytitle1=ytitle, title=title)
    
    # 제목 여부에 따른 여백 조정
    if title:
        fig.update_layout(
            title=dict(text=title, font=dict(family=font_family, size=FONT_SIZE_TITLE)),
            margin=margins
        )
    else:
        fig.update_layout(
            title=None,
            margin=margins
        )
    
    fig.show()
    return fig

def df_multi_line_chart(df, columns=None, title=None, xtitle=None, ytitle=None, labels=None, 
                       width_cm=9.5, height_cm=6.5):
    """
    데이터프레임에서 다중 시리즈 라인 차트 생성 (동적 데이터 수 지원)
    
    Args:
        df: 데이터프레임 (index는 date, columns는 여러 시리즈)
        columns: 그릴 열 이름 리스트 (None이면 모든 열 사용)
        title: 차트 제목
        xtitle: X축 제목
        ytitle: Y축 제목
        labels: 시리즈 라벨 딕셔너리 (None이면 column명 사용)
        width_cm: 차트 너비 (cm, 기본값: 9.5)
        height_cm: 차트 높이 (cm, 기본값: 6.5)
    """
    # columns 자동 선택
    if columns is None:
        columns = df.columns.tolist()
    
    fig = go.Figure()
    
    # 동적으로 데이터 수를 판단해서 색상 할당
    for i, col in enumerate(columns):
        label = labels.get(col, col) if labels else col
        color = get_kpds_color(i)  # KPDS 색상 순서대로 할당
        
        fig.add_trace(
            go.Scatter(
                x=df.index, 
                y=df[col], 
                name=label,
                line_color=color
            )
        )
    
    # 폰트 설정
    font_family = 'NanumGothic'
    chart_font = dict(
        family=font_family,
        size=FONT_SIZE_GENERAL,
        color="black"
    )
    
    fig.update_layout(
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686, 
        height=400, 
        font=chart_font,
        xaxis=dict(
            title=dict(text=xtitle, font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE)),
            showline=True, linewidth=1.3, linecolor='lightgrey', 
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            showgrid=False
        ),
        yaxis=dict(
            title=dict(text=None, font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE)),
            showline=False, 
            tickcolor='white',
            tickformat=',',
            showgrid=False
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.05,
            xanchor="center", x=0.5,
            font=dict(family=font_family, size=FONT_SIZE_LEGEND),
            borderwidth=0,
            bordercolor="rgba(0,0,0,0)"
        ),
        showlegend=True
    )
    
    # 날짜 포맷 적용 (자동 간격 계산, zoom 지원)
    fig = format_date_ticks(fig, '%b-%y', "auto", df.index)

    # 0선 추가
    fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)

    # Y축 단위 annotation (자동 위치 계산)
    if ytitle:
        x_pos = calculate_title_position(ytitle, 'left')
        fig.add_annotation(
            text=ytitle,
            xref="paper", yref="paper",
            x=x_pos, y=1.1,
            showarrow=False,
            font=dict(size=FONT_SIZE_ANNOTATION, family=font_family, color="black"),
            align='left'
        )

    # 동적 margin 적용
    margins = get_dynamic_margins(ytitle1=ytitle, title=title)
    
    # 제목 여부에 따른 여백 조정
    if title:
        fig.update_layout(
            title=dict(text=title, font=dict(family=font_family, size=FONT_SIZE_TITLE)),
            margin=margins
        )
    else:
        fig.update_layout(
            title=None,
            margin=margins
        )
    
    fig.show()
    return fig

def df_historical_comparison(df, current_col=None, min_col=None, max_col=None, avg_col=None,
                           current_label=None, avg_label=None,
                           title=None, xtitle=None, ytitle=None, 
                           width_cm=9.5, height_cm=6.5):
    """
    데이터프레임에서 5년 평균 min/max 영역과 현재 데이터 비교 차트 생성
    
    Args:
        df: 데이터프레임 (index는 date)
        current_col: 현재 연도 데이터 열 이름 (None이면 자동 탐지)
        min_col: 과거 최솟값 열 이름 (None이면 자동 탐지)
        max_col: 과거 최댓값 열 이름 (None이면 자동 탐지)
        avg_col: 과거 평균값 열 이름 (None이면 자동 탐지)
        current_label: 현재 데이터 라벨
        avg_label: 평균 라벨
        title: 차트 제목
        xtitle: X축 제목
        ytitle: Y축 제목 (None이면 자동 설정)
        width_cm: 차트 너비 (cm, 기본값: 9.5)
        height_cm: 차트 높이 (cm, 기본값: 6.5)
    """
    # 열 이름 자동 탐지
    if current_col is None:
        # '현재', 'current', '2024' 등이 포함된 열 찾기
        for col in df.columns:
            if any(keyword in col.lower() for keyword in ['current', '현재', '2024', '2025']):
                current_col = col
                break
        if current_col is None:
            current_col = df.columns[0]  # 첫 번째 열 사용
    
    if min_col is None:
        # 'min', '최소', '최솟값' 등이 포함된 열 찾기
        for col in df.columns:
            if any(keyword in col.lower() for keyword in ['min', '최소', '최솟값']):
                min_col = col
                break
    
    if max_col is None:
        # 'max', '최대', '최댓값' 등이 포함된 열 찾기
        for col in df.columns:
            if any(keyword in col.lower() for keyword in ['max', '최대', '최댓값']):
                max_col = col
                break
    
    if avg_col is None:
        # 'avg', 'average', '평균' 등이 포함된 열 찾기
        for col in df.columns:
            if any(keyword in col.lower() for keyword in ['avg', 'average', '평균']):
                avg_col = col
                break
    
    # Y축 제목 자동 설정
    if ytitle is None:
        ytitle = current_col
    
    # 라벨 자동 설정
    if current_label is None:
        current_label = "2025년"
    if avg_label is None:
        avg_label = "20~24년 평균"
    
    min_max_label = "최소~최대(5년)"
    
    fig = go.Figure()
    
    # Min/Max 영역
    if min_col and max_col and min_col in df.columns and max_col in df.columns:
        fig.add_trace(
            go.Scatter(
                x=list(df.index) + list(df.index[::-1]),
                y=list(df[max_col]) + list(df[min_col][::-1]),
                fill='toself',
                fillcolor=lightbeige_pds,
                line=dict(color='rgba(255,255,255,0)'),
                name=min_max_label,
                showlegend=True
            )
        )
    
    # 현재 데이터
    fig.add_trace(
        go.Scatter(
            x=df.index, 
            y=df[current_col], 
            name=current_label,
            line_color=deepred_pds,
            line=dict(width=3)
        )
    )
    
    # 평균값 (제공된 경우)
    if avg_col and avg_col in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df.index, 
                y=df[avg_col], 
                name=avg_label,
                line_color=deepblue_pds,
                line=dict(dash='dash', width=2)
            )
        )
    
    # 폰트 설정
    font_family = 'NanumGothic'
    chart_font = dict(
        family=font_family,
        size=FONT_SIZE_GENERAL,
        color="black"
    )
    
    fig.update_layout(
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686, 
        height=400, 
        font=chart_font,
        xaxis=dict(
            title=dict(text=xtitle, font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE)),
            showline=True, linewidth=1.3, linecolor='lightgrey', 
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            showgrid=False
        ),
        yaxis=dict(
            title=dict(text=y_title if y_title else None, font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE)),
            showline=False, 
            tickcolor='white',
            tickformat=',',
            showgrid=False
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.05,
            xanchor="center", x=0.5,
            font=dict(family=font_family, size=FONT_SIZE_LEGEND),
            borderwidth=0,
            bordercolor="rgba(0,0,0,0)"
        ),
        showlegend=True
    )
    
    # 날짜 포맷 적용 (자동 간격 계산, zoom 지원)
    fig = format_date_ticks(fig, '%b-%y', "auto", df.index)

    # 0선 추가
    fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)

    # Y축 단위 annotation (자동 위치 계산)
    if ytitle:
        x_pos = calculate_title_position(ytitle, 'left')
        fig.add_annotation(
            text=ytitle,
            xref="paper", yref="paper",
            x=x_pos, y=1.1,
            showarrow=False,
            font=dict(size=FONT_SIZE_ANNOTATION, family=font_family, color="black"),
            align='left'
        )
    
    # 동적 margin 적용
    margins = get_dynamic_margins(ytitle1=ytitle, title=title)
    
    # 제목 여부에 따른 여백 조정
    if title:
        fig.update_layout(
            title=dict(text=title, font=dict(family=font_family, size=FONT_SIZE_TITLE)),
            margin=margins
        )
    else:
        fig.update_layout(
            title=None,
            margin=margins
        )
    
    fig.show()
    return fig

def create_five_year_comparison_chart(df, title=None, y_title=None, x_axis_type='month', recent_years=3):
    """
    five_year_format 함수 출력에 최적화된 5년 비교 차트 생성
    - legend는 그래프 위에 2줄로 표시
    - 최근 N년 데이터 표시 (recent_years 파라미터로 조정)
    - 연도별 라인 (solid line, 현재연도는 원형 마커)
    - 5년 평균 dashed line (deepblue_pds)  
    - Min~Max 5년 영역 (lightbeige_pds)
    - 실제 데이터만 표시 (ffill/bfill 안함)
    
    Args:
        df: five_year_format 함수의 출력 데이터프레임
        title: 차트 제목
        y_title: Y축 제목  
        x_axis_type: X축 타입 ('month' 또는 'week')
        recent_years: 표시할 최근 연도 수 (기본값: 3)
    """
    if df is None or len(df) == 0:
        print("데이터가 없습니다.")
        return None
    
    fig = go.Figure()
    
    # 컬럼 분석
    year_cols = [col for col in df.columns if str(col).isdigit() and len(str(col)) == 4]
    min_max_col = None
    min_col = None 
    avg_col = None
    
    for col in df.columns:
        if 'Min~Max' in str(col) or 'min~max' in str(col).lower():
            min_max_col = col
        elif 'Min' in str(col) or 'min' in str(col).lower():
            min_col = col
        elif '평균' in str(col) or 'avg' in str(col).lower() or 'average' in str(col).lower():
            avg_col = col
    
    # X축 데이터 준비
    if x_axis_type == 'month':
        x_data = list(range(1, len(df) + 1))
        x_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][:len(df)]
        x_title = ''  # 월간 데이터는 X축 제목 없음
        show_unit_annotation = False
    else:  # week
        x_data = list(range(1, len(df) + 1))
        x_labels = [str(i) for i in x_data]  # 숫자만 표시 (1, 2, 3...)
        x_title = ''  # 주간 데이터도 X축 제목 없음 (오른쪽 끝에 "주" 표시)
        show_unit_annotation = True
    
    # Min-Max 영역 추가 (가장 먼저 추가해야 배경에 표시됨)
    if min_max_col is not None and min_col is not None:
        # NaN 값 처리 안함 - 실제 데이터만 사용
        max_values = df[min_max_col]
        min_values = df[min_col]
        
        # NaN이 아닌 값들만 필터링
        valid_indices = ~(max_values.isna() | min_values.isna())
        if valid_indices.any():
            valid_x = [x_data[i] for i in range(len(x_data)) if valid_indices.iloc[i]]
            valid_max = [max_values.iloc[i] for i in range(len(max_values)) if valid_indices.iloc[i]]
            valid_min = [min_values.iloc[i] for i in range(len(min_values)) if valid_indices.iloc[i]]
            
            fig.add_trace(go.Scatter(
                x=valid_x + valid_x[::-1],
                y=valid_max + valid_min[::-1],
                fill='toself',
                fillcolor=lightbeige_pds,
                line=dict(color='rgba(255,255,255,0)'),
                name='Min~Max(5년)',
                showlegend=True
            ))
    
    # 5년 평균선 추가 (dashed line)
    if avg_col is not None:
        avg_data = df[avg_col]  # ffill 제거
        # NaN이 아닌 값들만 표시
        valid_avg_indices = ~avg_data.isna()
        if valid_avg_indices.any():
            valid_avg_x = [x_data[i] for i in range(len(x_data)) if valid_avg_indices.iloc[i]]
            valid_avg_y = [avg_data.iloc[i] for i in range(len(avg_data)) if valid_avg_indices.iloc[i]]
            
            fig.add_trace(go.Scatter(
                x=valid_avg_x,
                y=valid_avg_y,
                mode='lines',
                name=avg_col,
                line=dict(color=deepblue_pds, width=2, dash='dash')
            ))
    
    # 최근 N년 데이터 추가 (recent_years 파라미터로 조정)
    current_year = max([int(col) for col in year_cols]) if year_cols else 2025
    recent_year_list = [current_year - i for i in range(recent_years)]
    
    # 동적 색상 할당 (최근년도부터 KPDS 색상 순서대로)
    year_colors = {}
    year_widths = {}
    
    for i, year_int in enumerate(recent_year_list):
        year_colors[year_int] = get_kpds_color(i)
        year_widths[year_int] = 3 if i == 0 else 2  # 첫 번째(최신)는 굵게
    
    for year_int in recent_year_list:
        year_col = str(year_int)
        if year_col in df.columns:
            year_data = df[year_col]  # ffill 제거 - 실제 데이터만 사용
            
            # 색상 및 마커 설정
            color = year_colors[year_int]
            width = year_widths[year_int]
            
            if year_int == current_year:
                marker_dict = dict(size=8, symbol='circle')
            else:
                marker_dict = dict(size=6)
            
            # NaN이 아닌 값들만 표시
            valid_year_indices = ~year_data.isna()
            if valid_year_indices.any():
                valid_year_x = [x_data[i] for i in range(len(x_data)) if valid_year_indices.iloc[i]]
                valid_year_y = [year_data.iloc[i] for i in range(len(year_data)) if valid_year_indices.iloc[i]]
                
                fig.add_trace(go.Scatter(
                    x=valid_year_x,
                    y=valid_year_y,
                    mode='lines+markers',
                    name=str(year_col),
                    line=dict(color=color, width=width),
                    marker=marker_dict
                ))
    
    # 레이아웃 설정
    fig.update_layout(
        title=dict(text=title, font=dict(family='NanumGothic', size=FONT_SIZE_TITLE)),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686, 
        height=400, 
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color="black"),
        xaxis=dict(
            title=dict(text=x_title, font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE)),
            showline=True, linewidth=1.3, linecolor='lightgrey', 
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            tickmode='array',
            tickvals=x_data[::max(1, len(x_data)//10)] if x_axis_type == 'week' else x_data,  # 주간 데이터는 간격 조정
            ticktext=[x_labels[i] for i in range(0, len(x_labels), max(1, len(x_labels)//10))] if x_axis_type == 'week' else x_labels,
            tickangle=0,  # 가로 표시
            showgrid=False
        ),
        yaxis=dict(
            showline=False, 
            tickcolor='white',
            tickformat=',',
            showgrid=False
        ),
        # legend를 그래프 위에 2줄로 표시
        legend=dict(
            orientation="h",
            yanchor="bottom", 
            y=1.05,
            xanchor="center", 
            x=0.5,
            font=dict(family='NanumGothic', size=FONT_SIZE_LEGEND),
            traceorder='normal',
            borderwidth=0,
            bordercolor="rgba(0,0,0,0)"
        ),
        margin=dict(l=60, r=60, t=100, b=60),  # 위쪽 여백 증가 (legend 공간)
        showlegend=True
    )

    if y_title:
        x_pos = calculate_title_position(y_title, 'left')
        fig.add_annotation(
            text=y_title,
            xref="paper", yref="paper",
            x=x_pos, y=1.15,
            showarrow=False,
            font=dict(size=FONT_SIZE_ANNOTATION, family='NanumGothic', color="black"),
            align='left'
        )

    # 주간 데이터일 때 오른쪽 끝에 "주" 단위 표시
    if show_unit_annotation:
        fig.add_annotation(
            text="주",
            xref="paper", yref="paper",
            x=1.02, y=-0.1,  # X축 오른쪽 끝
            showarrow=False,
            font=dict(size=FONT_SIZE_ANNOTATION, family='NanumGothic', color="black"),
            align='left'
        )
    
    fig.show()
    return fig

def df_dual_axis_chart(df, left_cols=None, right_cols=None, 
                      left_labels=None, right_labels=None, 
                      left_title=None, right_title=None, 
                      title=None, xtitle=None, 
                      width_cm=9.5, height_cm=6.5):
    """
    데이터프레임에서 이중 Y축 차트 생성 (다중 시리즈 지원)
    
    Args:
        df: 데이터프레임 (index는 date)
        left_cols: 왼쪽 Y축 데이터 열 이름들 (리스트 또는 단일 문자열)
        right_cols: 오른쪽 Y축 데이터 열 이름들 (리스트 또는 단일 문자열)
        left_labels: 왼쪽 시리즈 라벨들 (리스트 또는 None)
        right_labels: 오른쪽 시리즈 라벨들 (리스트 또는 None)
        left_title: 왼쪽 Y축 제목
        right_title: 오른쪽 Y축 제목
        title: 차트 제목
        xtitle: X축 제목
        width_cm: 차트 너비 (cm, 기본값: 9.5)
        height_cm: 차트 높이 (cm, 기본값: 6.5)
    """
    # 열 자동 선택 및 리스트 변환
    if left_cols is None:
        left_cols = [df.columns[0]]
    elif isinstance(left_cols, str):
        left_cols = [left_cols]
    
    if right_cols is None:
        right_cols = [df.columns[1]] if len(df.columns) > 1 else [df.columns[0]]
    elif isinstance(right_cols, str):
        right_cols = [right_cols]
    
    # 라벨 자동 설정
    if left_labels is None:
        left_labels = left_cols
    elif isinstance(left_labels, str):
        left_labels = [left_labels]
    
    if right_labels is None:
        right_labels = right_cols
    elif isinstance(right_labels, str):
        right_labels = [right_labels]
    
    fig = go.Figure()
    
    # 왼쪽 Y축 데이터 추가 (deepred_pds부터 시작)
    left_color_index = 0
    for i, col in enumerate(left_cols):
        label = left_labels[i] if i < len(left_labels) else col
        color = get_kpds_color(left_color_index)
        
        fig.add_trace(
            go.Scatter(
                x=df.index, y=df[col], 
                name=label, 
                line_color=color, 
                yaxis='y'
            )
        )
        left_color_index += 1
    
    # 오른쪽 Y축 데이터 추가 (왼쪽에서 이어서 색상 선택)
    for i, col in enumerate(right_cols):
        label = right_labels[i] if i < len(right_labels) else col
        color = get_kpds_color(left_color_index + i)
        
        fig.add_trace(
            go.Scatter(
                x=df.index, y=df[col], 
                name=label, 
                line_color=color, 
                yaxis='y2'
            )
        )
    
    # 범위 자동 설정
    left_data = pd.concat([df[col] for col in left_cols])
    right_data = pd.concat([df[col] for col in right_cols])
    
    left_range = [left_data.min() * 0.95, left_data.max() * 1.05]
    right_range = [right_data.min() * 0.95, right_data.max() * 1.05]
    
    # 폰트 설정
    font_family = 'NanumGothic'
    chart_font = dict(
        family=font_family,
        size=FONT_SIZE_GENERAL,
        color="black"
    )
    
    fig.update_layout(
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686, 
        height=400, 
        font=chart_font,
        xaxis=dict(
            title=dict(text=xtitle, font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE)),
            showline=True, linewidth=1.3, linecolor='lightgrey', 
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            showgrid=False
        ),
        yaxis=dict(
            title=dict(text=None, font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE)),
            range=left_range,
            tickformat=',',
            showline=False, 
            tickcolor='white',
            showgrid=False
        ),
        yaxis2=dict(
            title=dict(text=None, font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE)),
            range=right_range,
            tickformat=',',
            showline=False, 
            tickcolor='white',
            anchor="x", 
            overlaying="y", 
            side="right",
            showgrid=False
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.05,
            xanchor="center", x=0.5,
            font=dict(family=font_family, size=FONT_SIZE_LEGEND),
            borderwidth=0,
            bordercolor="rgba(0,0,0,0)"
        ),
        showlegend=True
    )

    # 날짜 포맷 적용 (자동 간격 계산, zoom 지원)
    fig = format_date_ticks(fig, '%b-%y', "auto", df.index)

    # 0선 추가
    fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)

    # 동적 margin 적용
    margins = get_dynamic_margins(ytitle1=left_title, ytitle2=right_title, title=title)
    
    # 제목 및 margin 설정
    if title:
        fig.update_layout(
            title=dict(text=title, font=dict(family=font_family, size=FONT_SIZE_TITLE)),
            margin=margins
        )
    else:
        fig.update_layout(
            title=None,
            margin=margins
        )

    # Y축 제목 annotation
    if left_title:
        x_pos_left = calculate_title_position(left_title, 'left')
        fig.add_annotation(
            text=left_title,
            xref="paper", yref="paper",
            x=x_pos_left, y=1.1,
            showarrow=False,
            font=dict(family=font_family, size=FONT_SIZE_ANNOTATION, color="black"),
            align='left'
        )

    if right_title:
        x_pos_right = calculate_title_position(right_title, 'right')
        fig.add_annotation(
            text=right_title,
            xref="paper", yref="paper",
            x=x_pos_right, y=1.1,
            showarrow=False,
            font=dict(family=font_family, size=FONT_SIZE_ANNOTATION, color="black"),
            align='left'
        )
    
    fig.show()
    return fig

# 산점도 차트도 추가
def df_scatter_chart(df, x_col=None, y_col=None, color_col=None, size_col=None,
                   title=None, xtitle=None, ytitle=None, width_cm=9.5, height_cm=6.5):
    """
    데이터프레임에서 산점도 차트 생성
    
    Args:
        df: 데이터프레임
        x_col: X축 데이터 열 이름 (None이면 첫 번째 열)
        y_col: Y축 데이터 열 이름 (None이면 두 번째 열)
        color_col: 색상 구분 열 이름 (선택사항)
        size_col: 크기 구분 열 이름 (선택사항)
        title: 차트 제목
        xtitle: X축 제목 (None이면 column명 사용)
        ytitle: Y축 제목 (None이면 column명 사용)
        width_cm: 차트 너비 (cm, 기본값: 9.5)
        height_cm: 차트 높이 (cm, 기본값: 6.5)
    """
    # 열 자동 선택
    if x_col is None:
        x_col = df.columns[0]
    if y_col is None:
        y_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
    
    # 축 제목 자동 설정
    if xtitle is None:
        xtitle = x_col
    if ytitle is None:
        ytitle = y_col
    
    fig = go.Figure()
    
    if color_col and color_col in df.columns:
        # 색상 구분이 있는 경우
        unique_values = df[color_col].unique()
        for i, value in enumerate(unique_values):
            filtered_df = df[df[color_col] == value]
            fig.add_trace(
                go.Scatter(
                    x=filtered_df[x_col],
                    y=filtered_df[y_col],
                    mode='markers',
                    name=str(value),
                    marker=dict(
                        color=get_kpds_color(i),
                        size=filtered_df[size_col] if size_col and size_col in df.columns else 8,
                        line=dict(width=1, color='black')
                    )
                )
            )
    else:
        # 단일 색상
        fig.add_trace(
            go.Scatter(
                x=df[x_col],
                y=df[y_col],
                mode='markers',
                marker=dict(
                    color=deepred_pds,
                    size=df[size_col] if size_col and size_col in df.columns else 8,
                    line=dict(width=1, color='black')
                )
            )
        )
    
    # 범위 자동 설정
    x_range = [df[x_col].min() * 0.95, df[x_col].max() * 1.05]
    y_range = [df[y_col].min() * 0.95, df[y_col].max() * 1.05]
    
    # 레이아웃 설정
    fig.update_layout(
        title=dict(text=title, font=dict(family='NanumGothic', size=FONT_SIZE_TITLE)),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686, 
        height=400, 
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color="black"),
        xaxis=dict(
            title=dict(text=xtitle, font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE)),
            showline=True, linewidth=1.3, linecolor='lightgrey', 
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            range=x_range,
            showgrid=False
        ),
        yaxis=dict(
            title=dict(text=ytitle, font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE)),
            showline=False, tickcolor='white',
            tickformat=',',
            range=y_range,
            showgrid=False
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.05,
            xanchor="center", x=0.5,
            font=dict(family='NanumGothic', size=FONT_SIZE_LEGEND),
            borderwidth=0,
            bordercolor="rgba(0,0,0,0)"
        ),
        margin=dict(l=60, r=150, t=80, b=60)
    )
    
    fig.show()
    return fig

# 편의 함수들 - 호환성 유지
def quick_line(df, col=None, title=None):
    """빠른 라인 차트 생성"""
    return df_line_chart(df, col, title)

def quick_multi(df, cols=None, title=None):
    """빠른 다중 라인 차트 생성"""
    return df_multi_line_chart(df, cols, title)

def quick_comparison(df, title=None):
    """빠른 히스토리컬 비교 차트 생성"""
    return df_historical_comparison(df, title=title)

def quick_dual(df, left_cols=None, right_cols=None, title=None):
    """빠른 이중 축 차트 생성"""
    return df_dual_axis_chart(df, left_cols=left_cols, right_cols=right_cols, title=title)

def quick_scatter(df, title=None):
    """빠른 산점도 차트 생성"""
    return df_scatter_chart(df, title=title)

def quick_bar(df, column=None, title=None, positive_color=None, negative_color=None):
    """빠른 Bar 차트 생성 (양수/음수 색상 구분)"""
    return df_bar_chart(df, column=column, title=title, 
                       positive_color=positive_color, negative_color=negative_color)


# five_year_format 전용 편의 함수들
def quick_five_year(df, title=None, y_title=None, recent_years=3):
    """
    five_year_format 데이터로 빠른 5년 비교 차트 생성
    - 최근 N년 + 평균 + Min~Max 표시
    - 실제 데이터만 표시 (ffill 안함)
    """
    return create_five_year_comparison_chart(df, title=title, y_title=y_title, 
                                           x_axis_type='month', recent_years=recent_years)

def quick_five_year_week(df, title=None, y_title=None, recent_years=3):
    """
    five_year_format 주간 데이터로 빠른 5년 비교 차트 생성
    - 최근 N년 + 평균 + Min~Max 표시
    - 실제 데이터만 표시 (ffill 안함)
    """
    return create_five_year_comparison_chart(df, title=title, y_title=y_title, 
                                           x_axis_type='week', recent_years=recent_years)

def df_bar_chart(df, column=None, title=None, xtitle=None, ytitle=None, 
                 positive_color=None, negative_color=None, 
                 width_cm=9.5, height_cm=6.5):
    """
    데이터프레임에서 Bar 차트 생성 (양수/음수 색상 구분)
    
    Args:
        df: 데이터프레임 (index는 date, column은 값)
        column: 그릴 열 이름 (None이면 첫 번째 열 사용)
        title: 차트 제목
        xtitle: X축 제목
        ytitle: Y축 제목 (None이면 column명 사용)
        positive_color: 양수 색상 (None이면 deepred_pds)
        negative_color: 음수 색상 (None이면 beige_pds)
        width_cm: 차트 너비 (cm, 기본값: 9.5)
        height_cm: 차트 높이 (cm, 기본값: 6.5)
    """
    # column 자동 선택
    if column is None:
        column = df.columns[0]
    
    # Y축 제목 자동 설정
    if ytitle is None:
        ytitle = column
    
    # 색상 설정
    if positive_color is None:
        positive_color = deepred_pds
    if negative_color is None:
        negative_color = beige_pds
    
    # 데이터 분리 (양수/음수)
    data_values = df[column]
    bar_colors = [positive_color if x >= 0 else negative_color for x in data_values]
    
    fig = go.Figure()
    
    # Bar 차트 추가
    fig.add_trace(
        go.Bar(
            x=df.index,
            y=data_values,
            marker_color=bar_colors,
            name=column,
            showlegend=False,  # 단일 시리즈이므로 legend 숨김
            width=15*24*60*60*1000  # 15일 정도의 너비 (밀리초)
        )
    )
    
    # 폰트 설정
    font_family = 'NanumGothic'
    chart_font = dict(
        family=font_family,
        size=FONT_SIZE_GENERAL,
        color="black"
    )
    
    # 범위 설정 (약간의 여백 추가)
    y_min = data_values.min()
    y_max = data_values.max()
    y_range = y_max - y_min
    y_margin = y_range * 0.1 if y_range > 0 else abs(y_max) * 0.1
    
    fig.update_layout(
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686,
        height=400,
        font=chart_font,
        xaxis=dict(
            title=dict(text=xtitle, font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE)),
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            showgrid=False
        ),
        yaxis=dict(
            title=dict(text=None, font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE)),
            range=[y_min - y_margin, y_max + y_margin],
            showline=False,
            tickcolor='white',
            tickformat=',',
            showgrid=False
        ),
        showlegend=False
    )
    
    # 날짜 포맷 적용 (자동 간격 계산, zoom 지원)
    fig = format_date_ticks(fig, '%Y년', "M12", df.index)
    
    # 0선 추가
    fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)

    # Y축 단위 annotation
    if ytitle:
        x_pos = calculate_title_position(ytitle, 'left')
        fig.add_annotation(
            text=ytitle,
            xref="paper", yref="paper",
            x=x_pos, y=1.1,
            showarrow=False,
            font=dict(size=FONT_SIZE_ANNOTATION, family=font_family, color="black"),
            align='left'
        )

    # 동적 margin 적용
    margins = get_dynamic_margins(ytitle1=ytitle, title=title)
    
    # 제목 여부에 따른 여백 조정
    if title:
        fig.update_layout(
            title=dict(text=title, font=dict(family=font_family, size=FONT_SIZE_TITLE)),
            margin=margins
        )
    else:
        fig.update_layout(
            title=None,
            margin=margins
        )
    
    fig.show()
    return fig

def create_flexible_mixed_chart(df, line_config=None, bar_config=None, 
                               dual_axis=True, title=None, xtitle=None, 
                               left_ytitle=None, right_ytitle=None,
                               width_cm=9.5, height_cm=6.5):
    """
    유연한 혼합 차트 생성: 라인/바 차트 조합, 단일/이중 축 지원
    
    Args:
        df: 데이터프레임 (index는 날짜)
        line_config: 라인 차트 설정 dict
            {
                'columns': ['col1', 'col2'],  # 라인으로 그릴 컬럼들
                'labels': ['Label1', 'Label2'],  # 라벨 (선택사항)
                'axis': 'left' or 'right',  # 축 위치 ('left'=기본, 'right'=보조축)
                'colors': ['color1', 'color2'] or 'auto',  # 색상 (선택사항)
                'line_width': 3,  # 라인 두께 (선택사항)
                'markers': True   # 마커 표시 여부 (선택사항)
            }
        bar_config: 바 차트 설정 dict
            {
                'columns': ['col1', 'col2'],  # 바로 그릴 컬럼들
                'labels': ['Label1', 'Label2'],  # 라벨 (선택사항)
                'axis': 'left' or 'right',  # 축 위치 ('left'=기본, 'right'=보조축)
                'colors': ['color1', 'color2'] or 'auto',  # 색상 (선택사항)
                'color_by_value': True,  # 값에 따라 색상 변경 (양수/음수)
                'opacity': 0.7,  # 투명도 (선택사항)
                'width': 15*24*60*60*1000  # 바 너비 (선택사항)
            }
        dual_axis: 이중 축 사용 여부 (True/False)
        title: 차트 제목
        xtitle: X축 제목
        left_ytitle: 왼쪽 Y축 제목
        right_ytitle: 오른쪽 Y축 제목
        width_cm: 차트 너비 (cm, 기본값: 9.5)
        height_cm: 차트 높이 (cm, 기본값: 6.5)
    
    Returns:
        plotly figure 객체
    """
    
    # 기본 설정
    if line_config is None:
        line_config = {}
    if bar_config is None:
        bar_config = {}
    
    # subplot 생성 (dual_axis에 따라)
    if dual_axis:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
    else:
        fig = go.Figure()
    
    color_index = 0  # 색상 인덱스
    
    # 라인 차트 추가
    if line_config.get('columns'):
        columns = line_config['columns']
        if isinstance(columns, str):
            columns = [columns]
        
        labels = line_config.get('labels', columns)
        if isinstance(labels, str):
            labels = [labels]
        
        axis = line_config.get('axis', 'left')
        colors = line_config.get('colors', 'auto')
        line_width = line_config.get('line_width', 3)
        show_markers = line_config.get('markers', True)
        
        for i, col in enumerate(columns):
            if col not in df.columns:
                continue
                
            # 라벨 설정
            label = labels[i] if i < len(labels) else col
            
            # 색상 설정
            if colors == 'auto':
                color = get_kpds_color(color_index)
                color_index += 1
            else:
                color = colors[i] if i < len(colors) else get_kpds_color(color_index)
                color_index += 1
            
            # 마커 설정
            mode = 'lines+markers' if show_markers else 'lines'
            marker_dict = dict(size=6, color=color) if show_markers else None
            
            # 축 설정
            if dual_axis:
                secondary_y = (axis == 'right')
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df[col],
                        mode=mode,
                        name=label,
                        line=dict(color=color, width=line_width),
                        marker=marker_dict
                    ),
                    secondary_y=secondary_y
                )
            else:
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df[col],
                        mode=mode,
                        name=label,
                        line=dict(color=color, width=line_width),
                        marker=marker_dict
                    )
                )
    
    # 바 차트 추가
    if bar_config.get('columns'):
        columns = bar_config['columns']
        if isinstance(columns, str):
            columns = [columns]
        
        labels = bar_config.get('labels', columns)
        if isinstance(labels, str):
            labels = [labels]
        
        axis = bar_config.get('axis', 'left')
        colors = bar_config.get('colors', 'auto')
        color_by_value = bar_config.get('color_by_value', False)
        opacity = bar_config.get('opacity', 0.7)
        bar_width = bar_config.get('width', 15*24*60*60*1000)
        
        for i, col in enumerate(columns):
            if col not in df.columns:
                continue
                
            # 라벨 설정
            label = labels[i] if i < len(labels) else col
            
            # 색상 설정
            if color_by_value:
                # 값에 따라 색상 변경 (양수/음수) - 상승은 deepred_pds, 하락은 deepblue_pds
                bar_colors = [deepred_pds if x >= 0 else deepblue_pds for x in df[col]]
            else:
                if colors == 'auto':
                    color = get_kpds_color(color_index)
                    bar_colors = color
                    color_index += 1
                else:
                    color = colors[i] if i < len(colors) else get_kpds_color(color_index)
                    bar_colors = color
                    color_index += 1
            
            # 축 설정
            if dual_axis:
                secondary_y = (axis == 'right')
                fig.add_trace(
                    go.Bar(
                        x=df.index,
                        y=df[col],
                        name=label,
                        marker_color=bar_colors,
                        opacity=opacity,
                        width=bar_width
                    ),
                    secondary_y=secondary_y
                )
            else:
                fig.add_trace(
                    go.Bar(
                        x=df.index,
                        y=df[col],
                        name=label,
                        marker_color=bar_colors,
                        opacity=opacity,
                        width=bar_width
                    )
                )
    
    # 폰트 설정
    font_family = 'NanumGothic'
    chart_font = dict(
        family=font_family,
        size=FONT_SIZE_GENERAL,
        color="black"
    )
    
    # Y축 설정
    if dual_axis:
        fig.update_yaxes(
            tickformat=',',
            showline=False,
            tickcolor='white',
            secondary_y=False
        )
        fig.update_yaxes(
            tickformat=',',
            showline=False,
            tickcolor='white',
            secondary_y=True
        )
    else:
        # 단일 축일 때는 일반적인 Y축 제목 설정
        ytitle_text = left_ytitle if left_ytitle else right_ytitle
        fig.update_yaxes(
            title=dict(text=ytitle_text, font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE)),
            tickformat=',',
            showline=False,
            tickcolor='white'
        )
    
    # X축 설정
    fig.update_xaxes(
        title=dict(text=xtitle, font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE)),
        showline=True, linewidth=1.3, linecolor='lightgrey',
        tickwidth=1.3, tickcolor='lightgrey',
        ticks='outside'
    )
    
    # 동적 margin 계산
    margins = get_dynamic_margins(ytitle1=left_ytitle, ytitle2=right_ytitle, title=title)
    
    # 레이아웃 설정
    fig.update_layout(
        title=dict(text=title, font=dict(family=font_family, size=FONT_SIZE_TITLE)),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686,
        height=400,
        font=chart_font,
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.05,
            xanchor="center", x=0.5,
            font=dict(family=font_family, size=FONT_SIZE_LEGEND),
            borderwidth=0,
            bordercolor="rgba(0,0,0,0)"
        ),
        margin=margins
    )
    
    # 날짜 포맷 적용 (자동 간격 계산, zoom 지원)
    fig = format_date_ticks(fig, '%b-%y', "auto", df.index)
    
    # 왼쪽 Y축 제목 annotation
    if left_ytitle:
        x_pos_left = calculate_title_position(left_ytitle, 'left')
        fig.add_annotation(
            text=left_ytitle,
            xref="paper", yref="paper",
            x=x_pos_left, y=1.1,
            showarrow=False,
            font=dict(size=FONT_SIZE_ANNOTATION, family=font_family, color="black"),
            align='left'
        )
    
    # 오른쪽 Y축 제목 annotation (dual_axis일 때만)
    if dual_axis and right_ytitle:
        x_pos_right = calculate_title_position(right_ytitle, 'right')
        fig.add_annotation(
            text=right_ytitle,
            xref="paper", yref="paper",
            x=x_pos_right, y=1.1,
            showarrow=False,
            font=dict(size=FONT_SIZE_ANNOTATION, family=font_family, color="black"),
            align='left'
        )
    
    # 0선 추가 (바 차트가 있을 때)
    if bar_config.get('columns'):
        if dual_axis:
            # 바 차트가 어느 축에 있는지 확인
            bar_axis = bar_config.get('axis', 'left')
            secondary_y = (bar_axis == 'right')
            fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5, secondary_y=secondary_y)
        else:
            fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)
    
    fig.show()
    return fig

def create_sector_contribution_chart(data_dict, title=None, y_title="천명",
                                    positive_color=None, negative_color=None,
                                    width_cm=9.5, height_cm=6.5):
    """
    섹터별 기여도 스택 바 차트 생성 (KPDS 스타일)
    
    Args:
        data_dict: {날짜: {섹터명: 값}} 형태의 중첩 딕셔너리
        title: 차트 제목
        y_title: Y축 제목 (기본값: "천명")
        positive_color: 양수 색상 (기본: deepred_pds)
        negative_color: 음수 색상 (기본: deepblue_pds)
        width_cm: 차트 너비 (cm)
        height_cm: 차트 높이 (cm)
    """
    if positive_color is None:
        positive_color = deepred_pds
    if negative_color is None:
        negative_color = deepblue_pds
    
    # 데이터 준비
    dates = list(data_dict.keys())
    all_sectors = set()
    for date_data in data_dict.values():
        all_sectors.update(date_data.keys())
    all_sectors = sorted(list(all_sectors))
    
    # 폰트 설정
    font_family = "NanumGothic"
    chart_font = dict(
        family=font_family,
        size=FONT_SIZE_GENERAL,
        color="black"
    )
    
    # Plotly 차트 생성
    fig = go.Figure()
    
    # 각 섹터별로 스택 바 추가
    for i, sector in enumerate(all_sectors):
        values = []
        for date in dates:
            value = data_dict[date].get(sector, 0)
            values.append(value)
        
        color = get_kpds_color(i)
        
        fig.add_trace(go.Bar(
            name=sector,
            x=dates,
            y=values,
            marker_color=color,
            text=[f"{val:.0f}" if abs(val) >= 1 else f"{val:.1f}" for val in values],
            textposition="inside" if all(abs(v) > 10 for v in values) else "none"
        ))
    
    # 레이아웃 설정
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(family=font_family, size=FONT_SIZE_TITLE),
            x=0.5,
            xanchor="center"
        ) if title else None,
        barmode='stack',
        paper_bgcolor="white",
        plot_bgcolor="white",
        width=686,
        height=400,
        font=chart_font,
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor="lightgrey",
            tickwidth=1.3, tickcolor="lightgrey",
            ticks="outside",
            showgrid=False
        ),
        yaxis=dict(
            showline=False,
            tickcolor="white",
            tickformat=".0f",
            showgrid=False
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.05,
            xanchor="center", x=0.5,
            font=dict(family=font_family, size=FONT_SIZE_LEGEND),
            borderwidth=0,
            bordercolor="rgba(0,0,0,0)"
        ),
        margin=dict(l=60, r=60, t=100, b=60)
    )
    
    # 날짜 포맷 적용
    fig = format_date_ticks(fig, '%b-%y', "auto", dates)
    
    # 0선 추가
    fig.add_hline(y=0, line_width=2, line_color="black")
    
    # Y축 제목 annotation
    if y_title:
        x_pos = calculate_title_position(y_title, 'left')
        fig.add_annotation(
            text=y_title,
            xref="paper", yref="paper",
            x=x_pos, y=1.15,
            showarrow=False,
            font=dict(size=FONT_SIZE_ANNOTATION, family=font_family, color="black"),
            align='left'
        )
    
    fig.show()
    return fig

def create_kpds_cpi_bar_chart(data_dict, labels_dict=None, title=None, 
                             positive_color=None, negative_color=None,
                             width_cm=9.5, height_cm=6.5):
    """
    KPDS 스타일 CPI 바 차트 생성
    
    Args:
        data_dict: {구성요소: 값} 형태의 딕셔너리
        labels_dict: {구성요소: 표시라벨} 형태의 딕셔너리 (선택사항)
        title: 차트 제목
        positive_color: 양수 색상 (기본: deepred_pds)
        negative_color: 음수 색상 (기본: blue_pds)
        width_cm: 차트 너비 (cm)
        height_cm: 차트 높이 (cm)
    """
    if positive_color is None:
        positive_color = deepred_pds
    if negative_color is None:
        negative_color = blue_pds
    
    # 라벨 설정
    if labels_dict is None:
        labels_dict = {}
    
    # 데이터 준비
    categories = []
    values = []
    colors = []
    
    for comp, value in data_dict.items():
        label = labels_dict.get(comp, comp)
        categories.append(label)
        values.append(value)
        colors.append(positive_color if value >= 0 else negative_color)
    
    # 폰트 설정
    font_family = "NanumGothic"
    chart_font = dict(
        family=font_family,
        size=FONT_SIZE_GENERAL,
        color="black"
    )
    
    # Plotly 차트 생성
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=categories,
        y=values,
        marker_color=colors,
        showlegend=False,
        text=[f"{val:.1f}" for val in values],
        textposition="outside" if all(v >= 0 for v in values) else "auto",
        textfont=dict(size=FONT_SIZE_GENERAL-1, family=font_family)
    ))
    
    # Y축 범위 계산
    y_min = min(values) if values else 0
    y_max = max(values) if values else 0
    y_range = y_max - y_min
    y_margin = max(0.5, y_range * 0.1)
    
    # 레이아웃 설정
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(family=font_family, size=FONT_SIZE_TITLE),
            x=0.5,
            xanchor="center"
        ) if title else None,
        paper_bgcolor="white",
        plot_bgcolor="white",
        width=max(600, 120 * len(categories)),
        height=400,
        font=chart_font,
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor="lightgrey",
            tickwidth=1.3, tickcolor="lightgrey",
            ticks="outside",
            tickfont=dict(size=FONT_SIZE_GENERAL-1, family=font_family),
            tickangle=45 if len(categories) > 4 else 0
        ),
        yaxis=dict(
            showline=False,
            tickcolor="white",
            tickformat=".1f",
            range=[y_min - y_margin, y_max + y_margin],
            tickfont=dict(size=FONT_SIZE_GENERAL, family=font_family)
        ),
        margin=dict(l=60, r=60, t=80 if title else 40, b=120)
    )
    
    # 격자선 및 0선
    fig.update_yaxes(showgrid=False)
    fig.update_xaxes(showgrid=False)
    fig.add_hline(y=0, line_width=2, line_color="black")
    
    fig.show()
    return fig

def create_waterfall_chart(df, column=None, title=None, xtitle=None, ytitle=None,
                           width_cm=9.5, height_cm=6.5):
    """
    워터폴 차트 생성 (KPDS 스타일)
    
    Args:
        df: 데이터프레임 (index는 카테고리, column은 값)
        column: 그릴 열 이름 (None이면 첫 번째 열 사용)
        title: 차트 제목
        xtitle: X축 제목
        ytitle: Y축 제목
        width_cm: 차트 너비 (cm)
        height_cm: 차트 높이 (cm)
    """
    if column is None:
        column = df.columns[0]
    
    values = df[column].values
    categories = df.index.tolist()
    
    # 워터폴 계산
    cumulative = [0]
    for i, val in enumerate(values):
        cumulative.append(cumulative[-1] + val)
    
    fig = go.Figure()
    
    # 각 단계별 바 추가
    for i, (cat, val) in enumerate(zip(categories, values)):
        color = deepred_pds if val >= 0 else deepblue_pds
        
        fig.add_trace(go.Bar(
            name=cat,
            x=[cat],
            y=[val],
            base=cumulative[i],
            marker_color=color,
            text=f"{val:+.1f}",
            textposition="middle",
            showlegend=False
        ))
    
    # 폰트 설정
    font_family = 'NanumGothic'
    chart_font = dict(
        family=font_family,
        size=FONT_SIZE_GENERAL,
        color="black"
    )
    
    fig.update_layout(
        title=dict(text=title, font=dict(family=font_family, size=FONT_SIZE_TITLE)),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686,
        height=400,
        font=chart_font,
        xaxis=dict(
            title=dict(text=xtitle, font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE)),
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            showgrid=False
        ),
        yaxis=dict(
            title=dict(text=ytitle, font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE)),
            showline=False,
            tickcolor='white',
            tickformat=',',
            showgrid=False
        ),
        margin=dict(l=60, r=60, t=80, b=60)
    )
    
    # 0선 추가
    fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.8)
    
    fig.show()
    return fig

def create_horizontal_bar_chart(data_dict, title=None, positive_color=None, negative_color=None, 
                                num_categories=None, sort_data=True, ascending=True, unit=None):
    """
    일반화된 가로 바 차트 생성 함수
    
    Args:
        data_dict: {시리즈명: 값} 형태의 딕셔너리 또는 pandas Series
        title: 차트 제목
        positive_color: 양수 색상 (None이면 deepred_pds)
        negative_color: 음수 색상 (None이면 deepblue_pds)
        num_categories: 표시할 카테고리 수 (None이면 모든 데이터)
        sort_data: 데이터 정렬 여부 (기본값: True)
        ascending: 정렬 순서 (True=오름차순, False=내림차순)
        unit: 값의 단위 (None이면 %로 표시)
    
    Returns:
        plotly figure
    """
    import pandas as pd
    
    # 데이터 타입 처리
    if isinstance(data_dict, pd.Series):
        data_dict = data_dict.to_dict()
    
    if not data_dict:
        print("⚠️ 표시할 데이터가 없습니다.")
        return None
    
    # 기본 색상 설정
    if positive_color is None:
        positive_color = deepred_pds
    if negative_color is None:
        negative_color = deepblue_pds
    
    # 데이터 정렬
    if sort_data:
        if ascending:
            sorted_items = sorted(data_dict.items(), key=lambda x: x[1])
        else:
            sorted_items = sorted(data_dict.items(), key=lambda x: x[1], reverse=True)
    else:
        sorted_items = list(data_dict.items())
    
    # 표시할 카테고리 수 제한
    if num_categories and len(sorted_items) > num_categories:
        if ascending:
            sorted_items = sorted_items[-num_categories:]  # 상위 num_categories개
        else:
            sorted_items = sorted_items[:num_categories]   # 상위 num_categories개
    
    # 데이터 준비
    categories = []
    values = []
    colors = []
    
    for label, value in sorted_items:
        categories.append(label)
        values.append(value)
        
        # 값에 따라 색상 결정
        if value >= 0:
            colors.append(positive_color)
        else:
            colors.append(negative_color)
    
    # 가로 바 차트 생성
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=categories,
        x=values,
        orientation='h',
        marker_color=colors,
        text=[f'{v:.1f}{unit if unit else "%"}' if abs(v) >= 0.1 else f'{v:.2f}{unit if unit else "%"}' for v in values],
        textposition='outside' if all(v >= 0 for v in values) else 'auto',
        showlegend=False
    ))
    
    # 폰트 설정
    font_family = 'NanumGothic'
    
    # 레이아웃 설정
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(family=font_family, size=FONT_SIZE_TITLE),
            x=0.5, xanchor="center"
        ),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=800,
        height=max(400, len(categories) * 25),
        font=dict(family=font_family, size=FONT_SIZE_GENERAL-1, color="black"),
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            tickformat='.1f',
            range=[min(values) * 1.2 if values and min(values) < 0 else -abs(max(values)) * 0.1 if values else -1, 
                   max(values) * 1.2 if values else 1]
        ),
        yaxis=dict(
            showline=False,
            tickcolor='white'
        ),
        margin=dict(l=max(250, max(len(str(cat)) for cat in categories) * 8), r=80, t=80, b=60)
    )
    
    # 격자선 및 0선
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=False)
    fig.add_vline(x=0, line_width=2, line_color="black")
    
    fig.show()
    return fig
