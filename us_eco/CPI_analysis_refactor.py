# %%
"""
CPI 데이터 분석 (리팩토링 버전)
- us_eco_utils를 사용한 통합 구조
- 시리즈 정의와 분석 로직만 포함
- 325개 시리즈 지원
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sys
import warnings
warnings.filterwarnings('ignore')

# 통합 유틸리티 함수 불러오기
from us_eco_utils import *

# %%
# === BLS API 키 설정 ===
api_config.BLS_API_KEY = '56b193612b614cdc9416359fd1c73a74'
api_config.BLS_API_KEY2 = '0450ef37363c48b5bedd2ae6fc92dd6e'
api_config.BLS_API_KEY3 = 'daf1ca7970b74e81b6a5c7a80a8b8a7f'

# %%
# === CPI 시리즈 정의 ===

# 완전한 CPI 계층 구조 불러오기 (325개 시리즈)
from cpi_complete_all_series import COMPLETE_ALL_CPI_HIERARCHY, ALL_BLS_SERIES_MAP, ALL_KOREAN_NAMES

# 기존 변수명과 호환성을 위해 매핑
CPI_HIERARCHY = COMPLETE_ALL_CPI_HIERARCHY

# BLS 시리즈 ID 맵 (325개 시리즈)
CPI_SERIES = ALL_BLS_SERIES_MAP

# 한국어 이름 매핑
CPI_KOREAN_NAMES = ALL_KOREAN_NAMES

# %%
# === 전역 변수 ===

# CSV 파일 경로
CSV_FILE_PATH = data_path('cpi_data.csv')

# 전역 데이터 저장소
CPI_DATA = {
    'raw_data': pd.DataFrame(),
    'mom_data': pd.DataFrame(),
    'mom_change': pd.DataFrame(),
    'yoy_data': pd.DataFrame(),
    'yoy_change': pd.DataFrame(),
    'latest_values': {},
    'load_info': {
        'loaded': False,
        'load_time': None,
        'start_date': None,
        'series_count': 0,
        'data_points': 0,
    },
}

# %%
# === 데이터 로드 함수 ===

def load_cpi_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """
    CPI 데이터 로드 (통합 함수 사용)
    
    Args:
        start_date: 시작 날짜
        smart_update: 스마트 업데이트 사용 여부
        force_reload: 강제 재로드 여부
    
    Returns:
        bool: 로드 성공 여부
    """
    global CPI_DATA
    
    # 통합 함수 사용하여 데이터 로드
    result = load_economic_data(
        series_dict=CPI_SERIES,
        data_source='BLS',
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=10.0  # CPI 데이터는 일반적으로 작은 수치
    )
    
    if result:
        CPI_DATA = result
        print_load_info()
        return True
    else:
        print("❌ CPI 데이터 로드 실패")
        return False

def print_load_info():
    """?? ?? ??"""
    info = CPI_DATA.get('load_info') or {}
    if not info.get('loaded'):
        print("?? ???? ???? ?????.")
        return

    print(f"?? ??? CPI ??? ??:")
    print(f"   ??? ??: {info['series_count']}")
    print(f"   ??? ???: {info['data_points']}")
    print(f"   ?? ??: {info['start_date']}")
    load_time = info.get('load_time')
    if load_time:
        print(f"   ?? ??: {load_time.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print("   ?? ??: N/A")
    print(f"   ??? ??: {info.get('source', 'API')}")

    raw_data = CPI_DATA.get('raw_data')
    if raw_data is not None and not raw_data.empty:
        date_range = f"{raw_data.index[0].strftime('%Y-%m')} ~ {raw_data.index[-1].strftime('%Y-%m')}"
        print(f"   ??? ??: {date_range}")
def plot_cpi_series_advanced(series_list, chart_type='multi_line', data_type='mom',
                            periods=None, target_date=None):
    """
    범용 CPI 시각화 함수 - plot_economic_series를 사용한 고급 시각화
    
    Args:
        series_list: 시각화할 시리즈 리스트
        chart_type: 'multi_line', 'single_line', 'dual_axis', 'horizontal_bar'
        data_type: 'mom', 'raw', 'mom_change', 'yoy', 'yoy_change'
        periods: 표시할 기간 (개월, None이면 전체 데이터)
        target_date: 특정 날짜 기준 (예: '2025-06-01')
    
    Returns:
        plotly figure
    """
    if not CPI_DATA:
        print("⚠️ 먼저 load_cpi_data()를 실행하세요.")
        return None
    
    # 단위 설정
    if data_type in ['mom', 'yoy']:
        left_ytitle = "%"
        right_ytitle = "%"
    elif data_type in ['mom_change', 'yoy_change']:
        left_ytitle = "포인트"
        right_ytitle = "포인트"
    else:  # raw
        left_ytitle = "지수"
        right_ytitle = "지수"
    
    return plot_economic_series(
        data_dict=CPI_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        left_ytitle=left_ytitle,
        right_ytitle=right_ytitle,
        target_date=target_date,
        korean_names=CPI_KOREAN_NAMES
    )

def export_cpi_data(series_list, data_type='mom', periods=None, target_date=None,
                   export_path=None, file_format='excel'):
    """
    CPI 데이터를 엑셀/CSV로 export하는 함수
    
    Args:
        series_list: export할 시리즈 리스트
        data_type: 'mom', 'raw', 'mom_change', 'yoy', 'yoy_change'
        periods: 표시할 기간 (개월, None이면 전체 데이터)
        target_date: 특정 날짜 기준 (예: '2025-06-01')
        export_path: export할 파일 경로 (None이면 자동 생성)
        file_format: 파일 형식 ('excel', 'csv')
    
    Returns:
        str: export된 파일 경로 (성공시) 또는 None (실패시)
    """
    if not CPI_DATA:
        print("⚠️ 먼저 load_cpi_data()를 실행하세요.")
        return None
    
    return export_economic_data(
        data_dict=CPI_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=CPI_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# === 메인 데이터 로드 함수 ===
def clear_data():
    """데이터 초기화"""
    global CPI_DATA
    CPI_DATA = {
        'raw_data': pd.DataFrame(),
        'yoy_data': pd.DataFrame(),
        'latest_values': {},
        'load_info': {'loaded': False, 'load_time': None, 'start_date': None, 'series_count': 0, 'data_points': 0}
    }
    print("🗑️ 데이터가 초기화되었습니다")

# %%
# === 데이터 접근 함수들 ===

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_cpi_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return CPI_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in CPI_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return CPI_DATA['raw_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """YoY 변화율 데이터 반환"""
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_cpi_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return CPI_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in CPI_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return CPI_DATA['yoy_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화율 데이터 반환"""
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_cpi_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return CPI_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in CPI_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return CPI_DATA['mom_data'][available_series].copy()

def get_latest_values(series_names=None):
    """최신 YoY 값들 반환"""
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_cpi_data()를 먼저 실행하세요.")
        return {}
    
    if series_names is None:
        return CPI_DATA['latest_values'].copy()
    
    return {name: CPI_DATA['latest_values'].get(name, 0) for name in series_names if name in CPI_DATA['latest_values']}

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not CPI_DATA['load_info']['loaded']:
        return []
    return list(CPI_DATA['raw_data'].columns)

# %%
# === 시각화 함수들 ===

def determine_analysis_type(series_name):
    """
    시리즈 이름에 따라 적절한 분석 방법(YoY/MoM) 결정
    
    Args:
        series_name: 시리즈 이름
    
    Returns:
        str: 'yoy' 또는 'mom'
    """
    # 비계절조정 시리즈들은 YoY로 분석 (시리즈 이름에 'non_sa'가 포함되거나 명시적으로 지정된 것들)
    if 'non_sa' in series_name or series_name in ['headline_non_sa', 'core_non_sa']:
        return 'yoy'
    else:
        return 'mom'

def create_cpi_timeseries_chart(series_names=None, chart_type='auto'):
    """
    저장된 데이터를 사용한 CPI 시계열 차트 생성
    
    Args:
        series_names: 차트에 표시할 시리즈 리스트
        chart_type: 'yoy' (전년동기대비), 'mom' (전월대비), 'level' (수준), 또는 'auto' (자동 선택)
        title: 차트 제목
    """
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    if series_names is None:
        series_names = ['headline', 'core']
    
    # 자동 분석 타입 결정
    if chart_type == 'auto':
        # 비계절조정 시리즈가 포함되어 있으면 혼합 차트 생성
        non_sa_in_series = any(series in ['headline_non_sa', 'core_non_sa'] for series in series_names)
        sa_in_series = any(series not in ['headline_non_sa', 'core_non_sa'] for series in series_names)
        
        if non_sa_in_series and sa_in_series:
            # 혼합된 경우: 각 시리즈별로 적절한 데이터 사용
            yoy_series = [s for s in series_names if determine_analysis_type(s) == 'yoy']
            mom_series = [s for s in series_names if determine_analysis_type(s) == 'mom']
            
            yoy_df = get_yoy_data(yoy_series) if yoy_series else pd.DataFrame()
            mom_df = get_mom_data(mom_series) if mom_series else pd.DataFrame()
            
            # 두 데이터프레임 합치기
            df = pd.concat([yoy_df, mom_df], axis=1)
            ytitle = "%"
            print("소비자물가지수 - YoY(비계절조정) / MoM(계절조정)")
        elif non_sa_in_series:
            df = get_yoy_data(series_names)
            ytitle = "%"
            print("소비자물가지수 - 전년동월대비 변화율 (비계절조정)")
        else:
            df = get_mom_data(series_names)
            ytitle = "%"
            print("소비자물가지수 - 전월대비 변화율 (계절조정)")
    
    # 수동 분석 타입
    elif chart_type == 'yoy':
        df = get_yoy_data(series_names)
        ytitle = "%"
        print("소비자물가지수 - 전년동월대비 변화율")
    elif chart_type == 'mom':
        df = get_mom_data(series_names)
        ytitle = "%"
        print("소비자물가지수 - 전월대비 변화율")
    else:  # level
        df = get_raw_data(series_names)
        ytitle = "지수"
        print("소비자물가지수 - 수준")
    
    if df.empty:
        print("⚠️ 요청한 데이터가 없습니다.")
        return None
    
    # 라벨 매핑 (한국어)
    label_mapping = {
        'headline': '전체 CPI',
        'core': '코어 CPI (음식·에너지 제외)',
        'headline_non_sa': '전체 CPI (비계절조정)',
        'core_non_sa': '코어 CPI (비계절조정)',
        'food': '음식',
        'energy': '에너지',
        'shelter': '주거',
        'medical': '의료',
        'transport': '교통',
        'apparel': '의류',
        'recreation': '여가',
        'education': '교육·통신',
        'other_goods': '기타 상품·서비스'
    }
    
    # 데이터 준비
    chart_data = {}
    for col in df.columns:
        label = label_mapping.get(col, col)
        chart_data[label] = df[col].dropna()
    
    # KPDS 포맷 사용하여 차트 생성
    chart_df = pd.DataFrame(chart_data)
    
    if chart_type in ['yoy', 'mom']:
        fig = df_multi_line_chart(chart_df, ytitle=ytitle)
        fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)
        
        if chart_type == 'yoy':
            fig.add_hline(y=2, line_width=1, line_color="red", opacity=0.3, line_dash="dash")
            # 2% 목표 라벨 추가
            if not chart_df.empty:
                fig.add_annotation(
                    text="2% Target",
                    x=chart_df.index[-1],
                    y=2.1,
                    showarrow=False,
                    font=dict(size=10, color="red")
                )
    else:
        fig = df_multi_line_chart(chart_df, ytitle=ytitle)
    
    return fig

def create_cpi_component_comparison(components=None):
    """
    저장된 데이터를 사용한 CPI 구성요소 비교 차트
    
    Args:
        components: 비교할 구성요소 리스트
        title: 차트 제목
    """
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    if components is None:
        components = ['food', 'energy', 'shelter', 'medical', 'transport']
    
    # YoY 데이터 가져오기
    df = get_yoy_data(components)
    
    if df.empty:
        print("⚠️ 요청한 데이터가 없습니다.")
        return None
    
    # 라벨 매핑 (한국어)
    label_mapping = {
        'food': '음식', 'energy': '에너지', 'shelter': '주거',
        'medical': '의료', 'transport': '교통',
        'apparel': '의류', 'recreation': '여가',
        'education': '교육·통신', 'other': '기타 상품·서비스',
        'gasoline': '휘발유', 'used_cars': '중고차', 'rent': '주거임대료'
    }
    
    # 데이터 준비
    chart_data = {}
    for col in df.columns:
        label = label_mapping.get(col, col)
        chart_data[label] = df[col].dropna()
    
    print("소비자물가지수 구성요소 - 전년동월대비 변화율")
    
    # KPDS 포맷 사용하여 차트 생성
    chart_df = pd.DataFrame(chart_data)
    fig = df_multi_line_chart(chart_df, ytitle="%")
    
    # 0선 추가
    fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)
    
    return fig

def create_cpi_bar_chart(selected_components=None, custom_labels=None):
    """
    저장된 데이터를 사용한 CPI 바 차트 생성
    
    Args:
        selected_components: 선택할 구성요소 리스트
        custom_labels: 사용자 정의 라벨
    """
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    if selected_components is None:
        selected_components = ['headline', 'food', 'energy', 'core']
    
    # 최신 데이터 가져오기
    latest_data = get_latest_values(selected_components)
    
    if not latest_data:
        print("⚠️ 요청한 데이터가 없습니다.")
        return None
    
    # 기본 라벨 설정 (한국어)
    default_labels = {
        'headline': '전체 CPI',
        'core': '코어 CPI\n(음식·에너지\n제외)',
        'food': '음식', 'energy': '에너지', 'shelter': '주거',
        'medical': '의료', 'transport': '교통',
        'apparel': '의류', 'recreation': '여가',
        'education': '교육·통신',
        'other': '기타 상품·\n서비스',
        'gasoline': '휘발유', 'used_cars': '중고차', 'rent': '주거임대료'
    }
    
    # 라벨 설정
    labels = {**default_labels, **(custom_labels or {})}
    
    # 차트 데이터 준비
    chart_data = {}
    for comp in selected_components:
        if comp in latest_data:
            label = labels.get(comp, comp)
            chart_data[label] = latest_data[comp]
    
    # 제목 출력
    if not CPI_DATA['raw_data'].empty:
        latest_date = CPI_DATA['raw_data'].index[-1].strftime('%Y년 %m월')
        print(f"소비자물가지수 12개월 변화율 - 주요 품목, {latest_date}, 계절조정 미적용")
    else:
        print("소비자물가지수 12개월 변화율")
    
    # KPDS 포맷 사용하여 바 차트 생성
    fig = create_kpds_cpi_bar_chart(chart_data)
    
    return fig

# %%
# === 유틸리티 함수들 ===

def show_available_components():
    """사용 가능한 CPI 구성요소 표시"""
    print("=== 사용 가능한 CPI 구성요소 ===")
    
    components = {
        'headline': 'All items (전체 CPI)',
        'core': 'All items less food and energy (코어 CPI)', 
        'food': 'Food (식품)', 'energy': 'Energy (에너지)',
        'shelter': 'Shelter (주거)', 'medical': 'Medical care (의료)',
        'transport': 'Transportation (교통)', 'apparel': 'Apparel (의류)',
        'recreation': 'Recreation (여가)', 'education': 'Education and communication (교육통신)',
        'other': 'Other goods and services (기타)', 'gasoline': 'Gasoline (휘발유)',
        'used_cars': 'Used vehicles (중고차)', 'rent': 'Rent of primary residence (주거임대료)',
        'sticky': 'Sticky Price CPI (스티키 CPI)', 'super_sticky': 'Super Sticky CPI (슈퍼 스티키 CPI)'
    }
    
    for key, desc in components.items():
        print(f"  '{key}': {desc}")

def get_data_status():
    """현재 데이터 상태 반환"""
    return {
        'loaded': CPI_DATA['load_info']['loaded'],
        'series_count': CPI_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': CPI_DATA['load_info']
    }

# %%
# === 세부 분석 함수들 ===

def calculate_mom_changes(raw_data):
    """
    Month-over-Month 변화율 계산
    
    Args:
        raw_data: 원본 레벨 데이터
    
    Returns:
        DataFrame: MoM 변화율 데이터
    """
    mom_data = {}
    for col in raw_data.columns:
        series = raw_data[col]
        # Sticky Price CPI는 이미 YoY 처리된 데이터이므로 MoM 계산 안함
        if 'sticky' in col.lower() or 'CORESTICKM159SFRBATL' in str(series.name) or 'STICKCPIXSHLTRM159SFRBATL' in str(series.name):
            mom_data[col] = series  # 이미 처리된 데이터 그대로 사용
        else:
            mom_change = ((series / series.shift(1)) - 1) * 100
            mom_data[col] = mom_change
    
    return pd.DataFrame(mom_data, index=raw_data.index)

def create_cpi_contribution_chart(months_back=2):
    """
    Core CPI MoM 기여도 차트 생성 (첫 번째 이미지 스타일)
    
    Args:
        months_back: 표시할 과거 개월 수
    
    Returns:
        plotly figure
    """
    print("미국: 근원 소비자물가지수 전월대비 기여도 (%)")
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 최신 MoM 데이터 계산
    raw_data = get_raw_data()
    mom_data = calculate_mom_changes(raw_data)
    
    # 최근 N개월 데이터
    recent_data = mom_data.tail(months_back)
    
    # 실제 CPI 구성요소별 매핑 (한국어)
    components_mapping = {
        'core': '코어 CPI',
        'food': '핵심 상품',
        'used_cars': '중고차',
        'gasoline': '신차',  # 실제로는 gasoline을 new cars로 매핑
        'energy': '기타 상품',  # energy를 other goods로 매핑
        'shelter': '핵심 서비스',
        'rent': '주거',
        'owners_eq': '자가주거비용',
        'medical': '주거임대료',
        'transport': '숙박',
        'apparel': '의료서비스',
        'other': '자동차보험'
    }
    
    # 데이터 준비
    chart_data = {}
    for comp, label in components_mapping.items():
        if comp in recent_data.columns:
            chart_data[label] = recent_data[comp].dropna()
    
    if not chart_data:
        print("⚠️ 표시할 데이터가 없습니다.")
        return None
    
    # 월별로 바 차트 생성
    months = recent_data.index[-months_back:]
    month_labels = [month.strftime('%b') for month in months]
    
    fig = go.Figure()
    
    # 각 컴포넌트별로 바 추가
    for i, (label, data) in enumerate(chart_data.items()):
        color = get_kpds_color(i)
        values = [data.loc[month] if month in data.index and not pd.isna(data.loc[month]) else 0 
                 for month in months]
        
        fig.add_trace(go.Bar(
            name=label,
            x=month_labels,
            y=values,
            marker_color=color,
            text=[f'{v:.2f}' for v in values],
            textposition='auto'
        ))
    
    # 레이아웃 설정
    fig.update_layout(
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686,
        height=500,
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color="black"),
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside'
        ),
        yaxis=dict(
            showline=False,
            tickcolor='white',
            tickformat='.2f',
            title=dict(text='기여도 (%)', font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE))
        ),
        legend=dict(
            orientation="v",
            yanchor="top", y=1,
            xanchor="left", x=1.02,
            font=dict(family='NanumGothic', size=FONT_SIZE_LEGEND)
        ),
        margin=dict(l=80, r=150, t=80, b=60)
    )
    
    # 0선 추가
    fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.7)
    
    fig.show()
    return fig

# %%
# === 계층적 시각화 함수들 ===

def create_hierarchical_cpi_chart(level='level2', title=None):
    """
    계층별 CPI 차트 생성
    
    Args:
        level: 표시할 계층 ('level1', 'level2', 'level3', 'level4')
        title: 차트 제목
    
    Returns:
        plotly figure
    """
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    if level not in CPI_HIERARCHY:
        print(f"⚠️ 잘못된 계층: {level}")
        return None
    
    # 해당 계층의 데이터 가져오기
    level_data = CPI_HIERARCHY[level]
    raw_data = get_raw_data()
    mom_data = calculate_mom_changes(raw_data)
    latest_mom = mom_data.iloc[-1]
    
    # 데이터 준비
    categories = []
    values = []
    colors = []
    
    for i, (key, info) in enumerate(level_data.items()):
        if key in latest_mom.index and not pd.isna(latest_mom[key]):
            categories.append(info['name_kr'])
            value = latest_mom[key]
            values.append(value)
            
            # 계층에 따른 색상 구분
            if level == 'level1':
                colors.append('#FF6B35' if value >= 0 else '#4ECDC4')
            elif level == 'level2':
                colors.append(get_kpds_color(i))
            else:
                colors.append(deepred_pds if value >= 0 else deepblue_pds)
    
    if not categories:
        print(f"⚠️ {level} 데이터가 없습니다.")
        return None
    
    # 가로 바 차트 생성
    fig = go.Figure()
    
    # 데이터 정렬 (내림차순)
    sorted_indices = sorted(range(len(values)), key=lambda i: values[i])
    
    sorted_categories = [categories[i] for i in sorted_indices]
    sorted_values = [values[i] for i in sorted_indices]
    sorted_colors = [colors[i] for i in sorted_indices]
    
    fig.add_trace(go.Bar(
        y=sorted_categories,
        x=sorted_values,
        orientation='h',
        marker_color=sorted_colors,
        text=[f'{v:+.2f}%' for v in sorted_values],
        textposition='outside' if all(v >= 0 for v in sorted_values) else 'auto',
        showlegend=False
    ))
    
    # 레이아웃 설정
    level_titles = {
        'level1': 'CPI 최상위 분류',
        'level2': 'CPI 주요 카테고리',
        'level3': 'CPI 중위 분류',
        'level4': 'CPI 세부 분류'
    }
    
    fig.update_layout(
        title=dict(
            text=title or f"{level_titles[level]} (6월 MoM 변화율)",
            font=dict(family='NanumGothic', size=FONT_SIZE_TITLE),
            x=0.5, xanchor="center"
        ),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=900,
        height=max(400, len(categories) * 30),
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL-1, color="black"),
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            tickformat='.2f',
            title=dict(text='MoM 변화율 (%)', font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE))
        ),
        yaxis=dict(
            showline=False,
            tickcolor='white'
        ),
        margin=dict(l=200, r=80, t=80, b=80)
    )
    
    # 격자선 및 0선
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="lightgrey")
    fig.update_yaxes(showgrid=False)
    fig.add_vline(x=0, line_width=2, line_color="black")
    
    fig.show()
    return fig

def show_cpi_hierarchy_info():
    """
    CPI 계층 구조 정보 표시 (325개 시리즈)
    """
    print("=== CPI 계층 구조 (325개 시리즈) ===\n")
    
    level_names = {
        'level1': '레벨 1: 최상위 종합지표',
        'level2': '레벨 2: 상품/서비스 구분',
        'level3': '레벨 3: 주요 생활비 카테고리',
        'level4': '레벨 4: 세부 카테고리',
        'level5': '레벨 5: 더 세부적인 분류',
        'level6': '레벨 6: 가장 세부적인 분류',
        'level7': '레벨 7: 매우 세부적인 분류',
        'level8': '레벨 8: 개별 품목'
    }
    
    for level, level_data in CPI_HIERARCHY.items():
        print(f"### {level_names.get(level, level)} ({len(level_data)}개) ###")
        
        # 처음 5개만 보여주기 (너무 많아서)
        for i, (key, info) in enumerate(level_data.items()):
            if i >= 5:
                print(f"  ... 그 외 {len(level_data) - 5}개")
                break
            
            parent_info = f" → {info['parent']}" if 'parent' in info else ""
            korean_name = ALL_KOREAN_NAMES.get(key, info['name'])
            
            print(f"  {key}: {korean_name}{parent_info}")
        print()

# %%
# 계층 구조 정보 표시
show_cpi_hierarchy_info()

# %%
# === 계층적 CPI 차트 함수 ===

def create_hierarchical_cpi_chart(level='level2', chart_type='auto', title=None):
    """
    계층별 CPI 차트 생성
    
    Args:
        level: 표시할 계층 ('level1', 'level2', 'level3', 'level4')
        chart_type: 차트 타입 ('yoy', 'mom', 'auto')
        title: 차트 제목
    
    Returns:
        plotly figure
    """
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    if level not in CPI_HIERARCHY:
        print(f"⚠️ 잘못된 계층: {level}")
        return None
    
    # 해당 계층의 시리즈 키들 가져오기
    level_data = CPI_HIERARCHY[level]
    series_keys = list(level_data.keys())
    
    # 레벨 이름 한국어 매핑
    level_korean_names = {
        'Total': '전체',
        'Core': '근원',
        'Food': '식품',
        'Energy': '에너지',
        'Goods': '상품',
        'Services': '서비스',
        'Housing': '주거',
        'Transportation': '교통',
        'Medical': '의료',
        'Recreation': '여가',
        'Education': '교육',
        'Apparel': '의류',
        'Other': '기타'
    }
    
    level_kr = level_korean_names.get(level, level)
    
    # 자동 분석 타입 결정
    if chart_type == 'auto':
        # 비계절조정 시리즈가 있는지 확인
        non_sa_in_series = any(series in ['headline_non_sa', 'core_non_sa'] for series in series_keys)
        
        if non_sa_in_series:
            # 혼합: 비계절조정은 YoY, 나머지는 MoM
            yoy_series = [s for s in series_keys if determine_analysis_type(s) == 'yoy']
            mom_series = [s for s in series_keys if determine_analysis_type(s) == 'mom']
            
            yoy_df = get_yoy_data(yoy_series) if yoy_series else pd.DataFrame()
            mom_df = get_mom_data(mom_series) if mom_series else pd.DataFrame()
            
            df = pd.concat([yoy_df, mom_df], axis=1)
            ytitle = "%"
            if title is None:
                title = f"소비자물가지수 {level_kr} - YoY(비계절조정) / MoM(계절조정)"
        else:
            # 모두 계절조정 시리즈: MoM 사용
            df = get_mom_data(series_keys)
            ytitle = "%"
            if title is None:
                title = f"소비자물가지수 {level_kr} - 전월대비 변화율"
    elif chart_type == 'yoy':
        df = get_yoy_data(series_keys)
        ytitle = "%"
        if title is None:
            title = f"소비자물가지수 {level_kr} - 전년동월대비 변화율"
    else:  # mom
        df = get_mom_data(series_keys)
        ytitle = "%"
        if title is None:
            title = f"소비자물가지수 {level_kr} - 전월대비 변화율"
    
    if df.empty:
        print(f"⚠️ {level} 데이터가 없습니다.")
        return None
    
    # 최신 데이터로 바 차트 생성
    latest_data = df.iloc[-1].dropna()
    
    # 데이터 준비
    categories = []
    values = []
    colors = []
    
    for series_key, value in latest_data.items():
        if series_key in level_data:
            info = level_data[series_key]
            categories.append(info['name_kr'])
            values.append(value)
            
            # 계층에 따른 색상 구분
            if level == 'level1':
                colors.append('#FF6B35' if value >= 0 else '#4ECDC4')
            elif level == 'level2':
                colors.append(get_kpds_color(len(categories) - 1))
            else:
                colors.append(deepred_pds if value >= 0 else deepblue_pds)
    
    if not categories:
        print(f"⚠️ {level} 데이터가 없습니다.")
        return None
    
    # 가로 바 차트 생성
    fig = go.Figure()
    
    # 데이터 정렬 (내림차순)
    sorted_indices = sorted(range(len(values)), key=lambda i: values[i])
    
    sorted_categories = [categories[i] for i in sorted_indices]
    sorted_values = [values[i] for i in sorted_indices]
    sorted_colors = [colors[i] for i in sorted_indices]
    
    fig.add_trace(go.Bar(
        y=sorted_categories,
        x=sorted_values,
        orientation='h',
        marker_color=sorted_colors,
        text=[f'{v:+.2f}%' for v in sorted_values],
        textposition='outside' if all(v >= 0 for v in sorted_values) else 'auto',
        showlegend=False
    ))
    
    # 레이아웃 설정
    level_titles = {
        'level1': 'CPI 최상위 분류',
        'level2': 'CPI 주요 카테고리',
        'level3': 'CPI 중위 분류',
        'level4': 'CPI 세부 분류'
    }
    
    fig.update_layout(
        title=dict(
            text=title or f"{level_titles[level]} ({chart_type.upper()} 변화율)",
            font=dict(family='NanumGothic', size=FONT_SIZE_TITLE),
            x=0.5, xanchor="center"
        ),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=900,
        height=max(400, len(categories) * 30),
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL-1, color="black"),
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            tickformat='.2f',
            title=dict(text=ytitle, font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE))
        ),
        yaxis=dict(
            showline=False,
            tickcolor='white'
        ),
        margin=dict(l=200, r=80, t=80, b=80)
    )
    
    # 격자선 및 0선
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="lightgrey")
    fig.update_yaxes(showgrid=False)
    fig.add_vline(x=0, line_width=2, line_color="black")
    
    fig.show()
    return fig

# %%
# === 사용 예시 ===

if __name__ == "__main__":
    print("\n=== 리팩토링된 CPI 데이터 분석 도구 사용법 ===")
    print("1. 데이터 로드:")
    print("   load_cpi_data()  # 스마트 업데이트 활성화")
    print("   load_cpi_data(smart_update=False)  # 전체 재로드")
    print("   load_cpi_data(force_reload=True)  # 강제 재로드")
    print()
    print("2. 🔥 범용 시각화 함수 (가장 강력한 함수!):")
    print("   # 헤드라인 vs 코어 CPI 비교")
    print("   plot_cpi_series_advanced(['headline', 'core'], 'multi_line', 'mom')")
    print("   # 식품, 에너지, 주거 비교")
    print("   plot_cpi_series_advanced(['food', 'energy', 'shelter'], 'multi_line', 'yoy')")
    print("   # 가로 바 차트 (최신값 기준)")
    print("   plot_cpi_series_advanced(['food', 'energy', 'shelter'], 'horizontal_bar', 'mom')")
    print("   # 최근 24개월만 보기")
    print("   plot_cpi_series_advanced(['headline', 'core'], 'single_line', 'mom', periods=24)")
    print("   # 특정 날짜 기준 차트")
    print("   plot_cpi_series_advanced(['headline'], 'single_line', 'mom', target_date='2024-06-01')")
    print("   # 이중축 차트 (레벨 vs 변화율)")
    print("   plot_cpi_series_advanced(['headline', 'core'], 'dual_axis', 'raw')")
    print()
    print("3. 기존 전용 시각화 함수들:")
    print("   create_cpi_timeseries_chart(['headline', 'core'], 'auto')  # 자동 분석")
    print("   create_cpi_component_comparison(['food', 'energy', 'shelter'], 'yoy')")
    print("   create_hierarchical_cpi_chart('level2', 'auto')  # 계층별 차트")
    print()
    print("4. 🔥 데이터 Export (새로운 기능!):")
    print("   # 엑셀로 export (전체 데이터)")
    print("   export_cpi_data(['headline', 'core'], 'mom')")
    print("   # CSV로 export (최근 24개월)")
    print("   export_cpi_data(['headline'], 'raw', periods=24, file_format='csv')")
    print("   # 특정 날짜까지만")
    print("   export_cpi_data(['food', 'energy'], 'yoy', target_date='2024-06-01')")
    print("   # 커스텀 경로 지정")
    print("   export_cpi_data(['headline'], 'mom', export_path=repo_path('my_cpi_data.xlsx'))")
    print()
    print("5. 통합 분석:")
    print("   run_cpi_analysis()  # 전체 CPI 분석")
    print()
    print("✅ plot_cpi_series_advanced()는 325개 시리즈 중 어떤 것도 원하는 형태로 시각화 가능!")
    print("✅ export_cpi_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
    print("✅ 모든 함수가 us_eco_utils의 통합 함수를 사용합니다.")
    print("✅ 기존 CPI 전용 분석 함수들도 그대로 사용 가능합니다!")


    # %%
    result = load_cpi_data()
    plot_cpi_series_advanced(['commodities', 'services', 'durables', 'nondurables', 'energy_commodities'], 'multi_line', 'yoy')

    # %%
    plot_cpi_series_advanced(['apparel', 'appliances', 'furniture_bedding', 'sports_equipment', 'toys'], 'multi_line', 'yoy')

    # %%
    plot_cpi_series_advanced(['headline', 'core'], 'multi_line', 'yoy')

    # %%
    plot_cpi_series_advanced(['commodities', 'services', 'durables',
                              'nondurables', 'energy_commodities'], 'multi_line', 'yoy')
    # %%
    create_hierarchical_cpi_chart('level4', 'yoy')
    # %%
