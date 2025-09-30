# %%
"""
New Residential Construction 데이터 분석 (리팩토링 버전)
- us_eco_utils를 사용한 통합 구조
- 시리즈 정의와 분석 로직만 포함
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
# === FRED API 키 설정 ===

# FRED 데이터인 경우:
api_config.FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'

# %%
# === New Residential Construction 시리즈 정의 ===

# 메인 시리즈 딕셔너리 (올바른 구조: 시리즈_이름: API_ID)
NEW_RESIDENTIAL_CONSTRUCTION_SERIES = {
    # === National Level - 5단계 건설 프로세스 ===
    
    # 1. Permits (건축 허가)
    'permits_total_sa': 'PERMIT',           # 전체 건축허가 (계절조정)
    'permits_total_nsa': 'PERMITNSA',       # 전체 건축허가 (원계열)
    'permits_single_sa': 'PERMIT1',         # 단독주택 허가 (계절조정)
    'permits_single_nsa': 'PERMIT1NSA',     # 단독주택 허가 (원계열)
    'permits_2to4_sa': 'PERMIT24',          # 2-4세대 허가 (계절조정)
    'permits_2to4_nsa': 'PERMIT24NSA',      # 2-4세대 허가 (원계열)
    'permits_5plus_sa': 'PERMIT5',          # 5세대 이상 허가 (계절조정)
    'permits_5plus_nsa': 'PERMIT5NSA',      # 5세대 이상 허가 (원계열)
    
    # 2. Authorized but Not Started (허가 후 미착공)
    'auth_not_started_total_sa': 'AUTHNOTTSA',     # 전체 허가 미착공 (계절조정)
    'auth_not_started_total_nsa': 'AUTHNOTT',      # 전체 허가 미착공 (원계열)
    'auth_not_started_single_sa': 'AUTHNOT1USA',   # 단독주택 허가 미착공 (계절조정)
    'auth_not_started_single_nsa': 'AUTHNOT1U',    # 단독주택 허가 미착공 (원계열)
    'auth_not_started_2to4_sa': 'AUTHNOT24USA',    # 2-4세대 허가 미착공 (계절조정)
    'auth_not_started_2to4_nsa': 'AUTHNOT24U',     # 2-4세대 허가 미착공 (원계열)
    'auth_not_started_5plus_sa': 'AUTHNOT5MUSA',   # 5세대 이상 허가 미착공 (계절조정)
    'auth_not_started_5plus_nsa': 'AUTHNOT5MU',    # 5세대 이상 허가 미착공 (원계열)
    
    # 3. Started (착공 - Housing Starts)
    'starts_total_sa': 'HOUST',             # 전체 착공 (계절조정)
    'starts_total_nsa': 'HOUSTNSA',         # 전체 착공 (원계열)
    'starts_single_sa': 'HOUST1F',          # 단독주택 착공 (계절조정)
    'starts_single_nsa': 'HOUST1FNSA',      # 단독주택 착공 (원계열)
    'starts_2to4_sa': 'HOUST2F',            # 2-4세대 착공 (계절조정)
    'starts_2to4_nsa': 'HOUST2FNSA',        # 2-4세대 착공 (원계열)
    'starts_5plus_sa': 'HOUST5F',           # 5세대 이상 착공 (계절조정)
    'starts_5plus_nsa': 'HOUST5FNSA',       # 5세대 이상 착공 (원계열)
    
    # 4. Under Construction (건설중)
    'under_construction_total_sa': 'UNDCONTSA',     # 전체 건설중 (계절조정)
    'under_construction_total_nsa': 'UNDCONTNSA',   # 전체 건설중 (원계열)
    'under_construction_single_sa': 'UNDCON1USA',   # 단독주택 건설중 (계절조정)
    'under_construction_single_nsa': 'UNDCON1UNSA', # 단독주택 건설중 (원계열)
    'under_construction_2to4_sa': 'UNDCON24USA',    # 2-4세대 건설중 (계절조정)
    'under_construction_2to4_nsa': 'UNDCON24UNSA',  # 2-4세대 건설중 (원계열)
    'under_construction_5plus_sa': 'UNDCON5MUSA',   # 5세대 이상 건설중 (계절조정)
    'under_construction_5plus_nsa': 'UNDCON5MUNSA', # 5세대 이상 건설중 (원계열)
    
    # 5. Completed (완공)
    'completed_total_sa': 'COMPUTSA',       # 전체 완공 (계절조정)
    'completed_total_nsa': 'COMPUTNSA',     # 전체 완공 (원계열)
    'completed_single_sa': 'COMPU1USA',     # 단독주택 완공 (계절조정)
    'completed_single_nsa': 'COMPU1UNSA',   # 단독주택 완공 (원계열)
    'completed_2to4_sa': 'COMPU24USA',      # 2-4세대 완공 (계절조정)
    'completed_2to4_nsa': 'COMPU24UNSA',    # 2-4세대 완공 (원계열)
    'completed_5plus_sa': 'COMPU5MUSA',     # 5세대 이상 완공 (계절조정)
    'completed_5plus_nsa': 'COMPU5MUNSA',   # 5세대 이상 완공 (원계열)
    
    # === Regional Level - 주요 지역별 시리즈 ===
    
    # Permits by Region (Total)
    'permits_northeast_sa': 'PERMITNE',     # 동북부 허가 (계절조정)
    'permits_northeast_nsa': 'PERMITNENSA', # 동북부 허가 (원계열)
    'permits_midwest_sa': 'PERMITMW',       # 중서부 허가 (계절조정)
    'permits_midwest_nsa': 'PERMITMWNSA',   # 중서부 허가 (원계열)
    'permits_south_sa': 'PERMITS',          # 남부 허가 (계절조정)
    'permits_south_nsa': 'PERMITSNSA',      # 남부 허가 (원계열)
    'permits_west_sa': 'PERMITW',           # 서부 허가 (계절조정)
    'permits_west_nsa': 'PERMITWNSA',       # 서부 허가 (원계열)
    
    # Permits by Region (Single-Family)
    'permits_northeast_single_sa': 'PERMITNE1',     # 동북부 단독주택 허가 (계절조정)
    'permits_northeast_single_nsa': 'PERMITNE1NSA', # 동북부 단독주택 허가 (원계열)
    'permits_midwest_single_sa': 'PERMITMW1',       # 중서부 단독주택 허가 (계절조정)
    'permits_midwest_single_nsa': 'PERMITMW1NSA',   # 중서부 단독주택 허가 (원계열)
    'permits_south_single_sa': 'PERMITS1',          # 남부 단독주택 허가 (계절조정)
    'permits_south_single_nsa': 'PERMITS1NSA',      # 남부 단독주택 허가 (원계열)
    'permits_west_single_sa': 'PERMITW1',           # 서부 단독주택 허가 (계절조정)
    'permits_west_single_nsa': 'PERMITW1NSA',       # 서부 단독주택 허가 (원계열)
    
    # Housing Starts by Region (Total)
    'starts_northeast_sa': 'HOUSTNE',       # 동북부 착공 (계절조정)
    'starts_northeast_nsa': 'HOUSTNENSA',   # 동북부 착공 (원계열)
    'starts_midwest_sa': 'HOUSTMW',         # 중서부 착공 (계절조정)
    'starts_midwest_nsa': 'HOUSTMWNSA',     # 중서부 착공 (원계열)
    'starts_south_sa': 'HOUSTS',            # 남부 착공 (계절조정)
    'starts_south_nsa': 'HOUSTSNSA',        # 남부 착공 (원계열)
    'starts_west_sa': 'HOUSTW',             # 서부 착공 (계절조정)
    'starts_west_nsa': 'HOUSTWNSA',         # 서부 착공 (원계열)
    
    # Under Construction by Region (Total)
    'under_construction_northeast_sa': 'UNDCONNETSA',   # 동북부 건설중 (계절조정)
    'under_construction_northeast_nsa': 'UNDCONNETNSA', # 동북부 건설중 (원계열)
    'under_construction_midwest_sa': 'UNDCONMWTSA',     # 중서부 건설중 (계절조정)
    'under_construction_midwest_nsa': 'UNDCONMWTNSA',   # 중서부 건설중 (원계열)
    'under_construction_south_sa': 'UNDCONSTSA',        # 남부 건설중 (계절조정)
    'under_construction_south_nsa': 'UNDCONSTNSA',      # 남부 건설중 (원계열)
    'under_construction_west_sa': 'UNDCONWTSA',         # 서부 건설중 (계절조정)
    'under_construction_west_nsa': 'UNDCONWTNSA',       # 서부 건설중 (원계열)
}

# 한국어 이름 매핑
NEW_RESIDENTIAL_CONSTRUCTION_KOREAN_NAMES = {
    # National Level - 5단계 건설 프로세스
    'permits_total_sa': '전체 건축허가 (계절조정)',
    'permits_total_nsa': '전체 건축허가 (원계열)',
    'permits_single_sa': '단독주택 허가 (계절조정)',
    'permits_single_nsa': '단독주택 허가 (원계열)',
    'permits_2to4_sa': '2-4세대 허가 (계절조정)',
    'permits_2to4_nsa': '2-4세대 허가 (원계열)',
    'permits_5plus_sa': '5세대 이상 허가 (계절조정)',
    'permits_5plus_nsa': '5세대 이상 허가 (원계열)',
    
    'auth_not_started_total_sa': '전체 허가 미착공 (계절조정)',
    'auth_not_started_total_nsa': '전체 허가 미착공 (원계열)',
    'auth_not_started_single_sa': '단독주택 허가 미착공 (계절조정)',
    'auth_not_started_single_nsa': '단독주택 허가 미착공 (원계열)',
    'auth_not_started_2to4_sa': '2-4세대 허가 미착공 (계절조정)',
    'auth_not_started_2to4_nsa': '2-4세대 허가 미착공 (원계열)',
    'auth_not_started_5plus_sa': '5세대 이상 허가 미착공 (계절조정)',
    'auth_not_started_5plus_nsa': '5세대 이상 허가 미착공 (원계열)',
    
    'starts_total_sa': '전체 주택착공 (계절조정)',
    'starts_total_nsa': '전체 주택착공 (원계열)',
    'starts_single_sa': '단독주택 착공 (계절조정)',
    'starts_single_nsa': '단독주택 착공 (원계열)',
    'starts_2to4_sa': '2-4세대 착공 (계절조정)',
    'starts_2to4_nsa': '2-4세대 착공 (원계열)',
    'starts_5plus_sa': '5세대 이상 착공 (계절조정)',
    'starts_5plus_nsa': '5세대 이상 착공 (원계열)',
    
    'under_construction_total_sa': '전체 건설중 주택 (계절조정)',
    'under_construction_total_nsa': '전체 건설중 주택 (원계열)',
    'under_construction_single_sa': '단독주택 건설중 (계절조정)',
    'under_construction_single_nsa': '단독주택 건설중 (원계열)',
    'under_construction_2to4_sa': '2-4세대 건설중 (계절조정)',
    'under_construction_2to4_nsa': '2-4세대 건설중 (원계열)',
    'under_construction_5plus_sa': '5세대 이상 건설중 (계절조정)',
    'under_construction_5plus_nsa': '5세대 이상 건설중 (원계열)',
    
    'completed_total_sa': '전체 주택완공 (계절조정)',
    'completed_total_nsa': '전체 주택완공 (원계열)',
    'completed_single_sa': '단독주택 완공 (계절조정)',
    'completed_single_nsa': '단독주택 완공 (원계열)',
    'completed_2to4_sa': '2-4세대 완공 (계절조정)',
    'completed_2to4_nsa': '2-4세대 완공 (원계열)',
    'completed_5plus_sa': '5세대 이상 완공 (계절조정)',
    'completed_5plus_nsa': '5세대 이상 완공 (원계열)',
    
    # Regional Level
    'permits_northeast_sa': '동북부 건축허가 (계절조정)',
    'permits_northeast_nsa': '동북부 건축허가 (원계열)',
    'permits_midwest_sa': '중서부 건축허가 (계절조정)',
    'permits_midwest_nsa': '중서부 건축허가 (원계열)',
    'permits_south_sa': '남부 건축허가 (계절조정)',
    'permits_south_nsa': '남부 건축허가 (원계열)',
    'permits_west_sa': '서부 건축허가 (계절조정)',
    'permits_west_nsa': '서부 건축허가 (원계열)',
    
    'permits_northeast_single_sa': '동북부 단독주택 허가 (계절조정)',
    'permits_northeast_single_nsa': '동북부 단독주택 허가 (원계열)',
    'permits_midwest_single_sa': '중서부 단독주택 허가 (계절조정)',
    'permits_midwest_single_nsa': '중서부 단독주택 허가 (원계열)',
    'permits_south_single_sa': '남부 단독주택 허가 (계절조정)',
    'permits_south_single_nsa': '남부 단독주택 허가 (원계열)',
    'permits_west_single_sa': '서부 단독주택 허가 (계절조정)',
    'permits_west_single_nsa': '서부 단독주택 허가 (원계열)',
    
    'starts_northeast_sa': '동북부 주택착공 (계절조정)',
    'starts_northeast_nsa': '동북부 주택착공 (원계열)',
    'starts_midwest_sa': '중서부 주택착공 (계절조정)',
    'starts_midwest_nsa': '중서부 주택착공 (원계열)',
    'starts_south_sa': '남부 주택착공 (계절조정)',
    'starts_south_nsa': '남부 주택착공 (원계열)',
    'starts_west_sa': '서부 주택착공 (계절조정)',
    'starts_west_nsa': '서부 주택착공 (원계열)',
    
    'under_construction_northeast_sa': '동북부 건설중 주택 (계절조정)',
    'under_construction_northeast_nsa': '동북부 건설중 주택 (원계열)',
    'under_construction_midwest_sa': '중서부 건설중 주택 (계절조정)',
    'under_construction_midwest_nsa': '중서부 건설중 주택 (원계열)',
    'under_construction_south_sa': '남부 건설중 주택 (계절조정)',
    'under_construction_south_nsa': '남부 건설중 주택 (원계열)',
    'under_construction_west_sa': '서부 건설중 주택 (계절조정)',
    'under_construction_west_nsa': '서부 건설중 주택 (원계열)',
}

# 카테고리 분류
NEW_RESIDENTIAL_CONSTRUCTION_CATEGORIES = {
    '건설 프로세스별 (전체)': {
        '1. Permits': ['permits_total_sa', 'permits_total_nsa'],
        '2. Auth Not Started': ['auth_not_started_total_sa', 'auth_not_started_total_nsa'],
        '3. Housing Starts': ['starts_total_sa', 'starts_total_nsa'],
        '4. Under Construction': ['under_construction_total_sa', 'under_construction_total_nsa'],
        '5. Completed': ['completed_total_sa', 'completed_total_nsa']
    },
    '주택 유형별 (계절조정)': {
        'Single-Family': ['permits_single_sa', 'starts_single_sa', 'under_construction_single_sa', 'completed_single_sa'],
        '2-4 Units': ['permits_2to4_sa', 'starts_2to4_sa', 'under_construction_2to4_sa', 'completed_2to4_sa'],
        '5+ Units': ['permits_5plus_sa', 'starts_5plus_sa', 'under_construction_5plus_sa', 'completed_5plus_sa']
    },
    '주요 지표 (계절조정)': {
        'Permits vs Starts': ['permits_total_sa', 'starts_total_sa'],
        'Single-Family Flow': ['permits_single_sa', 'starts_single_sa', 'completed_single_sa'],
        'Multi-Family Flow': ['permits_5plus_sa', 'starts_5plus_sa', 'completed_5plus_sa'],
        'Construction Pipeline': ['permits_total_sa', 'auth_not_started_total_sa', 'starts_total_sa', 'under_construction_total_sa', 'completed_total_sa']
    },
    '지역별 허가 (계절조정)': {
        'Regional Permits': ['permits_northeast_sa', 'permits_midwest_sa', 'permits_south_sa', 'permits_west_sa'],
        'Regional Single-Family': ['permits_northeast_single_sa', 'permits_midwest_single_sa', 'permits_south_single_sa', 'permits_west_single_sa']
    },
    '지역별 착공 (계절조정)': {
        'Regional Starts': ['starts_northeast_sa', 'starts_midwest_sa', 'starts_south_sa', 'starts_west_sa'],
        'Regional Construction': ['under_construction_northeast_sa', 'under_construction_midwest_sa', 'under_construction_south_sa', 'under_construction_west_sa']
    },
    '계절조정별': {
        'Seasonally Adjusted': [s for s in NEW_RESIDENTIAL_CONSTRUCTION_SERIES.keys() if s.endswith('_sa')],
        'Not Seasonally Adjusted': [s for s in NEW_RESIDENTIAL_CONSTRUCTION_SERIES.keys() if s.endswith('_nsa')]
    }
}

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/new_residential_construction_data_refactored.csv'
NEW_RESIDENTIAL_CONSTRUCTION_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_new_residential_construction_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 New Residential Construction 데이터 로드"""
    global NEW_RESIDENTIAL_CONSTRUCTION_DATA

    # 데이터 소스별 분기
    data_source = 'FRED'  # Census Bureau/HUD 데이터는 FRED에서 제공
    tolerance = 10.0  # 일반 지표 허용 오차
    
    result = load_economic_data(
        series_dict=NEW_RESIDENTIAL_CONSTRUCTION_SERIES,
        data_source=data_source,
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=tolerance
    )

    if result:
        NEW_RESIDENTIAL_CONSTRUCTION_DATA = result
        print_load_info()
        return True
    else:
        print("❌ New Residential Construction 데이터 로드 실패")
        return False

def print_load_info():
    """New Residential Construction 데이터 로드 정보 출력"""
    if not NEW_RESIDENTIAL_CONSTRUCTION_DATA or 'load_info' not in NEW_RESIDENTIAL_CONSTRUCTION_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = NEW_RESIDENTIAL_CONSTRUCTION_DATA['load_info']
    print(f"\n📊 New Residential Construction 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in NEW_RESIDENTIAL_CONSTRUCTION_DATA and not NEW_RESIDENTIAL_CONSTRUCTION_DATA['raw_data'].empty:
        latest_date = NEW_RESIDENTIAL_CONSTRUCTION_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_new_residential_construction_series_advanced(series_list, chart_type='multi_line', 
                                                    data_type='raw', periods=None, target_date=None,
                                                    left_ytitle=None, right_ytitle=None):
    """범용 New Residential Construction 시각화 함수 - plot_economic_series 활용"""
    if not NEW_RESIDENTIAL_CONSTRUCTION_DATA:
        print("⚠️ 먼저 load_new_residential_construction_data()를 실행하세요.")
        return None

    # 단위별 기본 Y축 제목 설정
    if data_type == 'mom':
        default_ytitle = "%"
    elif data_type == 'yoy':
        default_ytitle = "%"  
    else:  # raw
        default_ytitle = "천 개 (연율)"

    return plot_economic_series(
        data_dict=NEW_RESIDENTIAL_CONSTRUCTION_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        left_ytitle=left_ytitle or default_ytitle,
        right_ytitle=right_ytitle or default_ytitle,
        korean_names=NEW_RESIDENTIAL_CONSTRUCTION_KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_new_residential_construction_data(series_list, data_type='raw', periods=None, 
                                           target_date=None, export_path=None, file_format='excel'):
    """New Residential Construction 데이터 export 함수 - export_economic_data 활용"""
    if not NEW_RESIDENTIAL_CONSTRUCTION_DATA:
        print("⚠️ 먼저 load_new_residential_construction_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=NEW_RESIDENTIAL_CONSTRUCTION_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=NEW_RESIDENTIAL_CONSTRUCTION_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_new_residential_construction_data():
    """New Residential Construction 데이터 초기화"""
    global NEW_RESIDENTIAL_CONSTRUCTION_DATA
    NEW_RESIDENTIAL_CONSTRUCTION_DATA = {}
    print("🗑️ New Residential Construction 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not NEW_RESIDENTIAL_CONSTRUCTION_DATA or 'raw_data' not in NEW_RESIDENTIAL_CONSTRUCTION_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_new_residential_construction_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return NEW_RESIDENTIAL_CONSTRUCTION_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in NEW_RESIDENTIAL_CONSTRUCTION_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return NEW_RESIDENTIAL_CONSTRUCTION_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not NEW_RESIDENTIAL_CONSTRUCTION_DATA or 'mom_data' not in NEW_RESIDENTIAL_CONSTRUCTION_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_new_residential_construction_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return NEW_RESIDENTIAL_CONSTRUCTION_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in NEW_RESIDENTIAL_CONSTRUCTION_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return NEW_RESIDENTIAL_CONSTRUCTION_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not NEW_RESIDENTIAL_CONSTRUCTION_DATA or 'yoy_data' not in NEW_RESIDENTIAL_CONSTRUCTION_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_new_residential_construction_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return NEW_RESIDENTIAL_CONSTRUCTION_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in NEW_RESIDENTIAL_CONSTRUCTION_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return NEW_RESIDENTIAL_CONSTRUCTION_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not NEW_RESIDENTIAL_CONSTRUCTION_DATA or 'raw_data' not in NEW_RESIDENTIAL_CONSTRUCTION_DATA:
        return []
    return list(NEW_RESIDENTIAL_CONSTRUCTION_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 New Residential Construction 시리즈 표시"""
    print("=== 사용 가능한 New Residential Construction 시리즈 ===")
    
    for series_name, series_id in NEW_RESIDENTIAL_CONSTRUCTION_SERIES.items():
        korean_name = NEW_RESIDENTIAL_CONSTRUCTION_KOREAN_NAMES.get(series_name, series_name)
        print(f"  '{series_name}': {korean_name} ({series_id})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in NEW_RESIDENTIAL_CONSTRUCTION_CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            for series_name in series_list:
                korean_name = NEW_RESIDENTIAL_CONSTRUCTION_KOREAN_NAMES.get(series_name, series_name)
                api_id = NEW_RESIDENTIAL_CONSTRUCTION_SERIES.get(series_name, series_name)
                print(f"    - {series_name}: {korean_name} ({api_id})")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not NEW_RESIDENTIAL_CONSTRUCTION_DATA or 'load_info' not in NEW_RESIDENTIAL_CONSTRUCTION_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': NEW_RESIDENTIAL_CONSTRUCTION_DATA['load_info']['loaded'],
        'series_count': NEW_RESIDENTIAL_CONSTRUCTION_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': NEW_RESIDENTIAL_CONSTRUCTION_DATA['load_info']
    }

# %%
# === 사용 예시 ===

print("=== 리팩토링된 New Residential Construction 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_new_residential_construction_data()  # 스마트 업데이트")
print("   load_new_residential_construction_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_new_residential_construction_series_advanced(['permits_total_sa', 'starts_total_sa'], 'multi_line', 'raw')")
print("   plot_new_residential_construction_series_advanced(['permits_single_sa'], 'horizontal_bar', 'yoy', left_ytitle='%')")
print("   plot_new_residential_construction_series_advanced(['starts_total_sa'], 'single_line', 'mom', periods=24, left_ytitle='%')")
print("   plot_new_residential_construction_series_advanced(['permits_total_sa', 'starts_total_sa'], 'dual_axis', 'raw', left_ytitle='천 개', right_ytitle='천 개')")
print()
print("3. 🔥 데이터 Export:")
print("   export_new_residential_construction_data(['permits_total_sa', 'starts_total_sa'], 'raw')")
print("   export_new_residential_construction_data(['starts_single_sa'], 'mom', periods=24, file_format='csv')")
print("   export_new_residential_construction_data(['permits_south_sa'], 'yoy', target_date='2024-06-01')")
print()
print("✅ plot_new_residential_construction_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_new_residential_construction_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
load_new_residential_construction_data()
plot_new_residential_construction_series_advanced(['permits_total_sa', 'starts_total_sa'], 'multi_line', 'raw')