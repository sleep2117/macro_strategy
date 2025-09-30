# %%
"""
Construction Spending 데이터 분석 (리팩토링 버전)
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
api_config.FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'  # ADP 파일에서 가져온 키

# %%
# === Construction Spending 시리즈 정의 ===

# 메인 시리즈 딕셔너리 (올바른 구조: 시리즈_이름: API_ID)
CONSTRUCTION_SPENDING_SERIES = {
    # 총계 시리즈
    'total_construction_sa': 'TTLCONS',           # 전체 건설지출 (계절조정)
    'total_construction_nsa': 'TTLCON',          # 전체 건설지출 (원계열)
    'total_private_sa': 'TLPRVCONS',             # 전체 민간건설 (계절조정)
    'total_private_nsa': 'TLPRVCON',             # 전체 민간건설 (원계열)
    'total_public_sa': 'TLPBLCONS',              # 전체 공공건설 (계절조정)
    'total_public_nsa': 'TLPBLCON',              # 전체 공공건설 (원계열)
    
    # 주거/비주거 구분
    'residential_total_sa': 'TLRESCONS',         # 주거건설 전체 (계절조정)
    'residential_total_nsa': 'TLRESCON',         # 주거건설 전체 (원계열)
    'residential_private_sa': 'PRRESCONS',       # 주거건설 민간 (계절조정)
    'residential_private_nsa': 'PRRESCON',       # 주거건설 민간 (원계열)
    'residential_public_sa': 'PBRESCONS',        # 주거건설 공공 (계절조정)
    'residential_public_nsa': 'PBRESCON',        # 주거건설 공공 (원계열)
    
    'nonresidential_total_sa': 'TLNRESCONS',     # 비주거건설 전체 (계절조정)
    'nonresidential_total_nsa': 'TLNRESCON',     # 비주거건설 전체 (원계열)
    'nonresidential_private_sa': 'PNRESCONS',    # 비주거건설 민간 (계절조정)
    'nonresidential_private_nsa': 'PNRESCON',    # 비주거건설 민간 (원계열)
    'nonresidential_public_sa': 'PBNRESCONS',    # 비주거건설 공공 (계절조정)
    'nonresidential_public_nsa': 'PBNRESCON',    # 비주거건설 공공 (원계열)
    
    # 주요 산업별 (계절조정)
    'manufacturing_total_sa': 'TLMFGCONS',       # 제조업 전체 (계절조정)
    'manufacturing_private_sa': 'PRMFGCONS',     # 제조업 민간 (계절조정)
    'commercial_total_sa': 'TLCOMCONS',          # 상업시설 전체 (계절조정)
    'commercial_private_sa': 'PRCOMCONS',        # 상업시설 민간 (계절조정)
    'commercial_public_sa': 'PBCOMCONS',         # 상업시설 공공 (계절조정)
    'office_total_sa': 'TLOFCONS',               # 오피스 전체 (계절조정)
    'office_private_sa': 'PROFCONS',             # 오피스 민간 (계절조정)
    'office_public_sa': 'PBOFCONS',              # 오피스 공공 (계절조정)
    'healthcare_total_sa': 'TLHLTHCONS',         # 의료시설 전체 (계절조정)
    'healthcare_private_sa': 'PRHLTHCONS',       # 의료시설 민간 (계절조정)
    'healthcare_public_sa': 'PBHLTHCONS',        # 의료시설 공공 (계절조정)
    'educational_total_sa': 'TLEDUCONS',         # 교육시설 전체 (계절조정)
    'educational_private_sa': 'PREDUCONS',       # 교육시설 민간 (계절조정)
    'educational_public_sa': 'PBEDUCONS',        # 교육시설 공공 (계절조정)
    'power_total_sa': 'TLPWRCONS',               # 전력시설 전체 (계절조정)
    'power_private_sa': 'PRPWRCONS',             # 전력시설 민간 (계절조정)
    'power_public_sa': 'PBPWRCONS',              # 전력시설 공공 (계절조정)
    'highway_total_sa': 'TLHWYCONS',             # 도로 전체 (계절조정)
    'highway_public_sa': 'PBHWYCONS',            # 도로 공공 (계절조정)
    'transportation_total_sa': 'TLTRANSCONS',    # 교통시설 전체 (계절조정)
    'transportation_private_sa': 'PRTRANSCONS',  # 교통시설 민간 (계절조정)
    'transportation_public_sa': 'PBTRANSCONS',   # 교통시설 공공 (계절조정)
}

# 한국어 이름 매핑
CONSTRUCTION_SPENDING_KOREAN_NAMES = {
    'total_construction_sa': '전체 건설지출 (계절조정)',
    'total_construction_nsa': '전체 건설지출 (원계열)',
    'total_private_sa': '전체 민간건설 (계절조정)',
    'total_private_nsa': '전체 민간건설 (원계열)',
    'total_public_sa': '전체 공공건설 (계절조정)',
    'total_public_nsa': '전체 공공건설 (원계열)',
    
    'residential_total_sa': '주거건설 전체 (계절조정)',
    'residential_total_nsa': '주거건설 전체 (원계열)',
    'residential_private_sa': '주거건설 민간 (계절조정)',
    'residential_private_nsa': '주거건설 민간 (원계열)',
    'residential_public_sa': '주거건설 공공 (계절조정)',
    'residential_public_nsa': '주거건설 공공 (원계열)',
    
    'nonresidential_total_sa': '비주거건설 전체 (계절조정)',
    'nonresidential_total_nsa': '비주거건설 전체 (원계열)',
    'nonresidential_private_sa': '비주거건설 민간 (계절조정)',
    'nonresidential_private_nsa': '비주거건설 민간 (원계열)',
    'nonresidential_public_sa': '비주거건설 공공 (계절조정)',
    'nonresidential_public_nsa': '비주거건설 공공 (원계열)',
    
    'manufacturing_total_sa': '제조업 건설 전체 (계절조정)',
    'manufacturing_private_sa': '제조업 건설 민간 (계절조정)',
    'commercial_total_sa': '상업시설 건설 전체 (계절조정)',
    'commercial_private_sa': '상업시설 건설 민간 (계절조정)',
    'commercial_public_sa': '상업시설 건설 공공 (계절조정)',
    'office_total_sa': '오피스 건설 전체 (계절조정)',
    'office_private_sa': '오피스 건설 민간 (계절조정)',
    'office_public_sa': '오피스 건설 공공 (계절조정)',
    'healthcare_total_sa': '의료시설 건설 전체 (계절조정)',
    'healthcare_private_sa': '의료시설 건설 민간 (계절조정)',
    'healthcare_public_sa': '의료시설 건설 공공 (계절조정)',
    'educational_total_sa': '교육시설 건설 전체 (계절조정)',
    'educational_private_sa': '교육시설 건설 민간 (계절조정)',
    'educational_public_sa': '교육시설 건설 공공 (계절조정)',
    'power_total_sa': '전력시설 건설 전체 (계절조정)',
    'power_private_sa': '전력시설 건설 민간 (계절조정)',
    'power_public_sa': '전력시설 건설 공공 (계절조정)',
    'highway_total_sa': '도로 건설 전체 (계절조정)',
    'highway_public_sa': '도로 건설 공공 (계절조정)',
    'transportation_total_sa': '교통시설 건설 전체 (계절조정)',
    'transportation_private_sa': '교통시설 건설 민간 (계절조정)',
    'transportation_public_sa': '교통시설 건설 공공 (계절조정)',
}

# 카테고리 분류
CONSTRUCTION_SPENDING_CATEGORIES = {
    '주요 지표': {
        'Total Construction': ['total_construction_sa', 'total_private_sa', 'total_public_sa'],
        'Residential vs Nonresidential': ['residential_total_sa', 'nonresidential_total_sa'],
        'Private vs Public': ['total_private_sa', 'total_public_sa']
    },
    '주거/비주거 세부': {
        'Residential': ['residential_total_sa', 'residential_private_sa', 'residential_public_sa'],
        'Nonresidential': ['nonresidential_total_sa', 'nonresidential_private_sa', 'nonresidential_public_sa']
    },
    '산업별 (계절조정)': {
        'Manufacturing': ['manufacturing_total_sa', 'manufacturing_private_sa'],
        'Commercial': ['commercial_total_sa', 'commercial_private_sa', 'commercial_public_sa'],
        'Office': ['office_total_sa', 'office_private_sa', 'office_public_sa'],
        'Healthcare': ['healthcare_total_sa', 'healthcare_private_sa', 'healthcare_public_sa'],
        'Educational': ['educational_total_sa', 'educational_private_sa', 'educational_public_sa']
    },
    '인프라 (계절조정)': {
        'Power': ['power_total_sa', 'power_private_sa', 'power_public_sa'],
        'Highway': ['highway_total_sa', 'highway_public_sa'],
        'Transportation': ['transportation_total_sa', 'transportation_private_sa', 'transportation_public_sa']
    },
    '계절조정별': {
        'Seasonally Adjusted': [s for s in CONSTRUCTION_SPENDING_SERIES.keys() if s.endswith('_sa')],
        'Not Seasonally Adjusted': [s for s in CONSTRUCTION_SPENDING_SERIES.keys() if s.endswith('_nsa')]
    }
}

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/construction_spending_data_refactored.csv'
CONSTRUCTION_SPENDING_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_construction_spending_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 Construction Spending 데이터 로드"""
    global CONSTRUCTION_SPENDING_DATA

    # 데이터 소스별 분기
    data_source = 'FRED'  # Census Bureau 데이터는 FRED에서 제공
    tolerance = 10.0  # 일반 지표 허용 오차
    
    result = load_economic_data(
        series_dict=CONSTRUCTION_SPENDING_SERIES,
        data_source=data_source,
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=tolerance
    )

    if result:
        CONSTRUCTION_SPENDING_DATA = result
        print_load_info()
        return True
    else:
        print("❌ Construction Spending 데이터 로드 실패")
        return False

def print_load_info():
    """Construction Spending 데이터 로드 정보 출력"""
    if not CONSTRUCTION_SPENDING_DATA or 'load_info' not in CONSTRUCTION_SPENDING_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = CONSTRUCTION_SPENDING_DATA['load_info']
    print(f"\n📊 Construction Spending 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in CONSTRUCTION_SPENDING_DATA and not CONSTRUCTION_SPENDING_DATA['raw_data'].empty:
        latest_date = CONSTRUCTION_SPENDING_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_construction_spending_series_advanced(series_list, chart_type='multi_line', 
                                              data_type='raw', periods=None, target_date=None,
                                              left_ytitle=None, right_ytitle=None):
    """범용 Construction Spending 시각화 함수 - plot_economic_series 활용"""
    if not CONSTRUCTION_SPENDING_DATA:
        print("⚠️ 먼저 load_construction_spending_data()를 실행하세요.")
        return None

    # 단위별 기본 Y축 제목 설정
    if data_type == 'mom':
        default_ytitle = "%"
    elif data_type == 'yoy':
        default_ytitle = "%"  
    else:  # raw
        default_ytitle = "백만 달러"

    return plot_economic_series(
        data_dict=CONSTRUCTION_SPENDING_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        left_ytitle=left_ytitle or default_ytitle,
        right_ytitle=right_ytitle or default_ytitle,
        korean_names=CONSTRUCTION_SPENDING_KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_construction_spending_data(series_list, data_type='raw', periods=None, 
                                    target_date=None, export_path=None, file_format='excel'):
    """Construction Spending 데이터 export 함수 - export_economic_data 활용"""
    if not CONSTRUCTION_SPENDING_DATA:
        print("⚠️ 먼저 load_construction_spending_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=CONSTRUCTION_SPENDING_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=CONSTRUCTION_SPENDING_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_construction_spending_data():
    """Construction Spending 데이터 초기화"""
    global CONSTRUCTION_SPENDING_DATA
    CONSTRUCTION_SPENDING_DATA = {}
    print("🗑️ Construction Spending 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not CONSTRUCTION_SPENDING_DATA or 'raw_data' not in CONSTRUCTION_SPENDING_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_construction_spending_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return CONSTRUCTION_SPENDING_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in CONSTRUCTION_SPENDING_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return CONSTRUCTION_SPENDING_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not CONSTRUCTION_SPENDING_DATA or 'mom_data' not in CONSTRUCTION_SPENDING_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_construction_spending_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return CONSTRUCTION_SPENDING_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in CONSTRUCTION_SPENDING_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return CONSTRUCTION_SPENDING_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not CONSTRUCTION_SPENDING_DATA or 'yoy_data' not in CONSTRUCTION_SPENDING_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_construction_spending_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return CONSTRUCTION_SPENDING_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in CONSTRUCTION_SPENDING_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return CONSTRUCTION_SPENDING_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not CONSTRUCTION_SPENDING_DATA or 'raw_data' not in CONSTRUCTION_SPENDING_DATA:
        return []
    return list(CONSTRUCTION_SPENDING_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 Construction Spending 시리즈 표시"""
    print("=== 사용 가능한 Construction Spending 시리즈 ===")
    
    for series_name, series_id in CONSTRUCTION_SPENDING_SERIES.items():
        korean_name = CONSTRUCTION_SPENDING_KOREAN_NAMES.get(series_name, series_name)
        print(f"  '{series_name}': {korean_name} ({series_id})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in CONSTRUCTION_SPENDING_CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            for series_name in series_list:
                korean_name = CONSTRUCTION_SPENDING_KOREAN_NAMES.get(series_name, series_name)
                api_id = CONSTRUCTION_SPENDING_SERIES.get(series_name, series_name)
                print(f"    - {series_name}: {korean_name} ({api_id})")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not CONSTRUCTION_SPENDING_DATA or 'load_info' not in CONSTRUCTION_SPENDING_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': CONSTRUCTION_SPENDING_DATA['load_info']['loaded'],
        'series_count': CONSTRUCTION_SPENDING_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': CONSTRUCTION_SPENDING_DATA['load_info']
    }

# %%
# === 사용 예시 ===

print("=== 리팩토링된 Construction Spending 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_construction_spending_data()  # 스마트 업데이트")
print("   load_construction_spending_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_construction_spending_series_advanced(['total_construction_sa', 'total_private_sa'], 'multi_line', 'raw')")
print("   plot_construction_spending_series_advanced(['total_construction_sa'], 'horizontal_bar', 'yoy', left_ytitle='%')")
print("   plot_construction_spending_series_advanced(['residential_total_sa'], 'single_line', 'mom', periods=24, left_ytitle='%')")
print("   plot_construction_spending_series_advanced(['residential_total_sa', 'nonresidential_total_sa'], 'dual_axis', 'raw', left_ytitle='백만 달러', right_ytitle='백만 달러')")
print()
print("3. 🔥 데이터 Export:")
print("   export_construction_spending_data(['total_construction_sa', 'total_private_sa'], 'raw')")
print("   export_construction_spending_data(['manufacturing_total_sa'], 'mom', periods=24, file_format='csv')")
print("   export_construction_spending_data(['residential_total_sa'], 'yoy', target_date='2024-06-01')")
print()
print("✅ plot_construction_spending_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_construction_spending_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
load_construction_spending_data()
plot_construction_spending_series_advanced(['total_construction_sa'], 'multi_line', 'yoy', left_ytitle='%')
# %%
