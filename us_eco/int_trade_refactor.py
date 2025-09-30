# %%
"""
미국 국제무역 데이터 분석 (리팩토링 버전)
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

# FRED 데이터 사용
# api_config.FRED_API_KEY = '...'

# %%
# === 미국 국제무역 시리즈 정의 ===

# 메인 시리즈 딕셔너리 (올바른 구조: 시리즈_이름: API_ID)
INT_TRADE_SERIES = {
    # 무역수지
    'goods_services_balance': 'BOPGSTB',     # 상품 및 서비스 무역수지
    'goods_balance': 'BOPGTB',               # 상품 무역수지
    'services_balance': 'BOPSTB',            # 서비스 무역수지
    
    # 수출입 총계
    'goods_services_exports': 'BOPTEXP',     # 상품 및 서비스 수출
    'goods_services_imports': 'BOPTIMP',     # 상품 및 서비스 수입
    'goods_exports': 'BOPGEXP',              # 상품 수출
    'goods_imports': 'BOPGIMP',              # 상품 수입
    
    # 서비스 무역
    'services_exports': 'BOPSEXP',           # 서비스 수출
    'services_imports': 'BOPSIMP',           # 서비스 수입
    'financial_services_exports': 'ITXFISM133S',    # 금융서비스 수출
    'telecom_imports': 'ITMTCIM133S',        # 통신/컴퓨터/정보서비스 수입
    'ip_exports': 'ITXCIPM133S',             # 지식재산권 수출
    
    # 중국
    'china_exports': 'EXPCH',                # 중국 수출
    'china_imports': 'IMPCH',                # 중국 수입
    
    # 일본
    'japan_exports': 'EXPJP',                # 일본 수출
    'japan_imports': 'IMPJP',                # 일본 수입
    
    # 멕시코
    'mexico_exports': 'EXPMX',               # 멕시코 수출
    'mexico_imports': 'IMPMX',               # 멕시코 수입
    
    # 캐나다
    'canada_exports': 'EXPCA',               # 캐나다 수출
    'canada_imports': 'IMPCA',               # 캐나다 수입
    
    # 기타 주요국
    'uk_exports': 'EXPUK',                   # 영국 수출
    'uk_imports': 'IMPUK',                   # 영국 수입
    'germany_exports': 'EXPGE',              # 독일 수출
    'germany_imports': 'IMPGE',              # 독일 수입
    'korea_imports': 'IMPKR',                # 한국 수입
    'india_imports': 'IMP5330',              # 인도 수입
    'vietnam_exports': 'EXP5520',            # 베트남 수출
    'vietnam_imports': 'IMP5520',            # 베트남 수입
    
    # 지역별
    'asia_imports': 'IMP0016',               # 아시아 수입
    
    # 특수 품목
    'atp_exports': 'EXP0007',                # 첨단기술제품 수출
    'atp_imports': 'IMP0007',                # 첨단기술제품 수입
    'world_exports': 'EXP0015',              # 전세계 수출
    'world_imports': 'IMP0015',              # 전세계 수입
}

# 한국어 이름 매핑
INT_TRADE_KOREAN_NAMES = {
    # 무역수지
    'goods_services_balance': '상품 및 서비스 무역수지',
    'goods_balance': '상품 무역수지',
    'services_balance': '서비스 무역수지',
    
    # 수출입 총계
    'goods_services_exports': '상품 및 서비스 수출',
    'goods_services_imports': '상품 및 서비스 수입',
    'goods_exports': '상품 수출',
    'goods_imports': '상품 수입',
    
    # 서비스 무역
    'services_exports': '서비스 수출',
    'services_imports': '서비스 수입',
    'financial_services_exports': '금융서비스 수출',
    'telecom_imports': '통신/컴퓨터/정보서비스 수입',
    'ip_exports': '지식재산권 수출',
    
    # 중국
    'china_exports': '중국 수출',
    'china_imports': '중국 수입',
    
    # 일본
    'japan_exports': '일본 수출',
    'japan_imports': '일본 수입',
    
    # 멕시코
    'mexico_exports': '멕시코 수출',
    'mexico_imports': '멕시코 수입',
    
    # 캐나다
    'canada_exports': '캐나다 수출',
    'canada_imports': '캐나다 수입',
    
    # 기타 주요국
    'uk_exports': '영국 수출',
    'uk_imports': '영국 수입',
    'germany_exports': '독일 수출',
    'germany_imports': '독일 수입',
    'korea_imports': '한국 수입',
    'india_imports': '인도 수입',
    'vietnam_exports': '베트남 수출',
    'vietnam_imports': '베트남 수입',
    
    # 지역별
    'asia_imports': '아시아 수입',
    
    # 특수 품목
    'atp_exports': '첨단기술제품 수출',
    'atp_imports': '첨단기술제품 수입',
    'world_exports': '전세계 수출',
    'world_imports': '전세계 수입',
}

# 카테고리 분류
INT_TRADE_CATEGORIES = {
    '주요 지표': {
        'Trade Balance': ['goods_services_balance', 'goods_balance', 'services_balance'],
        'Total Trade': ['goods_services_exports', 'goods_services_imports'],
        'Goods Trade': ['goods_exports', 'goods_imports']
    },
    '주요 교역국': {
        'China': ['china_exports', 'china_imports'],
        'Japan': ['japan_exports', 'japan_imports'],
        'Mexico': ['mexico_exports', 'mexico_imports'],
        'Canada': ['canada_exports', 'canada_imports'],
        'Europe': ['uk_exports', 'uk_imports', 'germany_exports', 'germany_imports']
    },
    '서비스 무역': {
        'Services Total': ['services_exports', 'services_imports'],
        'Services Detail': ['financial_services_exports', 'telecom_imports', 'ip_exports']
    },
    '특수 품목': {
        'Advanced Technology': ['atp_exports', 'atp_imports'],
        'World Total': ['world_exports', 'world_imports']
    },
    '아시아 지역': {
        'Asia': ['asia_imports', 'china_imports', 'japan_imports', 'korea_imports', 'india_imports', 'vietnam_imports'],
        'Northeast Asia': ['china_imports', 'japan_imports', 'korea_imports'],
        'Southeast Asia': ['vietnam_exports', 'vietnam_imports']
    }
}

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/int_trade_data_refactored.csv'
INT_TRADE_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_int_trade_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 미국 국제무역 데이터 로드"""
    global INT_TRADE_DATA

    # 데이터 소스별 분기
    data_source = 'FRED'
    tolerance = 10.0  # 일반 지표 기준
    
    result = load_economic_data(
        series_dict=INT_TRADE_SERIES,
        data_source=data_source,
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=tolerance
    )

    if result:
        INT_TRADE_DATA = result
        print_load_info()
        return True
    else:
        print("❌ 미국 국제무역 데이터 로드 실패")
        return False

def print_load_info():
    """미국 국제무역 데이터 로드 정보 출력"""
    if not INT_TRADE_DATA or 'load_info' not in INT_TRADE_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = INT_TRADE_DATA['load_info']
    print(f"\n📊 미국 국제무역 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in INT_TRADE_DATA and not INT_TRADE_DATA['raw_data'].empty:
        latest_date = INT_TRADE_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_int_trade_series_advanced(series_list, chart_type='multi_line', 
                                  data_type='raw', periods=None, target_date=None,
                                  left_ytitle=None, right_ytitle=None):
    """범용 미국 국제무역 시각화 함수 - plot_economic_series 활용"""
    if not INT_TRADE_DATA:
        print("⚠️ 먼저 load_int_trade_data()를 실행하세요.")
        return None

    # 단위별 기본 Y축 제목 설정
    if data_type == 'mom':
        default_ytitle = "%"
    elif data_type == 'yoy':
        default_ytitle = "%"  
    else:  # raw
        # 무역 데이터는 대부분 십억 달러 또는 백만 달러 단위
        # BOP 기준은 십억 달러, 국가별은 백만 달러
        if any(series in series_list for series in ['goods_services_balance', 'goods_balance', 'services_balance',
                                                   'goods_services_exports', 'goods_services_imports',
                                                   'goods_exports', 'goods_imports', 'services_exports', 'services_imports']):
            default_ytitle = "십억 달러"
        else:
            default_ytitle = "백만 달러"

    return plot_economic_series(
        data_dict=INT_TRADE_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        left_ytitle=left_ytitle or default_ytitle,
        right_ytitle=right_ytitle or default_ytitle,
        korean_names=INT_TRADE_KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_int_trade_data(series_list, data_type='raw', periods=None, 
                         target_date=None, export_path=None, file_format='excel'):
    """미국 국제무역 데이터 export 함수 - export_economic_data 활용"""
    if not INT_TRADE_DATA:
        print("⚠️ 먼저 load_int_trade_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=INT_TRADE_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=INT_TRADE_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_int_trade_data():
    """미국 국제무역 데이터 초기화"""
    global INT_TRADE_DATA
    INT_TRADE_DATA = {}
    print("🗑️ 미국 국제무역 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not INT_TRADE_DATA or 'raw_data' not in INT_TRADE_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_int_trade_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return INT_TRADE_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in INT_TRADE_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return INT_TRADE_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not INT_TRADE_DATA or 'mom_data' not in INT_TRADE_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_int_trade_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return INT_TRADE_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in INT_TRADE_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return INT_TRADE_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not INT_TRADE_DATA or 'yoy_data' not in INT_TRADE_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_int_trade_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return INT_TRADE_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in INT_TRADE_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return INT_TRADE_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not INT_TRADE_DATA or 'raw_data' not in INT_TRADE_DATA:
        return []
    return list(INT_TRADE_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 미국 국제무역 시리즈 표시"""
    print("=== 사용 가능한 미국 국제무역 시리즈 ===")
    
    for series_name, series_id in INT_TRADE_SERIES.items():
        korean_name = INT_TRADE_KOREAN_NAMES.get(series_name, series_name)
        print(f"  '{series_name}': {korean_name} ({series_id})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in INT_TRADE_CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            for series_name in series_list:
                korean_name = INT_TRADE_KOREAN_NAMES.get(series_name, series_name)
                api_id = INT_TRADE_SERIES.get(series_name, series_name)
                print(f"    - {series_name}: {korean_name} ({api_id})")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not INT_TRADE_DATA or 'load_info' not in INT_TRADE_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': INT_TRADE_DATA['load_info']['loaded'],
        'series_count': INT_TRADE_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': INT_TRADE_DATA['load_info']
    }

# %%
# === 사용 예시 ===

print("=== 리팩토링된 미국 국제무역 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_int_trade_data()  # 스마트 업데이트")
print("   load_int_trade_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_int_trade_series_advanced(['goods_services_balance', 'goods_balance'], 'multi_line', 'raw')")
print("   plot_int_trade_series_advanced(['china_exports', 'china_imports'], 'multi_line', 'yoy', left_ytitle='%')")
print("   plot_int_trade_series_advanced(['goods_services_exports'], 'single_line', 'raw', periods=24, left_ytitle='십억 달러')")
print("   plot_int_trade_series_advanced(['china_exports', 'japan_exports'], 'dual_axis', 'raw', left_ytitle='백만 달러', right_ytitle='백만 달러')")
print()
print("3. 🔥 데이터 Export:")
print("   export_int_trade_data(['goods_services_balance', 'goods_balance'], 'raw')")
print("   export_int_trade_data(['china_exports', 'china_imports'], 'yoy', periods=24, file_format='csv')")
print("   export_int_trade_data(['goods_services_exports'], 'mom', target_date='2024-06-01')")
print()
print("✅ plot_int_trade_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_int_trade_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
load_int_trade_data()
plot_int_trade_series_advanced(['goods_services_balance', 'goods_balance'], 'multi_line', 'raw')