# %%
"""
US Durable Goods 데이터 분석 (리팩토링 버전)
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
api_config.FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'

# %%
# === US Durable Goods 시리즈 정의 ===

# 내구재 시리즈 맵 (월별 데이터 - seasonally adjusted)
DURABLE_GOODS_SERIES = {
    # === 출하량 (Shipments) ===
    'shipments_total': 'AMDMVS',                    # Total durable goods shipments
    'shipments_ex_transport': 'ADXTVS',             # Ex-Transportation shipments
    'shipments_ex_defense': 'ADXDVS',               # Ex-Defense shipments
    'shipments_primary_metals': 'A31SVS',           # Primary metals (31) shipments
    'shipments_fabricated_metals': 'A32SVS',        # Fabricated metal products (32) shipments
    'shipments_machinery': 'A33SVS',                # Machinery (33) shipments
    'shipments_computers_electronics': 'A34SVS',    # Computers & electronic products (34) shipments
    'shipments_computers': 'A34AVS',                # Computers & related products shipments
    'shipments_communications': 'A34XVS',           # Communications equipment shipments
    'shipments_electrical': 'A35SVS',               # Electrical equipment, appliances & components (35) shipments
    'shipments_transportation': 'A36SVS',           # Transportation equipment (36) shipments
    'shipments_motor_vehicles': 'AMVPVS',           # Motor vehicles & parts shipments
    'shipments_nondef_aircraft': 'ANAPVS',          # Non-defense aircraft & parts shipments
    'shipments_defense_aircraft': 'ADAPVS',         # Defense aircraft & parts shipments
    'shipments_other_durables': 'AODGVS',           # All other durable goods (38) shipments
    'shipments_capital_goods': 'ATCGVS',            # Capital goods shipments
    'shipments_nondef_capital': 'ANDEVS',          # Nondefense capital goods shipments
    'shipments_core_capital': 'ANXAVS',            # Core capital goods (nondefense ex-aircraft) shipments
    'shipments_defense_capital': 'ADEFVS',          # Defense capital goods shipments
    
    # === 신규 주문 (New Orders) ===
    'orders_total': 'DGORDER',                      # Total durable goods new orders
    'orders_ex_transport': 'ADXTNO',                # Ex-Transportation new orders
    'orders_ex_defense': 'ADXDNO',                  # Ex-Defense new orders
    'orders_primary_metals': 'A31SNO',              # Primary metals (31) new orders
    'orders_fabricated_metals': 'A32SNO',           # Fabricated metal products (32) new orders
    'orders_machinery': 'A33SNO',                   # Machinery (33) new orders
    'orders_computers_electronics': 'A34SNO',       # Computers & electronic products (34) new orders
    'orders_computers': 'A34ANO',                   # Computers & related products new orders
    'orders_communications': 'A34XNO',              # Communications equipment new orders
    'orders_electrical': 'A35SNO',                  # Electrical equipment, appliances & components (35) new orders
    'orders_transportation': 'A36SNO',              # Transportation equipment (36) new orders
    'orders_motor_vehicles': 'AMVPNO',              # Motor vehicles & parts new orders
    'orders_nondef_aircraft': 'ANAPNO',             # Non-defense aircraft & parts new orders
    'orders_defense_aircraft': 'ADAPNO',            # Defense aircraft & parts new orders
    'orders_other_durables': 'AODGNO',              # All other durable goods (38) new orders
    'orders_capital_goods': 'ATCGNO',               # Capital goods new orders
    'orders_nondef_capital': 'ANDENO',             # Nondefense capital goods new orders
    'orders_core_capital': 'UNXANO',              # Core capital goods (nondefense ex-aircraft) new orders
    'orders_defense_capital': 'ADEFNO',             # Defense capital goods new orders
    
    # === 미충족 주문 (Unfilled Orders) ===
    'unfilled_total': 'AMDMUO',                     # Total durable goods unfilled orders
    'unfilled_ex_transport': 'ADXDUO',              # Ex-Transportation unfilled orders
    'unfilled_ex_defense': 'AMXDUO',                # Ex-Defense unfilled orders
    'unfilled_primary_metals': 'A31SUO',            # Primary metals (31) unfilled orders
    'unfilled_fabricated_metals': 'A32SUO',         # Fabricated metals (32) unfilled orders
    'unfilled_machinery': 'A33SUO',                 # Machinery (33) unfilled orders
    'unfilled_computers_electronics': 'A34SUO',     # Computers & electronic (34) unfilled orders
    'unfilled_electrical': 'A35SUO',                # Electrical equipment (35) unfilled orders
    'unfilled_transportation': 'A36SUO',            # Transportation equipment (36) unfilled orders
    'unfilled_motor_vehicles': 'AMVPUO',            # Motor vehicles & parts unfilled orders
    'unfilled_nondef_aircraft': 'ANAPUO',           # Nondefense aircraft & parts unfilled orders
    'unfilled_defense_aircraft': 'ADAPUO',          # Defense aircraft & parts unfilled orders
    'unfilled_other_durables': 'AODGUO',            # All other durable goods (38) unfilled orders
    'unfilled_capital_goods': 'ATCGUO',             # Capital goods unfilled orders
    'unfilled_core_capital': 'ANXAUO',              # Core capital goods (nondefense ex-aircraft) unfilled orders
    
    # === 재고 (Total Inventories) ===
    'inventory_total': 'AMDMTI',                    # Total durable goods inventories
    'inventory_ex_transport': 'ADXTTI',             # Ex-Transportation inventories
    'inventory_ex_defense': 'ADXDTI',               # Ex-Defense inventories
    'inventory_primary_metals': 'A31STI',           # Primary metals (31) inventories
    'inventory_fabricated_metals': 'A32STI',        # Fabricated metals (32) inventories
    'inventory_machinery': 'A33STI',                # Machinery (33) inventories
    'inventory_computers_electronics': 'A34STI',    # Computers & electronic (34) inventories
    'inventory_electrical': 'A35STI',               # Electrical equipment (35) inventories
    'inventory_transportation': 'A36STI',           # Transportation equipment (36) inventories
    'inventory_motor_vehicles': 'AMVPTI',           # Motor vehicles & parts inventories
    'inventory_nondef_aircraft': 'ANAPTI',          # Nondefense aircraft & parts inventories
    'inventory_defense_aircraft': 'ADAPTI',         # Defense aircraft & parts inventories
    'inventory_other_durables': 'AODGTI',           # All other durable goods (38) inventories
    'inventory_capital_goods': 'ATCGTI',            # Capital goods inventories
    'inventory_core_capital': 'ANXATI'             # Core capital goods (nondefense ex-aircraft) inventories
}

# 한국어 매핑 딕셔너리
KOREAN_NAMES = {
    # Shipments
    'shipments_total': '전체 출하량',
    'shipments_ex_transport': '출하량 (운송장비 제외)',
    'shipments_ex_defense': '출하량 (국방 제외)',
    'shipments_primary_metals': '1차 금속 출하',
    'shipments_fabricated_metals': '금속 가공품 출하',
    'shipments_machinery': '기계장비 출하',
    'shipments_computers_electronics': '컴퓨터/전자제품 출하',
    'shipments_computers': '컴퓨터 관련 출하',
    'shipments_communications': '통신장비 출하',
    'shipments_electrical': '전기장비/가전 출하',
    'shipments_transportation': '운송장비 출하',
    'shipments_motor_vehicles': '자동차/부품 출하',
    'shipments_nondef_aircraft': '민간 항공기 출하',
    'shipments_defense_aircraft': '군용 항공기 출하',
    'shipments_other_durables': '기타 내구재 출하',
    'shipments_capital_goods': '자본재 출하',
    'shipments_nondef_capital': '민간 자본재 출하',
    'shipments_core_capital': '코어 자본재 출하',
    'shipments_defense_capital': '국방 자본재 출하',
    
    # Orders  
    'orders_total': '전체 신규주문',
    'orders_ex_transport': '신규주문 (운송장비 제외)',
    'orders_ex_defense': '신규주문 (국방 제외)',
    'orders_primary_metals': '1차 금속 주문',
    'orders_fabricated_metals': '금속 가공품 주문',
    'orders_machinery': '기계장비 주문',
    'orders_computers_electronics': '컴퓨터/전자제품 주문',
    'orders_computers': '컴퓨터 관련 주문',
    'orders_communications': '통신장비 주문',
    'orders_electrical': '전기장비/가전 주문',
    'orders_transportation': '운송장비 주문',
    'orders_motor_vehicles': '자동차/부품 주문',
    'orders_nondef_aircraft': '민간 항공기 주문',
    'orders_defense_aircraft': '군용 항공기 주문',
    'orders_other_durables': '기타 내구재 주문',
    'orders_capital_goods': '자본재 주문',
    'orders_nondef_capital': '민간 자본재 주문',
    'orders_core_capital': '코어 자본재 주문',
    'orders_defense_capital': '국방 자본재 주문',
    
    # Unfilled Orders
    'unfilled_total': '전체 미충족 주문',
    'unfilled_ex_transport': '미충족 주문 (운송장비 제외)',
    'unfilled_ex_defense': '미충족 주문 (국방 제외)',
    'unfilled_primary_metals': '1차 금속 미충족 주문',
    'unfilled_fabricated_metals': '금속 가공품 미충족 주문',
    'unfilled_machinery': '기계장비 미충족 주문',
    'unfilled_computers_electronics': '컴퓨터/전자 미충족 주문',
    'unfilled_electrical': '전기장비 미충족 주문',
    'unfilled_transportation': '운송장비 미충족 주문',
    'unfilled_motor_vehicles': '자동차/부품 미충족 주문',
    'unfilled_nondef_aircraft': '민간 항공기 미충족 주문',
    'unfilled_defense_aircraft': '군용 항공기 미충족 주문',
    'unfilled_other_durables': '기타 내구재 미충족 주문',
    'unfilled_capital_goods': '자본재 미충족 주문',
    'unfilled_core_capital': '코어 자본재 미충족 주문',
    
    # Inventories
    'inventory_total': '전체 재고',
    'inventory_ex_transport': '재고 (운송장비 제외)',
    'inventory_ex_defense': '재고 (국방 제외)',
    'inventory_primary_metals': '1차 금속 재고',
    'inventory_fabricated_metals': '금속 가공품 재고',
    'inventory_machinery': '기계장비 재고',
    'inventory_computers_electronics': '컴퓨터/전자제품 재고',
    'inventory_electrical': '전기장비 재고',
    'inventory_transportation': '운송장비 재고',
    'inventory_motor_vehicles': '자동차/부품 재고',
    'inventory_nondef_aircraft': '민간 항공기 재고',
    'inventory_defense_aircraft': '군용 항공기 재고',
    'inventory_other_durables': '기타 내구재 재고',
    'inventory_capital_goods': '자본재 재고',
    'inventory_core_capital': '코어 자본재 재고'
}

# 카테고리 분류
CATEGORIES = {
    'headline_measures': {
        'name': 'Headline Durable Goods Measures',
        'shipments': ['shipments_total', 'shipments_ex_transport', 'shipments_ex_defense'],
        'orders': ['orders_total', 'orders_ex_transport', 'orders_ex_defense'],
        'unfilled': ['unfilled_total', 'unfilled_ex_transport', 'unfilled_ex_defense'],
        'inventory': ['inventory_total', 'inventory_ex_transport', 'inventory_ex_defense']
    },
    'capital_goods': {
        'name': 'Capital Goods Analysis',
        'shipments': ['shipments_capital_goods', 'shipments_nondef_capital', 'shipments_core_capital'],
        'orders': ['orders_capital_goods', 'orders_nondef_capital', 'orders_core_capital'],
        'unfilled': ['unfilled_capital_goods', 'unfilled_core_capital'],
        'inventory': ['inventory_capital_goods', 'inventory_core_capital']
    },
    'transport_vs_non_transport': {
        'name': 'Transportation vs Non-Transportation',
        'transport': ['shipments_transportation', 'orders_transportation', 'unfilled_transportation'],
        'non_transport': ['shipments_ex_transport', 'orders_ex_transport', 'unfilled_ex_transport']
    },
    'sector_analysis': {
        'name': 'Sector-wise Analysis',
        'metals': ['shipments_primary_metals', 'orders_primary_metals', 'inventory_primary_metals'],
        'machinery': ['shipments_machinery', 'orders_machinery', 'inventory_machinery'],
        'electronics': ['shipments_computers_electronics', 'orders_computers_electronics', 'inventory_computers_electronics'],
        'transport': ['shipments_transportation', 'orders_transportation', 'inventory_transportation']
    }
}

print("✓ Durable Goods 데이터 구조 정의 완료")

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/durable_goods_data_refactored.csv'
DURABLE_GOODS_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_durable_goods_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 Durable Goods 데이터 로드"""
    global DURABLE_GOODS_DATA

    # 시리즈 딕셔너리를 올바른 {name: fred_id} 형태로 전달
    series_dict = DURABLE_GOODS_SERIES

    result = load_economic_data(
        series_dict=series_dict,
        data_source='FRED',
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=10.0  # 일반 지표용 허용 오차
    )

    if result:
        DURABLE_GOODS_DATA = result
        print_load_info()
        return True
    else:
        print("❌ Durable Goods 데이터 로드 실패")
        return False

def print_load_info():
    """Durable Goods 데이터 로드 정보 출력"""
    if not DURABLE_GOODS_DATA or 'load_info' not in DURABLE_GOODS_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = DURABLE_GOODS_DATA['load_info']
    print(f"\n📊 Durable Goods 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in DURABLE_GOODS_DATA and not DURABLE_GOODS_DATA['raw_data'].empty:
        latest_date = DURABLE_GOODS_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_durable_goods_series_advanced(series_list, chart_type='multi_line', 
                                       data_type='mom', periods=None, target_date=None,
                                       left_ytitle=None, right_ytitle=None):
    """범용 Durable Goods 시각화 함수 - plot_economic_series 활용"""
    if not DURABLE_GOODS_DATA:
        print("⚠️ 먼저 load_durable_goods_data()를 실행하세요.")
        return None

    return plot_economic_series(
        data_dict=DURABLE_GOODS_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        left_ytitle=left_ytitle,
        right_ytitle=right_ytitle,
        korean_names=KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_durable_goods_data(series_list, data_type='mom', periods=None, 
                              target_date=None, export_path=None, file_format='excel'):
    """Durable Goods 데이터 export 함수 - export_economic_data 활용"""
    if not DURABLE_GOODS_DATA:
        print("⚠️ 먼저 load_durable_goods_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=DURABLE_GOODS_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_durable_goods_data():
    """Durable Goods 데이터 초기화"""
    global DURABLE_GOODS_DATA
    DURABLE_GOODS_DATA = {}
    print("🗑️ Durable Goods 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not DURABLE_GOODS_DATA or 'raw_data' not in DURABLE_GOODS_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_durable_goods_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return DURABLE_GOODS_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in DURABLE_GOODS_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return DURABLE_GOODS_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not DURABLE_GOODS_DATA or 'mom_data' not in DURABLE_GOODS_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_durable_goods_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return DURABLE_GOODS_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in DURABLE_GOODS_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return DURABLE_GOODS_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not DURABLE_GOODS_DATA or 'yoy_data' not in DURABLE_GOODS_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_durable_goods_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return DURABLE_GOODS_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in DURABLE_GOODS_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return DURABLE_GOODS_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not DURABLE_GOODS_DATA or 'raw_data' not in DURABLE_GOODS_DATA:
        return []
    return list(DURABLE_GOODS_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 Durable Goods 시리즈 표시"""
    print("=== 사용 가능한 Durable Goods 시리즈 ===")
    
    for series_id, description in DURABLE_GOODS_SERIES.items():
        korean_name = KOREAN_NAMES.get(series_id, description)
        print(f"  '{series_id}': {korean_name} ({description})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            for series_id in series_list:
                korean_name = KOREAN_NAMES.get(series_id, series_id)
                print(f"    - {series_id}: {korean_name}")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not DURABLE_GOODS_DATA or 'load_info' not in DURABLE_GOODS_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': DURABLE_GOODS_DATA['load_info']['loaded'],
        'series_count': DURABLE_GOODS_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': DURABLE_GOODS_DATA['load_info']
    }

# %%
# === 사용 예시 ===

print("=== 리팩토링된 Durable Goods 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_durable_goods_data()  # 스마트 업데이트")
print("   load_durable_goods_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_durable_goods_series_advanced(['orders_total', 'shipments_total'], 'multi_line', 'mom')")
print("   plot_durable_goods_series_advanced(['orders_core_capital'], 'horizontal_bar', 'yoy', left_ytitle='%')")
print("   plot_durable_goods_series_advanced(['shipments_total'], 'single_line', 'mom', periods=24, left_ytitle='%')")
print("   plot_durable_goods_series_advanced(['orders_total', 'shipments_total'], 'dual_axis', 'raw', left_ytitle='십억$', right_ytitle='십억$')")
print()
print("3. 🔥 데이터 Export:")
print("   export_durable_goods_data(['orders_total', 'shipments_total'], 'mom')")
print("   export_durable_goods_data(['orders_core_capital'], 'raw', periods=24, file_format='csv')")
print("   export_durable_goods_data(['orders_total'], 'yoy', target_date='2024-06-01')")
print()
print("✅ plot_durable_goods_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_durable_goods_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
load_durable_goods_data()
plot_durable_goods_series_advanced(['orders_total', 'shipments_total'], 'multi_line', 'mom')