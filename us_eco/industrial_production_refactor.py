# %%
"""
US Industrial Production & Capacity Utilization 데이터 분석 (리팩토링 버전)
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
# === US Industrial Production & Capacity Utilization 데이터 계층 구조 정의 ===

# 산업생산(IP) 시리즈 맵 (월별 데이터 - seasonally adjusted)
INDUSTRIAL_PRODUCTION_SERIES = {
    # === 총지수·시장군 IP ===
    'ip_total': 'INDPRO',                    # 총 산업
    'ip_manufacturing': 'IPMAN',             # 제조업(NAICS)
    'ip_durable_mfg': 'IPDMAN',             # 내구재 제조
    'ip_nondurable_mfg': 'IPNMAN',          # 비내구재 제조
    'ip_mining': 'IPMINE',                  # 광업
    'ip_utilities': 'IPUTIL',               # 유틸리티
    'ip_electric_power': 'IPG2211S',        # 전력생산
    'ip_natural_gas': 'IPG2212S',           # 천연가스
    
    # === 소비재 ===
    'ip_consumer_goods': 'IPCONGD',         # 소비재
    'ip_consumer_durable': 'IPDCONGD',      # 내구소비재
    'ip_consumer_nondurable': 'IPNCONGD',   # 비내구소비재
    
    # === 기업설비(BE) ===
    'ip_business_equipment': 'IPBUSEQ',     # 기업설비
    'ip_construction_supplies': 'IPB54100S', # 건설자재
    'ip_business_supplies': 'IPB54200S',    # 기업용 자재
    
    # === 원자재 ===
    'ip_materials': 'IPMAT',                # 원자재
    'ip_durable_materials': 'IPDMAT',       # 내구 원자재
    'ip_nondurable_materials': 'IPNMAT',    # 비내구 원자재
    'ip_energy_materials': 'IPB53300S',        # 에너지 원자재
}

# 제조업 - 내구재 (NAICS 321–339)
DURABLE_GOODS_IP_SERIES = {
    'ip_wood_products': 'IPG321S',          # 목재제품 (321)
    'ip_nonmetallic_minerals': 'IPG327S',   # 비금속 광물 (327)
    'ip_primary_metals': 'IPG331S',         # 1차 금속 (331)
    'ip_fabricated_metals': 'IPG332S',      # 금속가공 (332)
    'ip_machinery': 'IPG333S',              # 기계 (333)
    'ip_computer_electronic': 'IPG334S',    # 컴퓨터·전자 (334)
    'ip_computer': 'IPG3341S',              # 컴퓨터 (3341)
    'ip_communications_equipment': 'IPG3342S', # 통신장비 (3342)
    'ip_semiconductors': 'IPG3344S',        # 반도체·부품 (3344)
    'ip_electrical_equipment': 'IPG335S',   # 전기장비·가전 (335)
    'ip_motor_vehicles': 'IPG3361T3S',      # 자동차·부품 (3361-3)
    'ip_aerospace': 'IPG3364T9S',           # 항공우주 등 (3364-6)
    'ip_furniture': 'IPG337S',              # 가구·관련 (337)
    'ip_misc_durables': 'IPG339S',          # 기타 내구재 (339)
}

# 제조업 - 비내구재 (NAICS 311–326)
NONDURABLE_GOODS_IP_SERIES = {
    'ip_food_tobacco': 'IPG311A2S',       # 식료품·담배 (311-2)
    'ip_textile_mills': 'IPG313A4S',      # 섬유·직물 제품 (313-4)
    'ip_apparel_leather': 'IPG315A6S',    # 의류·가죽 (315-6)
    'ip_paper': 'IPG322S',                  # 제지 (322)
    'ip_printing': 'IPG323S',               # 인쇄·지원 (323)
    'ip_petroleum_coal': 'IPG324S',         # 석유·석탄 제품 (324)
    'ip_chemicals': 'IPG325S',              # 화학제품 (325)
    'ip_plastics_rubber': 'IPG326S',        # 플라스틱·고무 (326)
}

# 가동률(CU) 시리즈 맵 (월별 데이터 - seasonally adjusted)
CAPACITY_UTILIZATION_SERIES = {
    # === 총지수·시장군 CU ===
    'cu_total': 'TCU',                      # 총 산업
    'cu_manufacturing': 'MCUMFN',           # 제조업(NAICS)
    'cu_durable_mfg': 'CAPUTLGMFDS',       # 내구재 제조
    'cu_nondurable_mfg': 'CAPUTLGMFNS',   # 비내구재 제조
    'cu_mining': 'CAPUTLG21S',             # 광업
    'cu_utilities': 'CAPUTLG2211A2S',          # 유틸리티
    'cu_electric_power': 'CAPUTLG2211S',   # 전력생산
    'cu_natural_gas': 'CAPUTLG2212S',      # 천연가스
}

# 제조업 - 내구재 CU
DURABLE_GOODS_CU_SERIES = {
    'cu_wood_products': 'CAPUTLG321S',          # 목재제품 (321)
    'cu_nonmetallic_minerals': 'CAPUTLG327S',   # 비금속 광물 (327)
    'cu_primary_metals': 'CAPUTLG331S',         # 1차 금속 (331)
    'cu_fabricated_metals': 'CAPUTLG332S',      # 금속가공 (332)
    'cu_machinery': 'CAPUTLG333S',              # 기계 (333)
    'cu_computer_electronic': 'CAPUTLG334S',    # 컴퓨터·전자 (334)
    'cu_computer': 'CAPUTLG3341S',              # 컴퓨터 (3341)
    'cu_communications_equipment': 'CAPUTLG3342S', # 통신장비 (3342)
    'cu_semiconductors': 'CAPUTLHITEK2S',       # 반도체·부품
    'cu_electrical_equipment': 'CAPUTLG335S',   # 전기장비·가전 (335)
    'cu_motor_vehicles': 'CAPUTLG3361T3S',      # 자동차·부품 (3361-3)
    'cu_aerospace': 'CAPUTLG3364T9S',           # 항공우주 등 (3364-6)
    'cu_furniture': 'CAPUTLG337S',              # 가구·관련 (337)
    'cu_misc_durables': 'CAPUTLG339S',          # 기타 내구재 (339)
}

# 제조업 - 비내구재 CU
NONDURABLE_GOODS_CU_SERIES = {
    'cu_food_tobacco': 'CAPUTLG311A2S',       # 식료품·담배 (311-2)
    'cu_textile_mills': 'CAPUTLG313A4S',      # 섬유·직물 제품 (313-4)
    'cu_apparel_leather': 'CAPUTLG315A6S',    # 의류·가죽 (315-6)
    'cu_paper': 'CAPUTLG322S',                  # 제지 (322)
    'cu_printing': 'CAPUTLG323S',               # 인쇄·지원 (323)
    'cu_petroleum_coal': 'CAPUTLG324S',         # 석유·석탄 제품 (324)
    'cu_chemicals': 'CAPUTLG325S',              # 화학제품 (325)
    'cu_plastics_rubber': 'CAPUTLG326S',        # 플라스틱·고무 (326)
}

# 전체 시리즈 통합
ALL_INDUSTRIAL_SERIES = {
    **INDUSTRIAL_PRODUCTION_SERIES,
    **DURABLE_GOODS_IP_SERIES,
    **NONDURABLE_GOODS_IP_SERIES,
    **CAPACITY_UTILIZATION_SERIES,
    **DURABLE_GOODS_CU_SERIES,
    **NONDURABLE_GOODS_CU_SERIES
}

# 산업생산 분석을 위한 계층 구조
INDUSTRIAL_ANALYSIS_HIERARCHY = {
    'headline_measures': {
        'name': 'Headline Industrial Production Measures',
        'ip': ['ip_total', 'ip_manufacturing', 'ip_durable_mfg', 'ip_nondurable_mfg'],
        'cu': ['cu_total', 'cu_manufacturing', 'cu_durable_mfg', 'cu_nondurable_mfg']
    },
    'market_groups': {
        'name': 'Market Group Analysis',
        'ip': ['ip_consumer_goods', 'ip_business_equipment', 'ip_materials'],
        'cu': ['cu_total', 'cu_manufacturing']  # CU는 시장그룹별로 세분화되지 않음
    },
    'sector_analysis': {
        'name': 'Sector-wise Analysis',
        'manufacturing': ['ip_manufacturing', 'cu_manufacturing'],
        'mining': ['ip_mining', 'cu_mining'],
        'utilities': ['ip_utilities', 'cu_utilities']
    },
    'durable_goods_detail': {
        'name': 'Durable Goods Detail Analysis',
        'ip': ['ip_computer_electronic', 'ip_machinery', 'ip_motor_vehicles', 'ip_primary_metals'],
        'cu': ['cu_computer_electronic', 'cu_machinery', 'cu_motor_vehicles', 'cu_primary_metals']
    }
}

print("✓ Industrial Production & Capacity Utilization 데이터 구조 정의 완료")

# 한국어 라벨 매핑 (FRED 시리즈 ID 기반)
INDUSTRIAL_KOREAN_NAMES = {
    # Industrial Production
    'INDPRO': '총 산업생산',
    'IPMAN': '제조업',
    'IPDMAN': '내구재 제조',
    'IPNMAN': '비내구재 제조',
    'IPMINE': '광업',
    'IPUTIL': '유틸리티',
    'IPG2211S': '전력생산',
    'IPG2212S': '천연가스',
    'IPCONGD': '소비재',
    'IPDCONGD': '내구소비재',
    'IPNCONGD': '비내구소비재',
    'IPBUSEQ': '기업설비',
    'IPB54100S': '건설자재',
    'IPB54200S': '기업용 자재',
    'IPMAT': '원자재',
    'IPDMAT': '내구 원자재',
    'IPNMAT': '비내구 원자재',
    'IPB53300S': '에너지 원자재',
    
    # Durable Goods IP
    'IPG321S': '목재제품',
    'IPG327S': '비금속 광물',
    'IPG331S': '1차 금속',
    'IPG332S': '금속가공',
    'IPG333S': '기계',
    'IPG334S': '컴퓨터/전자',
    'IPG3341S': '컴퓨터',
    'IPG3342S': '통신장비',
    'IPG3344S': '반도체/부품',
    'IPG335S': '전기장비/가전',
    'IPG3361T3S': '자동차/부품',
    'IPG3364T9S': '항공우주',
    'IPG337S': '가구',
    'IPG339S': '기타 내구재',
    
    # Nondurable Goods IP
    'IPG311A2S': '식료품/담배',
    'IPG313A4S': '섬유/직물',
    'IPG315A6S': '의류/가죽',
    'IPG322S': '제지',
    'IPG323S': '인쇄/지원',
    'IPG324S': '석유/석탄',
    'IPG325S': '화학제품',
    'IPG326S': '플라스틱/고무',
    
    # Capacity Utilization
    'TCU': '총 산업 가동률',
    'MCUMFN': '제조업 가동률',
    'CAPUTLGMFDS': '내구재 가동률',
    'CAPUTLGMFNS': '비내구재 가동률',
    'CAPUTLG21S': '광업 가동률',
    'CAPUTLG2211A2S': '유틸리티 가동률',
    'CAPUTLG2211S': '전력생산 가동률',
    'CAPUTLG2212S': '천연가스 가동률',
    
    # Durable Goods CU
    'CAPUTLG321S': '목재제품 가동률',
    'CAPUTLG327S': '비금속 광물 가동률',
    'CAPUTLG331S': '1차 금속 가동률',
    'CAPUTLG332S': '금속가공 가동률',
    'CAPUTLG333S': '기계 가동률',
    'CAPUTLG334S': '컴퓨터/전자 가동률',
    'CAPUTLG3341S': '컴퓨터 가동률',
    'CAPUTLG3342S': '통신장비 가동률',
    'CAPUTLHITEK2S': '반도체 가동률',
    'CAPUTLG335S': '전기장비 가동률',
    'CAPUTLG3361T3S': '자동차 가동률',
    'CAPUTLG3364T9S': '항공우주 가동률',
    'CAPUTLG337S': '가구 가동률',
    'CAPUTLG339S': '기타 내구재 가동률',
    
    # Nondurable Goods CU
    'CAPUTLG311A2S': '식료품/담배 가동률',
    'CAPUTLG313A4S': '섬유/직물 가동률',
    'CAPUTLG315A6S': '의류/가죽 가동률',
    'CAPUTLG322S': '제지 가동률',
    'CAPUTLG323S': '인쇄 가동률',
    'CAPUTLG324S': '석유/석탄 가동률',
    'CAPUTLG325S': '화학제품 가동률',
    'CAPUTLG326S': '플라스틱/고무 가동률'
}

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/industrial_production_data.csv'
INDUSTRIAL_PRODUCTION_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_industrial_production_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 Industrial Production 데이터 로드"""
    global INDUSTRIAL_PRODUCTION_DATA

    # 시리즈 딕셔너리를 {id: id} 형태로 변환 (load_economic_data가 예상하는 형태)
    series_dict = {series_id: series_id for series_id in ALL_INDUSTRIAL_SERIES.values()}

    result = load_economic_data(
        series_dict=series_dict,
        data_source='FRED',
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=10.0  # 일반 지표
    )

    if result:
        INDUSTRIAL_PRODUCTION_DATA = result
        print_load_info()
        return True
    else:
        print("❌ Industrial Production 데이터 로드 실패")
        return False

def print_load_info():
    """Industrial Production 데이터 로드 정보 출력"""
    if not INDUSTRIAL_PRODUCTION_DATA or 'load_info' not in INDUSTRIAL_PRODUCTION_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = INDUSTRIAL_PRODUCTION_DATA['load_info']
    print(f"\n📊 Industrial Production 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in INDUSTRIAL_PRODUCTION_DATA and not INDUSTRIAL_PRODUCTION_DATA['raw_data'].empty:
        latest_date = INDUSTRIAL_PRODUCTION_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_industrial_production_series_advanced(series_list, chart_type='multi_line', 
                                    data_type='mom', periods=None, target_date=None):
    """범용 Industrial Production 시각화 함수 - plot_economic_series 활용"""
    if not INDUSTRIAL_PRODUCTION_DATA:
        print("⚠️ 먼저 load_industrial_production_data()를 실행하세요.")
        return None

    return plot_economic_series(
        data_dict=INDUSTRIAL_PRODUCTION_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=INDUSTRIAL_KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_industrial_production_data(series_list, data_type='mom', periods=None, 
                           target_date=None, export_path=None, file_format='excel'):
    """Industrial Production 데이터 export 함수 - export_economic_data 활용"""
    if not INDUSTRIAL_PRODUCTION_DATA:
        print("⚠️ 먼저 load_industrial_production_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=INDUSTRIAL_PRODUCTION_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=INDUSTRIAL_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_industrial_production_data():
    """Industrial Production 데이터 초기화"""
    global INDUSTRIAL_PRODUCTION_DATA
    INDUSTRIAL_PRODUCTION_DATA = {}
    print("🗑️ Industrial Production 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not INDUSTRIAL_PRODUCTION_DATA or 'raw_data' not in INDUSTRIAL_PRODUCTION_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_industrial_production_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return INDUSTRIAL_PRODUCTION_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in INDUSTRIAL_PRODUCTION_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return INDUSTRIAL_PRODUCTION_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not INDUSTRIAL_PRODUCTION_DATA or 'mom_data' not in INDUSTRIAL_PRODUCTION_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_industrial_production_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return INDUSTRIAL_PRODUCTION_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in INDUSTRIAL_PRODUCTION_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return INDUSTRIAL_PRODUCTION_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not INDUSTRIAL_PRODUCTION_DATA or 'yoy_data' not in INDUSTRIAL_PRODUCTION_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_industrial_production_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return INDUSTRIAL_PRODUCTION_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in INDUSTRIAL_PRODUCTION_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return INDUSTRIAL_PRODUCTION_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not INDUSTRIAL_PRODUCTION_DATA or 'raw_data' not in INDUSTRIAL_PRODUCTION_DATA:
        return []
    return list(INDUSTRIAL_PRODUCTION_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 Industrial Production 시리즈 표시"""
    print("=== 사용 가능한 Industrial Production 시리즈 ===")
    
    for key, series_id in ALL_INDUSTRIAL_SERIES.items():
        korean_name = INDUSTRIAL_KOREAN_NAMES.get(series_id, key)
        print(f"  '{series_id}': {korean_name}")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in INDUSTRIAL_ANALYSIS_HIERARCHY.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            if isinstance(series_list, list):
                print(f"  {group_name}: {len(series_list)}개 시리즈")
                for series_id in series_list:
                    korean_name = INDUSTRIAL_KOREAN_NAMES.get(series_id, series_id)
                    print(f"    - {series_id}: {korean_name}")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not INDUSTRIAL_PRODUCTION_DATA or 'load_info' not in INDUSTRIAL_PRODUCTION_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': INDUSTRIAL_PRODUCTION_DATA['load_info']['loaded'],
        'series_count': INDUSTRIAL_PRODUCTION_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': INDUSTRIAL_PRODUCTION_DATA['load_info']
    }

# %%
# === 사용 예시 ===

print("=== 리팩토링된 Industrial Production 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_industrial_production_data()  # 스마트 업데이트")
print("   load_industrial_production_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_industrial_production_series_advanced(['INDPRO', 'IPMAN'], 'multi_line', 'mom')")
print("   plot_industrial_production_series_advanced(['TCU'], 'horizontal_bar', 'yoy')")
print("   plot_industrial_production_series_advanced(['INDPRO'], 'single_line', 'mom', periods=24)")
print()
print("3. 🔥 데이터 Export:")
print("   export_industrial_production_data(['INDPRO', 'IPMAN'], 'mom')")
print("   export_industrial_production_data(['TCU'], 'raw', periods=24, file_format='csv')")
print("   export_industrial_production_data(['INDPRO'], 'yoy', target_date='2024-06-01')")
print()
print("✅ plot_industrial_production_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_industrial_production_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
load_industrial_production_data()
plot_industrial_production_series_advanced(['INDPRO', 'IPMAN'], 'multi_line', 'mom')
# %%
plot_industrial_production_series_advanced(['TCU'], 'horizontal_bar', 'yoy')
# %%
