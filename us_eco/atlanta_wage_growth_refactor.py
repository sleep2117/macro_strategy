# %%
"""
Atlanta Fed Wage Growth Tracker 데이터 분석 (리팩토링 버전)
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
# === Atlanta Fed Wage Growth 시리즈 정의 ===

# 메인 시리즈 딕셔너리 (올바른 구조: 시리즈_이름: API_ID)
ATLANTA_WAGE_GROWTH_SERIES = {
    # 핵심 전체 지표 (Overall)
    'median_overall_3m': 'FRBATLWGT3MMAUMHWGO',        # 3개월 이동평균 중위값
    'median_overall_12m': 'FRBATLWGT12MMUMHGO',        # 12개월 이동평균 중위값
    'median_overall_1983_3m': 'FRBATLWGT3MMAUMHWG83O', # 3개월 이동평균 중위값 (1983 base)
    'median_overall_1983': 'FRBATLWGTUMHWG83O',        # 중위값 (1983 base)
    'median_overall': 'FRBATLWGTUMHWGO',               # 중위값 (기본)
    
    # 가중평균 지표
    'weighted_median_overall_3m': 'FRBATLWGT3MMAWMHWGO',  # 3개월 가중 중위값
    'weighted_median_overall_12m': 'FRBATLWGT12MMAWMHWGO', # 12개월 가중 중위값
    'weighted_median_1997_12m': 'FRBATLWGT12MMAWMHWG97O',  # 12개월 가중 중위값 (1997 base)
    'weighted_median_1997_3m': 'FRBATLWGT3MMAWMHWGW97O',   # 3개월 가중 중위값 (1997 base)
    
    # 고용 상태별 (Job Movement)
    'job_stayer_12m': 'FRBATLWGT12MMUMHWGJST',         # 직장유지자 12개월
    'job_stayer_3m': 'FRBATLWGT3MMAUMHWGJMJST',        # 직장유지자 3개월
    'job_switcher_12m': 'FRBATLWGT12MMUMHWGJSW',       # 직장이동자 12개월
    'job_switcher_3m': 'FRBATLWGT3MMAUMHWGJMJSW',      # 직장이동자 3개월
    
    # 연령대별 (Age Groups)
    'age_16_24_12m': 'FRBATLWGT12MMUMHWGA1644Y',       # 16-24세 12개월
    'age_25_54_12m': 'FRBATLWGT12MMUMHWGA2554Y',       # 25-54세 12개월
    'age_25_54_3m': 'FRBATLWGT3MMAUMHWGA2554Y',        # 25-54세 3개월
    'age_55_over_12m': 'FRBATLWGT12MMUMHWG55O',        # 55세 이상 12개월
    
    # 성별 (Sex)
    'male_12m': 'FRBATLWGT12MMUMHWGSM',               # 남성 12개월
    'male_3m': 'FRBATLWGT3MMAUMHWGSM',                # 남성 3개월
    'female_12m': 'FRBATLWGT12MMUMHWGSF',             # 여성 12개월
    'female_3m': 'FRBATLWGT3MMAUMHWGSF',              # 여성 3개월
    
    # 인종별 (Race)
    'white_12m': 'FRBATLWGT12MMUMHWGRW',              # 백인 12개월
    'nonwhite_12m': 'FRBATLWGT12MMUMHWGRNW',          # 비백인 12개월
    
    # 교육수준별 (Education)
    'high_school_12m': 'FRBATLWGT12MMUMHWGEHS',       # 고등학교 12개월
    'associates_12m': 'FRBATLWGT12MMUMHWGEAD',        # 전문대 12개월
    'bachelor_higher_12m': 'FRBATLWGT12MMUMHWGBDH',   # 학사 이상 12개월
    'college_degree_3m': 'FRBATLWGT3MMAUMHWGEACD',    # 대학 학위 3개월
    
    # 근무형태별 (Work Status)
    'fulltime_12m': 'FRBATLWGT12MMUMHWGHUFT',         # 풀타임 12개월
    'fulltime_3m': 'FRBATLWGT3MMAUMHWGWSFT',          # 풀타임 3개월
    'parttime_12m': 'FRBATLWGT12MMUMHWGHUPT',         # 파트타임 12개월
    'paid_hourly_12m': 'FRBATLWGT12MMUMHWGTPPH',      # 시간당 급여 12개월
    'paid_hourly_3m': 'FRBATLWGT3MMAUMHWGTOPPH',      # 시간당 급여 3개월
    'not_paid_hourly_12m': 'FRBATLWGT12MMUMHWGTPNPH', # 시간급 외 급여 12개월
    
    # 산업별 (Industry)
    'manufacturing_12m': 'FRBATLWGT12MMUMHWGIM',      # 제조업 12개월
    'construction_mining_12m': 'FRBATLWGT12MMUMHWGICM', # 건설/광업 12개월
    'trade_transport_12m': 'FRBATLWGT12MMUMHWGITT',   # 무역/운송 12개월
    'finance_business_12m': 'FRBATLWGT12MMUMHWGIFBS', # 금융/비즈니스 서비스 12개월
    'education_health_12m': 'FRBATLWGT12MMUMHWGIEH',  # 교육/의료 12개월
    'leisure_hospitality_12m': 'FRBATLWGT12MMUMHWGILH', # 여가/숙박 12개월
    'public_admin_12m': 'FRBATLWGT12MMUMHWGIPA',      # 공공행정 12개월
    'services_3m': 'FRBATLWGT3MMAUMHWGSS',            # 서비스업 3개월
    
    # 직업별 (Occupation)
    'professional_mgmt_12m': 'FRBATLWGT12MMUMHWGOSCPM', # 전문/관리직 12개월
    'service_12m': 'FRBATLWGT12MMUMHWGOSCS',          # 서비스직 12개월
    'optics_12m': 'FRBATLWGT12MMUMHWGOSCOPTC',        # OPTICS 12개월
    
    # 지역별 (Regional) - 주요 지역만
    'msa_area_12m': 'FRBATLWGT12MMUMHWGMSA',          # MSA 지역 12개월
    'non_msa_12m': 'FRBATLWGT12MMUMHWGNMSA',          # 비MSA 지역 12개월
    
    # 임금분포별 (Wage Distribution)
    'percentile_1_25_12m': 'FRBATLWGT12MMUMHWGWD1WP',   # 1-25% 분위 12개월
    'percentile_26_50_12m': 'FRBATLWGT12MMUMHWGWD26WP', # 26-50% 분위 12개월
    'percentile_51_75_12m': 'FRBATLWGT12MMUMHWGWD51WP', # 51-75% 분위 12개월
    'percentile_76_100_12m': 'FRBATLWGT12MMUMHWGWD76WP', # 76-100% 분위 12개월
    'percentile_1_50_3m': 'FRBATLWGT3MMAUMHGWD1WP',    # 1-50% 분위 3개월
    'percentile_51_100_3m': 'FRBATLWGT3MMAUMHWGWD51WP', # 51-100% 분위 3개월
    'percentile_25_3m': 'FRBATLWGT3MMAU25PHWGO',       # 25% 분위 3개월
    'percentile_75_3m': 'FRBATLWGT3MMAU75PHWGO',       # 75% 분위 3개월
    
    # 기타 통계 측도
    'mean_3m': 'FRBATLWGT3MMAUMEANHWGO',              # 평균 3개월
    'trimmed_mean_3m': 'FRBATLWGT3MMAU2520TMHWGO',    # 절사평균 3개월
    'unchanged_3m': 'FRBATLWGT3MMAUUHWGO',            # 무변화 3개월
    'weekly_wage_3m': 'FRBATLWGT3MMAUMWWGO',          # 주급 성장 3개월
}

# 한국어 이름 매핑
ATLANTA_WAGE_GROWTH_KOREAN_NAMES = {
    'median_overall_3m': '전체 중위값 (3개월)',
    'median_overall_12m': '전체 중위값 (12개월)',
    'median_overall_1983_3m': '전체 중위값 1983기준 (3개월)',
    'median_overall_1983': '전체 중위값 (1983기준)',
    'median_overall': '전체 중위값',
    
    'weighted_median_overall_3m': '가중 중위값 전체 (3개월)',
    'weighted_median_overall_12m': '가중 중위값 전체 (12개월)',
    'weighted_median_1997_12m': '가중 중위값 1997기준 (12개월)',
    'weighted_median_1997_3m': '가중 중위값 1997기준 (3개월)',
    
    'job_stayer_12m': '직장유지자 (12개월)',
    'job_stayer_3m': '직장유지자 (3개월)',
    'job_switcher_12m': '직장이동자 (12개월)',
    'job_switcher_3m': '직장이동자 (3개월)',
    
    'age_16_24_12m': '16-24세 (12개월)',
    'age_25_54_12m': '25-54세 (12개월)',
    'age_25_54_3m': '25-54세 (3개월)',
    'age_55_over_12m': '55세 이상 (12개월)',
    
    'male_12m': '남성 (12개월)',
    'male_3m': '남성 (3개월)',
    'female_12m': '여성 (12개월)',
    'female_3m': '여성 (3개월)',
    
    'white_12m': '백인 (12개월)',
    'nonwhite_12m': '비백인 (12개월)',
    
    'high_school_12m': '고등학교 졸업 (12개월)',
    'associates_12m': '전문대 졸업 (12개월)',
    'bachelor_higher_12m': '학사 이상 (12개월)',
    'college_degree_3m': '대학 학위 (3개월)',
    
    'fulltime_12m': '풀타임 근무 (12개월)',
    'fulltime_3m': '풀타임 근무 (3개월)',
    'parttime_12m': '파트타임 근무 (12개월)',
    'paid_hourly_12m': '시간당 급여 (12개월)',
    'paid_hourly_3m': '시간당 급여 (3개월)',
    'not_paid_hourly_12m': '월급/연봉 (12개월)',
    
    'manufacturing_12m': '제조업 (12개월)',
    'construction_mining_12m': '건설/광업 (12개월)',
    'trade_transport_12m': '무역/운송 (12개월)',
    'finance_business_12m': '금융/비즈니스 서비스 (12개월)',
    'education_health_12m': '교육/의료 (12개월)',
    'leisure_hospitality_12m': '여가/숙박업 (12개월)',
    'public_admin_12m': '공공행정 (12개월)',
    'services_3m': '서비스업 (3개월)',
    
    'professional_mgmt_12m': '전문/관리직 (12개월)',
    'service_12m': '서비스직 (12개월)',
    'optics_12m': 'OPTICS 직종 (12개월)',
    
    'msa_area_12m': '대도시권 (12개월)',
    'non_msa_12m': '비대도시권 (12개월)',
    
    'percentile_1_25_12m': '1-25% 분위 (12개월)',
    'percentile_26_50_12m': '26-50% 분위 (12개월)',
    'percentile_51_75_12m': '51-75% 분위 (12개월)',
    'percentile_76_100_12m': '76-100% 분위 (12개월)',
    'percentile_1_50_3m': '1-50% 분위 (3개월)',
    'percentile_51_100_3m': '51-100% 분위 (3개월)',
    'percentile_25_3m': '25% 분위 (3개월)',
    'percentile_75_3m': '75% 분위 (3개월)',
    
    'mean_3m': '평균 (3개월)',
    'trimmed_mean_3m': '절사평균 (3개월)',
    'unchanged_3m': '무변화 비율 (3개월)',
    'weekly_wage_3m': '주급 성장률 (3개월)',
}

# 카테고리 분류
ATLANTA_WAGE_GROWTH_CATEGORIES = {
    '핵심 지표': {
        'Overall Medians': ['median_overall_3m', 'median_overall_12m', 'median_overall'],
        'Weighted Medians': ['weighted_median_overall_3m', 'weighted_median_overall_12m'],
        'Job Movement': ['job_stayer_12m', 'job_switcher_12m']
    },
    '인구통계별': {
        'Age Groups': ['age_16_24_12m', 'age_25_54_12m', 'age_55_over_12m'],
        'Gender': ['male_12m', 'female_12m'],
        'Race': ['white_12m', 'nonwhite_12m'],
        'Education': ['high_school_12m', 'associates_12m', 'bachelor_higher_12m']
    },
    '근무형태별': {
        'Work Hours': ['fulltime_12m', 'parttime_12m'],
        'Pay Type': ['paid_hourly_12m', 'not_paid_hourly_12m'],
        'Job Status': ['job_stayer_12m', 'job_switcher_12m']
    },
    '산업별': {
        'Goods Producing': ['manufacturing_12m', 'construction_mining_12m'],
        'Service Providing': ['trade_transport_12m', 'finance_business_12m', 'education_health_12m'],
        'Government': ['public_admin_12m']
    },
    '임금분포별': {
        'Lower Half': ['percentile_1_25_12m', 'percentile_26_50_12m'],
        'Upper Half': ['percentile_51_75_12m', 'percentile_76_100_12m'],
        'Summary': ['percentile_25_3m', 'percentile_75_3m']
    },
    '기간별': {
        '3-Month MA': [s for s in ATLANTA_WAGE_GROWTH_SERIES.keys() if '_3m' in s],
        '12-Month MA': [s for s in ATLANTA_WAGE_GROWTH_SERIES.keys() if '_12m' in s]
    }
}

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/atlanta_wage_growth_data_refactored.csv'
ATLANTA_WAGE_GROWTH_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_atlanta_wage_growth_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 Atlanta Fed Wage Growth 데이터 로드"""
    global ATLANTA_WAGE_GROWTH_DATA

    # 데이터 소스별 분기
    data_source = 'FRED'  # Atlanta Fed 데이터는 FRED에서 제공
    tolerance = 1.0  # 임금 성장률은 퍼센트 단위로 작은 허용 오차 사용
    
    result = load_economic_data(
        series_dict=ATLANTA_WAGE_GROWTH_SERIES,
        data_source=data_source,
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=tolerance
    )

    if result:
        ATLANTA_WAGE_GROWTH_DATA = result
        print_load_info()
        return True
    else:
        print("❌ Atlanta Fed Wage Growth 데이터 로드 실패")
        return False

def print_load_info():
    """Atlanta Fed Wage Growth 데이터 로드 정보 출력"""
    if not ATLANTA_WAGE_GROWTH_DATA or 'load_info' not in ATLANTA_WAGE_GROWTH_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = ATLANTA_WAGE_GROWTH_DATA['load_info']
    print(f"\n📊 Atlanta Fed Wage Growth 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in ATLANTA_WAGE_GROWTH_DATA and not ATLANTA_WAGE_GROWTH_DATA['raw_data'].empty:
        latest_date = ATLANTA_WAGE_GROWTH_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_atlanta_wage_growth_series_advanced(series_list, chart_type='multi_line', 
                                           data_type='raw', periods=None, target_date=None,
                                           left_ytitle=None, right_ytitle=None):
    """범용 Atlanta Fed Wage Growth 시각화 함수 - plot_economic_series 활용"""
    if not ATLANTA_WAGE_GROWTH_DATA:
        print("⚠️ 먼저 load_atlanta_wage_growth_data()를 실행하세요.")
        return None

    # 단위별 기본 Y축 제목 설정 (모든 데이터가 이미 변화율)
    if data_type == 'mom':
        default_ytitle = "%"
    elif data_type == 'yoy':
        default_ytitle = "%"  
    else:  # raw
        default_ytitle = "%"  # Atlanta Fed 데이터는 이미 성장률

    return plot_economic_series(
        data_dict=ATLANTA_WAGE_GROWTH_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        left_ytitle=left_ytitle or default_ytitle,
        right_ytitle=right_ytitle or default_ytitle,
        korean_names=ATLANTA_WAGE_GROWTH_KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_atlanta_wage_growth_data(series_list, data_type='raw', periods=None, 
                                  target_date=None, export_path=None, file_format='excel'):
    """Atlanta Fed Wage Growth 데이터 export 함수 - export_economic_data 활용"""
    if not ATLANTA_WAGE_GROWTH_DATA:
        print("⚠️ 먼저 load_atlanta_wage_growth_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=ATLANTA_WAGE_GROWTH_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=ATLANTA_WAGE_GROWTH_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_atlanta_wage_growth_data():
    """Atlanta Fed Wage Growth 데이터 초기화"""
    global ATLANTA_WAGE_GROWTH_DATA
    ATLANTA_WAGE_GROWTH_DATA = {}
    print("🗑️ Atlanta Fed Wage Growth 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not ATLANTA_WAGE_GROWTH_DATA or 'raw_data' not in ATLANTA_WAGE_GROWTH_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_atlanta_wage_growth_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return ATLANTA_WAGE_GROWTH_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in ATLANTA_WAGE_GROWTH_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return ATLANTA_WAGE_GROWTH_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not ATLANTA_WAGE_GROWTH_DATA or 'mom_data' not in ATLANTA_WAGE_GROWTH_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_atlanta_wage_growth_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return ATLANTA_WAGE_GROWTH_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in ATLANTA_WAGE_GROWTH_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return ATLANTA_WAGE_GROWTH_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not ATLANTA_WAGE_GROWTH_DATA or 'yoy_data' not in ATLANTA_WAGE_GROWTH_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_atlanta_wage_growth_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return ATLANTA_WAGE_GROWTH_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in ATLANTA_WAGE_GROWTH_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return ATLANTA_WAGE_GROWTH_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not ATLANTA_WAGE_GROWTH_DATA or 'raw_data' not in ATLANTA_WAGE_GROWTH_DATA:
        return []
    return list(ATLANTA_WAGE_GROWTH_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 Atlanta Fed Wage Growth 시리즈 표시"""
    print("=== 사용 가능한 Atlanta Fed Wage Growth 시리즈 ===")
    
    for series_name, series_id in ATLANTA_WAGE_GROWTH_SERIES.items():
        korean_name = ATLANTA_WAGE_GROWTH_KOREAN_NAMES.get(series_name, series_name)
        print(f"  '{series_name}': {korean_name} ({series_id})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in ATLANTA_WAGE_GROWTH_CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            for series_name in series_list:
                korean_name = ATLANTA_WAGE_GROWTH_KOREAN_NAMES.get(series_name, series_name)
                api_id = ATLANTA_WAGE_GROWTH_SERIES.get(series_name, series_name)
                print(f"    - {series_name}: {korean_name} ({api_id})")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not ATLANTA_WAGE_GROWTH_DATA or 'load_info' not in ATLANTA_WAGE_GROWTH_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': ATLANTA_WAGE_GROWTH_DATA['load_info']['loaded'],
        'series_count': ATLANTA_WAGE_GROWTH_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': ATLANTA_WAGE_GROWTH_DATA['load_info']
    }

# %%
# === 사용 예시 ===

print("=== 리팩토링된 Atlanta Fed Wage Growth 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_atlanta_wage_growth_data()  # 스마트 업데이트")
print("   load_atlanta_wage_growth_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_atlanta_wage_growth_series_advanced(['median_overall_3m', 'median_overall_12m'], 'multi_line', 'raw')")
print("   plot_atlanta_wage_growth_series_advanced(['job_stayer_12m', 'job_switcher_12m'], 'multi_line', 'raw', left_ytitle='%')")
print("   plot_atlanta_wage_growth_series_advanced(['median_overall_12m'], 'single_line', 'raw', periods=24, left_ytitle='%')")
print("   plot_atlanta_wage_growth_series_advanced(['male_12m', 'female_12m'], 'dual_axis', 'raw', left_ytitle='%', right_ytitle='%')")
print()
print("3. 🔥 데이터 Export:")
print("   export_atlanta_wage_growth_data(['median_overall_3m', 'median_overall_12m'], 'raw')")
print("   export_atlanta_wage_growth_data(['manufacturing_12m'], 'raw', periods=24, file_format='csv')")
print("   export_atlanta_wage_growth_data(['percentile_25_3m', 'percentile_75_3m'], 'raw', target_date='2024-06-01')")
print()
print("✅ plot_atlanta_wage_growth_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_atlanta_wage_growth_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
load_atlanta_wage_growth_data()
plot_atlanta_wage_growth_series_advanced(['median_overall_3m', 'median_overall_12m'], 'multi_line', 'raw')