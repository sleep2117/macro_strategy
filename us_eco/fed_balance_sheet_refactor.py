# %%
"""
Federal Reserve Balance Sheet H.4.1 데이터 분석 (리팩토링 버전)
- us_eco_utils를 사용한 통합 구조
- 시리즈 정의와 분석 로직만 포함
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sys
import warnings
import matplotlib.pyplot as plt
warnings.filterwarnings('ignore')

# 통합 유틸리티 함수 불러오기
from us_eco_utils import *

# kpds 시각화 라이브러리 불러오기
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

# %%
# === FRED API 키 설정 ===

# FRED 데이터이므로 FRED API 키 필요
# api_config.FRED_API_KEY = '...'

# %%
# === Federal Reserve Balance Sheet H.4.1 시리즈 정의 ===

# 메인 시리즈 딕셔너리 (올바른 구조: 시리즈_이름: API_ID)
FED_BALANCE_SERIES = {
    # 공급 요인 (Assets)
    'total_assets': 'WALCL',                    # 연준 총자산
    'securities_outright': 'WSHOSHO',           # 보유 증권 총액
    'treasury_securities': 'TREAST',            # 미 국채 보유액
    'mbs_securities': 'WSHOMCB',                # MBS 보유액
    'loans_credit_facilities': 'WLCFLL',        # 유동성 지원 대출
    'unamortized_discounts': 'WUDSHO',          # 미상각 할인
    'unamortized_premiums': 'WUPSHO',           # 미상각 프리미엄
    'repurchase_agreements': 'WORAL',           # 환매조건부채권
    
    # 흡수 요인 (Liabilities)
    'currency_circulation': 'WCURCIR',          # 유통 화폐
    'reverse_repo': 'WLRRAL',                   # 역환매조건부채권 (RRP)
    'treasury_general_account': 'WDTGAL',       # 재무부 일반계정 (TGA)
    'foreign_deposits': 'WDFOL',                # 외국 공식 예금
    'other_deposits': 'WOFDRBORBL',            # 기타 예금
    'other_liabilities': 'WOTHLB',              # 기타 부채 및 자본
    
    # 결과 지표
    'reserve_balances': 'WRBWFRBL',             # 준비금 잔액 (수요일)
    
    # 총계 시리즈
    'total_supply_factors': 'WTFSRFL',          # 준비금 공급 요인 총계
    'total_absorb_factors': 'WTFORBAFL',        # 준비금 흡수 요인 총계
    
    # 추가 세부 시리즈
    'primary_credit': 'WLCFLPCL',               # 1차 신용
    'secondary_credit': 'WLCFLSCL',             # 2차 신용
    'bank_term_funding': 'H41RESPPALDKNWW',     # 은행 기간 자금 프로그램
    'treasury_bills': 'WSHOBL',                 # 단기 국채
    'treasury_notes': 'WSHONBNL',               # 중기 국채 (2-10Y)
    'treasury_bonds': 'WSHONBIIL',              # 장기 국채 (인플레이션 인덱스)
    'agency_debt': 'WSHOFADSL',                 # 연방기관 채권
    
    # 경제 지표
    'nominal_gdp': 'GDP'                        # 미국 명목 GDP
}

# 한국어 이름 매핑
FED_BALANCE_KOREAN_NAMES = {
    # 공급 요인 (자산)
    'total_assets': '연준 총자산',
    'securities_outright': '보유 증권 총액',
    'treasury_securities': '미 국채 보유액',
    'mbs_securities': 'MBS 보유액',
    'loans_credit_facilities': '유동성 지원 대출',
    'unamortized_discounts': '미상각 할인',
    'unamortized_premiums': '미상각 프리미엄',
    'repurchase_agreements': '환매조건부채권',
    
    # 흡수 요인 (부채)
    'currency_circulation': '유통 화폐',
    'reverse_repo': '역환매조건부채권 (RRP)',
    'treasury_general_account': '재무부 일반계정 (TGA)',
    'foreign_deposits': '외국 공식 예금',
    'other_deposits': '기타 예금',
    'other_liabilities': '기타 부채 및 자본',
    
    # 결과 지표
    'reserve_balances': '준비금 잔액 (수요일)',
    
    # 총계 시리즈
    'total_supply_factors': '준비금 공급 요인 총계',
    'total_absorb_factors': '준비금 흡수 요인 총계',
    
    # 추가 세부 시리즈
    'primary_credit': '1차 신용',
    'secondary_credit': '2차 신용',
    'bank_term_funding': '은행 기간 자금 프로그램',
    'treasury_bills': '단기 국채',
    'treasury_notes': '중기 국채 (2-10년)',
    'treasury_bonds': '인플레이션 인덱스 국채',
    'agency_debt': '연방기관 채권',
    
    # 경제 지표
    'nominal_gdp': '미국 명목 GDP'
}

# 카테고리 분류
FED_BALANCE_CATEGORIES = {
    '주요 지표': {
        'Core Balance Sheet': ['total_assets', 'reserve_balances', 'reverse_repo', 'treasury_general_account'],
        'Key Assets': ['securities_outright', 'treasury_securities', 'mbs_securities']
    },
    '자산 항목': {
        'Securities Holdings': ['securities_outright', 'treasury_securities', 'mbs_securities', 'agency_debt'],
        'Treasury Detail': ['treasury_bills', 'treasury_notes', 'treasury_bonds'],
        'Credit Facilities': ['loans_credit_facilities', 'primary_credit', 'secondary_credit', 'bank_term_funding']
    },
    '부채 항목': {
        'Major Liabilities': ['currency_circulation', 'reverse_repo', 'treasury_general_account'],
        'Deposits': ['foreign_deposits', 'other_deposits'],
        'Other': ['other_liabilities']
    },
    '유동성 분석': {
        'Liquidity Components': ['total_assets', 'treasury_general_account', 'reverse_repo', 'reserve_balances'],
        'Market Operations': ['repurchase_agreements', 'reverse_repo']
    },
    '경제 지표': {
        'Macroeconomic': ['nominal_gdp']
    }
}

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/fed_balance_sheet_data_refactored.csv'
FED_BALANCE_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_fed_balance_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 Federal Reserve Balance Sheet 데이터 로드"""
    global FED_BALANCE_DATA

    # 데이터 소스별 분기
    data_source = 'FRED'
    tolerance = 10.0  # Federal Reserve 데이터는 일반적으로 10 단위 허용 오차
    
    result = load_economic_data(
        series_dict=FED_BALANCE_SERIES,
        data_source=data_source,
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=tolerance
    )

    if result:
        FED_BALANCE_DATA = result
        print_load_info()
        return True
    else:
        print("❌ Federal Reserve Balance Sheet 데이터 로드 실패")
        return False

def print_load_info():
    """Federal Reserve Balance Sheet 데이터 로드 정보 출력"""
    if not FED_BALANCE_DATA or 'load_info' not in FED_BALANCE_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = FED_BALANCE_DATA['load_info']
    print(f"\n📊 Federal Reserve Balance Sheet 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in FED_BALANCE_DATA and not FED_BALANCE_DATA['raw_data'].empty:
        latest_date = FED_BALANCE_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_fed_balance_series_advanced(series_list, chart_type='multi_line', 
                                    data_type='raw', periods=None, target_date=None,
                                    left_ytitle=None, right_ytitle=None):
    """범용 Federal Reserve Balance Sheet 시각화 함수 - 개선된 연속성"""
    if not FED_BALANCE_DATA:
        print("⚠️ 먼저 load_fed_balance_data()를 실행하세요.")
        return None

    # 데이터 전처리로 연속성 확보
    processed_data = preprocess_fed_data_for_plotting(FED_BALANCE_DATA, data_type)
    
    if processed_data is None:
        return None

    # 단위별 기본 Y축 제목 설정
    if data_type == 'mom':
        default_ytitle = "%"
    elif data_type == 'yoy':
        default_ytitle = "%"  
    else:  # raw
        default_ytitle = "백만 달러"

    return plot_economic_series(
        data_dict=processed_data,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        left_ytitle=left_ytitle or default_ytitle,
        right_ytitle=right_ytitle or default_ytitle,
        korean_names=FED_BALANCE_KOREAN_NAMES
    )

def preprocess_fed_data_for_plotting(data_dict, data_type='raw'):
    """Federal Reserve 데이터 시각화 전처리 - 연속성 확보"""
    if not data_dict or f'{data_type}_data' not in data_dict:
        print(f"⚠️ {data_type} 데이터를 찾을 수 없습니다.")
        return None
    
    # 원본 데이터 복사
    processed_dict = data_dict.copy()
    raw_data = processed_dict[f'{data_type}_data'].copy()
    
    if raw_data.empty:
        return None
    
    # 각 시리즈별로 연속성 처리
    for column in raw_data.columns:
        series = raw_data[column]
        
        # GDP 같은 quarterly 데이터는 선형 보간
        if 'gdp' in column.lower():
            series_interpolated = series.interpolate(method='linear')
            series_filled = series_interpolated.ffill().bfill()
        else:
            # Weekly Fed 데이터는 forward fill로 처리
            series_filled = series.ffill()
        
        # 극값 제거 (outliers that might cause visual issues)
        if series_filled.dtype in ['float64', 'int64']:
            q95 = series_filled.quantile(0.95)
            q05 = series_filled.quantile(0.05)
            series_cleaned = series_filled.clip(lower=q05*0.5, upper=q95*1.5)
            raw_data[column] = series_cleaned
        else:
            raw_data[column] = series_filled
    
    # 처리된 데이터로 업데이트
    processed_dict[f'{data_type}_data'] = raw_data
    
    print(f"✅ 데이터 전처리 완료: {len(raw_data)}개 포인트, {len(raw_data.columns)}개 시리즈")
    
    return processed_dict

# %%
# === 데이터 Export 함수 ===
def export_fed_balance_data(series_list, data_type='raw', periods=None, 
                           target_date=None, export_path=None, file_format='excel'):
    """Federal Reserve Balance Sheet 데이터 export 함수 - export_economic_data 활용"""
    if not FED_BALANCE_DATA:
        print("⚠️ 먼저 load_fed_balance_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=FED_BALANCE_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=FED_BALANCE_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_fed_balance_data():
    """Federal Reserve Balance Sheet 데이터 초기화"""
    global FED_BALANCE_DATA
    FED_BALANCE_DATA = {}
    print("🗑️ Federal Reserve Balance Sheet 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not FED_BALANCE_DATA or 'raw_data' not in FED_BALANCE_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_fed_balance_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return FED_BALANCE_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in FED_BALANCE_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return FED_BALANCE_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not FED_BALANCE_DATA or 'mom_data' not in FED_BALANCE_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_fed_balance_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return FED_BALANCE_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in FED_BALANCE_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return FED_BALANCE_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not FED_BALANCE_DATA or 'yoy_data' not in FED_BALANCE_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_fed_balance_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return FED_BALANCE_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in FED_BALANCE_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return FED_BALANCE_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not FED_BALANCE_DATA or 'raw_data' not in FED_BALANCE_DATA:
        return []
    return list(FED_BALANCE_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 Federal Reserve Balance Sheet 시리즈 표시"""
    print("=== 사용 가능한 Federal Reserve Balance Sheet 시리즈 ===")
    
    for series_name, series_id in FED_BALANCE_SERIES.items():
        korean_name = FED_BALANCE_KOREAN_NAMES.get(series_name, series_name)
        print(f"  '{series_name}': {korean_name} ({series_id})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in FED_BALANCE_CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            for series_name in series_list:
                korean_name = FED_BALANCE_KOREAN_NAMES.get(series_name, series_name)
                api_id = FED_BALANCE_SERIES.get(series_name, series_name)
                print(f"    - {series_name}: {korean_name} ({api_id})")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not FED_BALANCE_DATA or 'load_info' not in FED_BALANCE_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': FED_BALANCE_DATA['load_info']['loaded'],
        'series_count': FED_BALANCE_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': FED_BALANCE_DATA['load_info']
    }

# %%
# === 특화 분석 함수들 ===

def calculate_liquidity_metrics():
    """유동성 지표 계산 함수"""
    if not FED_BALANCE_DATA or 'raw_data' not in FED_BALANCE_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다.")
        return pd.DataFrame()
    
    data = FED_BALANCE_DATA['raw_data'].copy()
    liquidity_df = pd.DataFrame()
    
    # 기본 유동성 = 총자산 - TGA - RRP
    if all(col in data.columns for col in ['total_assets', 'treasury_general_account', 'reverse_repo']):
        liquidity_df['basic_liquidity'] = data['total_assets'] - data['treasury_general_account'] - data['reverse_repo']
    
    # 순 유동성 = 준비금 잔액 + TGA
    if all(col in data.columns for col in ['reserve_balances', 'treasury_general_account']):
        liquidity_df['net_liquidity'] = data['reserve_balances'] + data['treasury_general_account']
    
    # RRP 비율 = RRP / 총자산
    if all(col in data.columns for col in ['reverse_repo', 'total_assets']):
        liquidity_df['rrp_ratio'] = (data['reverse_repo'] / data['total_assets']) * 100
    
    return liquidity_df

def plot_liquidity_dashboard():
    """유동성 대시보드 시각화"""
    print("Federal Reserve 유동성 대시보드")
    
    # 핵심 지표들
    core_series = ['total_assets', 'reserve_balances', 'reverse_repo', 'treasury_general_account']
    available_series = [s for s in core_series if s in list_available_series()]
    
    if available_series:
        return plot_fed_balance_series_advanced(
            available_series, 
            chart_type='multi_line', 
            data_type='raw',
            periods=104,  # 2년간
            left_ytitle='백만 달러'
        )
    else:
        print("⚠️ 필요한 시리즈가 로드되지 않았습니다.")
        return None

def calculate_reserve_gdp_ratio():
    """지준금 잔액/명목 GDP 비율 계산 함수 (개선된 보간 방식)"""
    if not FED_BALANCE_DATA or 'raw_data' not in FED_BALANCE_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다.")
        return pd.DataFrame()
    
    data = FED_BALANCE_DATA['raw_data'].copy()
    
    # 필요한 시리즈 확인
    if 'reserve_balances' not in data.columns or 'nominal_gdp' not in data.columns:
        print("⚠️ 지준금 잔액 또는 GDP 데이터가 없습니다.")
        available = list_available_series()
        print(f"사용 가능한 시리즈: {available}")
        return pd.DataFrame()
    
    # GDP 데이터 전처리 (quarterly → weekly 보간)
    gdp_data = data['nominal_gdp'].copy()
    
    # NaN이 아닌 GDP 데이터만 추출
    gdp_clean = gdp_data.dropna()
    
    if len(gdp_clean) == 0:
        print("⚠️ 유효한 GDP 데이터가 없습니다.")
        return pd.DataFrame()
    
    # 선형 보간으로 weekly GDP 데이터 생성
    gdp_weekly = gdp_data.interpolate(method='linear')
    
    # 보간 후에도 처음/마지막 NaN은 forward/backward fill로 처리
    gdp_weekly = gdp_weekly.ffill().bfill()
    
    # GDP를 Millions 단위로 변환 (원래 Billions)
    gdp_millions = gdp_weekly * 1000
    
    # 지준금 데이터에서 유효한 값만 추출
    reserves_clean = data['reserve_balances'].dropna()
    
    if len(reserves_clean) == 0:
        print("⚠️ 유효한 지준금 데이터가 없습니다.")
        return pd.DataFrame()
    
    # 공통 인덱스로 정렬 (두 시리즈 모두 유효한 날짜만)
    common_index = gdp_millions.index.intersection(reserves_clean.index)
    
    if len(common_index) == 0:
        print("⚠️ GDP와 지준금 데이터의 공통 날짜가 없습니다.")
        return pd.DataFrame()
    
    # 공통 날짜만으로 데이터 정렬
    gdp_aligned = gdp_millions.loc[common_index]
    reserves_aligned = data['reserve_balances'].loc[common_index]
    
    # 비율 계산 (퍼센트)
    reserve_gdp_ratio = (reserves_aligned / gdp_aligned) * 100
    
    # 결과 DataFrame 생성
    result_df = pd.DataFrame({
        'reserve_balances': reserves_aligned,
        'nominal_gdp_millions': gdp_aligned,
        'reserve_gdp_ratio': reserve_gdp_ratio
    }, index=common_index)
    
    # 최종 검증: 모든 값이 유효한지 확인
    result_df = result_df.dropna()
    
    print(f"✅ 계산 완료: {len(result_df)}개 데이터 포인트")
    print(f"📅 기간: {result_df.index[0].strftime('%Y-%m-%d')} ~ {result_df.index[-1].strftime('%Y-%m-%d')}")
    
    return result_df

def plot_reserve_gdp_ratio(periods=None, target_ratio=None, target_date=None):
    """지준금/GDP 비율 시각화 함수 (kpds 스타일, target lines 포함)"""
    
    # 비율 데이터 계산
    ratio_data = calculate_reserve_gdp_ratio()
    
    if ratio_data.empty:
        print("⚠️ 비율 데이터를 계산할 수 없습니다.")
        return None
    
    # 기간 제한 적용
    if periods:
        ratio_data = ratio_data.tail(periods)
    
    # 연속성 확인 및 데이터 품질 점검
    ratio_series = ratio_data['reserve_gdp_ratio'].copy()
    
    # 유효한 값만 사용 (inf, -inf 제거)
    ratio_series = ratio_series.replace([np.inf, -np.inf], np.nan).dropna()
    
    if len(ratio_series) == 0:
        print("⚠️ 유효한 비율 데이터가 없습니다.")
        return None
    
    print(f"📊 시각화 데이터: {len(ratio_series)}개 포인트")
    
    # DataFrame 준비 (kpds 함수는 DataFrame을 받음)
    plot_df = pd.DataFrame({'지준금_GDP_비율': ratio_series})
    
    # Target ratio나 target date가 있는 경우 복합 차트 사용
    if target_ratio is not None or target_date is not None:
        # 복합 차트를 위한 라인 설정
        line_config = {
            'columns': ['지준금_GDP_비율'],
            'labels': ['지준금/GDP 비율'],
            'colors': [deepred_pds],  # KPDS 메인 컬러
            'widths': [3],
            'markers': False  # 마커 없이 순수한 라인만
        }
        
        # Target ratio horizontal line 추가
        if target_ratio is not None:
            target_line_data = pd.Series([target_ratio] * len(plot_df), index=plot_df.index)
            plot_df['목표_비율'] = target_line_data
            line_config['columns'].append('목표_비율')
            line_config['labels'].append(f'목표 비율: {target_ratio}%')
            line_config['colors'].append('rgba(128, 128, 128, 0.6)')  # 회색에 투명도 0.6
            line_config['widths'].append(2)
            line_config['dash'] = ['solid', 'dash']
        
        # 복합 차트 생성
        return create_flexible_mixed_chart(
            df=plot_df,
            line_config=line_config,
            dual_axis=False,
            left_ytitle='%',
            width_cm=12,
            height_cm=8
        )
    else:
        # 단순 라인 차트 (target 없는 경우)
        return df_line_chart(
            df=plot_df,
            column='지준금_GDP_비율',
            ytitle='%',
            label='지준금/GDP 비율',
            width_cm=12,
            height_cm=8
        )

# %%
# === 사용 예시 ===

print("=== 리팩토링된 Federal Reserve Balance Sheet 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_fed_balance_data()  # 스마트 업데이트")
print("   load_fed_balance_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_fed_balance_series_advanced(['total_assets', 'reserve_balances'], 'multi_line', 'raw')")
print("   plot_fed_balance_series_advanced(['reverse_repo'], 'horizontal_bar', 'yoy', left_ytitle='%')")
print("   plot_fed_balance_series_advanced(['total_assets'], 'single_line', 'mom', periods=24, left_ytitle='%')")
print("   plot_fed_balance_series_advanced(['total_assets', 'reserve_balances'], 'dual_axis', 'raw', left_ytitle='백만 달러', right_ytitle='백만 달러')")
print()
print("3. 🔥 데이터 Export:")
print("   export_fed_balance_data(['total_assets', 'reserve_balances'], 'raw')")
print("   export_fed_balance_data(['reverse_repo'], 'mom', periods=24, file_format='csv')")
print("   export_fed_balance_data(['treasury_general_account'], 'yoy', target_date='2024-06-01')")
print()
print("4. 🔥 특화 분석:")
print("   calculate_liquidity_metrics()  # 유동성 지표 계산")
print("   plot_liquidity_dashboard()     # 유동성 대시보드")
print()
print("5. 🔥 지준금/GDP 비율 분석 (KPDS 스타일):")
print("   calculate_reserve_gdp_ratio()  # 지준금/GDP 비율 계산")
print("   plot_reserve_gdp_ratio()       # 기본 KPDS 라인 차트")
print("   plot_reserve_gdp_ratio(periods=52, target_ratio=15.0)  # 목표 비율 라인 포함")
print("   plot_reserve_gdp_ratio_simple() # 간단한 버전")
print()
print("6. 🔥 타이틀 출력 방식 (CLAUDE.md 규칙):")
print('   print("지준금/GDP 비율 추이")')
print("   plot_reserve_gdp_ratio()")
print()
print("✅ plot_fed_balance_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_fed_balance_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")
print("✅ Federal Reserve H.4.1 Balance Sheet 전용 유동성 분석 기능 포함!")
print("✅ 지준금/GDP 비율 분석을 KPDS 표준 스타일로 시각화!")
print("✅ 목표 비율 horizontal line과 KPDS 색상 팔레트 지원!")

# %%
# === 테스트 및 실행 ===
print("🚀 Federal Reserve Balance Sheet 분석 시작...")

# 데이터 로드
load_fed_balance_data()

# %%
# 유동성 대시보드 테스트 (개선된 연속성)
print("💧 유동성 대시보드")
plot_liquidity_dashboard()

# %%
# 지준금/GDP 비율 분석 테스트 (KPDS 스타일)
print("📊 지준금/GDP 비율 추이")
plot_reserve_gdp_ratio()

# %%
# 목표 비율이 포함된 분석 (KPDS 스타일)
print("🎯 지준금/GDP 비율 vs 목표 10%")
plot_reserve_gdp_ratio(target_ratio=10.0, periods=104)
