# %%
"""
FRED API 전용 필립스 커브 분석 도구 (Enhanced Version)
- FRED API를 사용하여 실업률(UNRATE)과 인플레이션 데이터 수집  
- 스마트 업데이트 시스템: CSV 캐싱 + 일치성 확인
- 코로나 전후 시기별 필립스 커브 시각화
- 다양한 인플레이션 지표 지원 (CPI, PCE, 코어, 트림평균 등)
- 세부 기간별 분석 및 노동시장-인플레이션 관계 분석
- KPDS 시각화 포맷 적용
"""
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import datetime as dt
import sys
import warnings
import os
warnings.filterwarnings('ignore')

# 필수 라이브러리들
try:
    import requests
    import json
    FRED_API_AVAILABLE = True
    print("✓ FRED API 연동 가능 (requests 라이브러리)")
except ImportError:
    print("⚠️ requests 라이브러리가 없습니다. 샘플 데이터로 진행합니다.")
    FRED_API_AVAILABLE = False

# FRED API 키 설정
FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'  # https://fred.stlouisfed.org/docs/api/api_key.html 에서 발급

# KPDS 시각화 라이브러리 불러오기 (필수)
sys.path.append('/home/jyp0615')
try:
    from kpds_fig_format_enhanced import *
    print("✓ KPDS 시각화 포맷 로드됨")
    KPDS_AVAILABLE = True
except ImportError:
    print("⚠️ KPDS 시각화 포맷을 불러올 수 없습니다.")
    KPDS_AVAILABLE = False

# %%
# === 필립스 커브 시리즈 정의 ===

# FRED 시리즈 ID
PHILLIPS_SERIES = {
    # 실업률
    'unemployment_rate': 'UNRATE',
    
    # 지수 형태 인플레이션 (연간 변화율 변환 필요)
    'cpi_headline': 'CPIAUCSL',        # CPI 헤드라인
    'cpi_core': 'CPILFESL',            # CPI 코어
    'pce_headline': 'PCEPI',           # PCE 헤드라인
    'pce_core': 'PCEPILFE',            # PCE 코어
    
    # 이미 변화율 형태 인플레이션
    'pce_trimmed_mean': 'PCETRIM1M158SFRBDAL',    # 트림 평균 PCE
    'core_sticky_cpi': 'CORESTICKM159SFRBATL',    # 코어 스티키 CPI
}

# 한국어 이름 매핑
SERIES_KOREAN_NAMES = {
    'unemployment_rate': '실업률',
    'cpi_headline': 'CPI 헤드라인 인플레이션',
    'cpi_core': 'CPI 코어 인플레이션',
    'pce_headline': 'PCE 헤드라인 인플레이션',
    'pce_core': 'PCE 코어 인플레이션',
    'pce_trimmed_mean': '트림 평균 PCE 인플레이션',
    'core_sticky_cpi': '코어 스티키 CPI 인플레이션'
}

# 지수에서 변화율로 변환이 필요한 시리즈들
INDEX_SERIES = ['cpi_headline', 'cpi_core', 'pce_headline', 'pce_core']

# %%
# === 전역 데이터 저장소 ===

# FRED 세션
FRED_SESSION = None

# CSV 파일 경로
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/phillips_curve_data.csv'

# 전역 데이터 저장소
PHILLIPS_DATA = {
    'raw_data': pd.DataFrame(),      # 원본 데이터
    'processed_data': pd.DataFrame(), # 처리된 데이터 (인플레이션 변화율 변환 후)
    'combined_data': pd.DataFrame(),  # 결합 데이터 (기간 구분 포함)
    'latest_values': {},             # 최신 값들
    'load_info': {
        'loaded': False,
        'load_time': None,
        'start_date': None,
        'series_count': 0,
        'data_points': 0
    }
}

# %%
# === CSV 데이터 관리 함수들 ===

def ensure_data_directory():
    """데이터 디렉터리 생성 확인"""
    data_dir = os.path.dirname(CSV_FILE_PATH)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"📁 데이터 디렉터리 생성: {data_dir}")

def save_data_to_csv():
    """현재 데이터를 CSV에 저장"""
    if PHILLIPS_DATA['processed_data'].empty:
        print("⚠️ 저장할 데이터가 없습니다.")
        return False
    
    ensure_data_directory()
    
    try:
        # 날짜를 컬럼으로 추가하여 저장
        df_to_save = PHILLIPS_DATA['processed_data'].copy()
        df_to_save.index.name = 'date'
        df_to_save.to_csv(CSV_FILE_PATH)
        print(f"💾 데이터 저장 완료: {CSV_FILE_PATH}")
        return True
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        return False

def load_data_from_csv():
    """CSV에서 데이터 로드"""
    if not os.path.exists(CSV_FILE_PATH):
        print("📂 저장된 CSV 파일이 없습니다.")
        return None
    
    try:
        df = pd.read_csv(CSV_FILE_PATH, index_col=0, parse_dates=True)
        print(f"📂 CSV 데이터 로드: {len(df)}개 데이터 포인트")
        return df
    except Exception as e:
        print(f"❌ CSV 로드 실패: {e}")
        return None

def check_recent_data_consistency(csv_data=None, tolerance=0.5):
    """
    최근 데이터의 일치성 확인 (필립스 커브 데이터용 - % 단위)
    
    Args:
        csv_data: CSV에서 로드된 데이터 (None이면 자동 로드)
        tolerance: 허용 오차 (% 단위, 기본값: 0.5%)
    
    Returns:
        dict: 일치성 확인 결과
    """
    if csv_data is None:
        csv_data = load_data_from_csv()
    
    if csv_data is None or csv_data.empty:
        return {'needs_update': True, 'reason': 'CSV 데이터 없음'}
    
    # 최근 3개월 데이터 확인
    try:
        # 날짜 정규화
        csv_data.index = pd.to_datetime(csv_data.index)
        csv_latest = csv_data.tail(3)
        
        # 동일한 기간의 API 데이터와 비교
        if PHILLIPS_DATA['processed_data'].empty:
            return {'needs_update': True, 'reason': 'API 데이터 없음'}
        
        api_data = PHILLIPS_DATA['processed_data']
        api_data.index = pd.to_datetime(api_data.index)
        
        # 공통 날짜 찾기
        common_dates = csv_latest.index.intersection(api_data.index)
        
        if len(common_dates) == 0:
            return {'needs_update': True, 'reason': '공통 날짜 없음'}
        
        # 최근 데이터 비교
        inconsistencies = []
        
        for date in common_dates[-3:]:  # 최근 3개 데이터만 확인
            csv_row = csv_latest.loc[date]
            api_row = api_data.loc[date]
            
            for column in csv_row.index:
                if column in api_row.index:
                    csv_val = csv_row[column]
                    api_val = api_row[column]
                    
                    # NaN 값 처리
                    if pd.isna(csv_val) and pd.isna(api_val):
                        continue
                    if pd.isna(csv_val) or pd.isna(api_val):
                        inconsistencies.append({
                            'date': date.strftime('%Y-%m'),
                            'series': column,
                            'csv_value': csv_val,
                            'api_value': api_val,
                            'difference': 'NaN 불일치',
                            'significant': True
                        })
                        continue
                    
                    # 차이 계산 (% 단위)
                    try:
                        diff = abs(float(csv_val) - float(api_val))
                    except (ValueError, TypeError):
                        # 숫자로 변환할 수 없는 경우 문자열 비교로 처리
                        diff = float('inf') if str(csv_val) != str(api_val) else 0.0
                    
                    # 유의미한 차이인지 확인
                    is_significant = diff > tolerance
                    
                    if is_significant:
                        inconsistencies.append({
                            'date': date.strftime('%Y-%m'),
                            'series': column,
                            'csv_value': csv_val,
                            'api_value': api_val,
                            'difference': diff,
                            'significant': True
                        })
        
        # 유의미한 불일치가 있는지 확인
        significant_inconsistencies = [inc for inc in inconsistencies if inc['significant']]
        
        result = {
            'needs_update': len(significant_inconsistencies) > 0,
            'inconsistencies': inconsistencies,
            'significant_count': len(significant_inconsistencies),
            'total_compared': len(common_dates),
            'tolerance': tolerance
        }
        
        if result['needs_update']:
            result['reason'] = f'유의미한 데이터 불일치 {len(significant_inconsistencies)}건 발견'
        else:
            result['reason'] = '최근 데이터 일치'
        
        return result
        
    except Exception as e:
        print(f"⚠️ 데이터 일치성 확인 중 오류: {e}")
        return {'needs_update': True, 'reason': f'확인 오류: {str(e)}'}

def print_consistency_results(consistency_result):
    """일치성 확인 결과 출력"""
    result = consistency_result
    
    print(f"\n🔍 데이터 일치성 확인 결과")
    print(f"   업데이트 필요: {result['needs_update']}")
    print(f"   사유: {result['reason']}")
    
    if 'inconsistencies' in result and result['inconsistencies']:
        print(f"   허용 오차: {result['tolerance']:.1f}%")
        print(f"   비교 기간: {result['total_compared']}개월")
        print(f"   전체 불일치: {len(result['inconsistencies'])}건")
        print(f"   유의미한 불일치: {result['significant_count']}건")
        
        if result['significant_count'] > 0:
            print(f"\n   📊 유의미한 차이 세부 내용:")
            for inc in result['inconsistencies']:
                if inc['significant']:
                    if isinstance(inc['difference'], str):
                        print(f"   - {inc['date']} {inc['series']}: {inc['difference']}")
                    else:
                        csv_val = inc['csv_value']
                        api_val = inc['api_value']
                        diff = inc['difference']
                        print(f"   - {inc['date']} {inc['series']}: CSV={csv_val:.1f}% vs API={api_val:.1f}% (차이: {diff:.1f}%)")

# %%
# === FRED API 초기화 및 데이터 가져오기 함수 ===

def initialize_fred_api():
    """FRED API 세션 초기화"""
    global FRED_SESSION
    
    if not FRED_API_AVAILABLE:
        print("⚠️ FRED API 사용 불가 (requests 라이브러리 없음)")
        return False
    
    if not FRED_API_KEY or FRED_API_KEY == 'YOUR_FRED_API_KEY_HERE':
        print("⚠️ FRED API 키가 설정되지 않았습니다. FRED_API_KEY를 설정하세요.")
        print("  https://fred.stlouisfed.org/docs/api/api_key.html 에서 무료로 발급 가능합니다.")
        return False
    
    try:
        FRED_SESSION = requests.Session()
        print("✓ FRED API 세션 초기화 성공")
        return True
    except Exception as e:
        print(f"⚠️ FRED API 초기화 실패: {e}")
        return False

def get_fred_data(series_id, start_date='2000-01-01', end_date=None):
    """
    FRED API에서 데이터 가져오기
    
    Args:
        series_id: FRED 시리즈 ID
        start_date: 시작 날짜
        end_date: 종료 날짜 (None이면 현재)
    
    Returns:
        pandas.Series: 날짜를 인덱스로 하는 시리즈 데이터
    """
    if not FRED_API_AVAILABLE or FRED_SESSION is None:
        print(f"❌ FRED API 사용 불가 - {series_id}")
        # 샘플 데이터로 대체
        return create_sample_series(series_id, start_date, end_date)
    
    if end_date is None:
        end_date = dt.datetime.now().strftime('%Y-%m-%d')
    
    # FRED API URL 구성
    url = f'https://api.stlouisfed.org/fred/series/observations'
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'observation_start': start_date,
        'observation_end': end_date,
        'sort_order': 'asc'
    }
    
    try:
        print(f"📊 FRED에서 로딩: {series_id}")
        response = FRED_SESSION.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if 'observations' in data:
            observations = data['observations']
            
            # 데이터 정리
            dates = []
            values = []
            
            for obs in observations:
                try:
                    date = pd.to_datetime(obs['date'])
                    # '.' 값은 결측치로 처리
                    if obs['value'] != '.':
                        value = float(obs['value'])
                        dates.append(date)
                        values.append(value)
                except (ValueError, KeyError):
                    continue
            
            if dates and values:
                series = pd.Series(values, index=dates, name=series_id)
                series = series.sort_index()
                
                print(f"✓ FRED 성공: {series_id} ({len(series)}개 포인트)")
                return series
            else:
                print(f"❌ FRED 데이터 없음: {series_id} - 샘플 데이터 사용")
                return create_sample_series(series_id, start_date, end_date)
        else:
            print(f"❌ FRED 응답에 데이터 없음: {series_id} - 샘플 데이터 사용")
            return create_sample_series(series_id, start_date, end_date)
            
    except Exception as e:
        print(f"❌ FRED 요청 실패: {series_id} - {e}")
        print("   샘플 데이터로 대체합니다.")
        return create_sample_series(series_id, start_date, end_date)

def create_sample_series(series_id, start_date, end_date):
    """샘플 데이터 생성 (FRED API 실패시 대체용)"""
    if end_date is None:
        end_date = dt.datetime.now().strftime('%Y-%m-%d')
    
    date_range = pd.date_range(start=start_date, end=end_date, freq='MS')
    np.random.seed(42)
    
    if series_id == 'UNRATE':  # 실업률 (Unemployment Rate) - 베버리지 커브와 동일
        base_values = []
        for date in date_range:
            if date.year < 2008:
                base = 5.0 + np.sin((date.year - 2000) * 0.5) * 1.0 + np.random.normal(0, 0.3)
                base_values.append(max(3.5, min(6.5, base)))
            elif 2008 <= date.year <= 2009:
                if date.year == 2008:
                    base = 5.8 + (date.month / 12) * 4.0 + np.random.normal(0, 0.2)
                else:
                    base = 9.5 + np.sin(date.month) * 0.5 + np.random.normal(0, 0.3)
                base_values.append(max(5.0, min(10.0, base)))
            elif 2010 <= date.year <= 2019:
                trend = 9.5 - (date.year - 2010) * 0.6
                base = trend + np.random.normal(0, 0.2)
                base_values.append(max(3.5, min(9.5, base)))
            elif date.year == 2020:
                if date.month < 4:
                    base_values.append(3.5 + np.random.normal(0, 0.1))
                elif date.month == 4:
                    base_values.append(14.8)
                elif date.month < 8:
                    base_values.append(11.0 + np.random.normal(0, 1.0))
                else:
                    base = 11.0 - (date.month - 8) * 1.5 + np.random.normal(0, 0.3)
                    base_values.append(max(6.0, base))
            else:
                if date.year == 2021:
                    base = 6.0 - (date.month / 12) * 2.5 + np.random.normal(0, 0.2)
                else:
                    base = 3.7 + np.random.normal(0, 0.2)
                base_values.append(max(3.2, min(6.0, base)))
        
        return pd.Series(base_values, index=date_range, name=series_id)
        
    elif series_id in ['CPIAUCSL', 'CPILFESL']:  # CPI 지수들
        base_values = []
        base_index = 100.0  # 시작 지수
        
        for i, date in enumerate(date_range):
            # 연간 인플레이션률 설정
            if date.year < 2008:
                annual_inflation = 2.5 + np.sin((date.year - 2000) * 0.3) * 1.0 + np.random.normal(0, 0.5)
            elif 2008 <= date.year <= 2009:
                # 금융위기 시 디플레이션
                annual_inflation = -0.5 + np.random.normal(0, 1.0)
            elif 2010 <= date.year <= 2019:
                # 저인플레이션 기조
                annual_inflation = 1.8 + np.random.normal(0, 0.8)
            elif date.year == 2020:
                # 코로나 초기 디플레이션
                annual_inflation = 0.5 + np.random.normal(0, 1.0)
            elif date.year >= 2021:
                # 인플레이션 급등
                if date.year == 2021:
                    annual_inflation = 4.0 + (date.month / 12) * 3.0 + np.random.normal(0, 1.0)
                else:
                    annual_inflation = 6.0 + np.random.normal(0, 2.0)
            
            # 월간 변화율로 변환 (복리 계산)
            monthly_rate = ((1 + annual_inflation/100) ** (1/12)) - 1
            
            if i == 0:
                base_values.append(base_index)
            else:
                new_value = base_values[-1] * (1 + monthly_rate)
                base_values.append(max(90.0, new_value))  # 최소값 제한
        
        return pd.Series(base_values, index=date_range, name=series_id)
        
    elif series_id in ['PCEPI', 'PCEPILFE']:  # PCE 지수들 (CPI와 유사하지만 약간 낮은 수준)
        base_values = []
        base_index = 100.0
        
        for i, date in enumerate(date_range):
            # PCE는 CPI보다 약간 낮은 인플레이션
            if date.year < 2008:
                annual_inflation = 2.0 + np.sin((date.year - 2000) * 0.3) * 0.8 + np.random.normal(0, 0.4)
            elif 2008 <= date.year <= 2009:
                annual_inflation = -0.3 + np.random.normal(0, 0.8)
            elif 2010 <= date.year <= 2019:
                annual_inflation = 1.5 + np.random.normal(0, 0.6)
            elif date.year == 2020:
                annual_inflation = 0.8 + np.random.normal(0, 0.8)
            elif date.year >= 2021:
                if date.year == 2021:
                    annual_inflation = 3.5 + (date.month / 12) * 2.5 + np.random.normal(0, 0.8)
                else:
                    annual_inflation = 5.5 + np.random.normal(0, 1.5)
            
            monthly_rate = ((1 + annual_inflation/100) ** (1/12)) - 1
            
            if i == 0:
                base_values.append(base_index)
            else:
                new_value = base_values[-1] * (1 + monthly_rate)
                base_values.append(max(90.0, new_value))
        
        return pd.Series(base_values, index=date_range, name=series_id)
        
    elif series_id in ['PCETRIM1M158SFRBDAL', 'CORESTICKM159SFRBATL']:  # 이미 변화율인 시리즈들
        base_values = []
        for date in date_range:
            if date.year < 2008:
                base = 2.2 + np.sin((date.year - 2000) * 0.2) * 0.5 + np.random.normal(0, 0.3)
            elif 2008 <= date.year <= 2009:
                base = 0.8 + np.random.normal(0, 0.8)
            elif 2010 <= date.year <= 2019:
                base = 1.7 + np.random.normal(0, 0.4)
            elif date.year == 2020:
                base = 1.2 + np.random.normal(0, 0.6)
            elif date.year >= 2021:
                if date.year == 2021:
                    base = 3.0 + (date.month / 12) * 1.5 + np.random.normal(0, 0.6)
                else:
                    base = 4.8 + np.random.normal(0, 1.0)
            
            base_values.append(max(0.0, base))
        
        return pd.Series(base_values, index=date_range, name=series_id)
    
    else:
        print(f"⚠️ 지원하지 않는 시리즈 ID: {series_id}")
        return pd.Series()

# %%
# === 데이터 처리 함수들 ===

def calculate_inflation_rate(index_series, periods=12):
    """
    가격 지수에서 연간 변화율(인플레이션율) 계산
    
    Args:
        index_series: 가격 지수 시리즈
        periods: 비교 기간 (기본값: 12개월)
    
    Returns:
        pandas.Series: 인플레이션율 (%)
    """
    return ((index_series / index_series.shift(periods)) - 1) * 100

def process_phillips_data():
    """필립스 커브 데이터 후처리"""
    if PHILLIPS_DATA['raw_data'].empty:
        return
    
    df_raw = PHILLIPS_DATA['raw_data'].copy()
    df_processed = pd.DataFrame(index=df_raw.index)
    
    # 실업률은 그대로 복사
    if 'unemployment_rate' in df_raw.columns:
        df_processed['unemployment_rate'] = df_raw['unemployment_rate']
    
    # 지수 형태 시리즈들을 인플레이션율로 변환
    for series_name in INDEX_SERIES:
        if series_name in df_raw.columns:
            inflation_series = calculate_inflation_rate(df_raw[series_name])
            df_processed[series_name] = inflation_series
    
    # 이미 변화율 형태인 시리즈들은 그대로 복사
    rate_series = [s for s in PHILLIPS_SERIES.keys() if s not in INDEX_SERIES and s != 'unemployment_rate']
    for series_name in rate_series:
        if series_name in df_raw.columns:
            df_processed[series_name] = df_raw[series_name]
    
    # NaN 제거 (12개월 변화율 계산으로 인한 초기 NaN 제거)
    df_processed = df_processed.dropna()
    
    # 코로나 전후 구분을 위한 컬럼 추가
    df_processed['period'] = df_processed.index.to_series().apply(
        lambda x: 'Pre-COVID' if x < pd.Timestamp('2020-03-01') else 'COVID & Post-COVID'
    )
    
    # 세부적인 기간 구분 추가
    def classify_detailed_period(date):
        if date < pd.Timestamp('2008-01-01'):
            return '2000년대 초반'
        elif date < pd.Timestamp('2010-01-01'):
            return '금융위기 시기 (2008-2009)'
        elif date < pd.Timestamp('2020-03-01'):
            return '경기회복 시기 (2010-2019)'
        elif date < pd.Timestamp('2021-01-01'):
            return '코로나 초기 (2020)'
        elif date < pd.Timestamp('2023-01-01'):
            return '코로나 회복 (2021-2022)'
        else:
            return '최근 시기 (2023-)'
    
    df_processed['detailed_period'] = df_processed.index.to_series().apply(classify_detailed_period)
    
    # 처리된 데이터 저장
    PHILLIPS_DATA['processed_data'] = df_processed
    PHILLIPS_DATA['combined_data'] = df_processed
    
    # 최신 값 저장
    latest_values = {}
    for col in df_processed.columns:
        if col not in ['period', 'detailed_period']:
            latest_values[col] = {
                'value': df_processed[col].iloc[-1],
                'date': df_processed.index[-1].strftime('%Y-%m')
            }
    
    PHILLIPS_DATA['latest_values'] = latest_values

# %%
# === 데이터 로드 함수들 ===

def load_phillips_data(start_date='2000-01-01', force_reload=False, smart_update=True, 
                      inflation_series=['cpi_headline', 'cpi_core', 'pce_core']):
    """
    필립스 커브 데이터 로드 (스마트 업데이트 지원)
    
    Args:
        start_date: 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부 (기본값: True)
        inflation_series: 로드할 인플레이션 시리즈 리스트
    
    Returns:
        bool: 로드 성공 여부
    """
    global PHILLIPS_DATA
    
    # 이미 로드된 경우 스킵 (강제 재로드가 아닌 경우)
    if PHILLIPS_DATA['load_info']['loaded'] and not force_reload and not smart_update:
        print("💾 이미 로드된 데이터 사용 중")
        print_load_info()
        return True
    
    # 스마트 업데이트 로직
    needs_api_call = True
    consistency_result = None
    
    if smart_update and not force_reload:
        print("🤖 스마트 업데이트 모드 활성화")
        
        # 먼저 CSV에서 데이터 로드 시도
        csv_data = load_data_from_csv()
        
        if csv_data is not None and not csv_data.empty:
            # CSV 데이터를 임시로 전역 저장소에 로드
            PHILLIPS_DATA['processed_data'] = csv_data
            
            # 최신 몇 개 데이터만 API로 가져와서 비교
            print("🔍 최근 데이터 일치성 확인 중...")
            
            # FRED API 초기화
            if initialize_fred_api():
                # 최근 3개월 데이터만 API에서 가져오기
                recent_start = (dt.datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
                
                # 실업률 데이터로 빠른 체크
                series_data = get_fred_data(PHILLIPS_SERIES['unemployment_rate'], recent_start)
                if series_data is not None and len(series_data) > 0:
                    # 임시로 API 데이터를 저장
                    temp_api_data = PHILLIPS_DATA['processed_data'].copy()
                    
                    # 최신 API 데이터로 업데이트
                    for date in series_data.index:
                        if 'unemployment_rate' in temp_api_data.columns:
                            temp_api_data.loc[date, 'unemployment_rate'] = series_data[date]
                    
                    # 임시로 API 데이터를 전역 저장소에 저장
                    original_processed_data = PHILLIPS_DATA['processed_data'].copy()
                    PHILLIPS_DATA['processed_data'] = temp_api_data
                    
                    # 일치성 확인 (필립스 커브는 % 단위이므로 0.5%를 허용 오차로 설정)
                    consistency_result = check_recent_data_consistency(csv_data, tolerance=0.5)
                    
                    # 원본 데이터 복원
                    PHILLIPS_DATA['processed_data'] = original_processed_data
                    
                    print_consistency_results(consistency_result)
                    
                    needs_api_call = consistency_result['needs_update']
                    
                    if not needs_api_call:
                        print("✅ 최근 데이터가 일치함 - API 호출 건너뛰기")
                        # CSV 데이터를 그대로 사용
                        PHILLIPS_DATA['combined_data'] = PHILLIPS_DATA['processed_data']
                        
                        # 최신 값 저장
                        latest_values = {}
                        for col in PHILLIPS_DATA['processed_data'].columns:
                            if col not in ['period', 'detailed_period']:
                                latest_values[col] = {
                                    'value': PHILLIPS_DATA['processed_data'][col].iloc[-1],
                                    'date': PHILLIPS_DATA['processed_data'].index[-1].strftime('%Y-%m')
                                }
                        PHILLIPS_DATA['latest_values'] = latest_values
                        
                        PHILLIPS_DATA['load_info'] = {
                            'loaded': True,
                            'load_time': dt.datetime.now(),
                            'start_date': start_date,
                            'series_count': len(PHILLIPS_DATA['processed_data'].columns),
                            'data_points': len(PHILLIPS_DATA['processed_data']),
                            'source': 'CSV (스마트 업데이트)',
                            'consistency_check': consistency_result
                        }
                        
                        print("💾 CSV 데이터 사용 (일치성 확인됨)")
                        print_load_info()
                        return True
                    else:
                        print("📡 데이터 불일치 감지 - 전체 API 호출 진행")
    
    # API를 통한 전체 데이터 로드
    if needs_api_call:
        print("🚀 필립스 커브 데이터 로딩 시작... (FRED API)")
        print("="*60)
        
        # FRED API 초기화
        if not initialize_fred_api():
            print("❌ FRED API 초기화 실패 - 샘플 데이터 사용")
        
        # 데이터 수집 (실업률 + 선택된 인플레이션 시리즈)
        raw_data_dict = {}
        
        print("\n📊 필립스 커브 데이터 로딩...")
        
        # 실업률 로드
        series_data = get_fred_data(PHILLIPS_SERIES['unemployment_rate'], start_date)
        if series_data is not None and len(series_data) > 0:
            raw_data_dict['unemployment_rate'] = series_data
            
        # 선택된 인플레이션 시리즈들 로드
        for series_name in inflation_series:
            if series_name in PHILLIPS_SERIES:
                series_id = PHILLIPS_SERIES[series_name]
                series_data = get_fred_data(series_id, start_date)
                if series_data is not None and len(series_data) > 0:
                    raw_data_dict[series_name] = series_data
                else:
                    print(f"❌ 데이터 로드 실패: {series_name}")
        
        if len(raw_data_dict) < 2:  # 최소 실업률 + 인플레이션 1개는 있어야 함
            error_msg = f"❌ 로드된 시리즈가 너무 적습니다: {len(raw_data_dict)}개"
            print(error_msg)
            return False
        
        # 전역 저장소에 저장
        PHILLIPS_DATA['raw_data'] = pd.DataFrame(raw_data_dict)
        
        # 데이터 처리 (인플레이션 변화율 계산 등)
        process_phillips_data()
        
        PHILLIPS_DATA['load_info'] = {
            'loaded': True,
            'load_time': dt.datetime.now(),
            'start_date': start_date,
            'series_count': len(raw_data_dict),
            'data_points': len(PHILLIPS_DATA['processed_data']),
            'source': 'API (전체 로드)',
            'inflation_series': inflation_series
        }
        
        if consistency_result:
            PHILLIPS_DATA['load_info']['consistency_check'] = consistency_result
        
        # CSV에 저장
        save_data_to_csv()
        
        print("\n✅ 필립스 커브 데이터 로딩 완료!")
        print_load_info()
        
        return True
    
    return False

def print_load_info():
    """로드 정보 출력"""
    info = PHILLIPS_DATA['load_info']
    if not info['loaded']:
        print("❌ 데이터가 로드되지 않음")
        return
    
    print(f"\n📊 로드된 필립스 커브 데이터 정보:")
    print(f"   시리즈 개수: {info['series_count']}")
    print(f"   데이터 포인트: {info['data_points']}")
    print(f"   시작 날짜: {info['start_date']}")
    print(f"   로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   데이터 소스: {info.get('source', 'API')}")
    
    if 'inflation_series' in info:
        print(f"   인플레이션 시리즈: {', '.join(info['inflation_series'])}")
    
    if not PHILLIPS_DATA['combined_data'].empty:
        date_range = f"{PHILLIPS_DATA['combined_data'].index[0].strftime('%Y-%m')} ~ {PHILLIPS_DATA['combined_data'].index[-1].strftime('%Y-%m')}"
        print(f"   데이터 기간: {date_range}")
    
    if 'consistency_check' in info:
        consistency = info['consistency_check']
        print(f"   일치성 확인: {consistency.get('reason', 'N/A')}")

# %%
# === 시각화 함수들 ===

def create_phillips_curve(inflation_type='cpi_headline', color_by_period=True, show_labels=True):
    """
    필립스 커브 생성 (KPDS 포맷)
    
    Args:
        inflation_type: 사용할 인플레이션 지표 ('cpi_headline', 'cpi_core', 'pce_core' 등)
        color_by_period: 시기별 색상 구분 여부
        show_labels: 날짜 라벨 표시 여부
    
    Returns:
        plotly figure
    """
    if not PHILLIPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_phillips_data()를 실행하세요.")
        return None
    
    df = PHILLIPS_DATA['combined_data']
    
    if df.empty or 'unemployment_rate' not in df.columns or inflation_type not in df.columns:
        print(f"❌ 시각화할 데이터가 없습니다. 인플레이션 타입: {inflation_type}")
        print(f"   사용 가능한 컬럼: {df.columns.tolist()}")
        return None
    
    inflation_name = SERIES_KOREAN_NAMES.get(inflation_type, inflation_type)
    print(f"필립스 커브 ({inflation_name})")
    
    # 기본 설정
    fig = go.Figure()
    
    if color_by_period and 'period' in df.columns:
        # 시기별로 나누어 표시
        periods = df['period'].unique()
        colors = [deepblue_pds, deepred_pds] if KPDS_AVAILABLE else ['blue', 'red']
        
        for i, period in enumerate(periods):
            period_data = df[df['period'] == period].sort_index()
            
            # 산점도 + 라인
            fig.add_trace(go.Scatter(
                x=period_data['unemployment_rate'],
                y=period_data[inflation_type],
                mode='markers+lines',
                name=period,
                marker=dict(
                    size=6,
                    color=colors[i % len(colors)],
                    opacity=0.7
                ),
                line=dict(
                    color=colors[i % len(colors)],
                    width=2,
                    dash='solid' if i == 0 else 'dash'
                ),
                hovertemplate='<b>%{customdata}</b><br>' +
                            '실업률: %{x:.1f}%<br>' +
                            f'{inflation_name}: %{{y:.1f}}%<br>' +
                            '<extra></extra>',
                customdata=period_data.index.strftime('%Y-%m')
            ))
        
        # 화살표로 시간 흐름 표시 (최근 몇 개 점에 대해)
        if show_labels:
            recent_data = df.tail(12)  # 최근 1년
            for i in range(len(recent_data) - 1):
                if i % 3 == 0:  # 3개월마다 화살표 표시
                    fig.add_annotation(
                        x=recent_data.iloc[i+1]['unemployment_rate'],
                        y=recent_data.iloc[i+1][inflation_type],
                        ax=recent_data.iloc[i]['unemployment_rate'],
                        ay=recent_data.iloc[i][inflation_type],
                        xref='x', yref='y',
                        axref='x', ayref='y',
                        showarrow=True,
                        arrowhead=2,
                        arrowsize=1,
                        arrowwidth=1,
                        arrowcolor='gray',
                        opacity=0.6
                    )
    else:
        # 단일 색상으로 표시
        color = deepblue_pds if KPDS_AVAILABLE else 'blue'
        fig.add_trace(go.Scatter(
            x=df['unemployment_rate'],
            y=df[inflation_type],
            mode='markers+lines',
            name='필립스 커브',
            marker=dict(
                size=6,
                color=color,
                opacity=0.7
            ),
            line=dict(
                color=color,
                width=2
            ),
            hovertemplate='<b>%{customdata}</b><br>' +
                        '실업률: %{x:.1f}%<br>' +
                        f'{inflation_name}: %{{y:.1f}}%<br>' +
                        '<extra></extra>',
            customdata=df.index.strftime('%Y-%m')
        ))
    
    # KPDS 폰트 설정
    font_family = 'NanumGothic' if KPDS_AVAILABLE else 'Arial'
    font_size = FONT_SIZE_GENERAL if KPDS_AVAILABLE else 12
    axis_title_size = FONT_SIZE_AXIS_TITLE if KPDS_AVAILABLE else 14
    
    # 레이아웃 설정
    fig.update_layout(
        font=dict(family=font_family, size=font_size, color='black'),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=900,
        height=700,
        xaxis=dict(
            title=dict(text='실업률 (%)', font=dict(size=axis_title_size)),
            showline=True,
            linewidth=1.3,
            linecolor='lightgrey',
            tickwidth=1.3,
            tickcolor='lightgrey',
            ticks='outside',
            gridcolor='lightgray',
            gridwidth=0.8,
            showgrid=True
        ),
        yaxis=dict(
            title=dict(text=f'{inflation_name} (%)', font=dict(size=axis_title_size)),
            showline=True,
            linewidth=1.3,
            linecolor='lightgrey',
            tickwidth=1.3,
            tickcolor='lightgrey',
            ticks='outside',
            gridcolor='lightgray',
            gridwidth=0.8,
            showgrid=True
        ),
        legend=dict(
            x=1.02,
            y=1,
            xanchor='left',
            yanchor='top',
            bgcolor='rgba(255, 255, 255, 0.9)',
            bordercolor='lightgray',
            borderwidth=1
        ),
        margin=dict(l=80, r=150, t=100, b=80)  # 오른쪽 마진 증가 (범례 공간 확보)
    )
    
    # 주요 시점 라벨링
    if show_labels:
        key_dates = [
            ('2008-09', '금융위기'),
            ('2020-04', '코로나 정점'),
            (df.index.max().strftime('%Y-%m'), '최근')
        ]
        
        for date_str, label in key_dates:
            try:
                date_point = pd.to_datetime(date_str)
                point_data = df[df.index == date_point]
                if not point_data.empty:
                    fig.add_annotation(
                        x=point_data['unemployment_rate'].iloc[0],
                        y=point_data[inflation_type].iloc[0],
                        text=f'<b>{label}</b><br>{date_str}',
                        showarrow=True,
                        arrowhead=2,
                        arrowsize=1,
                        arrowwidth=2,
                        arrowcolor='black',
                        ax=20,
                        ay=-30,
                        font=dict(size=font_size-2, color='black'),
                        bgcolor='rgba(255, 255, 255, 0.8)',
                        bordercolor='black',
                        borderwidth=1
                    )
            except:
                continue
    
    return fig

def create_phillips_curve_detailed(inflation_type='cpi_headline', show_labels=True):
    """
    세부 기간별 필립스 커브 생성 (KPDS 포맷)
    
    Args:
        inflation_type: 사용할 인플레이션 지표
        show_labels: 날짜 라벨 표시 여부
    
    Returns:
        plotly figure
    """
    if not PHILLIPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_phillips_data()를 실행하세요.")
        return None
    
    df = PHILLIPS_DATA['combined_data']
    
    if df.empty or 'unemployment_rate' not in df.columns or inflation_type not in df.columns:
        print(f"❌ 시각화할 데이터가 없습니다. 인플레이션 타입: {inflation_type}")
        return None
    
    if 'detailed_period' not in df.columns:
        print("⚠️ 세부 기간 정보가 없습니다. 먼저 데이터를 다시 로드하세요.")
        return None
    
    inflation_name = SERIES_KOREAN_NAMES.get(inflation_type, inflation_type)
    print(f"필립스 커브 (세부 기간별 구분, {inflation_name})")
    
    # 기본 설정
    fig = go.Figure()
    
    # 세부 기간별로 나누어 표시
    detailed_periods = df['detailed_period'].unique()
    
    # 더 다양한 색상 팔레트 설정
    if KPDS_AVAILABLE:
        colors = [deepblue_pds, deepred_pds, beige_pds, '#2E8B57', '#FF8C00', '#9932CC']
    else:
        colors = ['blue', 'red', 'green', 'orange', 'purple', 'brown']
    
    line_styles = ['solid', 'dash', 'dot', 'dashdot', 'longdash', 'longdashdot']
    
    # 시간 순서대로 정렬
    period_order = [
        '2000년대 초반',
        '금융위기 시기 (2008-2009)', 
        '경기회복 시기 (2010-2019)',
        '코로나 초기 (2020)',
        '코로나 회복 (2021-2022)',
        '최근 시기 (2023-)'
    ]
    
    # 실제 존재하는 기간만 필터링
    existing_periods = [p for p in period_order if p in detailed_periods]
    
    for i, period in enumerate(existing_periods):
        period_data = df[df['detailed_period'] == period].sort_index()
        
        if len(period_data) == 0:
            continue
            
        # 산점도 + 라인
        fig.add_trace(go.Scatter(
            x=period_data['unemployment_rate'],
            y=period_data[inflation_type],
            mode='markers+lines',
            name=period,
            marker=dict(
                size=6,
                color=colors[i % len(colors)],
                opacity=0.8
            ),
            line=dict(
                color=colors[i % len(colors)],
                width=2,
                dash=line_styles[i % len(line_styles)]
            ),
            hovertemplate='<b>%{customdata}</b><br>' +
                        f'{period}<br>' +
                        '실업률: %{x:.1f}%<br>' +
                        f'{inflation_name}: %{{y:.1f}}%<br>' +
                        '<extra></extra>',
            customdata=period_data.index.strftime('%Y-%m')
        ))
    
    # KPDS 폰트 설정
    font_family = 'NanumGothic' if KPDS_AVAILABLE else 'Arial'
    font_size = FONT_SIZE_GENERAL if KPDS_AVAILABLE else 12
    axis_title_size = FONT_SIZE_AXIS_TITLE if KPDS_AVAILABLE else 14
    
    # 레이아웃 설정
    fig.update_layout(
        font=dict(family=font_family, size=font_size, color='black'),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=1000,
        height=700,
        xaxis=dict(
            title=dict(text='실업률 (%)', font=dict(size=axis_title_size)),
            showline=True,
            linewidth=1.3,
            linecolor='lightgrey',
            tickwidth=1.3,
            tickcolor='lightgrey',
            ticks='outside',
            gridcolor='lightgray',
            gridwidth=0.8,
            showgrid=True
        ),
        yaxis=dict(
            title=dict(text=f'{inflation_name} (%)', font=dict(size=axis_title_size)),
            showline=True,
            linewidth=1.3,
            linecolor='lightgrey',
            tickwidth=1.3,
            tickcolor='lightgrey',
            ticks='outside',
            gridcolor='lightgray',
            gridwidth=0.8,
            showgrid=True
        ),
        legend=dict(
            x=1.02,
            y=1,
            xanchor='left',
            yanchor='top',
            bgcolor='rgba(255, 255, 255, 0.9)',
            bordercolor='lightgray',
            borderwidth=1
        ),
        margin=dict(l=80, r=150, t=100, b=80)  # 오른쪽 마진 증가 (범례 공간 확보)
    )
    
    # 주요 시점 라벨링
    if show_labels:
        key_dates = [
            ('2008-09', '금융위기'),
            ('2020-04', '코로나 정점'),
            (df.index.max().strftime('%Y-%m'), '최근')
        ]
        
        for date_str, label in key_dates:
            try:
                date_point = pd.to_datetime(date_str)
                point_data = df[df.index == date_point]
                if not point_data.empty:
                    fig.add_annotation(
                        x=point_data['unemployment_rate'].iloc[0],
                        y=point_data[inflation_type].iloc[0],
                        text=f'<b>{label}</b><br>{date_str}',
                        showarrow=True,
                        arrowhead=2,
                        arrowsize=1,
                        arrowwidth=2,
                        arrowcolor='black',
                        ax=20,
                        ay=-30,
                        font=dict(size=font_size-2, color='black'),
                        bgcolor='rgba(255, 255, 255, 0.8)',
                        bordercolor='black',
                        borderwidth=1
                    )
            except:
                continue
    
    return fig

def create_phillips_comparison(inflation_types=['cpi_headline', 'cpi_core', 'pce_core']):
    """
    여러 인플레이션 지표를 사용한 필립스 커브 비교
    
    Args:
        inflation_types: 비교할 인플레이션 지표들의 리스트
    
    Returns:
        plotly figure
    """
    if not PHILLIPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_phillips_data()를 실행하세요.")
        return None
    
    df = PHILLIPS_DATA['combined_data']
    
    print("필립스 커브 (인플레이션 지표별 비교)")
    
    # 서브플롯 생성
    n_plots = len(inflation_types)
    cols = min(2, n_plots)
    rows = (n_plots + 1) // 2
    
    fig = make_subplots(
        rows=rows, cols=cols,
        subplot_titles=[SERIES_KOREAN_NAMES.get(inf, inf) for inf in inflation_types],
        horizontal_spacing=0.12,
        vertical_spacing=0.15
    )
    
    colors = [deepblue_pds, deepred_pds] if KPDS_AVAILABLE else ['blue', 'red']
    
    for idx, inflation_type in enumerate(inflation_types):
        if inflation_type not in df.columns:
            continue
            
        row = (idx // cols) + 1
        col = (idx % cols) + 1
        
        # 시기별로 나누어 표시
        if 'period' in df.columns:
            periods = df['period'].unique()
            
            for i, period in enumerate(periods):
                period_data = df[df['period'] == period].sort_index()
                
                show_legend = (idx == 0)  # 첫 번째 플롯에만 범례 표시
                
                fig.add_trace(
                    go.Scatter(
                        x=period_data['unemployment_rate'],
                        y=period_data[inflation_type],
                        mode='markers+lines',
                        name=period,
                        marker=dict(size=4, color=colors[i], opacity=0.7),
                        line=dict(color=colors[i], width=1.5),
                        showlegend=show_legend,
                        hovertemplate=f'<b>%{{customdata}}</b><br>' +
                                    '실업률: %{x:.1f}%<br>' +
                                    f'{SERIES_KOREAN_NAMES.get(inflation_type, inflation_type)}: %{{y:.1f}}%<br>' +
                                    '<extra></extra>',
                        customdata=period_data.index.strftime('%Y-%m')
                    ),
                    row=row, col=col
                )
    
    # 레이아웃 업데이트
    font_family = 'NanumGothic' if KPDS_AVAILABLE else 'Arial'
    font_size = FONT_SIZE_GENERAL-1 if KPDS_AVAILABLE else 11
    
    fig.update_layout(
        height=400*rows,
        width=900,
        font=dict(family=font_family, size=font_size),
        paper_bgcolor='white',
        plot_bgcolor='white',
        showlegend=True,
        legend=dict(x=0.02, y=0.98)
    )
    
    # 축 라벨 설정
    for i in range(1, rows+1):
        for j in range(1, cols+1):
            fig.update_xaxes(title_text="실업률 (%)", row=i, col=j)
            fig.update_yaxes(title_text="인플레이션율 (%)", row=i, col=j)
    
    return fig

# %%
# === 분석 함수들 ===

def analyze_phillips_trends(inflation_type='cpi_headline'):
    """
    필립스 커브 트렌드 분석
    
    Args:
        inflation_type: 분석할 인플레이션 지표
    """
    if not PHILLIPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_phillips_data()를 실행하세요.")
        return None
    
    df = PHILLIPS_DATA['combined_data']
    
    if inflation_type not in df.columns:
        print(f"❌ {inflation_type} 데이터가 없습니다.")
        return None
        
    latest_date = df.index[-1]
    inflation_name = SERIES_KOREAN_NAMES.get(inflation_type, inflation_type)
    
    print(f"\n📊 필립스 커브 트렌드 분석 ({inflation_name}, {latest_date.strftime('%Y년 %m월')})")
    print("="*60)
    
    # 1. 현재 상황
    latest_unemployment = PHILLIPS_DATA['latest_values']['unemployment_rate']['value']
    latest_inflation = PHILLIPS_DATA['latest_values'][inflation_type]['value']
    
    print(f"\n1. 현재 상황:")
    print(f"   실업률: {latest_unemployment:.1f}%")
    print(f"   {inflation_name}: {latest_inflation:.1f}%")
    
    # 2. 시기별 평균 분석
    if 'period' in df.columns:
        print(f"\n2. 시기별 평균 비교:")
        
        for period in df['period'].unique():
            period_data = df[df['period'] == period]
            avg_unemployment = period_data['unemployment_rate'].mean()
            avg_inflation = period_data[inflation_type].mean()
            
            print(f"   {period}:")
            print(f"     평균 실업률: {avg_unemployment:.1f}%")
            print(f"     평균 {inflation_name}: {avg_inflation:.1f}%")
    
    # 3. 최근 트렌드 (6개월)
    recent_data = df.tail(6)
    unemployment_trend = recent_data['unemployment_rate'].iloc[-1] - recent_data['unemployment_rate'].iloc[0]
    inflation_trend = recent_data[inflation_type].iloc[-1] - recent_data[inflation_type].iloc[0]
    
    print(f"\n3. 최근 6개월 트렌드:")
    print(f"   실업률 변화: {unemployment_trend:+.1f}%p")
    print(f"   {inflation_name} 변화: {inflation_trend:+.1f}%p")
    
    # 4. 필립스 커브 관계 분석
    print(f"\n4. 필립스 커브 관계 분석:")
    
    # 상관관계 계산
    correlation = df['unemployment_rate'].corr(df[inflation_type])
    print(f"   실업률-인플레이션 상관계수: {correlation:.3f}")
    
    if correlation < -0.3:
        print("   📉 전통적 필립스 커브 관계 (음의 상관관계)")
    elif correlation > 0.3:
        print("   📈 역방향 필립스 커브 관계 (양의 상관관계)")
    else:
        print("   ➡️ 불분명한 관계")
    
    # 5. 스태그플레이션 분석
    stagflation_periods = df[(df['unemployment_rate'] > df['unemployment_rate'].median()) & 
                           (df[inflation_type] > df[inflation_type].median())]
    
    if len(stagflation_periods) > 0:
        print(f"\n5. 스태그플레이션 분석:")
        print(f"   스태그플레이션 기간: {len(stagflation_periods)}개월")
        print(f"   전체 기간 대비: {len(stagflation_periods)/len(df)*100:.1f}%")
        
        if len(stagflation_periods) > 0:
            recent_stagflation = stagflation_periods.index.max()
            print(f"   최근 스태그플레이션: {recent_stagflation.strftime('%Y-%m')}")
    
    return {
        'latest_date': latest_date,
        'latest_unemployment': latest_unemployment,
        'latest_inflation': latest_inflation,
        'unemployment_trend': unemployment_trend,
        'inflation_trend': inflation_trend,
        'correlation': correlation
    }

# %%
# === 통합 실행 함수 ===

def run_phillips_analysis(start_date='2000-01-01', force_reload=False, smart_update=True,
                         inflation_series=['cpi_headline', 'cpi_core', 'pce_core'],
                         main_inflation='cpi_headline'):
    """
    완전한 필립스 커브 분석 실행 (스마트 업데이트 지원)
    
    Args:
        start_date: 데이터 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부 (기본값: True)
        inflation_series: 로드할 인플레이션 시리즈들
        main_inflation: 주요 분석에 사용할 인플레이션 지표
    
    Returns:
        dict: 생성된 차트들과 분석 결과
    """
    print("🚀 필립스 커브 분석 시작")
    print("="*60)
    
    # 1. 데이터 로드
    print("\n1️⃣ 데이터 로딩")
    success = load_phillips_data(start_date=start_date, force_reload=force_reload, 
                               smart_update=smart_update, inflation_series=inflation_series)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    # 2. 분석 실행
    print("\n2️⃣ 트렌드 분석")
    analysis_results = analyze_phillips_trends(inflation_type=main_inflation)
    
    # 3. 시각화 생성
    print("\n3️⃣ 시각화 생성")
    
    results = {
        'analysis': analysis_results,
        'charts': {}
    }
    
    try:
        # 필립스 커브 (시기별)
        print(f"   📈 필립스 커브 (시기별, {main_inflation})...")
        results['charts']['phillips_curve'] = create_phillips_curve(inflation_type=main_inflation, 
                                                                   color_by_period=True, show_labels=True)
        
        # 필립스 커브 (세부 기간별)
        print(f"   📈 필립스 커브 (세부 기간별, {main_inflation})...")
        results['charts']['phillips_curve_detailed'] = create_phillips_curve_detailed(inflation_type=main_inflation, 
                                                                                     show_labels=True)
        
        # 인플레이션 지표별 비교
        if len(inflation_series) > 1:
            print("   📊 인플레이션 지표별 비교...")
            available_series = [s for s in inflation_series if s in PHILLIPS_DATA['combined_data'].columns]
            if available_series:
                results['charts']['inflation_comparison'] = create_phillips_comparison(inflation_types=available_series)
        
    except Exception as e:
        print(f"⚠️ 시각화 생성 중 오류: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n✅ 필립스 커브 분석 완료! 생성된 차트: {len(results['charts'])}개")
    
    return results

# %%
# === 사용 예시 및 안내 ===

print("\n=== 필립스 커브 분석 도구 사용법 (KPDS 포맷) ===")
print("1. 데이터 로드:")
print("   load_phillips_data()  # 기본 인플레이션 지표들 로드")
print("   load_phillips_data(inflation_series=['cpi_headline', 'pce_core'])  # 특정 지표 선택")
print()
print("2. 개별 차트 생성:")
print("   create_phillips_curve()                    # 필립스 커브 (기본)")
print("   create_phillips_curve_detailed()          # 필립스 커브 (세부 기간별)")
print("   create_phillips_comparison()               # 인플레이션 지표별 비교")
print()
print("3. 분석:")
print("   analyze_phillips_trends()                  # 트렌드 분석")
print()
print("4. 통합 실행:")
print("   run_phillips_analysis()                    # 전체 분석 및 시각화")
print("   run_phillips_analysis(main_inflation='pce_core')  # PCE 코어 기준 분석")
print()
print("5. 스마트 업데이트 기능:")
print("   - 데이터 일치성 자동 확인 (허용 오차: 0.5%)")
print("   - CSV 기반 로컬 캐싱")
print("   - 불필요한 API 호출 최소화")
print()
if KPDS_AVAILABLE:
    print("✅ KPDS 시각화 포맷이 활성화되었습니다.")
else:
    print("⚠️ KPDS 시각화 포맷을 사용할 수 없습니다. 기본 포맷을 사용합니다.")
print()
print(f"📊 사용 가능한 인플레이션 지표:")
for series_name, series_id in PHILLIPS_SERIES.items():
    if series_name != 'unemployment_rate':
        korean_name = SERIES_KOREAN_NAMES.get(series_name, series_name)
        conversion_note = " (지수→변화율 변환)" if series_name in INDEX_SERIES else " (이미 변화율)"
        print(f"  '{series_name}': {korean_name} ({series_id}){conversion_note}")

# %%
# 자동 실행
print("\n🔄 필립스 커브 분석 자동 실행 중...")
run_phillips_analysis(smart_update=True)

# %%
# 개별 차트 테스트
print("필립스 커브 (기본 버전)")
create_phillips_curve()

# %%
print("필립스 커브 (세부 기간별 버전)")  
create_phillips_curve_detailed()

# %%