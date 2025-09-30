# %%
"""
FRED API 전용 베버리지 커브 분석 도구 (Enhanced Version)
- FRED API를 사용하여 구인율(JTSJOR)과 실업률(UNRATE) 데이터 수집  
- 스마트 업데이트 시스템: CSV 캐싱 + 일치성 확인
- 코로나 전후 시기별 베버리지 커브 시각화
- 노동시장 효율성 및 구조적 변화 분석
- KPDS 시각화 포맷 적용
"""
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
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
# === 베버리지 커브 시리즈 정의 ===

# FRED 시리즈 ID
BEVERIDGE_SERIES = {
    'job_openings_rate': 'JTSJOR',    # Job Openings: Total Nonfarm Rate
    'unemployment_rate': 'UNRATE'     # Unemployment Rate
}

# 한국어 이름 매핑
SERIES_KOREAN_NAMES = {
    'job_openings_rate': '구인율',
    'unemployment_rate': '실업률'
}

# %%
# === 전역 데이터 저장소 ===

# FRED 세션
FRED_SESSION = None

# CSV 파일 경로
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/beveridge_curve_data.csv'

# 전역 데이터 저장소
BEVERIDGE_DATA = {
    'raw_data': pd.DataFrame(),      # 원본 데이터
    'combined_data': pd.DataFrame(), # 결합 데이터 (코로나 전후 구분 포함)
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
    if BEVERIDGE_DATA['raw_data'].empty:
        print("⚠️ 저장할 데이터가 없습니다.")
        return False
    
    ensure_data_directory()
    
    try:
        # 날짜를 컬럼으로 추가하여 저장
        df_to_save = BEVERIDGE_DATA['raw_data'].copy()
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
    최근 데이터의 일치성 확인 (베버리지 커브 데이터용 - % 단위)
    
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
        if BEVERIDGE_DATA['raw_data'].empty:
            return {'needs_update': True, 'reason': 'API 데이터 없음'}
        
        api_data = BEVERIDGE_DATA['raw_data']
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
                    diff = abs(csv_val - api_val)
                    
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
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    
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
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    
    date_range = pd.date_range(start=start_date, end=end_date, freq='MS')
    np.random.seed(42)
    
    if series_id == 'JTSJOR':  # 구인율 (Job Openings Rate)
        base_values = []
        for date in date_range:
            if date.year < 2020:
                # 경기순환에 따른 변동
                cycle = np.sin((date.year - 2000) * 0.8) * 0.8
                base = 3.0 + cycle + np.random.normal(0, 0.3)
                base_values.append(max(1.5, min(4.5, base)))
            elif date.year == 2020:
                if date.month < 4:
                    base_values.append(3.5 + np.random.normal(0, 0.1))
                elif date.month < 8:
                    base_values.append(1.8 + np.random.normal(0, 0.2))
                else:
                    base_values.append(2.5 + (date.month - 8) * 0.3 + np.random.normal(0, 0.1))
            else:
                base = 4.2 + np.random.normal(0, 0.4)
                base_values.append(max(3.5, min(5.5, base)))
        
        return pd.Series(base_values, index=date_range, name=series_id)
        
    elif series_id == 'UNRATE':  # 실업률 (Unemployment Rate)
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
    
    else:
        print(f"⚠️ 지원하지 않는 시리즈 ID: {series_id}")
        return pd.Series()

# %%
# === 데이터 로드 함수들 ===

def load_beveridge_data(start_date='2000-01-01', force_reload=False, smart_update=True):
    """
    베버리지 커브 데이터 로드 (스마트 업데이트 지원)
    
    Args:
        start_date: 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부 (기본값: True)
    
    Returns:
        bool: 로드 성공 여부
    """
    global BEVERIDGE_DATA
    
    # 이미 로드된 경우 스킵 (강제 재로드가 아닌 경우)
    if BEVERIDGE_DATA['load_info']['loaded'] and not force_reload and not smart_update:
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
            BEVERIDGE_DATA['raw_data'] = csv_data
            
            # 최신 몇 개 데이터만 API로 가져와서 비교
            print("🔍 최근 데이터 일치성 확인 중...")
            
            # FRED API 초기화
            if initialize_fred_api():
                # 최근 3개월 데이터만 API에서 가져오기
                recent_start = (datetime.datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
                
                # 실업률 데이터로 빠른 체크
                series_data = get_fred_data(BEVERIDGE_SERIES['unemployment_rate'], recent_start)
                if series_data is not None and len(series_data) > 0:
                    # 임시로 API 데이터를 저장
                    temp_api_data = BEVERIDGE_DATA['raw_data'].copy()
                    
                    # 최신 API 데이터로 업데이트
                    for date in series_data.index:
                        if 'unemployment_rate' in temp_api_data.columns:
                            temp_api_data.loc[date, 'unemployment_rate'] = series_data[date]
                    
                    # 임시로 API 데이터를 전역 저장소에 저장
                    original_raw_data = BEVERIDGE_DATA['raw_data'].copy()
                    BEVERIDGE_DATA['raw_data'] = temp_api_data
                    
                    # 일치성 확인 (베버리지 커브는 % 단위이므로 0.5%를 허용 오차로 설정)
                    consistency_result = check_recent_data_consistency(csv_data, tolerance=0.5)
                    
                    # 원본 데이터 복원
                    BEVERIDGE_DATA['raw_data'] = original_raw_data
                    
                    print_consistency_results(consistency_result)
                    
                    needs_api_call = consistency_result['needs_update']
                    
                    if not needs_api_call:
                        print("✅ 최근 데이터가 일치함 - API 호출 건너뛰기")
                        # CSV 데이터를 그대로 사용
                        process_beveridge_data()
                        
                        BEVERIDGE_DATA['load_info'] = {
                            'loaded': True,
                            'load_time': datetime.datetime.now(),
                            'start_date': start_date,
                            'series_count': len(BEVERIDGE_DATA['raw_data'].columns),
                            'data_points': len(BEVERIDGE_DATA['raw_data']),
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
        print("🚀 베버리지 커브 데이터 로딩 시작... (FRED API)")
        print("="*60)
        
        # FRED API 초기화
        if not initialize_fred_api():
            print("❌ FRED API 초기화 실패 - 샘플 데이터 사용")
        
        # 데이터 수집
        raw_data_dict = {}
        
        print("\n📊 베버리지 커브 데이터 로딩...")
        for series_name, series_id in BEVERIDGE_SERIES.items():
            series_data = get_fred_data(series_id, start_date)
            if series_data is not None and len(series_data) > 0:
                raw_data_dict[series_name] = series_data
            else:
                print(f"❌ 데이터 로드 실패: {series_name}")
        
        if len(raw_data_dict) < 2:  # 최소 2개 시리즈는 있어야 함
            error_msg = f"❌ 로드된 시리즈가 너무 적습니다: {len(raw_data_dict)}개"
            print(error_msg)
            return False
        
        # 전역 저장소에 저장
        BEVERIDGE_DATA['raw_data'] = pd.DataFrame(raw_data_dict)
        
        # 데이터 처리
        process_beveridge_data()
        
        BEVERIDGE_DATA['load_info'] = {
            'loaded': True,
            'load_time': datetime.datetime.now(),
            'start_date': start_date,
            'series_count': len(raw_data_dict),
            'data_points': len(BEVERIDGE_DATA['raw_data']),
            'source': 'API (전체 로드)'
        }
        
        if consistency_result:
            BEVERIDGE_DATA['load_info']['consistency_check'] = consistency_result
        
        # CSV에 저장
        save_data_to_csv()
        
        print("\n✅ 베버리지 커브 데이터 로딩 완료!")
        print_load_info()
        
        return True
    
    return False

def process_beveridge_data():
    """베버리지 커브 데이터 후처리"""
    if BEVERIDGE_DATA['raw_data'].empty:
        return
    
    # 공통 기간으로 정렬
    df = BEVERIDGE_DATA['raw_data'].dropna()
    
    # 코로나 전후 구분을 위한 컬럼 추가
    df = df.copy()
    df['period'] = df.index.to_series().apply(
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
    
    df['detailed_period'] = df.index.to_series().apply(classify_detailed_period)
    
    # 결합된 데이터 저장
    BEVERIDGE_DATA['combined_data'] = df
    
    # 최신 값 저장
    latest_values = {}
    for col in ['job_openings_rate', 'unemployment_rate']:
        if col in df.columns:
            latest_values[col] = {
                'value': df[col].iloc[-1],
                'date': df.index[-1].strftime('%Y-%m')
            }
    
    BEVERIDGE_DATA['latest_values'] = latest_values

def print_load_info():
    """로드 정보 출력"""
    info = BEVERIDGE_DATA['load_info']
    if not info['loaded']:
        print("❌ 데이터가 로드되지 않음")
        return
    
    print(f"\n📊 로드된 베버리지 커브 데이터 정보:")
    print(f"   시리즈 개수: {info['series_count']}")
    print(f"   데이터 포인트: {info['data_points']}")
    print(f"   시작 날짜: {info['start_date']}")
    print(f"   로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   데이터 소스: {info.get('source', 'API')}")
    
    if not BEVERIDGE_DATA['combined_data'].empty:
        date_range = f"{BEVERIDGE_DATA['combined_data'].index[0].strftime('%Y-%m')} ~ {BEVERIDGE_DATA['combined_data'].index[-1].strftime('%Y-%m')}"
        print(f"   데이터 기간: {date_range}")
    
    if 'consistency_check' in info:
        consistency = info['consistency_check']
        print(f"   일치성 확인: {consistency.get('reason', 'N/A')}")

# %%
# === 시각화 함수들 ===

def create_beveridge_curve(color_by_period=True, show_labels=True):
    """
    베버리지 커브 생성 (KPDS 포맷)
    
    Args:
        color_by_period: 시기별 색상 구분 여부
        show_labels: 날짜 라벨 표시 여부
    
    Returns:
        plotly figure
    """
    if not BEVERIDGE_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_beveridge_data()를 실행하세요.")
        return None
    
    df = BEVERIDGE_DATA['combined_data']
    
    if df.empty or 'job_openings_rate' not in df.columns or 'unemployment_rate' not in df.columns:
        print("❌ 시각화할 데이터가 없습니다.")
        return None
    
    print("베버리지 커브 (시기별 구분)")
    
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
                y=period_data['job_openings_rate'],
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
                            '구인율: %{y:.1f}%<br>' +
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
                        y=recent_data.iloc[i+1]['job_openings_rate'],
                        ax=recent_data.iloc[i]['unemployment_rate'],
                        ay=recent_data.iloc[i]['job_openings_rate'],
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
            y=df['job_openings_rate'],
            mode='markers+lines',
            name='베버리지 커브',
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
                        '구인율: %{y:.1f}%<br>' +
                        '<extra></extra>',
            customdata=df.index.strftime('%Y-%m')
        ))
    
    # u=v 라인 추가 (45도 선, threshold)
    u_max = df['unemployment_rate'].max()
    v_max = df['job_openings_rate'].max()
    line_max = max(u_max, v_max) * 1.1  # 약간의 여유 공간
    
    fig.add_trace(go.Scatter(
        x=[0, line_max],
        y=[0, line_max],
        mode='lines',
        name='u = v (균형선)',
        line=dict(
            color='gray',
            width=1.5,
            dash='dash'
        ),
        hovertemplate='균형선 (u = v)<br>' +
                    '실업률 = 구인율<br>' +
                    '<extra></extra>',
        showlegend=True
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
            title=dict(text='구인율 (%)', font=dict(size=axis_title_size)),
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
                        y=point_data['job_openings_rate'].iloc[0],
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

def create_beveridge_curve_detailed(show_labels=True):
    """
    세부 기간별 베버리지 커브 생성 (KPDS 포맷)
    
    Args:
        show_labels: 날짜 라벨 표시 여부
    
    Returns:
        plotly figure
    """
    if not BEVERIDGE_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_beveridge_data()를 실행하세요.")
        return None
    
    df = BEVERIDGE_DATA['combined_data']
    
    if df.empty or 'job_openings_rate' not in df.columns or 'unemployment_rate' not in df.columns:
        print("❌ 시각화할 데이터가 없습니다.")
        return None
    
    if 'detailed_period' not in df.columns:
        print("⚠️ 세부 기간 정보가 없습니다. 먼저 데이터를 다시 로드하세요.")
        return None
    
    print("베버리지 커브 (세부 기간별 구분)")
    
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
            y=period_data['job_openings_rate'],
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
                        '구인율: %{y:.1f}%<br>' +
                        '<extra></extra>',
            customdata=period_data.index.strftime('%Y-%m')
        ))
    
    # u=v 라인 추가 (45도 선, threshold)
    u_max = df['unemployment_rate'].max()
    v_max = df['job_openings_rate'].max()
    line_max = max(u_max, v_max) * 1.1  # 약간의 여유 공간
    
    fig.add_trace(go.Scatter(
        x=[0, line_max],
        y=[0, line_max],
        mode='lines',
        name='u = v (균형선)',
        line=dict(
            color='gray',
            width=1.5,
            dash='dash'
        ),
        hovertemplate='균형선 (u = v)<br>' +
                    '실업률 = 구인율<br>' +
                    '<extra></extra>',
        showlegend=True
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
            title=dict(text='구인율 (%)', font=dict(size=axis_title_size)),
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
                        y=point_data['job_openings_rate'].iloc[0],
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

def create_timeseries_comparison():
    """
    시계열 비교 차트 생성 (KPDS 포맷)
    
    Returns:
        plotly figure
    """
    if not BEVERIDGE_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_beveridge_data()를 실행하세요.")
        return None
    
    df = BEVERIDGE_DATA['combined_data']
    
    if df.empty:
        print("❌ 시각화할 데이터가 없습니다.")
        return None
    
    print("시계열: 실업률 vs 구인율")
    
    if KPDS_AVAILABLE:
        # KPDS 포맷 사용
        df_chart = df[['unemployment_rate', 'job_openings_rate']].copy()
        labels = {
            'unemployment_rate': '실업률',
            'job_openings_rate': '구인율'  
        }
        
        fig = df_dual_axis_chart(
            df_chart,
            left_cols=['unemployment_rate'],
            right_cols=['job_openings_rate'], 
            left_labels=['실업률'],
            right_labels=['구인율'],
            left_title='%',
            right_title='%'
        )
        
    else:
        # 기본 plotly 사용
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # 실업률 (왼쪽 축)
        fig.add_trace(
            go.Scatter(
                x=df.index, 
                y=df['unemployment_rate'],
                name='실업률',
                line=dict(color='red', width=2)
            ),
            secondary_y=False,
        )
        
        # 구인율 (오른쪽 축)
        fig.add_trace(
            go.Scatter(
                x=df.index, 
                y=df['job_openings_rate'],
                name='구인율',
                line=dict(color='blue', width=2)
            ),
            secondary_y=True,
        )
        
        # 축 라벨 설정
        fig.update_xaxes(title_text="연도")
        fig.update_yaxes(title_text="실업률 (%)", secondary_y=False)
        fig.update_yaxes(title_text="구인율 (%)", secondary_y=True)
        
        # 레이아웃
        fig.update_layout(
            width=900, height=600,
            paper_bgcolor='white',
            plot_bgcolor='white'
        )
        
        # 코로나 시점 표시
        covid_date = pd.Timestamp('2020-03-01')
        fig.add_vline(x=covid_date, line_dash="dash", line_color="gray")
    
    return fig

def create_beveridge_dashboard():
    """
    베버리지 커브 종합 대시보드 생성
    
    Returns:
        plotly figure
    """
    if not BEVERIDGE_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_beveridge_data()를 실행하세요.")
        return None
    
    print("베버리지 커브 분석 대시보드")
    
    df = BEVERIDGE_DATA['combined_data']
    
    # 2x2 서브플롯 생성
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('베버리지 커브 (전체)', '시계열: 실업률 vs 구인율', 
                       '코로나 전후 비교', '최근 3년 동향'),
        specs=[[{"type": "scatter"}, {"type": "scatter"}],
               [{"type": "scatter"}, {"type": "scatter"}]],
        horizontal_spacing=0.1,
        vertical_spacing=0.12
    )
    
    colors = [deepblue_pds, deepred_pds] if KPDS_AVAILABLE else ['blue', 'red']
    
    # 1. 전체 베버리지 커브 (좌상)
    if 'period' in df.columns:
        periods = df['period'].unique()
        for i, period in enumerate(periods):
            period_data = df[df['period'] == period].sort_index()
            fig.add_trace(
                go.Scatter(
                    x=period_data['unemployment_rate'],
                    y=period_data['job_openings_rate'],
                    mode='markers+lines',
                    name=period,
                    marker=dict(size=4, color=colors[i], opacity=0.7),
                    line=dict(color=colors[i], width=1.5),
                    showlegend=True
                ),
                row=1, col=1
            )
    
    # 2. 시계열 비교 (우상)
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df['unemployment_rate'],
            name='실업률',
            line=dict(color=colors[1], width=2),
            yaxis='y2'
        ),
        row=1, col=2
    )
    
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df['job_openings_rate'],
            name='구인율',
            line=dict(color=colors[0], width=2),
            yaxis='y3'
        ),
        row=1, col=2
    )
    
    # 3. 코로나 전후 분리 (좌하)
    if 'period' in df.columns:
        pre_covid = df[df['period'] == 'Pre-COVID']
        post_covid = df[df['period'] == 'COVID & Post-COVID']
        
        fig.add_trace(
            go.Scatter(
                x=pre_covid['unemployment_rate'],
                y=pre_covid['job_openings_rate'],
                mode='markers',
                name='코로나 이전',
                marker=dict(size=5, color=colors[0], opacity=0.6),
                showlegend=False
            ),
            row=2, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=post_covid['unemployment_rate'],
                y=post_covid['job_openings_rate'],
                mode='markers',
                name='코로나 이후',
                marker=dict(size=5, color=colors[1], opacity=0.6),
                showlegend=False
            ),
            row=2, col=1
        )
    
    # 4. 최근 3년 동향 (우하)
    recent_data = df[df.index >= df.index.max() - pd.DateOffset(years=3)].sort_index()
    fig.add_trace(
        go.Scatter(
            x=recent_data['unemployment_rate'],
            y=recent_data['job_openings_rate'],
            mode='markers+lines',
            name='최근 3년',
            marker=dict(size=4, color=beige_pds if KPDS_AVAILABLE else 'orange'),
            line=dict(color=beige_pds if KPDS_AVAILABLE else 'orange', width=2),
            showlegend=False
        ),
        row=2, col=2
    )
    
    # 레이아웃 업데이트
    font_family = 'NanumGothic' if KPDS_AVAILABLE else 'Arial'
    font_size = FONT_SIZE_GENERAL-1 if KPDS_AVAILABLE else 11
    
    fig.update_layout(
        height=900,
        width=1200,
        font=dict(family=font_family, size=font_size),
        paper_bgcolor='white',
        plot_bgcolor='white',
        showlegend=True,
        legend=dict(x=0.02, y=0.98)
    )
    
    # 축 라벨 설정
    fig.update_xaxes(title_text="실업률 (%)", row=1, col=1)
    fig.update_yaxes(title_text="구인율 (%)", row=1, col=1)
    
    fig.update_xaxes(title_text="연도", row=1, col=2)
    fig.update_yaxes(title_text="비율 (%)", row=1, col=2)
    
    fig.update_xaxes(title_text="실업률 (%)", row=2, col=1)
    fig.update_yaxes(title_text="구인율 (%)", row=2, col=1)
    
    fig.update_xaxes(title_text="실업률 (%)", row=2, col=2)
    fig.update_yaxes(title_text="구인율 (%)", row=2, col=2)
    
    return fig

# %%
# === 분석 함수들 ===

def analyze_beveridge_trends():
    """
    베버리지 커브 트렌드 분석
    """
    if not BEVERIDGE_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_beveridge_data()를 실행하세요.")
        return None
    
    df = BEVERIDGE_DATA['combined_data']
    latest_date = df.index[-1]
    
    print(f"\n📊 베버리지 커브 트렌드 분석 ({latest_date.strftime('%Y년 %m월')})")
    print("="*60)
    
    # 1. 현재 상황
    latest_unemployment = BEVERIDGE_DATA['latest_values']['unemployment_rate']['value']
    latest_job_openings = BEVERIDGE_DATA['latest_values']['job_openings_rate']['value']
    
    print(f"\n1. 현재 노동시장 상황:")
    print(f"   실업률: {latest_unemployment:.1f}%")
    print(f"   구인율: {latest_job_openings:.1f}%")
    print(f"   베버리지 비율: {latest_job_openings/latest_unemployment:.2f}")
    
    # 2. 시기별 평균 분석
    if 'period' in df.columns:
        print(f"\n2. 시기별 평균 비교:")
        period_stats = df.groupby('period')[['unemployment_rate', 'job_openings_rate']].agg(['mean', 'std']).round(1)
        
        for period in df['period'].unique():
            period_data = df[df['period'] == period]
            avg_unemployment = period_data['unemployment_rate'].mean()
            avg_job_openings = period_data['job_openings_rate'].mean()
            avg_ratio = avg_job_openings / avg_unemployment
            
            print(f"   {period}:")
            print(f"     평균 실업률: {avg_unemployment:.1f}%")
            print(f"     평균 구인율: {avg_job_openings:.1f}%")  
            print(f"     베버리지 비율: {avg_ratio:.2f}")
    
    # 3. 최근 트렌드 (6개월)
    recent_data = df.tail(6)
    unemployment_trend = recent_data['unemployment_rate'].iloc[-1] - recent_data['unemployment_rate'].iloc[0]
    job_openings_trend = recent_data['job_openings_rate'].iloc[-1] - recent_data['job_openings_rate'].iloc[0]
    
    print(f"\n3. 최근 6개월 트렌드:")
    print(f"   실업률 변화: {unemployment_trend:+.1f}%p")
    print(f"   구인율 변화: {job_openings_trend:+.1f}%p")
    
    # 4. 베버리지 커브 이동 방향
    print(f"\n4. 베버리지 커브 분석:")
    if latest_job_openings > 3.5 and latest_unemployment < 4.5:
        print("   ✅ 노동시장 타이트 (높은 구인율, 낮은 실업률)")
    elif latest_job_openings < 2.5 and latest_unemployment > 6.0:
        print("   ⚠️ 노동시장 슬랙 (낮은 구인율, 높은 실업률)")
    else:
        print("   🔄 노동시장 균형 상태")
    
    # 코로나 이후 구조적 변화 분석
    if 'period' in df.columns:
        pre_covid = df[df['period'] == 'Pre-COVID']
        post_covid = df[df['period'] == 'COVID & Post-COVID']
        
        if not pre_covid.empty and not post_covid.empty:
            pre_avg_ratio = pre_covid['job_openings_rate'].mean() / pre_covid['unemployment_rate'].mean()
            post_avg_ratio = post_covid['job_openings_rate'].mean() / post_covid['unemployment_rate'].mean()
            
            print(f"\n5. 구조적 변화 분석:")
            print(f"   코로나 이전 베버리지 비율: {pre_avg_ratio:.2f}")
            print(f"   코로나 이후 베버리지 비율: {post_avg_ratio:.2f}")
            print(f"   변화: {post_avg_ratio - pre_avg_ratio:+.2f}")
            
            if post_avg_ratio > pre_avg_ratio * 1.1:
                print("   📈 베버리지 커브가 외부로 이동 (구조적 변화)")
            elif post_avg_ratio < pre_avg_ratio * 0.9:
                print("   📉 베버리지 커브가 내부로 이동")
            else:
                print("   ➡️ 베버리지 커브 위치 안정")
    
    return {
        'latest_date': latest_date,
        'latest_unemployment': latest_unemployment,
        'latest_job_openings': latest_job_openings,
        'unemployment_trend': unemployment_trend,
        'job_openings_trend': job_openings_trend
    }

# %%
# === 통합 실행 함수 ===

def run_beveridge_analysis(start_date='2000-01-01', force_reload=False, smart_update=True):
    """
    완전한 베버리지 커브 분석 실행 (스마트 업데이트 지원)
    
    Args:
        start_date: 데이터 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부 (기본값: True)
    
    Returns:
        dict: 생성된 차트들과 분석 결과
    """
    print("🚀 베버리지 커브 분석 시작")
    print("="*60)
    
    # 1. 데이터 로드
    print("\n1️⃣ 데이터 로딩")
    success = load_beveridge_data(start_date=start_date, force_reload=force_reload, smart_update=smart_update)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    # 2. 분석 실행
    print("\n2️⃣ 트렌드 분석")
    analysis_results = analyze_beveridge_trends()
    
    # 3. 시각화 생성
    print("\n3️⃣ 시각화 생성")
    
    results = {
        'analysis': analysis_results,
        'charts': {}
    }
    
    try:
        # 베버리지 커브 (시기별)
        print("   📈 베버리지 커브 (시기별)...")
        results['charts']['beveridge_curve'] = create_beveridge_curve(color_by_period=True, show_labels=True)
        
        # 베버리지 커브 (세부 기간별)
        print("   📈 베버리지 커브 (세부 기간별)...")
        results['charts']['beveridge_curve_detailed'] = create_beveridge_curve_detailed(show_labels=True)
        
        # 시계열 비교
        print("   📊 시계열 비교...")
        results['charts']['timeseries'] = create_timeseries_comparison()
        
        # 종합 대시보드
        print("   🗺️ 종합 대시보드...")
        results['charts']['dashboard'] = create_beveridge_dashboard()
        
    except Exception as e:
        print(f"⚠️ 시각화 생성 중 오류: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n✅ 베버리지 커브 분석 완료! 생성된 차트: {len(results['charts'])}개")
    
    return results

# %%
# === 사용 예시 및 안내 ===

print("\n=== 베버리지 커브 분석 도구 사용법 (KPDS 포맷) ===")
print("1. 데이터 로드:")
print("   load_beveridge_data()  # 베버리지 커브 데이터 로드 (스마트 업데이트)")
print()
print("2. 개별 차트 생성:")
print("   create_beveridge_curve()        # 베버리지 커브 (기본)")
print("   create_beveridge_curve_detailed() # 베버리지 커브 (세부 기간별)")
print("   create_timeseries_comparison()  # 시계열 비교")
print("   create_beveridge_dashboard()    # 종합 대시보드")
print()
print("3. 분석:")
print("   analyze_beveridge_trends()      # 트렌드 분석")
print()
print("4. 통합 실행:")
print("   run_beveridge_analysis()        # 전체 분석 및 시각화")
print("   run_beveridge_analysis(smart_update=False)  # 스마트 업데이트 비활성화")
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
print(f"📊 사용 가능한 시리즈:")
for series_name, series_id in BEVERIDGE_SERIES.items():
    korean_name = SERIES_KOREAN_NAMES.get(series_name, series_name)
    print(f"  '{series_name}': {korean_name} ({series_id})")

# %%
# 자동 실행
print("\n🔄 베버리지 커브 분석 자동 실행 중...")
run_beveridge_analysis(smart_update=True)

# %%
# 개별 차트 테스트
print("베버리지 커브 (기본 버전)")
create_beveridge_curve()

# %%
print("베버리지 커브 (세부 기간별 버전)")  
create_beveridge_curve_detailed()

# %%
