# %%
"""
FRED API 전용 ADP Employment 분석 및 시각화 도구
- FRED API를 사용하여 ADP Employment 데이터 수집
- 사업 규모별/산업별 데이터 분류
- MoM/3M/6M/1Y 기준 시각화 지원
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
    print("⚠️ requests 라이브러리가 없습니다. 설치하세요: pip install requests")
    FRED_API_AVAILABLE = False

# FRED API 키 설정 (여기에 실제 API 키를 입력하세요)
FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'  # https://fred.stlouisfed.org/docs/api/api_key.html 에서 발급

# KPDS 시각화 라이브러리 불러오기 (필수)
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

print("✓ KPDS 시각화 포맷 로드됨")

# %%
# === ADP 데이터 계층 구조 정의 ===
# 업데이트 체크

# ADP 시리즈 맵 - 사업 규모별
ADP_SIZE_SERIES = {
    'total': 'ADPMNUSNERSA',  # Total nonfarm private
    'size_1_19': 'ADPMES1T19ENERSA',  # 1-19 employees
    'size_20_49': 'ADPMES20T49ENERSA',  # 20-49 employees
    'size_50_249': 'ADPMES50T249ENERSA',  # 50-249 employees
    'size_250_499': 'ADPMES250T499ENERSA',  # 250-499 employees
    'size_500_plus': 'ADPMES500PENERSA'  # 500+ employees
}

# ADP 시리즈 맵 - 산업별
ADP_INDUSTRY_SERIES = {
    'construction': 'ADPMINDCONNERSA',
    'education_health': 'ADPMINDEDHLTNERSA',
    'financial': 'ADPMINDFINNERSA',
    'information': 'ADPMINDINFONERSA',
    'leisure_hospitality': 'ADPMINDLSHPNERSA',
    'manufacturing': 'ADPMINDMANNERSA',
    'natural_resources': 'ADPMINDNRMINNERSA',
    'other_services': 'ADPMINDOTHSRVNERSA',
    'professional_business': 'ADPMINDPROBUSNERSA',
    'trade_transport_utilities': 'ADPMINDTTUNERSA'
}

# 모든 ADP 시리즈 통합
ALL_ADP_SERIES = {**ADP_SIZE_SERIES, **ADP_INDUSTRY_SERIES}

# 한국어 이름 매핑
ADP_KOREAN_NAMES = {
    # 사업 규모별
    'total': '전체 민간 고용',
    'size_1_19': '1-19명',
    'size_20_49': '20-49명',
    'size_50_249': '50-249명',
    'size_250_499': '250-499명',
    'size_500_plus': '500명 이상',
    
    # 산업별
    'construction': '건설업',
    'education_health': '교육·보건',
    'financial': '금융',
    'information': '정보',
    'leisure_hospitality': '레저·숙박',
    'manufacturing': '제조업',
    'natural_resources': '천연자원·채굴',
    'other_services': '기타 서비스',
    'professional_business': '전문·비즈니스 서비스',
    'trade_transport_utilities': '무역·운송·유틸리티'
}

# %%
# === 전역 데이터 저장소 ===

# FRED 세션
FRED_SESSION = None

# CSV 파일 경로
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/adp_employ_data.csv'

# 전역 데이터 저장소
ADP_DATA = {
    'raw_data': pd.DataFrame(),      # 원본 레벨 데이터 (천 명 단위)
    'mom_data': pd.DataFrame(),      # 전월대비 변화율 데이터
    'mom_change': pd.DataFrame(),    # 전월대비 변화량 데이터 (천 명)
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
    if ADP_DATA['raw_data'].empty:
        print("⚠️ 저장할 데이터가 없습니다.")
        return False
    
    ensure_data_directory()
    
    try:
        # 날짜를 컬럼으로 추가하여 저장
        df_to_save = ADP_DATA['raw_data'].copy()
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

def check_recent_data_consistency(csv_data=None, tolerance=1000.0):
    """
    최근 데이터의 일치성 확인 (ADP 고용 데이터용 - 천명 단위)
    
    Args:
        csv_data: CSV에서 로드된 데이터 (None이면 자동 로드)
        tolerance: 허용 오차 (천명 단위, 기본값: 1000천명 = 100만명)
    
    Returns:
        dict: 일치성 확인 결과
    """
    if csv_data is None:
        csv_data = load_data_from_csv()
    
    if csv_data is None or csv_data.empty:
        return {'needs_update': True, 'reason': 'CSV 데이터 없음'}
    
    # 최근 3개월 데이터 확인
    try:
        # 날짜 정규화 (년-월)
        csv_data.index = pd.to_datetime(csv_data.index)
        csv_latest = csv_data.tail(3)
        
        # 동일한 기간의 API 데이터와 비교
        if ADP_DATA['raw_data'].empty:
            return {'needs_update': True, 'reason': 'API 데이터 없음'}
        
        api_data = ADP_DATA['raw_data']
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
                    
                    # 차이 계산 (천명 단위)
                    diff = abs(csv_val - api_val)
                    
                    # 유의미한 차이인지 확인 (tolerance 이상)
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
        print(f"   허용 오차: {result['tolerance']:,.0f}천명")
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
                        print(f"   - {inc['date']} {inc['series']}: CSV={csv_val:,.0f} vs API={api_val:,.0f} (차이: {diff:,.0f}천명)")

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

def get_fred_data(series_id, start_date='2020-01-01', end_date=None):
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
        return None
    
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
                print(f"❌ FRED 데이터 없음: {series_id}")
                return None
        else:
            print(f"❌ FRED 응답에 데이터 없음: {series_id}")
            return None
            
    except Exception as e:
        print(f"❌ FRED 요청 실패: {series_id} - {e}")
        return None

# %%
# === 데이터 계산 함수들 ===

def calculate_mom_change(data):
    """전월대비 변화량 계산 (천 명 단위)"""
    return data.diff()

def calculate_mom_percent(data):
    """전월대비 변화율 계산 (%)"""
    return (data.pct_change() * 100)

def calculate_average_change(data, months):
    """특정 기간 평균 대비 변화량 계산"""
    avg = data.rolling(window=months).mean()
    return data - avg.shift(1)

def calculate_average_percent_change(data, months):
    """특정 기간 평균 대비 변화율 계산"""
    avg = data.rolling(window=months).mean()
    return ((data / avg.shift(1)) - 1) * 100

# %%
# === 데이터 로드 함수들 ===

def load_all_adp_data(start_date='2020-01-01', force_reload=False, smart_update=True):
    """
    모든 ADP 데이터 로드 (스마트 업데이트 지원)
    
    Args:
        start_date: 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부 (기본값: True)
    
    Returns:
        bool: 로드 성공 여부
    """
    global ADP_DATA
    
    # 이미 로드된 경우 스킵 (강제 재로드가 아닌 경우)
    if ADP_DATA['load_info']['loaded'] and not force_reload and not smart_update:
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
            ADP_DATA['raw_data'] = csv_data
            ADP_DATA['mom_change'] = calculate_mom_change(ADP_DATA['raw_data'])
            ADP_DATA['mom_data'] = calculate_mom_percent(ADP_DATA['raw_data'])
            
            # 최신 몇 개 데이터만 API로 가져와서 비교
            print("🔍 최근 데이터 일치성 확인 중...")
            
            # FRED API 초기화
            if initialize_fred_api():
                # 최근 3개월 데이터만 API에서 가져오기
                recent_start = (datetime.datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
                
                recent_data_dict = {}
                # 전체 데이터만 우선 확인 (빠른 체크)
                series_data = get_fred_data(ADP_SIZE_SERIES['total'], recent_start)
                if series_data is not None:
                    recent_data_dict['total'] = series_data
                    
                    # 임시로 API 데이터를 저장
                    temp_api_data = ADP_DATA['raw_data'].copy()
                    
                    # 최신 API 데이터로 업데이트
                    for date in series_data.index:
                        if 'total' in temp_api_data.columns:
                            temp_api_data.loc[date, 'total'] = series_data[date]
                    
                    # 임시로 API 데이터를 전역 저장소에 저장
                    original_raw_data = ADP_DATA['raw_data'].copy()
                    ADP_DATA['raw_data'] = temp_api_data
                    
                    # 일치성 확인 (ADP 고용 데이터는 큰 수치이므로 1000천명=100만명을 허용 오차로 설정)
                    consistency_result = check_recent_data_consistency(csv_data, tolerance=1000.0)
                    
                    # 원본 데이터 복원
                    ADP_DATA['raw_data'] = original_raw_data
                    
                    print_consistency_results(consistency_result)
                    
                    needs_api_call = consistency_result['needs_update']
                    
                    if not needs_api_call:
                        print("✅ 최근 데이터가 일치함 - API 호출 건너뛰기")
                        # CSV 데이터를 그대로 사용
                        # 메타데이터 업데이트
                        latest_values = {}
                        for col in ADP_DATA['raw_data'].columns:
                            latest_values[col] = {
                                'value': ADP_DATA['raw_data'][col].iloc[-1],
                                'mom_change': ADP_DATA['mom_change'][col].iloc[-1],
                                'mom_percent': ADP_DATA['mom_data'][col].iloc[-1]
                            }
                        
                        ADP_DATA['latest_values'] = latest_values
                        ADP_DATA['load_info'] = {
                            'loaded': True,
                            'load_time': datetime.datetime.now(),
                            'start_date': start_date,
                            'series_count': len(ADP_DATA['raw_data'].columns),
                            'data_points': len(ADP_DATA['raw_data']),
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
        print("🚀 ADP 데이터 로딩 시작... (FRED API)")
        print("="*50)
        
        # FRED API 초기화
        if not initialize_fred_api():
            print("❌ FRED API 초기화 실패")
            return False
        
        # 데이터 수집
        raw_data_dict = {}
        
        # 사업 규모별 데이터 로드
        print("\n📊 사업 규모별 데이터 로딩...")
        for series_name, series_id in ADP_SIZE_SERIES.items():
            series_data = get_fred_data(series_id, start_date)
            if series_data is not None and len(series_data) > 0:
                raw_data_dict[series_name] = series_data
            else:
                print(f"❌ 데이터 로드 실패: {series_name}")
        
        # 산업별 데이터 로드
        print("\n🏭 산업별 데이터 로딩...")
        for series_name, series_id in ADP_INDUSTRY_SERIES.items():
            series_data = get_fred_data(series_id, start_date)
            if series_data is not None and len(series_data) > 0:
                raw_data_dict[series_name] = series_data
            else:
                print(f"❌ 데이터 로드 실패: {series_name}")
        
        if len(raw_data_dict) < 5:  # 최소 5개 시리즈는 있어야 함
            error_msg = f"❌ 로드된 시리즈가 너무 적습니다: {len(raw_data_dict)}개"
            print(error_msg)
            return False
        
        # 전역 저장소에 저장
        ADP_DATA['raw_data'] = pd.DataFrame(raw_data_dict)
        ADP_DATA['mom_change'] = calculate_mom_change(ADP_DATA['raw_data'])
        ADP_DATA['mom_data'] = calculate_mom_percent(ADP_DATA['raw_data'])
        
        # 최신 값 저장
        latest_values = {}
        for col in ADP_DATA['raw_data'].columns:
            latest_values[col] = {
                'value': ADP_DATA['raw_data'][col].iloc[-1],
                'mom_change': ADP_DATA['mom_change'][col].iloc[-1],
                'mom_percent': ADP_DATA['mom_data'][col].iloc[-1]
            }
        
        ADP_DATA['latest_values'] = latest_values
        ADP_DATA['load_info'] = {
            'loaded': True,
            'load_time': datetime.datetime.now(),
            'start_date': start_date,
            'series_count': len(raw_data_dict),
            'data_points': len(ADP_DATA['raw_data']),
            'source': 'API (전체 로드)'
        }
        
        if consistency_result:
            ADP_DATA['load_info']['consistency_check'] = consistency_result
        
        # CSV에 저장
        save_data_to_csv()
        
        print("\n✅ 데이터 로딩 완료!")
        print_load_info()
        
        return True
    
    return False

def print_load_info():
    """로드 정보 출력"""
    info = ADP_DATA['load_info']
    if not info['loaded']:
        print("❌ 데이터가 로드되지 않음")
        return
    
    print(f"📊 로드된 데이터 정보:")
    print(f"   시리즈 개수: {info['series_count']}")
    print(f"   데이터 포인트: {info['data_points']}")
    print(f"   시작 날짜: {info['start_date']}")
    print(f"   로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   데이터 소스: {info.get('source', 'API')}")
    
    if not ADP_DATA['raw_data'].empty:
        date_range = f"{ADP_DATA['raw_data'].index[0].strftime('%Y-%m')} ~ {ADP_DATA['raw_data'].index[-1].strftime('%Y-%m')}"
        print(f"   데이터 기간: {date_range}")
    
    if 'consistency_check' in info:
        consistency = info['consistency_check']
        print(f"   일치성 확인: {consistency.get('reason', 'N/A')}")

# %%
# === 데이터 접근 함수들 ===

def get_raw_data(series_names=None):
    """원본 데이터 반환"""
    if not ADP_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_adp_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return ADP_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in ADP_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return ADP_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화율 데이터 반환"""
    if not ADP_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다.")
        return pd.DataFrame()
    
    if series_names is None:
        return ADP_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in ADP_DATA['mom_data'].columns]
    return ADP_DATA['mom_data'][available_series].copy()

def get_latest_values(series_names=None):
    """최신 값들 반환"""
    if not ADP_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다.")
        return {}
    
    if series_names is None:
        return ADP_DATA['latest_values'].copy()
    
    return {name: ADP_DATA['latest_values'].get(name) for name in series_names if name in ADP_DATA['latest_values']}

# %%
# === 시각화 함수들 ===

def create_adp_timeseries_chart(series_names=None, chart_type='level'):
    """
    ADP 시계열 차트 생성 (KPDS 포맷)
    
    Args:
        series_names: 차트에 표시할 시리즈 리스트
        chart_type: 'level' (천 명), 'mom' (전월대비 %), 'mom_change' (전월대비 천 명)
        title: 차트 제목
    """
    if not ADP_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_adp_data()를 실행하세요.")
        return None
    
    if series_names is None:
        series_names = ['total']
    
    # 데이터 가져오기 및 제목 설정
    if chart_type == 'level':
        df = get_raw_data(series_names)
        ytitle = "고용자 수 (천 명)"
        title = "ADP 고용 - 수준"
    elif chart_type == 'mom':
        df = get_mom_data(series_names)
        ytitle = "전월대비 변화율 (%)"
        title = "ADP 고용 - 전월대비 변화율"
    else:  # mom_change
        df = ADP_DATA['mom_change'][series_names].copy()
        ytitle = "전월대비 변화 (천 명)"
        title = "ADP 고용 - 전월대비 변화"
    
    print(title)
    
    if df.empty:
        print("⚠️ 요청한 데이터가 없습니다.")
        return None
    
    # 시리즈 개수에 따른 차트 함수 선택
    if len(df.columns) == 1:
        # 단일 시리즈
        fig = df_line_chart(df, df.columns[0], ytitle=ytitle)
    else:
        # 다중 시리즈
        labels = {col: ADP_KOREAN_NAMES.get(col, col) for col in df.columns}
        fig = df_multi_line_chart(df, df.columns.tolist(), ytitle=ytitle, labels=labels)
    
    # 0선 추가 (변화율/변화량 차트인 경우)
    if chart_type in ['mom', 'mom_change']:
        fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)
    
    return fig

def create_adp_comparison_chart(comparison_type='size', periods=[1, 3, 6, 12]):
    """
    ADP 비교 차트 생성 (KPDS 포맷 가로 바 차트)
    
    Args:
        comparison_type: 'size' (사업 규모별) 또는 'industry' (산업별)
        periods: 비교할 기간들 (개월 수) [1, 3, 6, 12]
        title: 차트 제목
    """
    if not ADP_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_adp_data()를 실행하세요.")
        return None
    
    # 최신 날짜
    latest_date = ADP_DATA['raw_data'].index[-1]
    
    # 시리즈 선택 및 제목 설정
    if comparison_type == 'size':
        series_list = list(ADP_SIZE_SERIES.keys())
        title = f"ADP 고용 변화 - 사업 규모별 ({latest_date.strftime('%Y년 %m월')})"
    else:
        series_list = list(ADP_INDUSTRY_SERIES.keys())
        title = f"ADP 고용 변화 - 산업별 ({latest_date.strftime('%Y년 %m월')})"
    
    print(title)
    
    # 서브플롯 생성
    fig = make_subplots(
        rows=1, cols=len(periods),
        subplot_titles=[f"vs {p}개월 전" if p == 1 else f"vs {p}개월 평균" for p in periods],
        shared_yaxes=True,
        horizontal_spacing=0.05
    )
    
    for i, period in enumerate(periods):
        # 데이터 계산
        categories = []
        values = []
        colors = []
        
        for series in series_list:
            if series not in ADP_DATA['raw_data'].columns:
                continue
            
            series_data = ADP_DATA['raw_data'][series]
            latest_value = series_data.iloc[-1]
            
            if period == 1:
                # 전월 대비
                prev_value = series_data.iloc[-2] if len(series_data) > 1 else latest_value
                change = ((latest_value / prev_value) - 1) * 100
            else:
                # N개월 평균 대비
                avg_value = series_data.iloc[-(period+1):-1].mean()
                change = ((latest_value / avg_value) - 1) * 100
            
            korean_name = ADP_KOREAN_NAMES.get(series, series)
            categories.append(korean_name)
            values.append(change)
            colors.append(deepred_pds if change >= 0 else deepblue_pds)
        
        # 정렬
        sorted_data = sorted(zip(categories, values, colors), key=lambda x: x[1])
        categories, values, colors = zip(*sorted_data)
        
        # 바 차트 추가
        fig.add_trace(
            go.Bar(
                y=list(categories),
                x=list(values),
                orientation='h',
                marker_color=list(colors),
                text=[f'{v:+.1f}%' for v in values],
                textposition='outside',
                showlegend=False
            ),
            row=1, col=i+1
        )
        
        # x축 범위 설정
        max_val = max(abs(min(values)), max(values)) * 1.2
        fig.update_xaxes(range=[-max_val, max_val], row=1, col=i+1)
        
        # 0선 추가
        fig.add_vline(x=0, line_width=1, line_color="black", row=1, col=i+1)
    
    # KPDS 폰트 설정
    font_family = 'NanumGothic'
    
    # 레이아웃 업데이트
    fig.update_layout(
        height=max(400, len(categories) * 40),
        width=1200,
        paper_bgcolor='white',
        plot_bgcolor='white',
        font=dict(family=font_family, size=FONT_SIZE_GENERAL, color="black")
    )
    
    # x축 라벨
    for i in range(len(periods)):
        fig.update_xaxes(
            title_text="변화율 (%)",
            title_font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE),
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            row=1, col=i+1
        )
    
    return fig

def create_adp_heatmap():
    """
    ADP 변화율 히트맵 생성 (KPDS 포맷)
    
    Returns:
        plotly figure
    """
    if not ADP_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_adp_data()를 실행하세요.")
        return None
    
    # 최근 12개월 데이터
    mom_data = ADP_DATA['mom_data'].tail(12)
    
    # 카테고리 정렬
    size_cols = [col for col in ADP_SIZE_SERIES.keys() if col in mom_data.columns]
    industry_cols = [col for col in ADP_INDUSTRY_SERIES.keys() if col in mom_data.columns]
    
    # 데이터 준비
    all_cols = size_cols + industry_cols
    heatmap_data = mom_data[all_cols].T
    
    # 라벨 변환
    y_labels = [ADP_KOREAN_NAMES.get(col, col) for col in all_cols]
    x_labels = [date.strftime('%Y-%m') for date in heatmap_data.columns]
    
    # KPDS 폰트 설정
    font_family = 'NanumGothic'
    
    # 히트맵 생성
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_data.values,
        x=x_labels,
        y=y_labels,
        colorscale='RdBu_r',
        zmid=0,
        text=heatmap_data.values,
        texttemplate='%{text:.1f}%',
        textfont={"size": 10, "family": font_family},
        colorbar=dict(title="전월대비 %", titlefont=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE))
    ))
    
    # 구분선 추가
    if size_cols and industry_cols:
        fig.add_shape(
            type="line",
            x0=-0.5, x1=len(x_labels)-0.5,
            y0=len(size_cols)-0.5, y1=len(size_cols)-0.5,
            line=dict(color="black", width=2)
        )
    
    # 제목 출력
    title = "ADP 고용 - 월별 변화율 히트맵 (%)"
    print(title)
    
    fig.update_layout(
        xaxis=dict(
            title="월",
            title_font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE),
            side="bottom",
            tickfont=dict(family=font_family, size=FONT_SIZE_GENERAL)
        ),
        yaxis=dict(
            title="카테고리",
            title_font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE),
            tickfont=dict(family=font_family, size=FONT_SIZE_GENERAL)
        ),
        width=1000,
        height=600,
        paper_bgcolor='white',
        plot_bgcolor='white',
        font=dict(family=font_family, size=FONT_SIZE_GENERAL, color="black")
    )
    
    return fig

# %%
# === 분석 함수들 ===

def analyze_adp_trends():
    """
    ADP 고용 트렌드 분석
    """
    if not ADP_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_adp_data()를 실행하세요.")
        return None
    
    latest_date = ADP_DATA['raw_data'].index[-1]
    
    print(f"\n📊 ADP 고용 트렌드 분석 ({latest_date.strftime('%Y년 %m월')})")
    print("="*60)
    
    # 1. 전체 고용 현황
    total_employment = ADP_DATA['latest_values']['total']
    print(f"\n1. 전체 민간 고용: {total_employment['value']:,.0f}천명")
    print(f"   - 전월대비: {total_employment['mom_change']:+.0f}천명 ({total_employment['mom_percent']:+.1f}%)")
    
    # 2. 사업 규모별 분석
    print("\n2. 사업 규모별 고용 변화 (전월대비)")
    for size_key in ADP_SIZE_SERIES.keys():
        if size_key == 'total':
            continue
        if size_key in ADP_DATA['latest_values']:
            data = ADP_DATA['latest_values'][size_key]
            name = ADP_KOREAN_NAMES[size_key]
            print(f"   {name}: {data['mom_change']:+.0f}천명 ({data['mom_percent']:+.1f}%)")
    
    # 3. 산업별 분석
    print("\n3. 산업별 고용 변화 (전월대비)")
    industry_changes = []
    for ind_key in ADP_INDUSTRY_SERIES.keys():
        if ind_key in ADP_DATA['latest_values']:
            data = ADP_DATA['latest_values'][ind_key]
            name = ADP_KOREAN_NAMES[ind_key]
            industry_changes.append((name, data['mom_change'], data['mom_percent']))
    
    # 변화량 기준 정렬
    industry_changes.sort(key=lambda x: x[1], reverse=True)
    
    print("\n   상위 증가 산업:")
    for name, change, percent in industry_changes[:3]:
        print(f"   - {name}: {change:+.0f}천명 ({percent:+.1f}%)")
    
    print("\n   하위 증가/감소 산업:")
    for name, change, percent in industry_changes[-3:]:
        print(f"   - {name}: {change:+.0f}천명 ({percent:+.1f}%)")
    
    # 4. 장기 트렌드
    print("\n4. 장기 트렌드 (6개월 평균 대비)")
    total_6m_avg = ADP_DATA['raw_data']['total'].iloc[-7:-1].mean()
    total_current = ADP_DATA['raw_data']['total'].iloc[-1]
    total_6m_change = ((total_current / total_6m_avg) - 1) * 100
    
    print(f"   전체 고용: {total_6m_change:+.1f}% (6개월 평균 대비)")
    
    return {
        'latest_date': latest_date,
        'total_employment': total_employment,
        'industry_changes': industry_changes
    }

# %%
# === 통합 실행 함수 ===

def run_adp_analysis(start_date='2020-01-01', force_reload=False, smart_update=True):
    """
    완전한 ADP 분석 실행 (스마트 업데이트 지원)
    
    Args:
        start_date: 데이터 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부 (기본값: True)
    
    Returns:
        dict: 생성된 차트들과 분석 결과
    """
    print("🚀 ADP Employment 분석 시작")
    print("="*50)
    
    # 1. 데이터 로드
    print("\n1️⃣ 데이터 로딩")
    success = load_all_adp_data(start_date=start_date, force_reload=force_reload, smart_update=smart_update)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    # 2. 분석 실행
    print("\n2️⃣ 트렌드 분석")
    analysis_results = analyze_adp_trends()
    
    # 3. 시각화 생성
    print("\n3️⃣ 시각화 생성")
    
    results = {
        'analysis': analysis_results,
        'charts': {}
    }
    
    try:
        # 전체 고용 시계열
        print("   📈 전체 고용 시계열...")
        results['charts']['total_timeseries'] = create_adp_timeseries_chart(['total'], 'level')
        
        # 전월대비 변화 시계열
        print("   📊 전월대비 변화 시계열...")
        results['charts']['mom_timeseries'] = create_adp_timeseries_chart(['total'], 'mom_change')
        
        # 사업 규모별 비교
        print("   🏢 사업 규모별 비교...")
        results['charts']['size_comparison'] = create_adp_comparison_chart('size', [1, 3, 6, 12])
        
        # 산업별 비교
        print("   🏭 산업별 비교...")
        results['charts']['industry_comparison'] = create_adp_comparison_chart('industry', [1, 3, 6, 12])
        
        # 히트맵
        print("   🗺️ 변화율 히트맵...")
        results['charts']['heatmap'] = create_adp_heatmap()
        
        # 사업 규모별 시계열 (KPDS 포맷)
        print("   📈 사업 규모별 시계열...")
        size_series = [k for k in ADP_SIZE_SERIES.keys() if k != 'total']
        results['charts']['size_timeseries'] = create_adp_timeseries_chart(size_series[:3], 'mom')
        
    except Exception as e:
        print(f"⚠️ 시각화 생성 중 오류: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n✅ 분석 완료! 생성된 차트: {len(results['charts'])}개")
    
    # 차트 표시
    for chart_name, chart in results['charts'].items():
        if chart is not None:
            print(f"\n📊 {chart_name} 표시 중...")
            chart.show()
    
    return results

# %%
# === 사용 예시 ===

print("\n=== ADP Employment 분석 도구 사용법 (KPDS 포맷) ===")
print("1. API 키 설정:")
print("   FRED_API_KEY = 'your_api_key_here'")
print("   # https://fred.stlouisfed.org/docs/api/api_key.html 에서 무료 발급")
print()
print("2. 데이터 로드:")
print("   load_all_adp_data()  # 모든 ADP 데이터 로드")
print()
print("3. 시계열 차트 (KPDS 포맷):")
print("   create_adp_timeseries_chart(['total'], 'level')     # 고용 수준")
print("   create_adp_timeseries_chart(['total'], 'mom')       # 전월대비 %")
print("   create_adp_timeseries_chart(['total'], 'mom_change') # 전월대비 천명")
print()
print("4. 비교 차트 (KPDS 포맷):")
print("   create_adp_comparison_chart('size', [1, 3, 6, 12])     # 사업 규모별")
print("   create_adp_comparison_chart('industry', [1, 3, 6, 12]) # 산업별")
print()
print("5. 히트맵 (KPDS 포맷):")
print("   create_adp_heatmap()  # 전체 변화율 히트맵")
print()
print("6. 통합 분석:")
print("   run_adp_analysis()  # 전체 분석 및 시각화 (KPDS 포맷)")
print("   run_adp_analysis(smart_update=False)  # 스마트 업데이트 비활성화")
print()
print("7. 트렌드 분석:")
print("   analyze_adp_trends()  # 상세 트렌드 분석")
print()
print("8. 스마트 업데이트 기능:")
print("   - 데이터 일치성 자동 확인 (허용 오차: 1,000천명)")
print("   - CSV 기반 로컬 캐싱")
print("   - 불필요한 API 호출 최소화")
print()
print("✅ 모든 시각화가 KPDS 포맷(/home/jyp0615/kpds_fig_format_enhanced.py)을 사용합니다.")

# %%
# 사용 가능한 시리즈 표시
def show_available_series():
    """사용 가능한 ADP 시리즈 표시"""
    print("\n=== 사용 가능한 ADP 시리즈 ===")
    
    print("\n📊 사업 규모별:")
    for key, series_id in ADP_SIZE_SERIES.items():
        korean_name = ADP_KOREAN_NAMES.get(key, key)
        print(f"  '{key}': {korean_name} ({series_id})")
    
    print("\n🏭 산업별:")
    for key, series_id in ADP_INDUSTRY_SERIES.items():
        korean_name = ADP_KOREAN_NAMES.get(key, key)
        print(f"  '{key}': {korean_name} ({series_id})")

show_available_series()

# %%
# 스마트 업데이트 활성화하여 분석 실행
run_adp_analysis(smart_update=True)

# %%
