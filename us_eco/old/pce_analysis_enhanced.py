# %%
"""
FRED API 전용 US Personal Consumption Expenditures (PCE) 분석 및 시각화 도구
- FRED API를 사용하여 US PCE 관련 데이터 수집
- PCE 구성 요소별 데이터 분류 및 분석
- MoM/YoY 기준 시각화 지원
- 스마트 업데이트 기능 (실행 시마다 최신 데이터 확인 및 업데이트)
- CSV 파일 자동 저장 및 업데이트
- 투자은행/이코노미스트 관점의 심층 분석
"""
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime
import sys
import warnings
import os
warnings.filterwarnings('ignore')

# 필수 라이브러리들
try:
    import requests
    FRED_API_AVAILABLE = True
    print("✓ FRED API 연동 가능 (requests 라이브러리)")
except ImportError:
    print("⚠️ requests 라이브러리가 없습니다. 설치하세요: pip install requests")
    FRED_API_AVAILABLE = False

# FRED API 키 설정
FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'

# KPDS 시각화 라이브러리 불러오기 (필수)
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

print("✓ KPDS 시각화 포맷 로드됨")

# %%
# === Personal Consumption Expenditures 데이터 계층 구조 정의 ===

# PCE 주요 구성 요소 시리즈 맵 (월별 MoM 변화율 데이터 - 이미 seasonally adjusted)
PCE_MAIN_SERIES = {
    # 핵심 PCE 지표 (실질 소비 MoM 변화율) - 이미 MoM 처리됨
    'pce_total': 'DPCERAM1M225NBEA',  # Real PCE - total (MoM %)
    'pce_goods': 'DGDSRAM1M225NBEA',  # Goods (MoM %)
    'pce_durable': 'DDURRAM1M225NBEA',  # Durable goods (MoM %)
    'pce_nondurable': 'DNDGRAM1M225NBEA',  # Nondurable goods (MoM %)
    'pce_services': 'DSERRAM1M225NBEA',  # Services (MoM %)
    'pce_core': 'DPCCRAM1M225NBEA',  # PCE ex-food & energy (MoM %)
    'pce_food': 'DFXARAM1M225NBEA',  # Food (MoM %)
    'pce_energy': 'DNRGRAM1M225NBEA',  # Energy goods & services (MoM %)
    'pce_market_based': 'DPCMRAM1M225NBEA',  # Market-based PCE (MoM %)
    'pce_market_core': 'DPCXRAM1M225NBEA',  # Market-based PCE ex-food & energy (MoM %)
    
    # PCE 가격지수 (물가 MoM 변화율) - 이미 MoM 처리됨
    'pce_price_headline': 'DPCERGM1M225SBEA',  # PCE Price index (headline) (MoM %)
    'pce_price_goods': 'DGDSRGM1M225SBEA',  # Goods prices (MoM %)
    'pce_price_durable': 'DDURRGM1M225SBEA',  # Durable goods prices (MoM %)
    'pce_price_nondurable': 'DNDGRGM1M225SBEA',  # Nondurable goods prices (MoM %)
    'pce_price_services': 'DSERRGM1M225SBEA',  # Services prices (MoM %)
    'pce_price_core': 'DPCCRGM1M225SBEA',  # Core PCE (ex-food & energy) (MoM %)
    'pce_price_food': 'DFXARGM1M225SBEA',  # Food prices (MoM %)
    'pce_price_energy': 'DNRGRGM1M225SBEA',  # Energy prices (MoM %)
    'pce_price_market': 'DPCMRGM1M225SBEA',  # Market-based PCE prices (MoM %)
    'pce_price_market_core': 'DPCXRGM1M225SBEA',  # Market-based core PCE prices (MoM %)
    
    # 추가 경제지표 (레벨 데이터 - MoM 계산 필요)
    'personal_income': 'PI',  # Personal income (level data)
    'disposable_income': 'DSPI',  # Disposable personal income (level data)
    'saving_rate': 'PSAVERT',  # Personal saving rate (level data)
}

# PCE 분석을 위한 계층 구조
PCE_HIERARCHY = {
    'total_consumption': {
        'series_id': 'DPCERAM1M225NBEA',
        'name': 'Total PCE',
        'components': ['goods', 'services']
    },
    'goods_consumption': {
        'series_id': 'DGDSRAM1M225NBEA',
        'name': 'Goods Consumption',
        'components': ['durable_goods', 'nondurable_goods']
    },
    'services_consumption': {
        'series_id': 'DSERRAM1M225NBEA',
        'name': 'Services Consumption',
        'components': []
    },
    'core_vs_headline': {
        'name': 'Core vs Headline',
        'components': ['pce_total', 'pce_core', 'pce_food', 'pce_energy']
    },
    'price_indices': {
        'name': 'PCE Price Indices',
        'components': ['pce_price_headline', 'pce_price_core', 'pce_price_goods', 'pce_price_services']
    }
}

# 색상 매핑 (KPDS 색상 팔레트 사용)
PCE_COLORS = {
    'pce_total': KPDS_COLORS[0],          # deepred_pds
    'pce_core': KPDS_COLORS[1],           # deepblue_pds  
    'pce_goods': KPDS_COLORS[2],          # beige_pds
    'pce_services': KPDS_COLORS[3],       # blue_pds
    'pce_durable': KPDS_COLORS[4],        # grey_pds
    'pce_nondurable': KPDS_COLORS[0],     # deepred_pds (순환)
    'pce_food': KPDS_COLORS[1],           # deepblue_pds
    'pce_energy': KPDS_COLORS[2],         # beige_pds
    'pce_price_headline': KPDS_COLORS[3], # blue_pds
    'pce_price_core': KPDS_COLORS[4]      # grey_pds
}

print("✓ PCE 데이터 구조 정의 완료")

# %%
# === FRED API 데이터 수집 함수들 ===

def fetch_fred_data(series_id, start_date='2000-01-01', end_date=None):
    """
    FRED API에서 단일 시리즈 데이터를 가져오는 함수
    
    Parameters:
    - series_id: FRED 시리즈 ID
    - start_date: 시작 날짜 (YYYY-MM-DD 형식)
    - end_date: 종료 날짜 (None이면 현재 날짜까지)
    
    Returns:
    - pandas.Series: 날짜를 인덱스로 하는 시리즈 데이터
    """
    if not FRED_API_AVAILABLE:
        print("❌ FRED API 사용 불가능")
        return None
    
    if end_date is None:
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    
    url = f'https://api.stlouisfed.org/fred/series/observations'
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'start_date': start_date,
        'end_date': end_date,
        'sort_order': 'asc'
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if 'observations' not in data:
            print(f"❌ {series_id}: API 응답에 데이터가 없습니다")
            return None
        
        # 데이터프레임으로 변환
        observations = data['observations']
        df = pd.DataFrame(observations)
        
        # 날짜 변환 및 인덱스 설정
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        
        # 숫자 변환 (. 값은 NaN으로 처리)
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        # 시리즈로 반환
        series_data = df['value'].dropna()
        
        print(f"✓ {series_id}: {len(series_data)}개 데이터포인트 수집 ({series_data.index[0].strftime('%Y-%m')} ~ {series_data.index[-1].strftime('%Y-%m')})")
        return series_data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ {series_id}: API 요청 실패 - {e}")
        return None
    except Exception as e:
        print(f"❌ {series_id}: 데이터 처리 실패 - {e}")
        return None

def fetch_pce_dataset(series_dict, start_date='2000-01-01', end_date=None):
    """
    여러 PCE 시리즈를 한번에 가져와서 데이터프레임으로 반환
    
    Parameters:
    - series_dict: 시리즈 딕셔너리 {name: series_id}
    - start_date: 시작 날짜
    - end_date: 종료 날짜
    
    Returns:
    - pandas.DataFrame: 각 시리즈를 컬럼으로 하는 데이터프레임
    """
    print(f"📊 PCE 데이터 수집 시작 ({len(series_dict)}개 시리즈)")
    
    data_dict = {}
    successful_series = []
    failed_series = []
    
    for name, series_id in series_dict.items():
        print(f"수집 중: {name} ({series_id})")
        series_data = fetch_fred_data(series_id, start_date, end_date)
        
        if series_data is not None and len(series_data) > 0:
            data_dict[name] = series_data
            successful_series.append(name)
        else:
            failed_series.append(name)
    
    if not data_dict:
        print("❌ 수집된 데이터가 없습니다")
        return None
    
    # 데이터프레임으로 결합
    df = pd.DataFrame(data_dict)
    
    # 월말 기준으로 리샘플링 (일부 시리즈가 월중 다른 날짜일 수 있음)
    df = df.resample('M').last()
    
    print(f"✅ 데이터 수집 완료:")
    print(f"   - 성공: {len(successful_series)}개 시리즈")
    print(f"   - 실패: {len(failed_series)}개 시리즈")
    if failed_series:
        print(f"   - 실패 목록: {failed_series}")
    print(f"   - 기간: {df.index[0].strftime('%Y-%m')} ~ {df.index[-1].strftime('%Y-%m')}")
    print(f"   - 데이터 포인트: {len(df)}개월")
    
    return df

def save_pce_data(df, filename='pce_data.csv'):
    """PCE 데이터를 CSV 파일로 저장"""
    if df is None:
        print("❌ 저장할 데이터가 없습니다")
        return False
    
    try:
        # 현재 디렉토리에 저장
        filepath = os.path.join(os.getcwd(), filename)
        df.to_csv(filepath)
        print(f"✅ 데이터 저장 완료: {filepath}")
        return True
    except Exception as e:
        print(f"❌ 데이터 저장 실패: {e}")
        return False

def load_pce_data(filename='pce_data.csv'):
    """저장된 PCE 데이터를 로드"""
    try:
        filepath = os.path.join(os.getcwd(), filename)
        if os.path.exists(filepath):
            df = pd.read_csv(filepath, index_col=0, parse_dates=True)
            print(f"✅ 기존 데이터 로드: {len(df)}개월 ({df.index[0].strftime('%Y-%m')} ~ {df.index[-1].strftime('%Y-%m')})")
            return df
        else:
            print(f"ℹ️ 기존 파일 없음: {filename}")
            return None
    except Exception as e:
        print(f"❌ 데이터 로드 실패: {e}")
        return None

def check_recent_data_consistency(filename='pce_data_complete.csv', check_count=3):
    """
    최근 N개 데이터의 일치성을 확인하여 스마트 업데이트 필요 여부 판단 (PCE 특화)
    
    Parameters:
    - filename: 확인할 CSV 파일명
    - check_count: 확인할 최근 데이터 개수
    
    Returns:
    - dict: {'need_update': bool, 'reason': str, 'mismatches': list}
    """
    print(f"🔍 PCE 스마트 업데이트 디버깅: 파일 '{filename}'에서 최근 {check_count}개 데이터 확인")
    print(f"📁 작업 디렉토리: {os.getcwd()}")
    print(f"📄 확인할 파일 경로: {os.path.join(os.getcwd(), filename)}")
    print(f"📊 파일 존재 여부: {os.path.exists(os.path.join(os.getcwd(), filename))}")
    
    # 기존 CSV 파일 존재 여부 확인
    existing_df = load_pce_data(filename)
    if existing_df is None:
        print("📄 기존 CSV 파일이 없습니다. 새로 수집이 필요합니다.")
        return {'need_update': True, 'reason': '기존 파일 없음', 'mismatches': []}
    
    # 사용 가능한 시리즈 확인
    available_series = [col for col in existing_df.columns if col in PCE_MAIN_SERIES.keys()]
    if not available_series:
        print("❌ 확인할 시리즈가 없습니다")
        return {'need_update': True, 'reason': '시리즈 없음', 'mismatches': []}
    
    print(f"📊 확인 대상 시리즈: {len(available_series)}개")
    
    mismatches = []
    new_data_available = False
    all_data_identical = True
    
    for col_name in available_series:
        if col_name not in PCE_MAIN_SERIES:
            continue
            
        series_id = PCE_MAIN_SERIES[col_name]
        existing_data = existing_df[col_name].dropna()
        
        if len(existing_data) == 0:
            continue
        
        # 최근 데이터 가져오기
        current_year = datetime.datetime.now().year
        api_data = fetch_fred_data(series_id, f'{current_year-1}-01-01')
        
        if api_data is None or len(api_data) == 0:
            mismatches.append({
                'series': col_name,
                'reason': 'API 데이터 없음'
            })
            all_data_identical = False
            continue
        
        # 새로운 데이터가 있는지 확인
        latest_existing = existing_data.index[-1]
        latest_api = api_data.index[-1]
        
        if latest_api > latest_existing:
            new_data_available = True
            mismatches.append({
                'series': col_name,
                'reason': '새로운 데이터 존재',
                'existing_latest': latest_existing,
                'api_latest': latest_api
            })
            all_data_identical = False
            continue
        
        # 최근 N개 데이터 비교 (날짜 정규화)
        existing_recent = existing_data.tail(check_count)
        api_recent = api_data.tail(check_count)
        
        # 날짜를 년-월 형식으로 정규화하여 비교
        existing_normalized = {}
        for date, value in existing_recent.items():
            key = (date.year, date.month)
            existing_normalized[key] = value
            
        api_normalized = {}
        for date, value in api_recent.items():
            key = (date.year, date.month)
            api_normalized[key] = value
        
        # 정규화된 날짜로 비교 (PCE 특화: 0.1 허용 오차)
        for key in existing_normalized.keys():
            if key in api_normalized:
                existing_val = existing_normalized[key]
                api_val = api_normalized[key]
                
                # 값이 다르면 불일치 (0.1 이상 차이 - 가격지수 데이터 특성 고려)
                if abs(existing_val - api_val) > 0.1:
                    mismatches.append({
                        'series': col_name,
                        'date': pd.Timestamp(key[0], key[1], 1),  # 비교용 날짜 객체
                        'existing': existing_val,
                        'api': api_val,
                        'diff': abs(existing_val - api_val)
                    })
                    all_data_identical = False
            else:
                mismatches.append({
                    'series': col_name,
                    'date': pd.Timestamp(key[0], key[1], 1),
                    'existing': existing_normalized[key],
                    'api': None,
                    'reason': '날짜 없음'
                })
                all_data_identical = False
    
    # 결과 판정 (디버깅 강화)
    print(f"🎯 PCE 스마트 업데이트 판정:")
    print(f"   - 새로운 데이터 감지: {new_data_available}")
    print(f"   - 전체 데이터 일치: {all_data_identical}")
    print(f"   - 총 불일치 수: {len(mismatches)}")
    
    if new_data_available:
        print(f"🆕 새로운 데이터 감지: 업데이트 필요")
        for mismatch in mismatches[:3]:
            if 'reason' in mismatch and mismatch['reason'] == '새로운 데이터 존재':
                print(f"   - {mismatch['series']}: {mismatch['existing_latest']} → {mismatch['api_latest']}")
        print(f"📤 스마트 업데이트 결과: API 호출 필요 (새로운 데이터)")
        return {'need_update': True, 'reason': '새로운 데이터', 'mismatches': mismatches}
    elif not all_data_identical:
        value_mismatches = [m for m in mismatches if m.get('reason') != '새로운 데이터 존재']
        print(f"⚠️ 데이터 불일치 발견: {len(value_mismatches)}개")
        
        # 디버깅: 실제 불일치 내용 출력
        print("🔍 불일치 세부 내용 (최대 5개):")
        for i, mismatch in enumerate(value_mismatches[:5]):
            if 'existing' in mismatch and 'api' in mismatch:
                print(f"   {i+1}. {mismatch['series']} ({mismatch['date'].strftime('%Y-%m')}): CSV={mismatch['existing']:.6f}, API={mismatch['api']:.6f}, 차이={mismatch['diff']:.6f}")
            else:
                print(f"   {i+1}. {mismatch}")
        
        # PCE 특화: 0.1 이상만 실제 불일치로 간주
        significant_mismatches = [m for m in value_mismatches if m.get('diff', 0) > 0.1]
        if len(significant_mismatches) == 0:
            print("📝 모든 차이가 0.1 미만입니다. 저장 정밀도 차이로 간주하여 업데이트를 건너뜁니다.")
            print(f"📤 스마트 업데이트 결과: API 호출 스킵 (정밀도 차이)")
            return {'need_update': False, 'reason': '미세한 정밀도 차이', 'mismatches': mismatches}
        else:
            print(f"🚨 유의미한 차이 발견: {len(significant_mismatches)}개 (0.1 이상)")
            return {'need_update': True, 'reason': '데이터 불일치', 'mismatches': mismatches}
    else:
        print("✅ 최근 데이터가 완전히 일치합니다. 업데이트를 건너뜁니다.")
        print(f"📤 스마트 업데이트 결과: API 호출 스킵 (데이터 일치)")
        return {'need_update': False, 'reason': '데이터 일치', 'mismatches': []}

print("✅ FRED API 데이터 수집 함수 정의 완료")

# %%
# === PCE 핵심 지표 계산 함수들 ===

def calculate_mom_change(series, periods=1):
    """
    월간 변화율 계산 (Month-over-Month)
    
    Parameters:
    - series: 시계열 데이터
    - periods: 비교 기간 (기본값 1 = 전월 대비)
    
    Returns:
    - pandas.Series: MoM 변화율 (%)
    """
    if series is None or len(series) == 0:
        return None
    
    mom_change = ((series / series.shift(periods)) - 1) * 100
    return mom_change

def calculate_yoy_change(series, periods=12):
    """
    연간 변화율 계산 (Year-over-Year)
    
    Parameters:
    - series: 시계열 데이터  
    - periods: 비교 기간 (기본값 12 = 전년동월 대비)
    
    Returns:
    - pandas.Series: YoY 변화율 (%)
    """
    if series is None or len(series) == 0:
        return None
    
    yoy_change = ((series / series.shift(periods)) - 1) * 100
    return yoy_change

def calculate_3m_annualized(series):
    """
    3개월 연율화 변화율 계산 (투자은행 스타일 분석)
    
    Parameters:
    - series: 시계열 데이터
    
    Returns:
    - pandas.Series: 3개월 연율화 변화율 (%)
    """
    if series is None or len(series) == 0:
        return None
    
    # 3개월 변화율을 연율화 ((1 + r/100)^4 - 1) * 100
    three_month_change = ((series / series.shift(3)) - 1) * 100
    annualized = ((1 + three_month_change/100) ** 4 - 1) * 100
    return annualized

def calculate_moving_average(series, window=3):
    """
    이동평균 계산
    
    Parameters:
    - series: 시계열 데이터
    - window: 평균 계산 기간 (기본값 3개월)
    
    Returns:
    - pandas.Series: 이동평균 데이터
    """
    if series is None or len(series) == 0:
        return None
    
    return series.rolling(window=window, center=True).mean()

def generate_pce_summary_stats(df, recent_months=12):
    """
    PCE 데이터 요약 통계 생성
    
    Parameters:
    - df: PCE 데이터프레임
    - recent_months: 최근 분석 기간 (월)
    
    Returns:
    - dict: 요약 통계
    """
    if df is None or len(df) == 0:
        return None
    
    stats = {}
    recent_data = df.tail(recent_months)
    
    # 주요 지표별 최신값 및 변화율
    for col in df.columns:
        if col in PCE_MAIN_SERIES.keys():
            latest_value = df[col].iloc[-1] if not pd.isna(df[col].iloc[-1]) else None
            mom_change = calculate_mom_change(df[col]).iloc[-1] if len(df) > 1 else None
            yoy_change = calculate_yoy_change(df[col]).iloc[-1] if len(df) >= 12 else None
            
            stats[col] = {
                'latest_value': latest_value,
                'mom_change': mom_change,
                'yoy_change': yoy_change,
                'avg_recent': recent_data[col].mean() if len(recent_data) > 0 else None,
                'volatility': recent_data[col].std() if len(recent_data) > 1 else None
            }
    
    # 최신 업데이트 정보
    stats['metadata'] = {
        'latest_date': df.index[-1].strftime('%Y-%m'),
        'data_points': len(df),
        'start_date': df.index[0].strftime('%Y-%m'),
        'analysis_period': f"{recent_months}개월"
    }
    
    return stats

print("✅ PCE 핵심 지표 계산 함수 정의 완료")

# %%
# === 다양한 자유도 높은 시각화 함수들 ===

def plot_pce_overview(df):
    """
    PCE 개요 차트 (개선된 버전)
    """
    if df is None or len(df) == 0:
        print("❌ 시각화할 데이터가 없습니다")
        return None
    
    # 최신 36개월 데이터 (3년 트렌드)
    chart_data = df.tail(36).copy()
    
    # 사용 가능한 컬럼 확인
    available_cols = []
    col_labels = {}
    
    if 'pce_total' in df.columns:
        available_cols.append('pce_total')
        col_labels['pce_total'] = '총 PCE'
    if 'pce_core' in df.columns:
        available_cols.append('pce_core')
        col_labels['pce_core'] = '핵심 PCE'
    if 'pce_goods' in df.columns:
        available_cols.append('pce_goods')
        col_labels['pce_goods'] = '상품 소비'
    if 'pce_services' in df.columns:
        available_cols.append('pce_services')
        col_labels['pce_services'] = '서비스 소비'
        
    if not available_cols:
        print("Warning: 표시할 PCE 지표가 없습니다.")
        return None
    
    # 라벨 딕셔너리 생성
    labels_dict = {col: col_labels[col] for col in available_cols}
    
    print("미국 개인소비지출 주요 지표 트렌드 (최근 3년)")
    
    fig = df_multi_line_chart(
        df=chart_data,
        columns=available_cols,
        ytitle="% 변화율",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_pce_components(df):
    """
    PCE 구성요소별 차트 (다양한 시각화)
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
        
    # 최신 24개월 데이터
    chart_data = df.tail(24).copy()
    
    # 사용 가능한 컬럼 확인
    available_cols = []
    col_labels = {}
    
    if 'pce_goods' in df.columns:
        available_cols.append('pce_goods')
        col_labels['pce_goods'] = '상품 소비'
    if 'pce_services' in df.columns:
        available_cols.append('pce_services')
        col_labels['pce_services'] = '서비스 소비'
    if 'pce_durable' in df.columns:
        available_cols.append('pce_durable')
        col_labels['pce_durable'] = '내구재'
    if 'pce_nondurable' in df.columns:
        available_cols.append('pce_nondurable')
        col_labels['pce_nondurable'] = '비내구재'
        
    if not available_cols:
        print("Warning: 표시할 PCE 구성요소가 없습니다.")
        return None
    
    labels_dict = {col: col_labels[col] for col in available_cols}
    
    print("미국 PCE 구성요소 분석 (최근 2년)")
    
    fig = df_multi_line_chart(
        df=chart_data,
        columns=available_cols,
        ytitle="% 변화율",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_pce_core_vs_headline(df):
    """
    PCE 헤드라인 vs 코어 비교 차트
    """
    if df is None:
        print("❌ 데이터가 없습니다")
        return None
        
    # 최신 24개월 데이터
    chart_data = df.tail(24).copy()
    
    # 컬럼 설정
    available_cols = []
    col_labels = {}
    
    if 'pce_total' in df.columns:
        available_cols.append('pce_total')
        col_labels['pce_total'] = '헤드라인 PCE'
        
    if 'pce_core' in df.columns:
        available_cols.append('pce_core')
        col_labels['pce_core'] = '코어 PCE'
    
    if not available_cols:
        print("❌ PCE 헤드라인/코어 데이터가 없습니다")
        return None
        
    labels_dict = {col: col_labels[col] for col in available_cols}
    
    print("미국 PCE: 헤드라인 vs 코어 비교 (최근 2년)")
    
    fig = df_multi_line_chart(
        df=chart_data,
        columns=available_cols,
        ytitle="% 변화율",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_pce_price_analysis(df):
    """
    PCE 가격지수 분석 차트
    """
    if df is None:
        print("❌ 데이터가 없습니다")
        return None
        
    # 최신 36개월 데이터 (3년 트렌드)
    chart_data = df.tail(36).copy()
    
    # 가격지수 컬럼들
    price_cols = []
    col_labels = {}
    
    if 'pce_price_headline' in df.columns:
        price_cols.append('pce_price_headline')
        col_labels['pce_price_headline'] = 'PCE 가격지수'
    if 'pce_price_core' in df.columns:
        price_cols.append('pce_price_core')
        col_labels['pce_price_core'] = '코어 PCE 가격지수'
    if 'pce_price_goods' in df.columns:
        price_cols.append('pce_price_goods')
        col_labels['pce_price_goods'] = '상품 가격'
    if 'pce_price_services' in df.columns:
        price_cols.append('pce_price_services')
        col_labels['pce_price_services'] = '서비스 가격'
    
    if not price_cols:
        print("Warning: PCE 가격지수 데이터가 없습니다.")
        return None
    
    labels_dict = {col: col_labels[col] for col in price_cols}
    
    print("미국 PCE 가격지수 분석 (최근 3년)")
    
    fig = df_multi_line_chart(
        df=chart_data,
        columns=price_cols,
        ytitle="% 변화율",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_pce_mom_changes(df):
    """
    PCE MoM 변화율 차트 (데이터가 이미 MoM 변화율이므로 추가 계산 없이 직접 사용)
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
        
    # 데이터가 이미 MoM 변화율이므로 추가 계산 없이 직접 사용
    # 최신 24개월 데이터
    chart_data = df.tail(24)
    
    # 주요 지표들
    main_cols = []
    col_labels = {}
    
    if 'pce_total' in df.columns:
        main_cols.append('pce_total')
        col_labels['pce_total'] = '총 PCE (MoM)'
    if 'pce_core' in df.columns:
        main_cols.append('pce_core')
        col_labels['pce_core'] = '코어 PCE (MoM)'
    
    if not main_cols:
        print("Warning: 표시할 PCE MoM 지표가 없습니다.")
        return None
    
    labels_dict = {col: col_labels[col] for col in main_cols}
    
    print("미국 PCE 월별 변화율 (최근 2년)")
    
    fig = df_multi_line_chart(
        df=chart_data,
        columns=main_cols,
        ytitle="% (MoM)",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_pce_series_selection(df, series_list, chart_type='level', periods=24):
    """
    원하는 시리즈를 원하는 형태(MoM, YoY, Level)로 시계열로 그리는 자유도 높은 함수
    
    Args:
        df: PCE 데이터프레임
        series_list: 그릴 시리즈 리스트
        chart_type: 'level' (raw data), 'mom' (전월대비), 'yoy' (전년동월대비)
        title: 차트 제목
        periods: 표시할 기간 (개월)
    
    Returns:
        plotly.graph_objects.Figure
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
    
    # 시리즈 유효성 확인
    available_series = [s for s in series_list if s in df.columns]
    if not available_series:
        print(f"❌ 요청한 시리즈가 데이터에 없습니다: {series_list}")
        return None
    
    # 데이터 준비
    chart_data = df[available_series].tail(periods).copy()
    
    # 차트 타입에 따른 데이터 변환
    # 주의: PCE 데이터는 이미 MoM 변화율이므로 'level'이 실제MoM임
    if chart_type == 'mom':
        # 데이터가 이미 MoM이므로 추가 계산 없이 사용
        ytitle = "% (MoM)"
        print(f"PCE 월별 변화율 (최근 {periods//12:.0f}년)")
    elif chart_type == 'yoy':
        # YoY는 12개월 이동평균으로 계산
        chart_data = chart_data.rolling(window=12).mean()
        ytitle = "% (12M MA)"
        print(f"PCE 12개월 이동평균 (최근 {periods//12:.0f}년)")
    else:  # level (실은 MoM)
        ytitle = "% (MoM)"
        print(f"PCE 주요 지표 (최근 {periods//12:.0f}년)")
    
    # 라벨 설정 (한국어 매핑)
    korean_labels = {
        'pce_total': '총 PCE',
        'pce_core': '코어 PCE',
        'pce_goods': '상품 소비',
        'pce_services': '서비스 소비',
        'pce_durable': '내구재',
        'pce_nondurable': '비내구재',
        'pce_food': '식품',
        'pce_energy': '에너지',
        'pce_price_headline': 'PCE 가격지수',
        'pce_price_core': '코어 PCE 가격지수'
    }
    
    labels_dict = {col: korean_labels.get(col, col) for col in available_series}
    
    # KPDS 라인 차트 생성
    fig = df_multi_line_chart(
        df=chart_data,
        columns=available_series,
        ytitle=ytitle,
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_pce_latest_bar_chart(df, series_list=None, chart_type='mom'):
    """
    가장 최신 데이터를 기준으로 구성 요소별 변화를 바 그래프로 시각화 (레이블 잘림 방지 개선)
    
    Args:
        df: PCE 데이터프레임
        series_list: 표시할 시리즈 리스트 (None이면 주요 지표)
        chart_type: 'mom' (전월대비) 또는 'yoy' (전년동월대비)
        title: 차트 제목
    
    Returns:
        plotly.graph_objects.Figure
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
    
    # 기본 시리즈 설정
    if series_list is None:
        series_list = ['pce_total', 'pce_core', 'pce_goods', 'pce_services']
    
    # 시리즈 유효성 확인
    available_series = [s for s in series_list if s in df.columns]
    if not available_series:
        print(f"❌ 요청한 시리즈가 데이터에 없습니다: {series_list}")
        return None
    
    # 데이터 변환 (데이터가 이미 MoM 변화율이므로 추가 계산 없이 사용)
    if chart_type == 'mom':
        # 데이터가 이미 MoM 변화율이므로 최신값 그대로 사용
        change_data = df[available_series].iloc[-1]
        ytitle = "MoM 변화율 (%)"
        latest_date = df.index[-1].strftime('%Y년 %m월')
        print(f"PCE 구성요소 MoM 변화율 ({latest_date})")
    else:  # yoy (12개월 이동평균)
        # 12개월 이동평균으로 YoY 유사 효과
        ma_data = df[available_series].rolling(window=12).mean()
        change_data = ma_data.iloc[-1]
        ytitle = "12개월 이동평균 (%)"
        latest_date = df.index[-1].strftime('%Y년 %m월')
        print(f"PCE 구성요소 12개월 마 ({latest_date})")
    
    # 라벨 설정
    korean_labels = {
        'pce_total': '총 PCE',
        'pce_core': '코어 PCE',
        'pce_goods': '상품 소비',
        'pce_services': '서비스 소비',
        'pce_durable': '내구재',
        'pce_nondurable': '비내구재',
        'pce_food': '식품',
        'pce_energy': '에너지'
    }
    
    categories = [korean_labels.get(col, col) for col in available_series]
    values = [change_data[col] for col in available_series]
    
    # 색상 설정 (양수/음수에 따라)
    colors = [get_kpds_color(0) if v >= 0 else get_kpds_color(1) for v in values]
    
    # 가로 바 차트 생성
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=categories,
        x=values,
        orientation='h',
        marker_color=colors,
        text=[f'{v:+.2f}%' for v in values],
        textposition='outside' if all(v >= 0 for v in values) else 'auto',  # 개선: 조건부 텍스트 위치
        showlegend=False
    ))
    
    # 레이아웃 설정 (개선된 여백 및 크기 조정)
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(family='NanumGothic', size=FONT_SIZE_TITLE),
            x=0.5, xanchor="center"
        ),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=900,
        height=max(500, len(categories) * 50),  # 개선: 동적 높이 계산 증가
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color="black"),
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            title=dict(text=ytitle, font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE)),
            range=[min(-1, min(values) * 1.1), max(values) * 1.2]  # 개선: 동적 x축 범위
        ),
        yaxis=dict(
            showline=False,
        ),
        margin=dict(l=200, r=100, t=80, b=80)  # 개선: 여백 최적화
    )
    
    # 격자선 및 0선
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="lightgrey")
    fig.update_yaxes(showgrid=False)
    fig.add_vline(x=0, line_width=2, line_color="black")
    
    return fig

def create_pce_custom_dashboard(df, periods=24):
    """
    PCE 데이터를 위한 커스텀 대시보드 (여러 차트를 서브플롯으로 결합)
    
    Args:
        df: PCE 데이터프레임
        periods: 표시할 기간 (개월)
    
    Returns:
        plotly subplots Figure
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
    
    from plotly.subplots import make_subplots
    
    # 서브플롯 생성 (2x2 그리드)
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=[
            "주요 PCE 지표 트렌드",
            "MoM 변화율",
            "헤드라인 vs 코어 PCE",
            "최신 MoM 변화율"
        ],
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"type": "bar"}]]
    )
    
    chart_data = df.tail(periods)
    
    # 1. 주요 PCE 지표 (1,1)
    if 'pce_total' in df.columns:
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=chart_data['pce_total'],
                      name='총 PCE', line=dict(color=get_kpds_color(0))),
            row=1, col=1
        )
    if 'pce_goods' in df.columns:
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=chart_data['pce_goods'],
                      name='상품 소비', line=dict(color=get_kpds_color(1))),
            row=1, col=1
        )
    if 'pce_services' in df.columns:
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=chart_data['pce_services'],
                      name='서비스 소비', line=dict(color=get_kpds_color(2))),
            row=1, col=1
        )
    
    # 2. MoM 변화율 (1,2) - 데이터가 이미 MoM이므로 그대로 사용
    if 'pce_total' in df.columns:
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=chart_data['pce_total'],
                      name='총 PCE (MoM)', line=dict(color=get_kpds_color(0))),
            row=1, col=2
        )
    if 'pce_core' in df.columns:
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=chart_data['pce_core'],
                      name='코어 PCE (MoM)', line=dict(color=get_kpds_color(1))),
            row=1, col=2
        )
    
    # 3. 헤드라인 vs 코어 PCE (2,1)
    if 'pce_total' in df.columns:
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=chart_data['pce_total'],
                      name='헤드라인 PCE', line=dict(color=get_kpds_color(0))),
            row=2, col=1
        )
    if 'pce_core' in df.columns:
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=chart_data['pce_core'],
                      name='코어 PCE', line=dict(color=get_kpds_color(1))),
            row=2, col=1
        )
    
    # 4. 최신 MoM 변화율 바 차트 (2,2) - 데이터가 이미 MoM
    latest_values = chart_data.iloc[-1]
    series_to_show = ['pce_total', 'pce_core', 'pce_goods', 'pce_services']
    available_series = [s for s in series_to_show if s in latest_values.index]
    
    if available_series:
        korean_names = {
            'pce_total': '총 PCE',
            'pce_core': '코어 PCE',
            'pce_goods': '상품',
            'pce_services': '서비스'
        }
        
        categories = [korean_names.get(s, s) for s in available_series]
        values = [latest_values[s] for s in available_series]
        colors = [get_kpds_color(i) for i in range(len(values))]
        
        fig.add_trace(
            go.Bar(x=categories, y=values, marker_color=colors,
                  text=[f'{v:+.2f}%' for v in values], textposition='outside'),
            row=2, col=2
        )
    
    # 레이아웃 업데이트
    fig.update_layout(
        title_text=f"미국 PCE 종합 대시보드 (최근 {periods//12:.0f}년)",
        title_x=0.5,
        height=800,
        showlegend=False,
        font=dict(family='NanumGothic', size=10)
    )
    
    # 각 서브플롯의 y축 레이블
    fig.update_yaxes(title_text="% (MoM)", row=1, col=1)
    fig.update_yaxes(title_text="% (MoM)", row=1, col=2)
    fig.update_yaxes(title_text="% (MoM)", row=2, col=1)
    fig.update_yaxes(title_text="% (MoM)", row=2, col=2)
    
    return fig

print("✅ 고급 자유도 높은 시각화 함수들 정의 완료")

# %%
# === CPI 분석에서 가져온 추가 시각화 함수들 ===

def create_horizontal_pce_bar_chart(df, num_categories=15):
    """
    PCE 구성요소별 MoM 변화율 가로 바 차트 생성 (레이블 잘림 방지 완전 해결)
    
    Args:
        df: PCE 데이터프레임
        title: 차트 제목
        num_categories: 표시할 카테고리 수
    
    Returns:
        plotly figure
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
    
    # 최신 데이터 가져오기
    latest_data = df.iloc[-1].dropna()
    
    # PCE 카테고리 한국어 라벨
    category_labels = {
        'pce_total': '총 PCE',
        'pce_core': '코어 PCE\n(식품·에너지 제외)',
        'pce_goods': '상품 소비',
        'pce_services': '서비스 소비',
        'pce_durable': '내구재',
        'pce_nondurable': '비내구재',
        'pce_food': '식품',
        'pce_energy': '에너지',
        'pce_price_headline': 'PCE 가격지수',
        'pce_price_core': '코어 PCE 가격지수',
        'pce_price_goods': '상품 가격',
        'pce_price_services': '서비스 가격',
        'personal_income': '개인소득',
        'disposable_income': '가처분소득',
        'saving_rate': '개인저축률'
    }
    
    # 데이터 정렬 (내림차순)
    sorted_data = latest_data.sort_values(ascending=True)  # 가로 바차트이므로 ascending=True
    
    # 상위 num_categories개 선택
    if len(sorted_data) > num_categories:
        sorted_data = sorted_data.tail(num_categories)
    
    # 라벨 적용
    categories = []
    values = []
    colors = []
    
    for comp, value in sorted_data.items():
        label = category_labels.get(comp, comp)
        categories.append(label)
        values.append(value)
        
        # '총 PCE'는 특별 색상, 나머지는 양수/음수로 구분
        if comp == 'pce_total':
            colors.append('#FFA500')  # 주황색
        elif value >= 0:
            colors.append(get_kpds_color(0))  # 상승: KPDS 색상
        else:
            colors.append(get_kpds_color(1))  # 하락: KPDS 색상
    
    # 가로 바 차트 생성
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=categories,
        x=values,
        orientation='h',
        marker_color=colors,
        text=[f'{v:.1f}%' for v in values],
        textposition='outside' if all(v >= 0 for v in values) else 'auto',
        showlegend=False
    ))
    
    # 레이아웃 설정 (CPI에서 검증된 최적 설정)
    fig.update_layout(
        title=dict(
            text=title or "PCE 구성요소별 월간 변화율 (전월대비 %)",
            font=dict(family='NanumGothic', size=FONT_SIZE_TITLE),
            x=0.5, xanchor="center"
        ),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=900,
        height=max(500, len(categories) * 25),  # 동적 높이 계산
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL-1, color="black"),
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            tickformat='.1f',
            range=[min(-1, min(values) * 1.1), max(values) * 1.2]  # 레이블 공간 확보
        ),
        yaxis=dict(
            showline=False,
            tickcolor='white'
        ),
        margin=dict(l=250, r=80, t=80, b=60)  # 최적화된 여백 설정
    )
    
    # 격자선 및 0선
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="lightgrey")
    fig.update_yaxes(showgrid=False)
    fig.add_vline(x=0, line_width=2, line_color="black")
    
    return fig

def create_pce_table_matplotlib(df, periods=6):
    """
    PCE 상승률 테이블을 Matplotlib으로 이미지 생성
    
    Args:
        df: PCE 데이터프레임
        periods: 표시할 기간 (개월)
    
    Returns:
        matplotlib figure: 테이블 이미지
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("❌ matplotlib이 설치되지 않았습니다. pip install matplotlib")
        return None
    
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
    
    # 최근 periods개월 데이터
    recent_data = df.tail(periods)
    
    # 한국어 카테고리 매핑
    categories_kr = {
        'pce_total': '총 PCE',
        'pce_core': '코어 PCE (식품·에너지 제외)',
        'pce_goods': '상품 소비',
        'pce_services': '서비스 소비',
        'pce_durable': '내구재',
        'pce_nondurable': '비내구재',
        'pce_food': '식품',
        'pce_energy': '에너지',
        'pce_price_headline': 'PCE 가격지수',
        'pce_price_core': '코어 PCE 가격지수'
    }
    
    # 테이블 데이터 생성
    table_data = []
    months = recent_data.index
    month_cols = [month.strftime('%Y년 %m월') for month in months]
    
    # 헤더 추가
    headers = ['전월 대비 상승률 (계절조정)'] + month_cols
    
    for comp in categories_kr.keys():
        if comp in recent_data.columns:
            row = [categories_kr[comp]]
            
            # 최근 기간 데이터
            for month in months:
                if month in recent_data.index and not pd.isna(recent_data.loc[month, comp]):
                    value = recent_data.loc[month, comp]
                    row.append(f"{value:.1f}")
                else:
                    row.append("-")
            
            table_data.append(row)
    
    # Matplotlib 테이블 생성
    fig, ax = plt.subplots(figsize=(14, 10))
    ax.axis('tight')
    ax.axis('off')
    
    # 테이블 생성
    table = ax.table(cellText=table_data, colLabels=headers, 
                    cellLoc='center', loc='center')
    
    # 테이블 스타일링
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.2, 1.8)
    
    # 헤더 스타일
    for i in range(len(headers)):
        table[(0, i)].set_facecolor('#E6E6FA')
        table[(0, i)].set_text_props(weight='bold')
    
    # 값에 따른 색상 적용
    for i, row in enumerate(table_data):
        for j, cell in enumerate(row[1:], 1):  # 첫 번째 열(카테고리명) 제외
            try:
                value = float(cell)
                if value > 0:
                    table[(i+1, j)].set_facecolor('#FFE4E1')  # 연한 빨간색
                elif value < 0:
                    table[(i+1, j)].set_facecolor('#E0F6FF')  # 연한 파란색
            except:
                pass
    
    plt.title('PCE 전월 대비 상승률', fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()
    plt.show()
    
    return fig

def create_pce_category_deep_dive(df, category='goods', periods=24):
    """
    PCE 카테고리별 심화 분석 (상품/서비스 등)
    
    Args:
        df: PCE 데이터프레임
        category: 분석할 카테고리 ('goods', 'services', 'core' 등)
        periods: 분석할 기간 (개월)
        title: 차트 제목
    
    Returns:
        plotly subplots Figure
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
    
    from plotly.subplots import make_subplots
    
    # 카테고리별 관련 시리즈 정의
    category_series = {
        'goods': ['pce_goods', 'pce_durable', 'pce_nondurable'],
        'services': ['pce_services'],
        'core': ['pce_core', 'pce_total'],
        'prices': ['pce_price_headline', 'pce_price_core', 'pce_price_goods', 'pce_price_services']
    }
    
    if category not in category_series:
        print(f"❌ 지원하지 않는 카테고리: {category}")
        return None
    
    # 해당 카테고리의 시리즈들
    target_series = category_series[category]
    available_series = [s for s in target_series if s in df.columns]
    
    if not available_series:
        print(f"❌ {category} 카테고리의 데이터가 없습니다")
        return None
    
    # 서브플롯 생성 (2x2)
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=[
            f"{category.title()} 시계열",
            "3개월 이동평균",
            "최근 12개월 분포",
            "월별 변화율"
        ],
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"type": "box"}, {"type": "bar"}]]
    )
    
    chart_data = df[available_series].tail(periods)
    
    # 한국어 라벨
    korean_labels = {
        'pce_goods': '상품',
        'pce_services': '서비스',
        'pce_durable': '내구재',
        'pce_nondurable': '비내구재',
        'pce_core': '코어 PCE',
        'pce_total': '총 PCE'
    }
    
    # 1. 시계열 (1,1)
    for i, series in enumerate(available_series):
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=chart_data[series],
                      name=korean_labels.get(series, series),
                      line=dict(color=get_kpds_color(i))),
            row=1, col=1
        )
    
    # 2. 3개월 이동평균 (1,2)
    ma_data = chart_data.rolling(window=3).mean()
    for i, series in enumerate(available_series):
        fig.add_trace(
            go.Scatter(x=ma_data.index, y=ma_data[series],
                      name=f"{korean_labels.get(series, series)} (3M MA)",
                      line=dict(color=get_kpds_color(i), dash='dash')),
            row=1, col=2
        )
    
    # 3. 최근 12개월 분포 (2,1)
    recent_12m = chart_data.tail(12)
    for i, series in enumerate(available_series):
        fig.add_trace(
            go.Box(y=recent_12m[series],
                  name=korean_labels.get(series, series),
                  marker_color=get_kpds_color(i)),
            row=2, col=1
        )
    
    # 4. 최신 월별 변화율 바 차트 (2,2)
    if available_series:
        latest_values = chart_data.iloc[-1]
        categories = [korean_labels.get(s, s) for s in available_series]
        values = [latest_values[s] for s in available_series]
        colors = [get_kpds_color(i) for i in range(len(values))]
        
        fig.add_trace(
            go.Bar(x=categories, y=values, marker_color=colors,
                  text=[f'{v:+.2f}%' for v in values], textposition='outside'),
            row=2, col=2
        )
    
    if title is None:
        title = f"PCE {category.title()} 카테고리 심화 분석 (최근 {periods//12:.0f}년)"
    
    fig.update_layout(
        title_text=title,
        title_x=0.5,
        height=800,
        showlegend=False,
        font=dict(family='NanumGothic', size=10)
    )
    
    # Y축 레이블
    fig.update_yaxes(title_text="% (MoM)", row=1, col=1)
    fig.update_yaxes(title_text="% (3M MA)", row=1, col=2)
    fig.update_yaxes(title_text="% (MoM)", row=2, col=1)
    fig.update_yaxes(title_text="% (MoM)", row=2, col=2)
    
    return fig

print("✅ CPI에서 가져온 추가 시각화 함수들 정의 완료")

# %%  
# === 간편 사용 함수들 ===

def quick_pce_chart(series_list, chart_type='level', periods=24):
    """
    빠른 PCE 차트 생성 (데이터 자동 로드)
    
    Args:
        series_list: 그릴 시리즈 리스트
        chart_type: 'level', 'mom', 'yoy'
        periods: 표시할 기간 (개월)
    
    Examples:
        quick_pce_chart(['pce_total', 'pce_core'], 'level', 24)
        quick_pce_chart(['pce_total'], 'mom', 12)
    """
    # 기존 데이터 로드 또는 새로 로드
    df = load_pce_data('pce_data_complete.csv')
    if df is None:
        print("데이터를 새로 수집합니다...")
        df = fetch_pce_dataset(PCE_MAIN_SERIES, start_date='2019-01-01')
        if df is not None:
            save_pce_data(df, 'pce_data_complete.csv')
    
    if df is None:
        print("❌ 데이터 로드 실패")
        return None
    
    return plot_pce_series_selection(df, series_list, chart_type, periods=periods)

def quick_pce_bar():
    """최신 MoM 변화율 바 차트 빠른 생성"""
    df = load_pce_data('pce_data_complete.csv')
    if df is None:
        print("데이터를 새로 수집합니다...")
        df = fetch_pce_dataset(PCE_MAIN_SERIES, start_date='2019-01-01')
        if df is not None:
            save_pce_data(df, 'pce_data_complete.csv')
    
    if df is None:
        print("❌ 데이터 로드 실패")
        return None
    
    return plot_pce_latest_bar_chart(df, chart_type='mom')

def quick_pce_dashboard():
    """커스텀 대시보드 빠른 생성"""
    df = load_pce_data('pce_data_complete.csv')
    if df is None:
        print("데이터를 새로 수집합니다...")
        df = fetch_pce_dataset(PCE_MAIN_SERIES, start_date='2019-01-01')
        if df is not None:
            save_pce_data(df, 'pce_data_complete.csv')
    
    if df is None:
        print("❌ 데이터 로드 실패")
        return None
    
    return create_pce_custom_dashboard(df)

def get_available_pce_series():
    """사용 가능한 PCE 시리즈 목록 출력"""
    print("=== 사용 가능한 Personal Consumption Expenditures 시리즈 ===")
    print("\\n📊 핵심 PCE 지표 (실질 소비):")
    core_series = {
        'pce_total': 'Real PCE - total',
        'pce_core': 'PCE ex-food & energy',
        'pce_goods': 'Goods',
        'pce_services': 'Services',
        'pce_durable': 'Durable goods',
        'pce_nondurable': 'Nondurable goods'
    }
    
    for key, desc in core_series.items():
        print(f"  '{key}': {desc}")
    
    print("\\n💰 PCE 가격지수:")
    price_series = {
        'pce_price_headline': 'PCE Price index (headline)',
        'pce_price_core': 'Core PCE (ex-food & energy)',
        'pce_price_goods': 'Goods prices',
        'pce_price_services': 'Services prices',
        'pce_price_food': 'Food prices',
        'pce_price_energy': 'Energy prices'
    }
    
    for key, desc in price_series.items():
        print(f"  '{key}': {desc}")
    
    print("\\n🏪 세부 카테고리:")
    detail_series = {
        'pce_food': 'Food',
        'pce_energy': 'Energy goods & services',
        'pce_market_based': 'Market-based PCE',
        'pce_market_core': 'Market-based PCE ex-food & energy'
    }
    
    for key, desc in detail_series.items():
        print(f"  '{key}': {desc}")

print("✅ 간편 사용 함수들 정의 완료")

# %%
# === 고급 자유도 높은 추가 시각화 함수들 ===

def create_pce_timeseries_chart(series_names=None, chart_type='level', periods=36):
    """
    원하는 PCE 시리즈들을 시계열로 그리는 고급 함수
    
    Args:
        series_names: 그릴 시리즈 리스트 (None이면 주요 지표)
        chart_type: 'level' (MoM 그대로), 'yoy' (12개월 이동평균), 'trend' (3개월 이동평균)
        periods: 표시할 기간 (개월)
        title: 차트 제목
    
    Returns:
        plotly.graph_objects.Figure
    """
    # 데이터 로드
    df = load_pce_data('pce_data_complete.csv')
    if df is None:
        print("데이터를 새로 수집합니다...")
        df = fetch_pce_dataset(PCE_MAIN_SERIES, start_date='2019-01-01')
        if df is not None:
            save_pce_data(df, 'pce_data_complete.csv')
    
    if df is None:
        print("❌ 데이터 로드 실패")
        return None
    
    # 기본 시리즈 설정
    if series_names is None:
        series_names = ['pce_total', 'pce_core', 'pce_goods', 'pce_services']
    
    # 시리즈 유효성 확인
    available_series = [s for s in series_names if s in df.columns]
    if not available_series:
        print(f"❌ 요청한 시리즈가 데이터에 없습니다: {series_names}")
        return None
    
    # 데이터 준비
    chart_data = df[available_series].tail(periods).copy()
    
    # 차트 타입에 따른 데이터 변환
    if chart_type == 'yoy':
        chart_data = chart_data.rolling(window=12).mean()
        ytitle = "% (12개월 MA)"
        if title is None:
            title = f"PCE 12개월 이동평균 트렌드 (최근 {periods//12:.0f}년)"
    elif chart_type == 'trend':
        chart_data = chart_data.rolling(window=3).mean()
        ytitle = "% (3개월 MA)"
        if title is None:
            title = f"PCE 3개월 이동평균 트렌드 (최근 {periods//12:.0f}년)"
    else:  # level (실제로는 MoM)
        ytitle = "% (MoM)"
        if title is None:
            title = f"PCE 월별 변화율 (최근 {periods//12:.0f}년)"
    
    # 한국어 라벨 매핑
    korean_labels = {
        'pce_total': '총 PCE',
        'pce_core': '코어 PCE',
        'pce_goods': '상품 소비',
        'pce_services': '서비스 소비',
        'pce_durable': '내구재',
        'pce_nondurable': '비내구재',
        'pce_food': '식품',
        'pce_energy': '에너지',
        'pce_price_headline': 'PCE 가격지수',
        'pce_price_core': '코어 PCE 가격지수',
        'pce_price_goods': '상품 가격',
        'pce_price_services': '서비스 가격'
    }
    
    labels_dict = {col: korean_labels.get(col, col) for col in available_series}
    
    # KPDS 멀티라인 차트 생성
    fig = df_multi_line_chart(
        df=chart_data,
        columns=available_series,
        ytitle=ytitle,
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def create_pce_dual_axis_chart(left_series=None, right_series=None, periods=24):
    """
    df_dual_axis_chart를 사용하여 PCE 시리즈들을 듀얼축으로 표시
    
    Args:
        left_series: 왼쪽 축에 표시할 시리즈 리스트
        right_series: 오른쪽 축에 표시할 시리즈 리스트
        periods: 표시할 기간 (개월)
        title: 차트 제목
    
    Returns:
        plotly.graph_objects.Figure
    """
    # 데이터 로드
    df = load_pce_data('pce_data_complete.csv')
    if df is None:
        print("데이터를 새로 수집합니다...")
        df = fetch_pce_dataset(PCE_MAIN_SERIES, start_date='2019-01-01')
        if df is not None:
            save_pce_data(df, 'pce_data_complete.csv')
    
    if df is None:
        print("❌ 데이터 로드 실패")
        return None
    
    # 기본 설정
    if left_series is None:
        left_series = ['pce_total', 'pce_core']
    if right_series is None:
        right_series = ['pce_price_headline', 'pce_price_core']
    
    # 시리즈 유효성 확인
    available_left = [s for s in left_series if s in df.columns]
    available_right = [s for s in right_series if s in df.columns]
    
    if not available_left and not available_right:
        print("❌ 표시할 시리즈가 없습니다")
        return None
    
    # 데이터 준비
    chart_data = df.tail(periods).copy()
    
    # 한국어 라벨 매핑
    korean_labels = {
        'pce_total': '총 PCE',
        'pce_core': '코어 PCE',
        'pce_goods': '상품 소비',
        'pce_services': '서비스 소비',
        'pce_price_headline': 'PCE 가격지수',
        'pce_price_core': '코어 PCE 가격지수',
        'pce_price_goods': '상품 가격',
        'pce_price_services': '서비스 가격'
    }
    
    # 라벨 리스트 생성
    left_labels = [korean_labels.get(col, col) for col in available_left] if available_left else []
    right_labels = [korean_labels.get(col, col) for col in available_right] if available_right else []
    
    print(f"PCE 듀얼축 비교 (최근 {periods//12:.0f}년)")
    
    # KPDS 듀얼축 차트 생성
    fig = df_dual_axis_chart(
        df=chart_data,
        left_cols=available_left if available_left else None,
        right_cols=available_right if available_right else None,
        left_title="실질 소비 (% MoM)",
        right_title="가격지수 (% MoM)",
        left_labels=left_labels,
        right_labels=right_labels,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def create_pce_contribution_chart(periods=24):
    """
    PCE 변화에 대한 구성요소별 기여도 분석 차트
    
    Args:
        title: 차트 제목
        periods: 분석할 기간 (개월)
    
    Returns:
        plotly.graph_objects.Figure
    """
    # 데이터 로드
    df = load_pce_data('pce_data_complete.csv')
    if df is None:
        print("데이터를 새로 수집합니다...")
        df = fetch_pce_dataset(PCE_MAIN_SERIES, start_date='2019-01-01')
        if df is not None:
            save_pce_data(df, 'pce_data_complete.csv')
    
    if df is None:
        print("❌ 데이터 로드 실패")
        return None
    
    # 최근 데이터
    recent_data = df.tail(periods)
    
    # 구성요소별 평균 기여도 계산 (대략적 가중치 사용)
    components = {
        'pce_goods': ('상품 소비', 0.4),      # 약 40% 가중치
        'pce_services': ('서비스 소비', 0.6),  # 약 60% 가중치
    }
    
    # 세부 구성요소도 포함
    if 'pce_durable' in df.columns and 'pce_nondurable' in df.columns:
        components = {
            'pce_durable': ('내구재', 0.12),
            'pce_nondurable': ('비내구재', 0.28),
            'pce_services': ('서비스', 0.6)
        }
    
    # 최신 몇 개월의 평균 기여도 계산
    latest_periods = 3  # 최근 3개월
    contribution_data = {}
    
    for series, (label, weight) in components.items():
        if series in recent_data.columns:
            # 최근 기간의 평균값에 가중치를 적용하여 기여도 계산
            avg_value = recent_data[series].tail(latest_periods).mean()
            contribution = avg_value * weight
            contribution_data[label] = contribution
    
    if not contribution_data:
        print("❌ 기여도 계산을 위한 데이터가 없습니다")
        return None
    
    # 데이터 정렬 (절댓값 기준)
    sorted_items = sorted(contribution_data.items(), key=lambda x: abs(x[1]), reverse=True)
    
    categories = [item[0] for item in sorted_items]
    values = [item[1] for item in sorted_items]
    
    # 색상 설정 (양수/음수)
    colors = [get_kpds_color(0) if v >= 0 else get_kpds_color(1) for v in values]
    
    # 가로 바 차트 생성
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=categories,
        x=values,
        orientation='h',
        marker_color=colors,
        text=[f'{v:+.3f}%p' for v in values],
        textposition='outside',
        showlegend=False
    ))
    
    # 제목 설정
    if title is None:
        title = f"PCE 구성요소별 기여도 분석 (최근 {latest_periods}개월 평균)"
    
    # 레이아웃 설정
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(family='NanumGothic', size=FONT_SIZE_TITLE),
            x=0.5, xanchor="center"
        ),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=900,
        height=max(400, len(categories) * 50),
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color="black"),
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            title=dict(text="기여도 (percentage point)", 
                      font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE))
        ),
        yaxis=dict(
            showline=False,
        ),
        margin=dict(l=180, r=100, t=80, b=80)
    )
    
    # 격자선 및 0선
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="lightgrey")
    fig.update_yaxes(showgrid=False)
    fig.add_vline(x=0, line_width=2, line_color="black")
    
    return fig

def create_pce_heatmap_chart(periods=12):
    """
    PCE 구성요소들의 히트맵 차트 (월별 변화율 패턴)
    
    Args:
        periods: 표시할 기간 (개월)
        title: 차트 제목
    
    Returns:
        plotly.graph_objects.Figure
    """
    # 데이터 로드
    df = load_pce_data('pce_data_complete.csv')
    if df is None:
        print("데이터를 새로 수집합니다...")
        df = fetch_pce_dataset(PCE_MAIN_SERIES, start_date='2019-01-01')
        if df is not None:
            save_pce_data(df, 'pce_data_complete.csv')
    
    if df is None:
        print("❌ 데이터 로드 실패")
        return None
    
    # 주요 PCE 구성요소 선택
    pce_components = ['pce_total', 'pce_core', 'pce_goods', 'pce_services']
    if 'pce_durable' in df.columns:
        pce_components.extend(['pce_durable', 'pce_nondurable'])
    if 'pce_food' in df.columns:
        pce_components.extend(['pce_food', 'pce_energy'])
    
    available_components = [c for c in pce_components if c in df.columns]
    
    # 최근 데이터
    recent_data = df[available_components].tail(periods)
    
    # 한국어 라벨
    korean_labels = {
        'pce_total': '총 PCE',
        'pce_core': '코어 PCE', 
        'pce_goods': '상품',
        'pce_services': '서비스',
        'pce_durable': '내구재',
        'pce_nondurable': '비내구재',
        'pce_food': '식품',
        'pce_energy': '에너지'
    }
    
    # 데이터 준비 (전치)
    heatmap_data = recent_data.T
    y_labels = [korean_labels.get(col, col) for col in heatmap_data.index]
    x_labels = [date.strftime('%Y-%m') for date in heatmap_data.columns]
    
    # 히트맵 생성
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_data.values,
        x=x_labels,
        y=y_labels,
        colorscale='RdBu_r',  # 빨강(높음) - 파랑(낮음)
        zmid=0,  # 0을 중심으로
        text=[[f'{val:.2f}%' for val in row] for row in heatmap_data.values],
        texttemplate='%{text}',
        textfont={"size": 10},
        colorbar=dict(title="MoM 변화율 (%)")
    ))
    
    if title is None:
        title = f"PCE 구성요소별 월별 변화율 히트맵 (최근 {periods}개월)"
    
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(family='NanumGothic', size=FONT_SIZE_TITLE),
            x=0.5, xanchor="center"
        ),
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL),
        width=max(800, periods * 50),
        height=max(500, len(y_labels) * 40),
        margin=dict(l=150, r=100, t=80, b=100)
    )
    
    return fig

def create_pce_component_comparison(components=None, periods=24):
    """
    PCE 구성요소들 간의 상세 비교 차트
    
    Args:
        components: 비교할 구성요소 리스트
        periods: 표시할 기간 (개월)
        title: 차트 제목
    
    Returns:
        plotly subplots Figure
    """
    # 데이터 로드
    df = load_pce_data('pce_data_complete.csv')
    if df is None:
        print("데이터를 새로 수집합니다...")
        df = fetch_pce_dataset(PCE_MAIN_SERIES, start_date='2019-01-01')
        if df is not None:
            save_pce_data(df, 'pce_data_complete.csv')
    
    if df is None:
        print("❌ 데이터 로드 실패")
        return None
    
    if components is None:
        components = ['pce_goods', 'pce_services']
        if 'pce_durable' in df.columns and 'pce_nondurable' in df.columns:
            components = ['pce_durable', 'pce_nondurable', 'pce_services']
    
    available_components = [c for c in components if c in df.columns]
    if not available_components:
        print(f"❌ 요청한 구성요소가 없습니다: {components}")
        return None
    
    from plotly.subplots import make_subplots
    
    # 서브플롯 생성
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=[
            "시계열 비교",
            "3개월 이동평균",
            "최근 12개월 분포",
            "상관관계"
        ],
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"type": "box"}, {"type": "scatter"}]]
    )
    
    chart_data = df[available_components].tail(periods)
    korean_labels = {
        'pce_goods': '상품',
        'pce_services': '서비스',
        'pce_durable': '내구재',
        'pce_nondurable': '비내구재'
    }
    
    # 1. 시계열 비교 (1,1)
    for i, component in enumerate(available_components):
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=chart_data[component],
                      name=korean_labels.get(component, component),
                      line=dict(color=get_kpds_color(i))),
            row=1, col=1
        )
    
    # 2. 3개월 이동평균 (1,2)
    ma_data = chart_data.rolling(window=3).mean()
    for i, component in enumerate(available_components):
        fig.add_trace(
            go.Scatter(x=ma_data.index, y=ma_data[component],
                      name=f"{korean_labels.get(component, component)} (3M MA)",
                      line=dict(color=get_kpds_color(i), dash='dash')),
            row=1, col=2
        )
    
    # 3. 최근 12개월 분포 (2,1)
    recent_12m = chart_data.tail(12)
    for i, component in enumerate(available_components):
        fig.add_trace(
            go.Box(y=recent_12m[component],
                  name=korean_labels.get(component, component),
                  marker_color=get_kpds_color(i)),
            row=2, col=1
        )
    
    # 4. 상관관계 (2,2) - 첫 번째와 두 번째 구성요소
    if len(available_components) >= 2:
        x_component = available_components[0]
        y_component = available_components[1]
        fig.add_trace(
            go.Scatter(x=chart_data[x_component], y=chart_data[y_component],
                      mode='markers',
                      name="상관관계",
                      marker=dict(color=get_kpds_color(0), size=8)),
            row=2, col=2
        )
    
    if title is None:
        title = f"PCE 구성요소 상세 비교 (최근 {periods//12:.0f}년)"
    
    fig.update_layout(
        title_text=title,
        title_x=0.5,
        height=800,
        showlegend=False,
        font=dict(family='NanumGothic', size=10)
    )
    
    # Y축 레이블
    fig.update_yaxes(title_text="% (MoM)", row=1, col=1)
    fig.update_yaxes(title_text="% (3M MA)", row=1, col=2)
    fig.update_yaxes(title_text="% (MoM)", row=2, col=1)
    if len(available_components) >= 2:
        fig.update_xaxes(title_text=korean_labels.get(available_components[0], available_components[0]), row=2, col=2)
        fig.update_yaxes(title_text=korean_labels.get(available_components[1], available_components[1]), row=2, col=2)
    
    return fig

print("✅ 고급 자유도 높은 추가 시각화 함수들 정의 완료")

# %%
# === 메인 실행 함수 ===

def run_pce_analysis(update_data=True, start_date='2000-01-01', charts=['overview', 'components', 'core_vs_headline', 'price_analysis', 'mom_analysis', 'latest_bar', 'horizontal_bar', 'dashboard_custom', 'category_deep_dive'], smart_update=True):
    """
    완전한 PCE 분석 실행 함수 (스마트 업데이트 지원)
    
    Parameters:
    - update_data: 데이터 업데이트 여부
    - start_date: 데이터 시작 날짜
    - charts: 생성할 차트 목록
    - smart_update: 스마트 업데이트 사용 여부 (최근 3개 데이터 비교)
    
    Returns:
    - dict: 분석 결과 및 차트들
    """
    print("🚀 Personal Consumption Expenditures 분석 시작")
    print("=" * 60)
    
    # 1) 데이터 로드 또는 업데이트 (스마트 업데이트 적용)
    if update_data:
        # 스마트 업데이트 활성화시 최근 데이터 일치성 확인
        if smart_update:
            print("🔍 스마트 업데이트: 최근 데이터 일치성 확인...")
            consistency_check = check_recent_data_consistency()
            
            # 업데이트 필요 없으면 기존 데이터 사용
            if not consistency_check['need_update']:
                print("🎯 데이터가 최신 상태입니다. 기존 CSV 파일 사용.")
                df = load_pce_data('pce_data_complete.csv')
                if df is None:
                    print("⚠️ CSV 파일 로드 실패, API에서 새로 수집합니다")
                    df = fetch_pce_dataset(PCE_MAIN_SERIES, start_date=start_date)
                    if df is not None:
                        save_pce_data(df, 'pce_data_complete.csv')
            else:
                print(f"🔄 업데이트 필요: {consistency_check['reason']}")
                print("📊 FRED API에서 최신 데이터 수집 중...")
                df = fetch_pce_dataset(PCE_MAIN_SERIES, start_date=start_date)
                if df is not None:
                    save_pce_data(df, 'pce_data_complete.csv')
        else:
            # 스마트 업데이트 비활성화시 무조건 새로 수집
            print("📊 FRED API에서 최신 데이터 수집 중...")
            df = fetch_pce_dataset(PCE_MAIN_SERIES, start_date=start_date)
            if df is not None:
                save_pce_data(df, 'pce_data_complete.csv')
    else:
        print("📁 기존 저장된 데이터 로드 중...")
        df = load_pce_data('pce_data_complete.csv')
        if df is None:
            print("⚠️ 기존 데이터가 없어서 새로 수집합니다")
            df = fetch_pce_dataset(PCE_MAIN_SERIES, start_date=start_date)
            if df is not None:
                save_pce_data(df, 'pce_data_complete.csv')
    
    if df is None:
        print("❌ 데이터 수집 실패")
        return None
    
    # 2) 기본 통계 생성
    print("\\n📈 기본 통계 생성 중...")
    stats = generate_pce_summary_stats(df)
    
    if stats:
        print(f"\\n✅ 데이터 요약:")
        metadata = stats['metadata']
        print(f"   - 분석 기간: {metadata['start_date']} ~ {metadata['latest_date']}")
        print(f"   - 총 데이터 포인트: {metadata['data_points']}개월")
        
        # 주요 지표 최신값 출력
        key_indicators = ['pce_total', 'pce_core', 'pce_price_headline', 'pce_price_core']
        for indicator in key_indicators:
            if indicator in stats:
                stat = stats[indicator]
                latest = stat['latest_value']
                mom = stat['mom_change']
                yoy = stat['yoy_change']
                
                print(f"   - {indicator}: {latest:.2f}% (MoM: {mom:+.2f}%, YoY: {yoy:+.2f}%)")
    
    # 3) 차트 생성
    print("\\n🎨 차트 생성 중...")
    figures = {}
    
    # 1. 개요 차트
    if 'overview' in charts:
        print("   - PCE 개요 (트렌드 분석)")
        figures['overview'] = plot_pce_overview(df)
    
    # 2. PCE 구성요소 분석
    if 'components' in charts:
        print("   - PCE 구성요소 분석")
        figures['components'] = plot_pce_components(df)
    
    # 3. 헤드라인 vs 코어 분석
    if 'core_vs_headline' in charts:
        print("   - 헤드라인 vs 코어 PCE")
        figures['core_vs_headline'] = plot_pce_core_vs_headline(df)
    
    # 4. 가격지수 분석
    if 'price_analysis' in charts:
        print("   - PCE 가격지수 분석")
        figures['price_analysis'] = plot_pce_price_analysis(df)
    
    # 5. 전월대비 변화율
    if 'mom_analysis' in charts:
        print("   - 전월대비 변화율")
        figures['mom_analysis'] = plot_pce_mom_changes(df)
    
    # 6. 최신 데이터 바 차트
    if 'latest_bar' in charts:
        print("   - 최신 MoM 변화율 바 차트")
        figures['latest_bar'] = plot_pce_latest_bar_chart(df, chart_type='mom')
    
    # 7. 가로 바 차트 (레이블 잘림 방지)
    if 'horizontal_bar' in charts:
        print("   - 가로 바 차트 (완전한 레이블 표시)")
        figures['horizontal_bar'] = create_horizontal_pce_bar_chart(df)
    
    # 8. 커스텀 대시보드
    if 'dashboard_custom' in charts:
        print("   - 커스텀 대시보드")
        figures['dashboard_custom'] = create_pce_custom_dashboard(df)
    
    # 9. 카테고리 심화 분석
    if 'category_deep_dive' in charts:
        print("   - 카테고리 심화 분석 (상품 vs 서비스)")
        figures['goods_deep_dive'] = create_pce_category_deep_dive(df, 'goods')
        figures['services_deep_dive'] = create_pce_category_deep_dive(df, 'services')
    
    print("\\n✅ PCE 분석 완료!")
    print(f"   - 생성된 차트: {len(figures)}개")
    print(f"   - 사용 가능한 지표: {len(df.columns)}개")
    
    return {
        'data': df,
        'stats': stats,
        'figures': figures
    }

# 빠른 테스트용 함수
def quick_pce_test():
    """빠른 PCE 분석 테스트 (최근 5년 데이터만)"""
    print("🧪 PCE 분석 빠른 테스트")
    return run_pce_analysis(
        update_data=True,
        start_date='2019-01-01',
        charts=['overview', 'core_vs_headline', 'mom_analysis', 'horizontal_bar']
    )

print("✅ 투자은행/이코노미스트 관점 PCE 분석 도구 완성! (바 차트 레이블 잘림 문제 해결)")
print("\\n🎯 사용법:")
print("   - 전체 분석: result = run_pce_analysis()")
print("   - 빠른 테스트: result = quick_pce_test()")
print("   - 차트 표시: result['figures']['overview'].show()")
print("   - 가로 바 차트: result['figures']['horizontal_bar'].show()")
print("   - 시리즈 목록: get_available_pce_series()")
print("\\n🎆 새로운 기능:")
print("   - create_horizontal_pce_bar_chart(): 레이블 잘림 완전 해결")
print("   - create_pce_table_matplotlib(): 테이블 시각화")
print("   - create_pce_category_deep_dive(): 카테고리 심화 분석")
print("="*60)

# %%
# 실행 예시 (주석 해제하여 사용)
result = run_pce_analysis()
result['figures']['overview'].show()
result['figures']['horizontal_bar'].show()  # 새로운 가로 바 차트

# %%
create_pce_table_matplotlib(result['data'])
# %%
create_horizontal_pce_bar_chart(result['data']).show()
# %%
create_pce_category_deep_dive(result['data']).show()
# %%
