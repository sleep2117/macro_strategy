# %%
"""
FRED API 전용 US Personal Income 분석 및 시각화 도구
- FRED API를 사용하여 US Personal Income 관련 데이터 수집
- PI 구성 요소별 데이터 분류 및 분석
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

# FRED API 키 설정 (여기에 실제 API 키를 입력하세요)
FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'  # https://fred.stlouisfed.org/docs/api/api_key.html 에서 발급

# KPDS 시각화 라이브러리 불러오기 (필수)
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

print("✓ KPDS 시각화 포맷 로드됨")

# %%
# === Personal Income 데이터 계층 구조 정의 ===

# PI 주요 구성 요소 시리즈 맵 (월별 레벨 데이터) - 수정된 FRED ID 적용
PI_MAIN_SERIES = {
    # 핵심 소득 지표
    'personal_income': 'PI',  # Personal income (총 개인소득)
    'disposable_income': 'DSPI',  # Disposable personal income (가처분소득) - 수정됨
    'real_disposable_income': 'DSPIC96',  # Real disposable personal income (실질 가처분소득) - 새로 추가
    'personal_consumption': 'PCE',  # Personal consumption expenditures (개인소비지출)
    'personal_saving': 'PMSAVE',  # Personal saving (개인저축)
    'saving_rate': 'PSAVERT',  # Personal saving rate (저축률)
    
    # 소득 구성 요소
    'compensation': 'W209RC1',  # Compensation of employees (임금 및 급여)
    'wages_salaries': 'A576RC1',  # Wages and salaries (임금)
    'private_wages': 'A132RC1',  # Private industry wages (민간부문 임금)
    'govt_wages': 'B202RC1',  # Government wages (정부부문 임금) - 수정됨
    'supplements': 'A038RC1',  # Supplements to wages & salaries (부가급여) - 수정됨
    
    # 사업소득 및 자본소득
    'proprietors_income': 'A041RC1',  # Proprietors' income (사업소득) - 수정됨
    'farm_income': 'B042RC1',  # Farm income (농업소득) - 수정됨
    'nonfarm_income': 'A045RC1',  # Nonfarm income (비농업소득) - 수정됨
    'rental_income': 'A048RC1',  # Rental income (임대소득) - 수정됨
    'interest_income': 'PII',  # Personal interest income (이자소득) - 수정됨
    'dividend_income': 'PDI',  # Personal dividend income (배당소득) - 수정됨
    
    # 정부 이전소득
    'transfer_receipts': 'PCTR',  # Personal current transfer receipts (이전수입) - 수정됨
    'social_security': 'A063RC1',  # Social Security (사회보장급여) - 기존 성공
    'medicare': 'W824RC1',  # Medicare (메디케어) - 수정됨
    'medicaid': 'W729RC1',  # Medicaid (메디케이드) - 수정됨
    'unemployment': 'W825RC1',  # Unemployment insurance (실업급여) - 수정됨
    'veterans': 'W826RC1',  # Veterans' benefits (재향군인급여) - 수정됨
    
    # 공제 항목
    'social_contributions': 'A061RC1',  # Contributions for govt social insurance (사회보장기여금) - 수정됨
    'personal_taxes': 'W055RC1',  # Personal current taxes (개인소득세) - 수정됨
    
    # 인구 및 1인당 지표
    'population': 'POPTHM',  # Population (인구)
    'dpi_per_capita': 'A229RC0',  # DPI per capita, current $ (1인당 가처분소득)
}

# PI 분석을 위한 계층 구조
PI_HIERARCHY = {
    'total_income': {
        'series_id': 'PI',
        'name': 'Personal Income',
        'components': ['compensation', 'proprietors_income', 'rental_income', 'asset_income', 'transfer_receipts', 'minus_contributions']
    },
    'labor_income': {
        'series_id': 'W209RC1',
        'name': 'Labor Income',
        'components': ['wages_salaries', 'supplements']
    },
    'capital_income': {
        'name': 'Capital Income',
        'components': ['proprietors_income', 'rental_income', 'interest_income', 'dividend_income']
    },
    'government_transfers': {
        'name': 'Government Transfers',
        'components': ['social_security', 'medicare', 'medicaid', 'unemployment', 'veterans']
    }
}

# 색상 매핑 (KPDS 색상 팔레트 사용)
PI_COLORS = {
    'personal_income': KPDS_COLORS[0],          # deepred_pds
    'disposable_income': KPDS_COLORS[1],        # deepblue_pds  
    'real_disposable_income': KPDS_COLORS[2],   # beige_pds - 새로 추가
    'personal_consumption': KPDS_COLORS[3],     # blue_pds
    'personal_saving': KPDS_COLORS[4],          # grey_pds
    'compensation': KPDS_COLORS[0],             # deepred_pds
    'proprietors_income': KPDS_COLORS[1],       # deepblue_pds
    'transfer_receipts': KPDS_COLORS[4],        # grey_pds
    'saving_rate': KPDS_COLORS[3]               # blue_pds
}

print("✓ Personal Income 데이터 구조 정의 완료")

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

def fetch_pi_dataset(series_dict, start_date='2000-01-01', end_date=None):
    """
    여러 PI 시리즈를 한번에 가져와서 데이터프레임으로 반환
    
    Parameters:
    - series_dict: 시리즈 딕셔너리 {name: series_id}
    - start_date: 시작 날짜
    - end_date: 종료 날짜
    
    Returns:
    - pandas.DataFrame: 각 시리즈를 컬럼으로 하는 데이터프레임
    """
    print(f"📊 PI 데이터 수집 시작 ({len(series_dict)}개 시리즈)")
    
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

def save_pi_data(df, filename='pi_data.csv'):
    """PI 데이터를 CSV 파일로 저장"""
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

def load_pi_data(filename='pi_data.csv'):
    """저장된 PI 데이터를 로드"""
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

def check_recent_data_consistency(filename='pi_data_complete.csv', check_count=3):
    """
    최근 N개 데이터의 일치성을 확인하여 스마트 업데이트 필요 여부 판단 (PI 특화)
    
    Parameters:
    - filename: 확인할 CSV 파일명
    - check_count: 확인할 최근 데이터 개수
    
    Returns:
    - dict: {'need_update': bool, 'reason': str, 'mismatches': list}
    """
    print(f"🔍 Personal Income 데이터 일치성 확인 (최근 {check_count}개 데이터)")
    
    # 기존 CSV 파일 존재 여부 확인
    existing_df = load_pi_data(filename)
    if existing_df is None:
        print("📄 기존 CSV 파일이 없습니다. 새로 수집이 필요합니다.")
        return {'need_update': True, 'reason': '기존 파일 없음', 'mismatches': []}
    
    # 사용 가능한 시리즈 확인
    available_series = [col for col in existing_df.columns if col in PI_MAIN_SERIES.keys()]
    if not available_series:
        print("❌ 확인할 시리즈가 없습니다")
        return {'need_update': True, 'reason': '시리즈 없음', 'mismatches': []}
    
    print(f"📊 확인 대상 시리즈: {len(available_series)}개")
    
    mismatches = []
    new_data_available = False
    all_data_identical = True
    
    for col_name in available_series:
        if col_name not in PI_MAIN_SERIES:
            continue
            
        series_id = PI_MAIN_SERIES[col_name]
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
        
        # 정규화된 날짜로 비교 (PI 특화: 1.0 허용 오차 - 개인소득 데이터 특성 고려)
        for key in existing_normalized.keys():
            if key in api_normalized:
                existing_val = existing_normalized[key]
                api_val = api_normalized[key]
                
                # 값이 다르면 불일치 (1.0 이상 차이 - 개인소득 데이터 특성 고려)
                if abs(existing_val - api_val) > 1.0:
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
    
    # 결과 판정
    if new_data_available:
        print(f"🆕 새로운 데이터 감지: 업데이트 필요")
        for mismatch in mismatches[:3]:
            if 'reason' in mismatch and mismatch['reason'] == '새로운 데이터 존재':
                print(f"   - {mismatch['series']}: {mismatch['existing_latest']} → {mismatch['api_latest']}")
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
        
        # PI 특화: 1.0 이상만 실제 불일치로 간주
        significant_mismatches = [m for m in value_mismatches if m.get('diff', 0) > 1.0]
        if len(significant_mismatches) == 0:
            print("📝 모든 차이가 1.0 미만입니다. 저장 정밀도 차이로 간주하여 업데이트를 건너뜁니다.")
            return {'need_update': False, 'reason': '미세한 정밀도 차이', 'mismatches': mismatches}
        else:
            print(f"🚨 유의미한 차이 발견: {len(significant_mismatches)}개 (1.0 이상)")
            return {'need_update': True, 'reason': '데이터 불일치', 'mismatches': mismatches}
    else:
        print("✅ 최근 데이터가 완전히 일치합니다. 업데이트를 건너뜁니다.")
        return {'need_update': False, 'reason': '데이터 일치', 'mismatches': []}

print("✅ FRED API 데이터 수집 함수 정의 완료")

# %%
# === PI 핵심 지표 계산 함수들 ===

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

def calculate_6m_annualized(series):
    """
    6개월 연율화 변화율 계산
    
    Parameters:
    - series: 시계열 데이터
    
    Returns:
    - pandas.Series: 6개월 연율화 변화율 (%)
    """
    if series is None or len(series) == 0:
        return None
    
    # 6개월 변화율을 연율화 ((1 + r/100)^2 - 1) * 100
    six_month_change = ((series / series.shift(6)) - 1) * 100
    annualized = ((1 + six_month_change/100) ** 2 - 1) * 100
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

def calculate_pi_ratios(df):
    """
    PI 주요 비율 지표 계산
    
    Parameters:
    - df: PI 데이터프레임
    
    Returns:
    - dict: 주요 비율 지표들
    """
    if df is None:
        return None
    
    ratios = {}
    
    # 가처분소득 대비 소비 비율 (소비성향)
    if 'disposable_income' in df.columns and 'personal_consumption' in df.columns:
        ratios['consumption_rate'] = (df['personal_consumption'] / df['disposable_income']) * 100
    
    # 개인소득 대비 임금 비중
    if 'personal_income' in df.columns and 'compensation' in df.columns:
        ratios['labor_share'] = (df['compensation'] / df['personal_income']) * 100
    
    # 개인소득 대비 이전소득 비중
    if 'personal_income' in df.columns and 'transfer_receipts' in df.columns:
        ratios['transfer_share'] = (df['transfer_receipts'] / df['personal_income']) * 100
    
    # 1인당 가처분소득 (실질)
    if 'disposable_income' in df.columns and 'population' in df.columns:
        ratios['dpi_per_capita_calc'] = df['disposable_income'] / (df['population'] / 1000)  # 인구는 천명 단위
    
    return ratios

def generate_pi_summary_stats(df, recent_months=12):
    """
    PI 데이터 요약 통계 생성
    
    Parameters:
    - df: PI 데이터프레임
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
        if col in PI_MAIN_SERIES.keys():
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

print("✅ PI 핵심 지표 계산 함수 정의 완료")

# %%
# === PI 전문 시각화 함수들 (KPDS 표준 적용) ===

def plot_pi_overview(df, title="US Personal Income Overview"):
    """
    PI 핵심 지표 개요 - KPDS 표준 함수 활용
    
    Parameters:
    - df: PI 데이터프레임
    - title: 차트 제목
    
    Returns:
    - plotly.graph_objects.Figure
    """
    if df is None or len(df) == 0:
        print("❌ 시각화할 데이터가 없습니다")
        return None
    
    # 데이터 준비 (조 달러 단위 변환)
    chart_data = df.copy()
    billion_cols = ['personal_income', 'disposable_income', 'real_disposable_income', 'personal_consumption']
    for col in billion_cols:
        if col in chart_data.columns:
            chart_data[col] = chart_data[col] / 1000  # 조 달러 변환
    
    # KPDS 멀티라인 차트 사용
    columns_to_plot = []
    labels_dict = {}
    
    # 핵심 소득지표 포함
    if 'personal_income' in chart_data.columns:
        columns_to_plot.append('personal_income')
        labels_dict['personal_income'] = 'Personal Income'
    
    if 'disposable_income' in chart_data.columns:
        columns_to_plot.append('disposable_income')
        labels_dict['disposable_income'] = 'Disposable Income'
        
    if 'real_disposable_income' in chart_data.columns:
        columns_to_plot.append('real_disposable_income')
        labels_dict['real_disposable_income'] = 'Real Disposable Income'
    
    if 'personal_consumption' in chart_data.columns:
        columns_to_plot.append('personal_consumption')
        labels_dict['personal_consumption'] = 'Personal Consumption'
    
    # KPDS 표준 멀티라인 차트 생성
    fig = df_multi_line_chart(
        df=chart_data,
        columns=columns_to_plot,
        title=title,
        ytitle="조 달러",
        labels=labels_dict,
        width_cm=12,
        height_cm=8
    )
    
    return fig

def plot_pi_growth_analysis(df, title="Personal Income Growth Analysis"):
    """
    PI 성장률 분석 - KPDS 표준 함수 활용
    
    Parameters:
    - df: PI 데이터프레임
    - title: 차트 제목
    
    Returns:
    - plotly.graph_objects.Figure  
    """
    if df is None or 'personal_income' not in df.columns:
        print("❌ Personal Income 데이터가 없습니다")
        return None
    
    # 성장률 계산
    pi_series = df['personal_income']
    growth_data = pd.DataFrame(index=df.index)
    
    # YoY 성장률 (주요 지표)
    yoy_growth = calculate_yoy_change(pi_series)
    if yoy_growth is not None:
        growth_data['YoY Growth'] = yoy_growth
    
    # 3개월 연율화 성장률
    three_m_growth = calculate_3m_annualized(pi_series)
    if three_m_growth is not None:
        growth_data['3M Annualized'] = three_m_growth
    
    # 6개월 연율화 성장률  
    six_m_growth = calculate_6m_annualized(pi_series)
    if six_m_growth is not None:
        growth_data['6M Annualized'] = six_m_growth
    
    # 데이터가 있는 컬럼만 선택
    growth_data = growth_data.dropna(axis=1, how='all')
    
    if growth_data.empty:
        print("❌ 성장률 데이터를 계산할 수 없습니다")
        return None
    
    # labels 딕셔너리 생성
    labels_dict = {col: col for col in growth_data.columns}
    
    # KPDS 멀티라인 차트로 성장률 시각화
    fig = df_multi_line_chart(
        df=growth_data,
        columns=list(growth_data.columns),
        title=title,
        ytitle="%",
        labels=labels_dict,
        width_cm=12,
        height_cm=7
    )
    
    return fig

def plot_pi_components_breakdown(df, title="Personal Income Components Breakdown"):
    """
    PI 구성요소 분석 - KPDS 바차트 및 라인차트 활용
    
    Parameters:
    - df: PI 데이터프레임
    - title: 차트 제목
    
    Returns:
    - plotly.graph_objects.Figure
    """
    if df is None:
        print("❌ 데이터가 없습니다")
        return None
    
    # 최근 12개월 평균으로 소득원별 구성 계산
    recent_data = df.tail(12)
    
    # 주요 소득원 데이터 준비 (조 달러 단위)
    income_components = {}
    
    if 'compensation' in recent_data.columns:
        income_components['Labor Income'] = recent_data['compensation'].mean() / 1000
    
    if 'proprietors_income' in recent_data.columns:
        income_components['Business Income'] = recent_data['proprietors_income'].mean() / 1000
    
    if 'rental_income' in recent_data.columns:
        income_components['Rental Income'] = recent_data['rental_income'].mean() / 1000
    
    # 자산소득 (이자+배당)
    asset_income = 0
    if 'interest_income' in recent_data.columns:
        asset_income += recent_data['interest_income'].mean()
    if 'dividend_income' in recent_data.columns:
        asset_income += recent_data['dividend_income'].mean()
    if asset_income > 0:
        income_components['Asset Income'] = asset_income / 1000
    
    if 'transfer_receipts' in recent_data.columns:
        income_components['Government Transfers'] = recent_data['transfer_receipts'].mean() / 1000
    
    # KPDS 바차트로 구성요소 시각화
    if income_components:
        fig = create_horizontal_bar_chart(
            data_dict=income_components,
            title=title,
            positive_color=KPDS_COLORS[0],
            negative_color=KPDS_COLORS[1]
        )
        return fig
    else:
        print("❌ 구성요소 데이터가 없습니다")
        return None

def plot_saving_rate_analysis(df, title="Personal Saving Rate Analysis"):
    """
    저축률 분석 - KPDS 라인차트 활용
    
    Parameters:
    - df: PI 데이터프레임
    - title: 차트 제목
    
    Returns:
    - plotly.graph_objects.Figure
    """
    if df is None or 'saving_rate' not in df.columns:
        print("❌ 저축률 데이터가 없습니다")
        return None
    
    # KPDS 단일 라인차트로 저축률 시각화
    fig = df_line_chart(
        df=df,
        column='saving_rate',
        title=title,
        ytitle="%",
        width_cm=12,
        height_cm=7
    )
    
    return fig

def plot_real_vs_nominal_income(df, title="Real vs Nominal Disposable Income"):
    """
    실질 vs 명목 가처분소득 비교 - KPDS 듀얼축 차트 활용
    
    Parameters:
    - df: PI 데이터프레임  
    - title: 차트 제목
    
    Returns:
    - plotly.graph_objects.Figure
    """
    if df is None:
        print("❌ 데이터가 없습니다")
        return None
    
    # 듀얼축 데이터 준비 (조 달러 단위)
    chart_data = df.copy()
    left_cols = []
    right_cols = []
    
    # 왼쪽 축: 명목 가처분소득
    if 'disposable_income' in chart_data.columns:
        chart_data['disposable_income'] = chart_data['disposable_income'] / 1000
        left_cols.append('disposable_income')
    
    # 오른쪽 축: 실질 가처분소득  
    if 'real_disposable_income' in chart_data.columns:
        chart_data['real_disposable_income'] = chart_data['real_disposable_income'] / 1000
        right_cols.append('real_disposable_income')
    
    if not left_cols and not right_cols:
        print("❌ 가처분소득 데이터가 없습니다")
        return None
    
    # labels 딕셔너리 생성
    left_labels = {left_cols[0]: "Nominal DPI"} if left_cols else None
    right_labels = {right_cols[0]: "Real DPI"} if right_cols else None
    
    # KPDS 듀얼축 차트 생성
    fig = df_dual_axis_chart(
        df=chart_data,
        left_cols=left_cols or None,
        right_cols=right_cols or None,
        title=title,
        left_title="명목 가처분소득 (조달러)",
        right_title="실질 가처분소득 (조달러)",
        left_labels=left_labels,
        right_labels=right_labels,
        width_cm=12,
        height_cm=7
    )
    
    return fig

print("✅ PI 전문 시각화 함수 정의 완료")

# %%
# === 다양한 자유도 높은 시각화 함수들 ===

def plot_personal_income_overview(df):
    """
    개인소득 개요 차트 (개선된 버전)
    """
    if df is None or len(df) == 0:
        print("❌ 시각화할 데이터가 없습니다")
        return None
    
    # 최신 36개월 데이터 (3년 트렌드)
    chart_data = df.tail(36).copy()
    
    # 사용 가능한 컬럼 확인
    available_cols = []
    col_labels = {}
    
    if 'personal_income' in df.columns:
        available_cols.append('personal_income')
        col_labels['personal_income'] = '개인소득'
    if 'disposable_income' in df.columns:
        available_cols.append('disposable_income')
        col_labels['disposable_income'] = '가처분소득'
    if 'real_disposable_income' in df.columns:
        available_cols.append('real_disposable_income')
        col_labels['real_disposable_income'] = '실질 가처분소득'
    if 'personal_consumption' in df.columns:
        available_cols.append('personal_consumption')
        col_labels['personal_consumption'] = '개인소비지출'
        
    if not available_cols:
        print("Warning: 표시할 개인소득 지표가 없습니다.")
        return None
    
    # 조달러 단위로 변환
    for col in available_cols:
        chart_data[col] = chart_data[col] / 1000
    
    # 라벨 딕셔너리 생성
    labels_dict = {col: col_labels[col] for col in available_cols}
    
    fig = df_multi_line_chart(
        df=chart_data,
        columns=available_cols,
        title="미국 개인소득 주요 지표 트렌드 (최근 3년)",
        ytitle="조달러",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_personal_income_components(df):
    """
    개인소득 구성요소별 차트 (다양한 시각화)
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
        
    # 최신 24개월 데이터
    chart_data = df.tail(24).copy()
    
    # 사용 가능한 컬럼 확인
    available_cols = []
    col_labels = {}
    
    if 'personal_income' in df.columns:
        available_cols.append('personal_income')
        col_labels['personal_income'] = '개인소득'
    if 'disposable_income' in df.columns:
        available_cols.append('disposable_income')
        col_labels['disposable_income'] = '가처분소득'
    if 'saving_rate' in df.columns:
        available_cols.append('saving_rate')
        col_labels['saving_rate'] = '개인저축률'
        
    if not available_cols:
        print("Warning: 표시할 개인소득 구성요소가 없습니다.")
        return None
    
    # 저축률과 소득을 분리 (단위가 다름)
    income_cols = [col for col in available_cols if col != 'saving_rate']
    savings_cols = [col for col in available_cols if col == 'saving_rate']
    
    if income_cols and savings_cols:
        # 듀얼축 차트 (소득 vs 저축률)
        left_labels = [col_labels[col] for col in income_cols]
        right_labels = [col_labels[col] for col in savings_cols]
        
        fig = df_dual_axis_chart(
            df=chart_data,
            left_cols=income_cols,
            right_cols=savings_cols,
            title="미국 개인소득 구성요소 (최근 2년)",
            left_title="조달러",
            right_title="%",
            left_labels=left_labels,
            right_labels=right_labels,
            width_cm=14,
            height_cm=8
        )
    else:
        # 단일축 차트
        cols_to_use = income_cols or savings_cols
        labels_dict = {col: col_labels[col] for col in cols_to_use}
        
        fig = df_multi_line_chart(
            df=chart_data,
            columns=cols_to_use,
            title="미국 개인소득 구성요소 (최근 2년)",
            ytitle="조달러" if income_cols else "%",
            labels=labels_dict,
            width_cm=14,
            height_cm=8
        )
    
    return fig

def plot_savings_rate_analysis(df):
    """
    개인저축률 분석 차트 (추가적인 컨텍스트 제공)
    """
    if df is None or 'saving_rate' not in df.columns:
        print("Warning: 개인저축률 데이터(saving_rate)가 없습니다.")
        return None
    
    # 최신 36개월 데이터 (3년 트렌드)
    chart_data = df[['saving_rate']].tail(36)
    
    # 평균선 추가를 위한 데이터 준비
    chart_data_with_avg = chart_data.copy()
    avg_rate = chart_data['saving_rate'].mean()
    chart_data_with_avg['평균선'] = avg_rate
    
    labels_dict = {
        'saving_rate': '개인저축률',
        '평균선': f'3년 평균 ({avg_rate:.1f}%)'
    }
    
    fig = df_multi_line_chart(
        df=chart_data_with_avg,
        columns=['saving_rate', '평균선'],
        title="미국 개인저축률 추이 (최근 3년)",
        ytitle="%",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_real_vs_nominal_income(df):
    """
    실질 vs 명목 소득 비교 차트
    """
    if df is None:
        print("❌ 데이터가 없습니다")
        return None
        
    # 최신 24개월 데이터 (더 긴 트렌드 확인)
    chart_data = df.tail(24).copy()
    
    # 컬럼 설정
    left_cols = []
    right_cols = []
    
    if 'disposable_income' in df.columns:
        left_cols = ['disposable_income']  # 명목 가처분소득
        
    if 'real_disposable_income' in df.columns:
        right_cols = ['real_disposable_income']  # 실질 가처분소득
    
    if not left_cols and not right_cols:
        print("❌ 가처분소득 데이터가 없습니다")
        return None
        
    title = "미국 가처분소득: 실질 vs 명목 비교 (최근 2년)"
    
    # 라벨 설정 (리스트 형태로 수정)
    left_labels = ["명목 가처분소득 (조달러)"] if left_cols else []
    right_labels = ["실질 가처분소득 (조달러)"] if right_cols else []
    
    # KPDS 듀얼축 차트 생성
    fig = df_dual_axis_chart(
        df=chart_data,
        left_cols=left_cols,
        right_cols=right_cols,
        title=title,
        left_title="명목 (조달러)",
        right_title="실질 (조달러)",
        left_labels=left_labels,
        right_labels=right_labels,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_income_mom_changes(df):
    """
    개인소득 전월대비 변화율 차트 (seasonally adjusted 데이터는 MoM만 의미있음)
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
        
    # MoM 계산
    mom_data = df.pct_change().fillna(0) * 100
    
    # 최신 24개월 데이터 (더 긴 트렌드)
    chart_data = mom_data.tail(24)
    
    # 주요 지표들
    main_cols = []
    col_labels = {}
    
    if 'personal_income' in df.columns:
        main_cols.append('personal_income')
        col_labels['personal_income'] = '개인소득 MoM'
    if 'disposable_income' in df.columns:
        main_cols.append('disposable_income')
        col_labels['disposable_income'] = '가처분소득 MoM'
    
    if not main_cols:
        print("Warning: MoM 변화율을 계산할 지표가 없습니다.")
        return None
    
    labels_dict = {col: col_labels[col] for col in main_cols}
    
    fig = df_multi_line_chart(
        df=chart_data,
        columns=main_cols,
        title="미국 개인소득 전월대비 변화율 (최근 2년)",
        ytitle="%",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_pi_series_selection(df, series_list, chart_type='level', title=None, periods=24):
    """
    원하는 시리즈를 원하는 형태(MoM, YoY, Level)로 시계열로 그리는 자유도 높은 함수
    
    Args:
        df: PI 데이터프레임
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
    if chart_type == 'mom':
        chart_data = chart_data.pct_change().fillna(0) * 100
        ytitle = "% (MoM)"
        if title is None:
            title = f"개인소득 전월대비 변화율 (최근 {periods//12:.0f}년)"
    elif chart_type == 'yoy':
        chart_data = ((chart_data / chart_data.shift(12)) - 1) * 100
        ytitle = "% (YoY)"
        if title is None:
            title = f"개인소득 전년동월대비 변화율 (최근 {periods//12:.0f}년)"
    else:  # level
        # 조달러 단위로 변환 (저축률 제외)
        for col in available_series:
            if col != 'saving_rate':
                chart_data[col] = chart_data[col] / 1000
        ytitle = "조달러"
        if title is None:
            title = f"개인소득 주요 지표 (최근 {periods//12:.0f}년)"
    
    # 라벨 설정 (한국어 매핑)
    korean_labels = {
        'personal_income': '개인소득',
        'disposable_income': '가처분소득',
        'real_disposable_income': '실질 가처분소득',
        'personal_consumption': '개인소비지출',
        'personal_saving': '개인저축',
        'saving_rate': '개인저축률',
        'compensation': '임금및급여',
        'transfer_receipts': '정부이전수입'
    }
    
    labels_dict = {col: korean_labels.get(col, col) for col in available_series}
    
    # KPDS 라인 차트 생성
    fig = df_multi_line_chart(
        df=chart_data,
        columns=available_series,
        title=title,
        ytitle=ytitle,
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_pi_latest_bar_chart(df, series_list=None, chart_type='mom', title=None):
    """
    가장 최신 데이터를 기준으로 구성 요소별 변화를 바 그래프로 시각화
    
    Args:
        df: PI 데이터프레임
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
        series_list = ['personal_income', 'disposable_income', 'personal_consumption', 'saving_rate']
    
    # 시리즈 유효성 확인
    available_series = [s for s in series_list if s in df.columns]
    if not available_series:
        print(f"❌ 요청한 시리즈가 데이터에 없습니다: {series_list}")
        return None
    
    # 데이터 변환
    if chart_type == 'mom':
        change_data = df[available_series].pct_change().iloc[-1] * 100
        ytitle = "전월대비 변화율 (%)"
        if title is None:
            latest_date = df.index[-1].strftime('%Y년 %m월')
            title = f"개인소득 구성요소 전월대비 변화율 ({latest_date})"
    else:  # yoy
        change_data = ((df[available_series].iloc[-1] / df[available_series].iloc[-13]) - 1) * 100
        ytitle = "전년동월대비 변화율 (%)"
        if title is None:
            latest_date = df.index[-1].strftime('%Y년 %m월')
            title = f"개인소득 구성요소 전년동월대비 변화율 ({latest_date})"
    
    # 라벨 설정
    korean_labels = {
        'personal_income': '개인소득',
        'disposable_income': '가처분소득',
        'real_disposable_income': '실질 가처분소득',
        'personal_consumption': '개인소비지출',
        'personal_saving': '개인저축',
        'saving_rate': '개인저축률',
        'compensation': '임금및급여',
        'transfer_receipts': '정부이전수입'
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
        textposition='outside',
        showlegend=False
    ))
    
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
        height=max(400, len(categories) * 40),
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color="black"),
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            title=dict(text=ytitle, font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE))
        ),
        yaxis=dict(
            showline=False,
        ),
        margin=dict(l=150, r=80, t=80, b=80)
    )
    
    # 격자선 및 0선
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="lightgrey")
    fig.update_yaxes(showgrid=False)
    fig.add_vline(x=0, line_width=2, line_color="black")
    
    return fig

def create_pi_custom_dashboard(df, periods=24):
    """
    PI 데이터를 위한 커스텀 대시보드 (여러 차트를 서브플롯으로 결합)
    
    Args:
        df: PI 데이터프레임
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
            "주요 소득지표 트렌드",
            "MoM 변화율",
            "저축률 추이",
            "최신 MoM 변화율"
        ],
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"type": "bar"}]]
    )
    
    chart_data = df.tail(periods)
    
    # 1. 주요 소득지표 (1,1)
    if 'personal_income' in df.columns:
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=chart_data['personal_income']/1000,
                      name='개인소득', line=dict(color=get_kpds_color(0))),
            row=1, col=1
        )
    if 'disposable_income' in df.columns:
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=chart_data['disposable_income']/1000,
                      name='가처분소득', line=dict(color=get_kpds_color(1))),
            row=1, col=1
        )
    
    # 2. MoM 변화율 (1,2)
    mom_data = chart_data.pct_change() * 100
    if 'personal_income' in df.columns:
        fig.add_trace(
            go.Scatter(x=mom_data.index, y=mom_data['personal_income'],
                      name='개인소득 MoM', line=dict(color=get_kpds_color(0))),
            row=1, col=2
        )
    if 'disposable_income' in df.columns:
        fig.add_trace(
            go.Scatter(x=mom_data.index, y=mom_data['disposable_income'],
                      name='가처분소득 MoM', line=dict(color=get_kpds_color(1))),
            row=1, col=2
        )
    
    # 3. 저축률 추이 (2,1)
    if 'saving_rate' in df.columns:
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=chart_data['saving_rate'],
                      name='저축률', line=dict(color=get_kpds_color(2))),
            row=2, col=1
        )
    
    # 4. 최신 MoM 변화율 바 차트 (2,2)
    latest_mom = mom_data.iloc[-1]
    series_to_show = ['personal_income', 'disposable_income', 'personal_consumption']
    available_series = [s for s in series_to_show if s in latest_mom.index]
    
    if available_series:
        korean_names = {
            'personal_income': '개인소득',
            'disposable_income': '가처분소득',
            'personal_consumption': '개인소비지출'
        }
        
        categories = [korean_names.get(s, s) for s in available_series]
        values = [latest_mom[s] for s in available_series]
        colors = [get_kpds_color(i) for i in range(len(values))]
        
        fig.add_trace(
            go.Bar(x=categories, y=values, marker_color=colors,
                  text=[f'{v:+.2f}%' for v in values], textposition='outside'),
            row=2, col=2
        )
    
    # 레이아웃 업데이트
    fig.update_layout(
        title_text=f"미국 개인소득 종합 대시보드 (최근 {periods//12:.0f}년)",
        title_x=0.5,
        height=800,
        showlegend=False,
        font=dict(family='NanumGothic', size=10)
    )
    
    # 각 서브플롯의 y축 레이블
    fig.update_yaxes(title_text="조달러", row=1, col=1)
    fig.update_yaxes(title_text="%", row=1, col=2)
    fig.update_yaxes(title_text="%", row=2, col=1)
    fig.update_yaxes(title_text="%", row=2, col=2)
    
    return fig

print("✅ 고급 자유도 높은 시각화 함수들 정의 완료")

# %%  
# === 간편 사용 함수들 ===

def quick_pi_chart(series_list, chart_type='level', periods=24):
    """
    빠른 PI 차트 생성 (데이터 자동 로드)
    
    Args:
        series_list: 그릴 시리즈 리스트
        chart_type: 'level', 'mom', 'yoy'
        periods: 표시할 기간 (개월)
    
    Examples:
        quick_pi_chart(['personal_income', 'disposable_income'], 'level', 24)
        quick_pi_chart(['personal_income'], 'mom', 12)
    """
    # 기존 데이터 로드 또는 새로 로드
    df = load_pi_data('pi_data_complete.csv')
    if df is None:
        print("데이터를 새로 수집합니다...")
        df = fetch_pi_dataset(PI_MAIN_SERIES, start_date='2019-01-01')
        if df is not None:
            save_pi_data(df, 'pi_data_complete.csv')
    
    if df is None:
        print("❌ 데이터 로드 실패")
        return None
    
    return plot_pi_series_selection(df, series_list, chart_type, periods=periods)

def quick_pi_bar():
    """최신 MoM 변화율 바 차트 빠른 생성"""
    df = load_pi_data('pi_data_complete.csv')
    if df is None:
        print("데이터를 새로 수집합니다...")
        df = fetch_pi_dataset(PI_MAIN_SERIES, start_date='2019-01-01')
        if df is not None:
            save_pi_data(df, 'pi_data_complete.csv')
    
    if df is None:
        print("❌ 데이터 로드 실패")
        return None
    
    return plot_pi_latest_bar_chart(df, chart_type='mom')

def quick_pi_dashboard():
    """커스텀 대시보드 빠른 생성"""
    df = load_pi_data('pi_data_complete.csv')
    if df is None:
        print("데이터를 새로 수집합니다...")
        df = fetch_pi_dataset(PI_MAIN_SERIES, start_date='2019-01-01')
        if df is not None:
            save_pi_data(df, 'pi_data_complete.csv')
    
    if df is None:
        print("❌ 데이터 로드 실패")
        return None
    
    return create_pi_custom_dashboard(df)

def get_available_pi_series():
    """사용 가능한 PI 시리즈 목록 출력"""
    print("=== 사용 가능한 Personal Income 시리즈 ===")
    print("\n📊 핵심 지표:")
    core_series = {
        'personal_income': '개인소득 (총)',
        'disposable_income': '가처분소득 (명목)',
        'real_disposable_income': '가처분소득 (실질)',
        'personal_consumption': '개인소비지출',
        'personal_saving': '개인저축',
        'saving_rate': '개인저축률'
    }
    
    for key, desc in core_series.items():
        print(f"  '{key}': {desc}")
    
    print("\n💼 소득 구성요소:")
    income_components = {
        'compensation': '임금 및 급여 (총)',
        'wages_salaries': '임금',
        'private_wages': '민간부문 임금',
        'govt_wages': '정부부문 임금',
        'proprietors_income': '사업소득',
        'rental_income': '임대소득',
        'interest_income': '이자소득',
        'dividend_income': '배당소득'
    }
    
    for key, desc in income_components.items():
        print(f"  '{key}': {desc}")
    
    print("\n🏛️ 정부 이전소득:")
    transfer_components = {
        'transfer_receipts': '이전수입 (총)',
        'social_security': '사회보장급여',
        'medicare': '메디케어',
        'medicaid': '메디케이드',
        'unemployment': '실업급여',
        'veterans': '재향군인급여'
    }
    
    for key, desc in transfer_components.items():
        print(f"  '{key}': {desc}")

print("✅ 간편 사용 함수들 정의 완료")

# %%
# === 투자은행/이코노미스트 관점 고급 분석 차트 (KPDS 표준) ===

def plot_economic_indicators_dashboard(df, title="Economic Indicators Dashboard"):
    """
    투자은행 스타일 경제지표 대시보드 - KPDS 표준 함수 활용
    
    Parameters:
    - df: PI 데이터프레임
    - title: 차트 제목
    
    Returns:
    - plotly.graph_objects.Figure
    """
    if df is None:
        print("❌ 데이터가 없습니다")
        return None
    
    # 주요 경제지표 YoY 성장률 계산
    dashboard_data = pd.DataFrame(index=df.index)
    
    # 개인소득 YoY 성장률
    if 'personal_income' in df.columns:
        pi_yoy = calculate_yoy_change(df['personal_income'])
        if pi_yoy is not None:
            dashboard_data['Personal Income YoY'] = pi_yoy
    
    # 가처분소득 YoY 성장률  
    if 'disposable_income' in df.columns:
        dpi_yoy = calculate_yoy_change(df['disposable_income'])
        if dpi_yoy is not None:
            dashboard_data['Disposable Income YoY'] = dpi_yoy
    
    # 실질 가처분소득 YoY 성장률
    if 'real_disposable_income' in df.columns:
        real_dpi_yoy = calculate_yoy_change(df['real_disposable_income'])
        if real_dpi_yoy is not None:
            dashboard_data['Real DPI YoY'] = real_dpi_yoy
    
    # 소비 YoY 성장률
    if 'personal_consumption' in df.columns:
        pce_yoy = calculate_yoy_change(df['personal_consumption'])
        if pce_yoy is not None:
            dashboard_data['Consumption YoY'] = pce_yoy
    
    # 데이터가 있는 컬럼만 선택
    dashboard_data = dashboard_data.dropna(axis=1, how='all')
    
    if dashboard_data.empty:
        print("❌ 대시보드용 데이터가 없습니다")
        return None
    
    # labels 딕셔너리 생성
    labels_dict = {col: col for col in dashboard_data.columns}
    
    # KPDS 멀티라인 차트로 주요 지표 시각화
    fig = df_multi_line_chart(
        df=dashboard_data,
        columns=list(dashboard_data.columns),
        title=title,
        ytitle="%",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_cyclical_analysis(df, title="Personal Income Cyclical Analysis"):
    """
    경기순환 분석 - KPDS 표준 함수 활용
    
    Parameters:
    - df: PI 데이터프레임
    - title: 차트 제목
    
    Returns:
    - plotly.graph_objects.Figure
    """
    if df is None or 'personal_income' not in df.columns:
        print("❌ Personal Income 데이터가 없습니다")
        return None
    
    # 경기순환 지표 계산
    pi_series = df['personal_income']
    cyclical_data = pd.DataFrame(index=df.index)
    
    # YoY 성장률 (기본 지표)
    yoy_growth = calculate_yoy_change(pi_series)
    if yoy_growth is not None:
        cyclical_data['YoY Growth'] = yoy_growth
        
        # 3개월 이동평균 (트렌드)
        cyclical_data['3M Trend'] = yoy_growth.rolling(window=3).mean()
    
    # 3개월 연율화 성장률 (모멘텀)
    three_m_momentum = calculate_3m_annualized(pi_series)
    if three_m_momentum is not None:
        cyclical_data['3M Momentum'] = three_m_momentum
    
    # 데이터가 있는 컬럼만 선택
    cyclical_data = cyclical_data.dropna(axis=1, how='all')
    
    if cyclical_data.empty:
        print("❌ 경기순환 분석용 데이터가 없습니다")
        return None
    
    # labels 딕셔너리 생성
    labels_dict = {col: col for col in cyclical_data.columns}
    
    # KPDS 멀티라인 차트로 경기순환 지표 시각화
    fig = df_multi_line_chart(
        df=cyclical_data,
        columns=list(cyclical_data.columns),
        title=title,
        ytitle="%",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

# %%
# === 메인 실행 함수 ===

def run_pi_analysis(update_data=True, start_date='2000-01-01', charts=['overview', 'components', 'saving_rate', 'real_vs_nominal', 'mom_analysis', 'latest_bar', 'dashboard_custom'], smart_update=True):
    """
    완전한 PI 분석 실행 함수 (스마트 업데이트 지원)
    
    Parameters:
    - update_data: 데이터 업데이트 여부
    - start_date: 데이터 시작 날짜
    - charts: 생성할 차트 목록
    - smart_update: 스마트 업데이트 사용 여부 (최근 3개 데이터 비교)
    
    Returns:
    - dict: 분석 결과 및 차트들
    """
    print("🚀 Personal Income 분석 시작")
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
                df = load_pi_data('pi_data_complete.csv')
                if df is None:
                    print("⚠️ CSV 파일 로드 실패, API에서 새로 수집합니다")
                    df = fetch_pi_dataset(PI_MAIN_SERIES, start_date=start_date)
                    if df is not None:
                        save_pi_data(df, 'pi_data_complete.csv')
            else:
                print(f"🔄 업데이트 필요: {consistency_check['reason']}")
                print("📊 FRED API에서 최신 데이터 수집 중...")
                df = fetch_pi_dataset(PI_MAIN_SERIES, start_date=start_date)
                if df is not None:
                    save_pi_data(df, 'pi_data_complete.csv')
        else:
            # 스마트 업데이트 비활성화시 무조건 새로 수집
            print("📊 FRED API에서 최신 데이터 수집 중...")
            df = fetch_pi_dataset(PI_MAIN_SERIES, start_date=start_date)
            if df is not None:
                save_pi_data(df, 'pi_data_complete.csv')
    else:
        print("📁 기존 저장된 데이터 로드 중...")
        df = load_pi_data('pi_data_complete.csv')
        if df is None:
            print("⚠️ 기존 데이터가 없어서 새로 수집합니다")
            df = fetch_pi_dataset(PI_MAIN_SERIES, start_date=start_date)
            if df is not None:
                save_pi_data(df, 'pi_data_complete.csv')
    
    if df is None:
        print("❌ 데이터 수집 실패")
        return None
    
    # 2) 기본 통계 생성
    print("\n📈 기본 통계 생성 중...")
    stats = generate_pi_summary_stats(df)
    
    if stats:
        print(f"\n✅ 데이터 요약:")
        metadata = stats['metadata']
        print(f"   - 분석 기간: {metadata['start_date']} ~ {metadata['latest_date']}")
        print(f"   - 총 데이터 포인트: {metadata['data_points']}개월")
        
        # 주요 지표 최신값 출력
        key_indicators = ['personal_income', 'disposable_income', 'saving_rate']
        for indicator in key_indicators:
            if indicator in stats:
                stat = stats[indicator]
                latest = stat['latest_value']
                mom = stat['mom_change']
                yoy = stat['yoy_change']
                
                if indicator == 'saving_rate':
                    print(f"   - {indicator}: {latest:.1f}% (MoM: {mom:+.1f}%p, YoY: {yoy:+.1f}%p)")
                else:
                    print(f"   - {indicator}: ${latest/1000:.1f}조 (MoM: {mom:+.1f}%, YoY: {yoy:+.1f}%)")
    
    # YoY 변화율은 제외 (seasonally adjusted 데이터이므로 의미 없음)
    exclude_charts = ['yoy_analysis', 'growth', 'cyclical']
    charts = [chart for chart in charts if chart not in exclude_charts]
    
    print(f"\n=== 분석 대상 차트: {len(charts)}개 ===")
    for chart in charts:
        print(f"  - {chart}")
    print("\n⚠️  주의: seasonally adjusted 데이터로 YoY 분석은 제외됩니다.\n")
    
    # 3) 차트 생성
    print("🎨 차트 생성 중...")
    figures = {}
    
    # 1. 개요 차트 (개선된 버전)
    if 'overview' in charts:
        print("   - 개인소득 개요 (트렌드 분석)")
        figures['overview'] = plot_personal_income_overview(df)
    
    # 2. 개인소득 구성요소 분석
    if 'components' in charts:
        print("   - 개인소득 구성요소 분석")
        figures['components'] = plot_personal_income_components(df)
    
    # 3. 개인저축률 분석
    if 'saving_rate' in charts:
        print("   - 개인저축률 분석")
        figures['saving_rate'] = plot_savings_rate_analysis(df)
    
    # 4. 실질 vs 명목 소득 비교
    if 'real_vs_nominal' in charts:
        print("   - 실질 vs 명목 소득 비교")
        figures['real_vs_nominal'] = plot_real_vs_nominal_income(df)
    
    # 5. 전월대비 변화율
    if 'mom_analysis' in charts:
        print("   - 전월대비 변화율")
        figures['mom_analysis'] = plot_income_mom_changes(df)
    
    # 6. 최신 데이터 바 차트
    if 'latest_bar' in charts:
        print("   - 최신 MoM 변화율 바 차트")
        figures['latest_bar'] = plot_pi_latest_bar_chart(df, chart_type='mom')
    
    # 7. 커스텀 대시보드
    if 'dashboard_custom' in charts:
        print("   - 커스텀 대시보드")
        figures['dashboard_custom'] = create_pi_custom_dashboard(df)
    
    if 'dashboard' in charts:
        print("   - 경제지표 대시보드")
        figures['dashboard'] = plot_economic_indicators_dashboard(df)
    
    if 'cyclical' in charts:
        print("   - 경기순환 분석 차트")
        figures['cyclical'] = plot_cyclical_analysis(df)
    
    print("\n✅ Personal Income 분석 완료!")
    print(f"   - 생성된 차트: {len(figures)}개")
    print(f"   - 사용 가능한 지표: {len(df.columns)}개")
    
    return {
        'data': df,
        'stats': stats,
        'figures': figures,
        'ratios': calculate_pi_ratios(df)
    }

# 빠른 테스트용 함수
def quick_pi_test():
    """빠른 PI 분석 테스트 (최근 5년 데이터만)"""
    print("🧪 PI 분석 빠른 테스트")
    return run_pi_analysis(
        update_data=True,
        start_date='2019-01-01',
        charts=['overview', 'saving_rate', 'mom_analysis']
    )

print("✅ 투자은행/이코노미스트 관점 분석 도구 완성!")
print("\n🎯 사용법:")
print("   - 전체 분석: result = run_pi_analysis()")
print("   - 빠른 테스트: result = quick_pi_test()")
print("   - 차트 표시: result['figures']['overview'].show()")
print("="*60)

# %%
# 실행 예시 (주석 해제하여 사용)
result = run_pi_analysis()
result['figures']['overview'].show()
# %%
