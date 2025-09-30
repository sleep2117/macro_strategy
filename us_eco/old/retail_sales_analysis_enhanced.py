# %%
"""
FRED API 전용 US Retail Sales 분석 및 시각화 도구
- FRED API를 사용하여 US Retail Sales 관련 데이터 수집
- 소매판매 업종별 데이터 분류 및 분석
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
# === US Retail Sales 데이터 계층 구조 정의 ===

# Advance Retail Sales 시리즈 맵 (FRED Advance 시리즈 사용 - seasonally adjusted)
RETAIL_SALES_SERIES = {
    # 핵심 총계 지표 (Advance series)
    'total': 'RSAFS',                             # Retail & Food Services - Total (Advance)
    'ex_auto': 'RSFSXMV',                         # Ex-Motor-Vehicle & Parts (Advance)
    'ex_gas': 'MARTSSM44Z72USS',                  # Ex-Gasoline Stations (Advance)
    'ex_auto_gas': 'MARTSSM44W72USS',             # Ex-Auto & Gas (Advance)
    'retail_only': 'RSXFS',                       # Retail Trade - Total (no restaurants, Advance)
    
    # 주요 업종별 세부 데이터 (Advance series)
    'motor_vehicles': 'RSMVPD',                   # Motor Vehicle & Parts Dealers (Advance)
    'furniture': 'RSFHFS',                        # Furniture & Home Furnishings Stores (Advance)
    'electronics': 'RSEAS',                       # Electronics & Appliance Stores (Advance)
    'building_materials': 'RSBMGESD',             # Building Materials & Garden Equipment Dealers (Advance)
    'food_beverage': 'RSDBS',                     # Food & Beverage Stores (Advance) - 수정
    'health_care': 'RSHPCS',                      # Health & Personal Care Stores (Advance)
    'gasoline': 'RSGASSN',                        # Gasoline Stations (Advance) - 수정
    'clothing': 'RSCCAS',                         # Clothing & Accessory Stores (Advance)
    'sporting_goods': 'RSSGHBMS',                  # Sporting Goods, Hobby, Musical Instr. & Books (Advance) - 수정
    'general_merchandise': 'RSGMS',               # General Merchandise Stores (Advance)
    'misc_retailers': 'RSMSR',                   # Miscellaneous Store Retailers (Advance) - 수정
    'nonstore_ecommerce': 'RSNSR',                # Non-store Retailers (incl. e-commerce, Advance)
    'food_services': 'RSFSDP'                     # Food Services & Drinking Places (Advance)
}

# Retail Sales 분석을 위한 계층 구조
RETAIL_HIERARCHY = {
    'core_measures': {
        'name': 'Core Retail Sales Measures',
        'components': ['total', 'ex_auto', 'ex_gas', 'ex_auto_gas', 'retail_only']
    },
    'discretionary_vs_nondiscretionary': {
        'name': 'Discretionary vs Non-Discretionary',
        'discretionary': ['clothing', 'electronics', 'furniture', 'sporting_goods', 'department_stores'],
        'nondiscretionary': ['grocery', 'gasoline', 'health_care', 'food_beverage']
    },
    'online_vs_offline': {
        'name': 'Online vs Traditional Retail',
        'online': ['nonstore_ecommerce'],
        'traditional': ['department_stores', 'warehouse_clubs', 'general_merchandise']
    },
    'big_ticket_items': {
        'name': 'Big Ticket Categories',
        'components': ['motor_vehicles', 'furniture', 'electronics', 'building_materials']
    }
}

# 색상 매핑 (KPDS 색상 팔레트 사용)
RETAIL_COLORS = {
    'total': KPDS_COLORS[0],                    # deepred_pds
    'ex_auto_gas': KPDS_COLORS[1],              # deepblue_pds  
    'motor_vehicles': KPDS_COLORS[2],           # beige_pds
    'clothing': KPDS_COLORS[3],                 # blue_pds
    'electronics': KPDS_COLORS[4],              # grey_pds
    'food_beverage': KPDS_COLORS[0],            # deepred_pds (순환)
    'gasoline': KPDS_COLORS[1],                 # deepblue_pds
    'nonstore_ecommerce': KPDS_COLORS[2],       # beige_pds
    'building_materials': KPDS_COLORS[3],       # blue_pds
    'food_services': KPDS_COLORS[4]             # grey_pds
}

print("✓ Retail Sales 데이터 구조 정의 완료")

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

def fetch_retail_sales_dataset(series_dict, start_date='2000-01-01', end_date=None):
    """
    여러 Retail Sales 시리즈를 한번에 가져와서 데이터프레임으로 반환
    
    Parameters:
    - series_dict: 시리즈 딕셔너리 {name: series_id}
    - start_date: 시작 날짜
    - end_date: 종료 날짜
    
    Returns:
    - pandas.DataFrame: 각 시리즈를 컬럼으로 하는 데이터프레임
    """
    print(f"📊 Retail Sales 데이터 수집 시작 ({len(series_dict)}개 시리즈)")
    
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

def save_retail_sales_data(df, filename='retail_sales_data.csv'):
    """Retail Sales 데이터를 CSV 파일로 저장"""
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

def load_retail_sales_data(filename='retail_sales_data.csv'):
    """저장된 Retail Sales 데이터를 로드"""
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

def check_recent_data_consistency(filename='retail_sales_data_complete.csv', check_count=3):
    """
    최근 N개 데이터의 일치성을 확인하여 스마트 업데이트 필요 여부 판단 (Retail Sales 특화)
    
    Parameters:
    - filename: 확인할 CSV 파일명
    - check_count: 확인할 최근 데이터 개수
    
    Returns:
    - dict: {'need_update': bool, 'reason': str, 'mismatches': list}
    """
    print(f"🔍 Retail Sales 데이터 일치성 확인 (최근 {check_count}개 데이터)")
    
    # 기존 CSV 파일 존재 여부 확인
    existing_df = load_retail_sales_data(filename)
    if existing_df is None:
        print("📄 기존 CSV 파일이 없습니다. 새로 수집이 필요합니다.")
        return {'need_update': True, 'reason': '기존 파일 없음', 'mismatches': []}
    
    # 사용 가능한 시리즈 확인
    available_series = [col for col in existing_df.columns if col in RETAIL_SALES_SERIES.keys()]
    if not available_series:
        print("❌ 확인할 시리즈가 없습니다")
        return {'need_update': True, 'reason': '시리즈 없음', 'mismatches': []}
    
    print(f"📊 확인 대상 시리즈: {len(available_series)}개")
    
    mismatches = []
    new_data_available = False
    all_data_identical = True
    
    for col_name in available_series:
        if col_name not in RETAIL_SALES_SERIES:
            continue
            
        series_id = RETAIL_SALES_SERIES[col_name]
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
        
        # 정규화된 날짜로 비교 (Retail Sales 특화: 1.0 허용 오차 - 판매 데이터 특성 고려)
        for key in existing_normalized.keys():
            if key in api_normalized:
                existing_val = existing_normalized[key]
                api_val = api_normalized[key]
                
                # 값이 다르면 불일치 (1.0 이상 차이 - 판매 데이터 특성 고려)
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
        
        # Retail Sales 특화: 1.0 이상만 실제 불일치로 간주
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
# === Retail Sales 핵심 지표 계산 함수들 ===

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

def generate_retail_sales_summary_stats(df, recent_months=12):
    """
    Retail Sales 데이터 요약 통계 생성
    
    Parameters:
    - df: Retail Sales 데이터프레임
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
        if col in RETAIL_SALES_SERIES.keys():
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

print("✅ Retail Sales 핵심 지표 계산 함수 정의 완료")

# %%
# === 다양한 자유도 높은 시각화 함수들 ===

def plot_retail_sales_overview(df):
    """
    Retail Sales 개요 차트 (개선된 버전)
    """
    if df is None or len(df) == 0:
        print("❌ 시각화할 데이터가 없습니다")
        return None
    
    # 최신 36개월 데이터 (3년 트렌드)
    chart_data = df.tail(36).copy()
    
    # 사용 가능한 컬럼 확인
    available_cols = []
    col_labels = {}
    
    if 'total' in df.columns:
        available_cols.append('total')
        col_labels['total'] = '총 소매판매'
    if 'ex_auto_gas' in df.columns:
        available_cols.append('ex_auto_gas')
        col_labels['ex_auto_gas'] = '코어 소매판매'
    if 'nonstore_ecommerce' in df.columns:
        available_cols.append('nonstore_ecommerce')
        col_labels['nonstore_ecommerce'] = '온라인 판매'
    if 'food_services' in df.columns:
        available_cols.append('food_services')
        col_labels['food_services'] = '외식업'
        
    if not available_cols:
        print("Warning: 표시할 소매판매 지표가 없습니다.")
        return None
    
    # MoM 변화율 계산
    mom_data = {}
    for col in available_cols:
        mom_data[col] = calculate_mom_change(chart_data[col])
    
    mom_df = pd.DataFrame(mom_data)
    
    # 라벨 딕셔너리 생성
    labels_dict = {col: col_labels[col] for col in available_cols}
    
    print("미국 소매판매 주요 지표 트렌드 (최근 3년, MoM %)")
    
    fig = df_multi_line_chart(
        df=mom_df,
        columns=available_cols,
        ytitle="MoM 변화율 (%)",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_retail_sales_components(df):
    """
    Retail Sales 구성요소별 차트 (다양한 시각화)
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
        
    # 최신 24개월 데이터
    chart_data = df.tail(24).copy()
    
    # 사용 가능한 컬럼 확인
    available_cols = []
    col_labels = {}
    
    if 'motor_vehicles' in df.columns:
        available_cols.append('motor_vehicles')
        col_labels['motor_vehicles'] = '자동차 판매'
    if 'clothing' in df.columns:
        available_cols.append('clothing')
        col_labels['clothing'] = '의류'
    if 'electronics' in df.columns:
        available_cols.append('electronics')
        col_labels['electronics'] = '전자제품'
    if 'food_beverage' in df.columns:
        available_cols.append('food_beverage')
        col_labels['food_beverage'] = '식료품'
    if 'gasoline' in df.columns:
        available_cols.append('gasoline')
        col_labels['gasoline'] = '주유소'
        
    if not available_cols:
        print("Warning: 표시할 소매판매 구성요소가 없습니다.")
        return None
    
    # MoM 변화율 계산
    mom_data = {}
    for col in available_cols:
        mom_data[col] = calculate_mom_change(chart_data[col])
    
    mom_df = pd.DataFrame(mom_data)
    
    labels_dict = {col: col_labels[col] for col in available_cols}
    
    print("미국 소매판매 업종별 분석 (최근 2년, MoM %)")
    
    fig = df_multi_line_chart(
        df=mom_df,
        columns=available_cols,
        ytitle="MoM 변화율 (%)",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_retail_sales_core_vs_headline(df):
    """
    Retail Sales 헤드라인 vs 코어 비교 차트
    """
    if df is None:
        print("❌ 데이터가 없습니다")
        return None
        
    # 최신 24개월 데이터
    chart_data = df.tail(24).copy()
    
    # 컬럼 설정
    available_cols = []
    col_labels = {}
    
    if 'total' in df.columns:
        available_cols.append('total')
        col_labels['total'] = '전체 소매판매'
        
    if 'ex_auto_gas' in df.columns:
        available_cols.append('ex_auto_gas')
        col_labels['ex_auto_gas'] = '코어 소매판매'
    
    if not available_cols:
        print("❌ 소매판매 헤드라인/코어 데이터가 없습니다")
        return None
    
    # MoM 변화율 계산
    mom_data = {}
    for col in available_cols:
        mom_data[col] = calculate_mom_change(chart_data[col])
    
    mom_df = pd.DataFrame(mom_data)
        
    labels_dict = {col: col_labels[col] for col in available_cols}
    
    print("미국 소매판매: 헤드라인 vs 코어 비교 (최근 2년, MoM %)")
    
    fig = df_multi_line_chart(
        df=mom_df,
        columns=available_cols,
        ytitle="MoM 변화율 (%)",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_retail_sales_latest_bar_chart(df, series_list=None, chart_type='mom'):
    """
    가장 최신 데이터를 기준으로 구성 요소별 변화를 바 그래프로 시각화 (레이블 잘림 방지)
    
    Args:
        df: Retail Sales 데이터프레임
        series_list: 표시할 시리즈 리스트 (None이면 주요 지표)
        chart_type: 'mom' (전월대비) 또는 'yoy' (전년동월대비)
    
    Returns:
        plotly.graph_objects.Figure
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
    
    # 기본 시리즈 설정
    if series_list is None:
        series_list = ['total', 'ex_auto_gas', 'motor_vehicles', 'clothing', 'electronics', 'nonstore_ecommerce']
    
    # 시리즈 유효성 확인
    available_series = [s for s in series_list if s in df.columns]
    if not available_series:
        print(f"❌ 요청한 시리즈가 데이터에 없습니다: {series_list}")
        return None
    
    # 데이터 변환
    if chart_type == 'mom':
        change_data = {}
        for col in available_series:
            mom_series = calculate_mom_change(df[col])
            change_data[col] = mom_series.iloc[-1]
        ytitle = "MoM 변화율 (%)"
        latest_date = df.index[-1].strftime('%Y년 %m월')
        print(f"소매판매 구성요소 MoM 변화율 ({latest_date})")
    else:  # yoy
        change_data = {}
        for col in available_series:
            yoy_series = calculate_yoy_change(df[col])
            change_data[col] = yoy_series.iloc[-1]
        ytitle = "YoY 변화율 (%)"
        latest_date = df.index[-1].strftime('%Y년 %m월')
        print(f"소매판매 구성요소 YoY 변화율 ({latest_date})")
    
    # 한국어 라벨 설정 (개선된 매핑)
    korean_labels = {
        'total': '전체 소매판매 및 외식업',
        'ex_auto_gas': '코어 소매판매 (자동차·주유소 제외)',
        'ex_auto': '전체 소매판매 (자동차 제외)',
        'ex_gas': '전체 소매판매 (주유소 제외)',
        'retail_only': '소매업 (외식업 제외)',
        'motor_vehicles': '자동차 및 부품 판매',
        'furniture': '가구 및 홈퍼니싱',
        'electronics': '전자제품 및 가전',
        'building_materials': '건설자재 및 원예용품',
        'food_beverage': '식료품 매장',
        'health_care': '의료 및 개인용품',
        'gasoline': '주유소',
        'clothing': '의류 및 액세서리',
        'sporting_goods': '스포츠용품·취미·도서',
        'general_merchandise': '종합소매업',
        'misc_retailers': '기타 소매업',
        'nonstore_ecommerce': '비매장 소매 (온라인 포함)',
        'food_services': '외식업 및 음료점'
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
        text=[f'{v:+.1f}%' for v in values],
        textposition='outside' if all(v >= 0 for v in values) else 'auto',  # 개선: 조건부 텍스트 위치
        showlegend=False
    ))
    
    # 레이아웃 설정 (개선된 여백 및 크기 조정)
    fig.update_layout(
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

def create_horizontal_retail_bar_chart(df, num_categories=20):
    """
    소매판매 구성요소별 MoM 변화율 가로 바 차트 생성 (레이블 잘림 방지 완전 해결)
    
    Args:
        df: Retail Sales 데이터프레임
        num_categories: 표시할 카테고리 수
    
    Returns:
        plotly figure
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
    
    # MoM 변화율 계산
    mom_data = {}
    for col in df.columns:
        mom_series = calculate_mom_change(df[col])
        if mom_series is not None:
            mom_data[col] = mom_series.iloc[-1]
    
    if not mom_data:
        print("❌ MoM 변화율 계산 실패")
        return None
    
    # 데이터 정렬 (내림차순)
    sorted_data = pd.Series(mom_data).dropna().sort_values(ascending=True)  # 가로 바차트이므로 ascending=True
    
    # 상위 num_categories개 선택
    if len(sorted_data) > num_categories:
        sorted_data = sorted_data.tail(num_categories)
    
    # 소매판매 카테고리 한국어 라벨 (개선된 매핑)
    category_labels = {
        'total': '전체 소매판매 및 외식업',
        'ex_auto_gas': '코어 소매판매\n(자동차·주유소 제외)',
        'ex_auto': '소매판매\n(자동차 제외)',
        'ex_gas': '소매판매\n(주유소 제외)',
        'retail_only': '소매업\n(외식업 제외)',
        'gafo': 'GAFO 상품군',
        'motor_vehicles': '자동차 및 부품 판매',
        'furniture': '가구 및 홈퍼니싱',
        'electronics': '전자제품 및 가전',
        'building_materials': '건설자재 및 원예용품',
        'food_beverage': '식료품 매장',
        'health_care': '의료 및 개인용품',
        'gasoline': '주유소',
        'clothing': '의류 및 액세서리',
        'sporting_goods': '스포츠용품·취미·도서',
        'general_merchandise': '종합소매업',
        'misc_retailers': '기타 소매업',
        'nonstore_ecommerce': '비매장 소매\n(온라인 포함)',
        'food_services': '외식업 및 음료점'
    }
    
    # 라벨 적용
    categories = []
    values = []
    colors = []
    
    for comp, value in sorted_data.items():
        label = category_labels.get(comp, comp)
        categories.append(label)
        values.append(value)
        
        # '전체 소매판매'는 특별 색상, 나머지는 양수/음수로 구분
        if comp == 'total':
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
    
    print("소매판매 구성요소별 월간 변화율 (전월대비 %)")
    
    # 레이아웃 설정 (최적화된 설정)
    fig.update_layout(
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

def create_custom_horizontal_retail_bar_chart(df, selected_series, chart_type='mom'):
    """
    사용자가 선택한 시리즈만으로 가로 바 차트 생성 (CPI 스타일)
    
    Args:
        df: Retail Sales 데이터프레임
        selected_series: 선택할 시리즈 리스트 (예: ['total', 'ex_auto_gas', 'motor_vehicles'])
        chart_type: 'mom' (전월대비) 또는 'yoy' (전년동월대비)
    
    Returns:
        plotly figure
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
    
    if not selected_series:
        print("❌ 선택한 시리즈가 없습니다")
        return None
    
    # 사용 가능한 시리즈 확인
    available_series = [s for s in selected_series if s in df.columns]
    if not available_series:
        print(f"❌ 요청한 시리즈가 데이터에 없습니다: {selected_series}")
        return None
    
    # 변화율 계산
    if chart_type == 'mom':
        change_data = {}
        for col in available_series:
            mom_series = calculate_mom_change(df[col])
            if mom_series is not None:
                change_data[col] = mom_series.iloc[-1]
        ytitle = "%"
        chart_type_label = "전월대비"
    else:  # yoy
        change_data = {}
        for col in available_series:
            yoy_series = calculate_yoy_change(df[col])
            if yoy_series is not None:
                change_data[col] = yoy_series.iloc[-1]
        ytitle = "%"
        chart_type_label = "전년동월대비"
    
    if not change_data:
        print("❌ 변화율 계산 실패")
        return None
    
    # 데이터 정렬 (내림차순)
    sorted_data = pd.Series(change_data).sort_values(ascending=True)  # 가로 바차트이므로 ascending=True
    
    # 소매판매 카테고리 한국어 라벨 (개선된 매핑)
    category_labels = {
        'total': '전체 소매판매 및 외식업',
        'ex_auto_gas': '코어 소매판매 (자동차·주유소 제외)',
        'ex_auto': '전체 소매판매 (자동차 제외)',
        'ex_gas': '전체 소매판매 (주유소 제외)',
        'retail_only': '소매업 (외식업 제외)',
        'gafo': 'GAFO 상품군',
        'motor_vehicles': '자동차 및 부품 판매',
        'furniture': '가구 및 홈퍼니싱',
        'electronics': '전자제품 및 가전',
        'building_materials': '건설자재 및 원예용품',
        'food_beverage': '식료품 매장',
        'health_care': '의료 및 개인용품',
        'gasoline': '주유소',
        'clothing': '의류 및 액세서리',
        'sporting_goods': '스포츠용품·취미·도서',
        'general_merchandise': '종합소매업',
        'misc_retailers': '기타 소매업',
        'nonstore_ecommerce': '비매장 소매 (온라인 포함)',
        'food_services': '외식업 및 음료점'
    }
    
    # 라벨 적용
    categories = []
    values = []
    colors = []
    
    for comp, value in sorted_data.items():
        label = category_labels.get(comp, comp)
        categories.append(label)
        values.append(value)
        
        # '전체 소매판매'는 특별 색상, 나머지는 양수/음수로 구분
        if comp == 'total':
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
    
    # 기본 제목 설정
    if not df.empty:
        latest_date = df.index[-1].strftime('%Y년 %m월')
        print(f"소매판매 주요 항목 {chart_type_label} 변화율 ({latest_date})")
    else:
        print(f"소매판매 주요 항목 {chart_type_label} 변화율")
    
    # 레이아웃 설정
    fig.update_layout(
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=900,
        height=max(400, len(categories) * 35),  # 동적 높이 계산
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
        margin=dict(l=300, r=80, t=80, b=60)  # 여백 최적화
    )
    
    # 격자선 및 0선
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="lightgrey")
    fig.update_yaxes(showgrid=False)
    fig.add_vline(x=0, line_width=2, line_color="black")
    
    return fig

# %%
# === 고급 시각화 함수들 ===

def create_retail_sales_dashboard(df, periods=24):
    """
    소매판매 데이터를 위한 커스텀 대시보드 (여러 차트를 서브플롯으로 결합)
    
    Args:
        df: 소매판매 데이터프레임
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
            "주요 소매판매 지표 트렌드",
            "MoM 변화율",
            "헤드라인 vs 코어 소매판매",
            "최신 MoM 변화율"
        ],
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"type": "bar"}]]
    )
    
    chart_data = df.tail(periods)
    
    # 1. 주요 소매판매 지표 (1,1) - MoM
    retail_indicators = ['total', 'motor_vehicles', 'nonstore_ecommerce']
    for i, indicator in enumerate(retail_indicators):
        if indicator in chart_data.columns:
            mom_data = calculate_mom_change(chart_data[indicator])
            fig.add_trace(
                go.Scatter(x=chart_data.index, y=mom_data,
                          name={'total': '전체', 'motor_vehicles': '자동차', 'nonstore_ecommerce': '온라인'}[indicator],
                          line=dict(color=get_kpds_color(i))),
                row=1, col=1
            )
    
    # 2. MoM 변화율 (1,2)
    if 'total' in chart_data.columns and 'ex_auto_gas' in chart_data.columns:
        total_mom = calculate_mom_change(chart_data['total'])
        core_mom = calculate_mom_change(chart_data['ex_auto_gas'])
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=total_mom,
                      name='전체 MoM', line=dict(color=get_kpds_color(0))),
            row=1, col=2
        )
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=core_mom,
                      name='코어 MoM', line=dict(color=get_kpds_color(1))),
            row=1, col=2
        )
    
    # 3. 헤드라인 vs 코어 (2,1) - YoY
    if 'total' in chart_data.columns and 'ex_auto_gas' in chart_data.columns:
        total_yoy = calculate_yoy_change(chart_data['total'])
        core_yoy = calculate_yoy_change(chart_data['ex_auto_gas'])
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=total_yoy,
                      name='헤드라인 YoY', line=dict(color=get_kpds_color(0))),
            row=2, col=1
        )
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=core_yoy,
                      name='코어 YoY', line=dict(color=get_kpds_color(1))),
            row=2, col=1
        )
    
    # 4. 최신 MoM 변화율 바 차트 (2,2)
    series_to_show = ['total', 'ex_auto_gas', 'motor_vehicles', 'clothing']
    available_series = [s for s in series_to_show if s in chart_data.columns]
    
    if available_series:
        korean_names = {
            'total': '전체',
            'ex_auto_gas': '코어',
            'motor_vehicles': '자동차',
            'clothing': '의류'
        }
        
        categories = [korean_names.get(s, s) for s in available_series]
        mom_values = []
        for s in available_series:
            mom_series = calculate_mom_change(chart_data[s])
            mom_values.append(mom_series.iloc[-1])
        
        colors = [get_kpds_color(i) for i in range(len(mom_values))]
        
        fig.add_trace(
            go.Bar(x=categories, y=mom_values, marker_color=colors,
                  text=[f'{v:+.1f}%' for v in mom_values], textposition='outside'),
            row=2, col=2
        )
    
    print(f"미국 소매판매 종합 대시보드 (최근 {periods//12:.0f}년)")
    
    # 레이아웃 업데이트
    fig.update_layout(
        height=800,
        showlegend=False,
        font=dict(family='NanumGothic', size=10)
    )
    
    # 각 서브플롯의 y축 레이블
    fig.update_yaxes(title_text="MoM %", row=1, col=1)
    fig.update_yaxes(title_text="MoM %", row=1, col=2)
    fig.update_yaxes(title_text="YoY %", row=2, col=1)
    fig.update_yaxes(title_text="MoM %", row=2, col=2)
    
    return fig

def create_retail_category_analysis(df, category_type='discretionary', periods=24):
    """
    소매판매 카테고리별 심화 분석
    
    Args:
        df: 소매판매 데이터프레임
        category_type: 'discretionary', 'nondiscretionary', 'online_vs_offline'
        periods: 분석할 기간 (개월)
    
    Returns:
        plotly Figure
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
    
    # 카테고리별 시리즈 정의
    if category_type == 'discretionary':
        target_series = ['clothing', 'electronics', 'furniture', 'sporting_goods']
        category_name = '재량 소비 품목'
    elif category_type == 'nondiscretionary':
        target_series = ['grocery', 'gasoline', 'health_care', 'food_beverage']
        category_name = '필수 소비 품목'
    elif category_type == 'online_vs_offline':
        target_series = ['nonstore_ecommerce', 'department_stores', 'general_merchandise']
        category_name = '온라인 vs 전통 소매'
    else:
        print(f"❌ 지원하지 않는 카테고리: {category_type}")
        return None
    
    available_series = [s for s in target_series if s in df.columns]
    
    if not available_series:
        print(f"❌ {category_name} 데이터가 없습니다")
        return None
    
    chart_data = df[available_series].tail(periods)
    
    # MoM 변화율 계산
    mom_data = {}
    for col in available_series:
        mom_data[col] = calculate_mom_change(chart_data[col])
    
    mom_df = pd.DataFrame(mom_data)
    
    # 한국어 라벨 (개선된 매핑)
    korean_labels = {
        'clothing': '의류 및 액세서리',
        'electronics': '전자제품 및 가전',
        'furniture': '가구 및 홈퍼니싱',
        'food_beverage': '식료품 매장',
        'gasoline': '주유소',
        'health_care': '의료 및 개인용품',
        'nonstore_ecommerce': '비매장 소매 (온라인 포함)',
        'general_merchandise': '종합소매업',
        'sporting_goods': '스포츠용품·취미·도서',
        'building_materials': '건설자재 및 원예용품',
        'misc_retailers': '기타 소매업',
        'food_services': '외식업 및 음료점'
    }
    
    labels_dict = {col: korean_labels.get(col, col) for col in available_series}
    
    print(f"{category_name} MoM 변화율 분석 (최근 {periods//12:.0f}년)")
    
    fig = df_multi_line_chart(
        df=mom_df,
        columns=available_series,
        ytitle="MoM 변화율 (%)",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

# %%
# === 메인 실행 함수 ===

def run_retail_sales_analysis(update_data=True, start_date='2000-01-01', 
                             charts=['overview', 'components', 'core_vs_headline', 'mom_bar', 'horizontal_bar', 'dashboard', 'category_analysis'], 
                             smart_update=True):
    """
    완전한 소매판매 분석 실행 함수 (스마트 업데이트 지원)
    
    Parameters:
    - update_data: 데이터 업데이트 여부
    - start_date: 데이터 시작 날짜
    - charts: 생성할 차트 목록
    - smart_update: 스마트 업데이트 사용 여부 (최근 3개 데이터 비교)
    
    Returns:
    - dict: 분석 결과 및 차트들
    """
    print("🚀 US Retail Sales 분석 시작")
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
                df = load_retail_sales_data('retail_sales_data_complete.csv')
                if df is None:
                    print("⚠️ CSV 파일 로드 실패, API에서 새로 수집합니다")
                    df = fetch_retail_sales_dataset(RETAIL_SALES_SERIES, start_date=start_date)
                    if df is not None:
                        save_retail_sales_data(df, 'retail_sales_data_complete.csv')
            else:
                print(f"🔄 업데이트 필요: {consistency_check['reason']}")
                print("📊 FRED API에서 최신 데이터 수집 중...")
                df = fetch_retail_sales_dataset(RETAIL_SALES_SERIES, start_date=start_date)
                if df is not None:
                    save_retail_sales_data(df, 'retail_sales_data_complete.csv')
        else:
            # 스마트 업데이트 비활성화시 무조건 새로 수집
            print("📊 FRED API에서 최신 데이터 수집 중...")
            df = fetch_retail_sales_dataset(RETAIL_SALES_SERIES, start_date=start_date)
            if df is not None:
                save_retail_sales_data(df, 'retail_sales_data_complete.csv')
    else:
        print("📁 기존 저장된 데이터 로드 중...")
        df = load_retail_sales_data('retail_sales_data_complete.csv')
        if df is None:
            print("⚠️ 기존 데이터가 없어서 새로 수집합니다")
            df = fetch_retail_sales_dataset(RETAIL_SALES_SERIES, start_date=start_date)
            if df is not None:
                save_retail_sales_data(df, 'retail_sales_data_complete.csv')
    
    if df is None:
        print("❌ 데이터 수집 실패")
        return None
    
    # 2) 기본 통계 생성
    print("\n📈 기본 통계 생성 중...")
    stats = generate_retail_sales_summary_stats(df)
    
    if stats:
        print(f"\n✅ 데이터 요약:")
        metadata = stats['metadata']
        print(f"   - 분석 기간: {metadata['start_date']} ~ {metadata['latest_date']}")
        print(f"   - 총 데이터 포인트: {metadata['data_points']}개월")
        
        # 주요 지표 최신값 출력
        key_indicators = ['total', 'ex_auto_gas', 'motor_vehicles', 'nonstore_ecommerce']
        for indicator in key_indicators:
            if indicator in stats:
                stat = stats[indicator]
                latest = stat['latest_value']
                mom = stat['mom_change']
                yoy = stat['yoy_change']
                
                if latest and mom and yoy:
                    print(f"   - {indicator}: ${latest:,.0f}M (MoM: {mom:+.1f}%, YoY: {yoy:+.1f}%)")
    
    # 3) 차트 생성
    print("\n🎨 차트 생성 중...")
    figures = {}
    
    # 1. 개요 차트
    if 'overview' in charts:
        print("   - 소매판매 개요 (트렌드 분석)")
        figures['overview'] = plot_retail_sales_overview(df)
    
    # 2. 소매판매 구성요소 분석
    if 'components' in charts:
        print("   - 소매판매 구성요소 분석")
        figures['components'] = plot_retail_sales_components(df)
    
    # 3. 헤드라인 vs 코어 분석
    if 'core_vs_headline' in charts:
        print("   - 헤드라인 vs 코어 소매판매")
        figures['core_vs_headline'] = plot_retail_sales_core_vs_headline(df)
    
    # 4. 최신 데이터 바 차트
    if 'mom_bar' in charts:
        print("   - 최신 MoM 변화율 바 차트")
        figures['mom_bar'] = plot_retail_sales_latest_bar_chart(df, chart_type='mom')
    
    # 5. 가로 바 차트 (레이블 잘림 방지)
    if 'horizontal_bar' in charts:
        print("   - 가로 바 차트 (완전한 레이블 표시)")
        figures['horizontal_bar'] = create_horizontal_retail_bar_chart(df, num_categories=25)  # 더 많은 카테고리 표시
    
    # 5-1. 전체 가로 바 차트 (모든 시리즈)
    if 'horizontal_bar_all' in charts:
        print("   - 전체 가로 바 차트 (모든 시리즈)")
        figures['horizontal_bar_all'] = create_horizontal_retail_bar_chart(df, num_categories=50)
    
    # 6. 커스텀 대시보드
    if 'dashboard' in charts:
        print("   - 커스텀 대시보드")
        figures['dashboard'] = create_retail_sales_dashboard(df)
    
    # 7. 카테고리 분석
    if 'category_analysis' in charts:
        print("   - 카테고리별 심화 분석")
        figures['discretionary'] = create_retail_category_analysis(df, 'discretionary')
        figures['nondiscretionary'] = create_retail_category_analysis(df, 'nondiscretionary')
        figures['online_vs_offline'] = create_retail_category_analysis(df, 'online_vs_offline')
    
    print(f"\n✅ 소매판매 분석 완료!")
    print(f"   - 생성된 차트: {len(figures)}개")
    print(f"   - 사용 가능한 지표: {len(df.columns)}개")
    
    return {
        'data': df,
        'stats': stats,
        'figures': figures
    }

# 빠른 테스트용 함수
def quick_retail_sales_test():
    """빠른 소매판매 분석 테스트 (최근 5년 데이터만)"""
    print("🧪 소매판매 분석 빠른 테스트")
    return run_retail_sales_analysis(
        update_data=True,
        start_date='2019-01-01',
        charts=['overview', 'core_vs_headline', 'mom_bar', 'horizontal_bar']
    )

def get_available_retail_series():
    """사용 가능한 소매판매 시리즈 목록 출력"""
    print("=== 사용 가능한 US Retail Sales 시리즈 ===")
    print("\n📊 핵심 소매판매 지표:")
    core_series = {
        'total': '소매판매 및 외식업 전체',
        'ex_auto_gas': '코어 소매판매 (자동차·주유소 제외)',
        'retail_only': '소매업만 (외식업 제외)',
        'gafo': 'GAFO (백화점 유형 상품)'
    }
    
    for key, desc in core_series.items():
        print(f"  '{key}': {desc}")
    
    print("\n💱 주요 업종별:")
    sector_series = {
        'motor_vehicles': '자동차 및 부품 판매',
        'clothing': '의류 및 액세서리',
        'electronics': '전자제품 및 가전',
        'food_beverage': '식료품 매장',
        'gasoline': '주유소',
        'nonstore_ecommerce': '비매장 소매 (온라인 포함)',
        'building_materials': '건설자재 및 원예용품',
        'food_services': '외식업'
    }
    
    for key, desc in sector_series.items():
        print(f"  '{key}': {desc}")
    
    print("\n💰 세부 카테고리:")
    detail_series = {
        'furniture': '가구 및 홈퍼니싱',
        'health_care': '의료 및 개인용품',
        'department_stores': '백화점',
        'warehouse_clubs': '대형마트 및 창고형 매장',
        'grocery': '식료품점',
        'auto_dealers': '자동차 대리점'
    }
    
    for key, desc in detail_series.items():
        print(f"  '{key}': {desc}")

print("✅ US Advance Retail Sales 분석 도구 - Seasonally Adjusted 시리즈 사용!")
print("\n🎯 사용법:")
print("   - 전체 분석: result = run_retail_sales_analysis()")
print("   - 빠른 테스트: result = quick_retail_sales_test()")
print("   - 차트 표시: result['figures']['overview'].show()")
print("   - 가로 바 차트: result['figures']['horizontal_bar'].show()")
print("   - 모든 시리즈: result = run_retail_sales_analysis(charts=['horizontal_bar_all'])")
print("   - 시리즈 목록: get_available_retail_series()")
print("\n🔄 업데이트 내용:")
print("   - FRED Advance Retail Sales 시리즈로 교체 완료")
print("   - 한국어 매핑 개선 (상세 업종명 포함)")
print("   - Seasonally Adjusted 데이터 사용 (Raw, MoM 계산 적용)")
print("   - 사용자 선택 시리즈 가로 바 차트 기능 추가")
print("   - 시리즈 ID 오류 수정 (gasoline, food_beverage 등)")
print("\n📊 새로운 기능:")
print("   - create_custom_horizontal_retail_bar_chart(): 선택한 시리즈만 표시")
print("   - 예시: create_custom_horizontal_retail_bar_chart(data, ['total', 'motor_vehicles', 'clothing'])")
print("   - 전체 시리즈 표시: charts=['horizontal_bar_all'] 옵션 사용")
print("="*60)

# %%
# 실행 예시 (주석 해제하여 사용)
result = run_retail_sales_analysis()
result['figures']['overview'].show()
result['figures']['horizontal_bar'].show()  # 새로운 가로 바 차트

print("✅ 기본 시각화 함수들 정의 완료")
# %%
