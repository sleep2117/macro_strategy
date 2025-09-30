# %%
"""
FRED API 전용 US Durable Goods 분석 및 시각화 도구
- FRED API를 사용하여 US 내구재 관련 데이터 수집
- 내구재 주문, 출하량, 미충족 주문, 재고 데이터 분류 및 분석
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
# === US Durable Goods 데이터 계층 구조 정의 ===

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

# 내구재 분석을 위한 계층 구조
DURABLE_GOODS_HIERARCHY = {
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

# 색상 매핑 (KPDS 색상 팔레트 사용)
DURABLE_GOODS_COLORS = {
    'shipments_total': KPDS_COLORS[0],              # deepred_pds
    'orders_total': KPDS_COLORS[1],                 # deepblue_pds  
    'unfilled_total': KPDS_COLORS[2],               # beige_pds
    'inventory_total': KPDS_COLORS[3],              # blue_pds
    'shipments_core_capital': KPDS_COLORS[4],       # grey_pds
    'orders_core_capital': KPDS_COLORS[0],          # deepred_pds (순환)
    'shipments_transportation': KPDS_COLORS[1],     # deepblue_pds
    'orders_transportation': KPDS_COLORS[2],        # beige_pds
    'shipments_machinery': KPDS_COLORS[3],          # blue_pds
    'orders_machinery': KPDS_COLORS[4]              # grey_pds
}

print("✓ Durable Goods 데이터 구조 정의 완료")

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

# %%
# === 스마트 업데이트 기능 추가 ===

def check_recent_durable_goods_consistency(file_path='durable_goods_data.csv', check_count=3):
    """
    최근 N개 데이터의 일치성을 확인하는 함수 (스마트 업데이트)
    
    Args:
        file_path: 기존 CSV 파일 경로
        check_count: 확인할 최근 데이터 개수
    
    Returns:
        dict: 일치성 확인 결과 (need_update, reason, mismatches)
    """
    # 기존 데이터 로드 시도
    existing_df = load_durable_goods_data(file_path)
    if existing_df is None:
        return {'need_update': True, 'reason': '기존 데이터 없음'}
    
    print(f"📊 최근 {check_count}개 데이터 일치성 확인 중...")
    
    # 주요 시리즈만 체크 (속도 향상)
    key_series = ['shipments_total', 'orders_total', 'unfilled_orders_total', 'inventories_total']
    available_series = [s for s in key_series if s in existing_df.columns]
    
    if not available_series:
        return {'need_update': True, 'reason': '주요 시리즈 없음'}
    
    mismatches = []
    new_data_available = False
    all_data_identical = True
    
    for col_name in available_series:
        if col_name not in DURABLE_GOODS_SERIES:
            continue
            
        series_id = DURABLE_GOODS_SERIES[col_name]
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
        
        # 정규화된 날짜로 비교
        for key in existing_normalized.keys():
            if key in api_normalized:
                existing_val = existing_normalized[key]
                api_val = api_normalized[key]
                
                # 값이 다르면 불일치 (1.0 이상 차이 - 내구재 주문/출하 데이터 특성 고려)
                if abs(existing_val - api_val) > 1.0:
                    mismatches.append({
                        'series': col_name,
                        'date': pd.Timestamp(key[0], key[1], 1),
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
                print(f"   - {mismatch['series']}: {mismatch['existing_latest'].strftime('%Y-%m')} → {mismatch['api_latest'].strftime('%Y-%m')}")
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
        
        # 큰 차이만 실제 불일치로 간주 (1.0 이상 - Durable Goods는 큰 단위 사용)
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

def fetch_durable_goods_dataset(series_dict, start_date='2000-01-01', end_date=None):
    """
    여러 Durable Goods 시리즈를 한번에 가져와서 데이터프레임으로 반환
    
    Parameters:
    - series_dict: 시리즈 딕셔너리 {name: series_id}
    - start_date: 시작 날짜
    - end_date: 종료 날짜
    
    Returns:
    - pandas.DataFrame: 각 시리즈를 컬럼으로 하는 데이터프레임
    """
    print(f"📊 Durable Goods 데이터 수집 시작 ({len(series_dict)}개 시리즈)")
    
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

def save_durable_goods_data(df, filename='durable_goods_data.csv'):
    """Durable Goods 데이터를 CSV 파일로 저장"""
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

def load_durable_goods_data(filename='durable_goods_data.csv'):
    """저장된 Durable Goods 데이터를 로드"""
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

print("✅ FRED API 데이터 수집 함수 정의 완료")

# %%
# === Durable Goods 핵심 지표 계산 함수들 ===

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

def generate_durable_goods_summary_stats(df, recent_months=12):
    """
    Durable Goods 데이터 요약 통계 생성
    
    Parameters:
    - df: Durable Goods 데이터프레임
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
        if col in DURABLE_GOODS_SERIES.keys():
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

print("✅ Durable Goods 핵심 지표 계산 함수 정의 완료")

# %%
# === 다양한 자유도 높은 시각화 함수들 ===

def plot_durable_goods_overview(df):
    """
    Durable Goods 개요 차트 (개선된 버전)
    """
    print("미국 내구재 주요 지표 트렌드 (최근 3년, MoM %)")
    if df is None or len(df) == 0:
        print("❌ 시각화할 데이터가 없습니다")
        return None
    
    # 최신 36개월 데이터 (3년 트렌드)
    chart_data = df.tail(36).copy()
    
    # 사용 가능한 컬럼 확인
    available_cols = []
    col_labels = {}
    
    if 'shipments_total' in df.columns:
        available_cols.append('shipments_total')
        col_labels['shipments_total'] = '총 출하량'
    if 'orders_total' in df.columns:
        available_cols.append('orders_total')
        col_labels['orders_total'] = '총 신규주문'
    if 'orders_core_capital' in df.columns:
        available_cols.append('orders_core_capital')
        col_labels['orders_core_capital'] = '코어 자본재 주문'
    if 'unfilled_total' in df.columns:
        available_cols.append('unfilled_total')
        col_labels['unfilled_total'] = '미충족 주문'
        
    if not available_cols:
        print("Warning: 표시할 내구재 지표가 없습니다.")
        return None
    
    # MoM 변화율 계산
    mom_data = {}
    for col in available_cols:
        mom_data[col] = calculate_mom_change(chart_data[col])
    
    mom_df = pd.DataFrame(mom_data)
    
    # 라벨 딕셔너리 생성
    labels_dict = {col: col_labels[col] for col in available_cols}
    
    fig = df_multi_line_chart(
        df=mom_df,
        columns=available_cols,
        ytitle="MoM 변화율 (%)",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_durable_goods_sectors(df):
    """
    Durable Goods 업종별 차트 (다양한 시각화)
    """
    print("미국 내구재 업종별 출하량 분석 (최근 2년, MoM %)")
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
        
    # 최신 24개월 데이터
    chart_data = df.tail(24).copy()
    
    # 사용 가능한 컬럼 확인
    available_cols = []
    col_labels = {}
    
    if 'shipments_transportation' in df.columns:
        available_cols.append('shipments_transportation')
        col_labels['shipments_transportation'] = '운송장비 출하'
    if 'shipments_machinery' in df.columns:
        available_cols.append('shipments_machinery')
        col_labels['shipments_machinery'] = '기계장비 출하'
    if 'shipments_computers_electronics' in df.columns:
        available_cols.append('shipments_computers_electronics')
        col_labels['shipments_computers_electronics'] = '컴퓨터/전자 출하'
    if 'shipments_primary_metals' in df.columns:
        available_cols.append('shipments_primary_metals')
        col_labels['shipments_primary_metals'] = '1차금속 출하'
        
    if not available_cols:
        print("Warning: 표시할 업종별 데이터가 없습니다.")
        return None
    
    # MoM 변화율 계산
    mom_data = {}
    for col in available_cols:
        mom_data[col] = calculate_mom_change(chart_data[col])
    
    mom_df = pd.DataFrame(mom_data)
    
    labels_dict = {col: col_labels[col] for col in available_cols}
    
    fig = df_multi_line_chart(
        df=mom_df,
        columns=available_cols,
        ytitle="MoM 변화율 (%)",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_durable_goods_orders_vs_shipments(df):
    """
    Durable Goods 주문 vs 출하량 비교 차트
    """
    print("미국 내구재: 주문 vs 출하량 비교 (최근 2년, MoM %)")
    if df is None:
        print("❌ 데이터가 없습니다")
        return None
        
    # 최신 24개월 데이터
    chart_data = df.tail(24).copy()
    
    # 컬럼 설정
    available_cols = []
    col_labels = {}
    
    if 'orders_total' in df.columns:
        available_cols.append('orders_total')
        col_labels['orders_total'] = '총 신규주문'
        
    if 'shipments_total' in df.columns:
        available_cols.append('shipments_total')
        col_labels['shipments_total'] = '총 출하량'
    
    if not available_cols:
        print("❌ 주문/출하량 데이터가 없습니다")
        return None
    
    # MoM 변화율 계산
    mom_data = {}
    for col in available_cols:
        mom_data[col] = calculate_mom_change(chart_data[col])
    
    mom_df = pd.DataFrame(mom_data)
        
    labels_dict = {col: col_labels[col] for col in available_cols}
    
    fig = df_multi_line_chart(
        df=mom_df,
        columns=available_cols,
        ytitle="MoM 변화율 (%)",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def create_horizontal_durable_goods_bar_chart(df, num_categories=20, metric_type='shipments'):
    """
    내구재 구성요소별 MoM 변화율 가로 바 차트 생성
    
    Args:
        df: Durable Goods 데이터프레임
        num_categories: 표시할 카테고리 수
        metric_type: 'shipments', 'orders', 'unfilled', 'inventory' 중 선택
    
    Returns:
        plotly figure
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
    
    # 선택한 메트릭 타입에 따라 시리즈 필터링
    metric_cols = [col for col in df.columns if col.startswith(f'{metric_type}_')]
    
    if not metric_cols:
        print(f"❌ {metric_type} 관련 데이터가 없습니다")
        return None
    
    # MoM 변화율 계산
    mom_data = {}
    for col in metric_cols:
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
    
    # 내구재 카테고리 한국어 라벨
    category_labels = {
        f'{metric_type}_total': f'전체 {get_metric_korean_name(metric_type)}',
        f'{metric_type}_ex_transport': f'{get_metric_korean_name(metric_type)} (운송장비 제외)',
        f'{metric_type}_ex_defense': f'{get_metric_korean_name(metric_type)} (국방 제외)',
        f'{metric_type}_primary_metals': '1차 금속',
        f'{metric_type}_fabricated_metals': '금속 가공품',
        f'{metric_type}_machinery': '기계장비',
        f'{metric_type}_computers_electronics': '컴퓨터/전자제품',
        f'{metric_type}_computers': '컴퓨터 관련',
        f'{metric_type}_communications': '통신장비',
        f'{metric_type}_electrical': '전기장비/가전',
        f'{metric_type}_transportation': '운송장비',
        f'{metric_type}_motor_vehicles': '자동차/부품',
        f'{metric_type}_nondef_aircraft': '민간 항공기',
        f'{metric_type}_defense_aircraft': '군용 항공기',
        f'{metric_type}_other_durables': '기타 내구재',
        f'{metric_type}_capital_goods': '자본재',
        f'{metric_type}_nondef_capital': '민간 자본재',
        f'{metric_type}_core_capital': '코어 자본재',
        f'{metric_type}_defense_capital': '국방 자본재'
    }
    
    # 라벨 적용
    categories = []
    values = []
    colors = []
    
    for comp, value in sorted_data.items():
        label = category_labels.get(comp, comp.replace(f'{metric_type}_', ''))
        categories.append(label)
        values.append(value)
        
        # '전체' 시리즈는 특별 색상, 나머지는 양수/음수로 구분
        if 'total' in comp:
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
    
    # 제목 출력
    metric_korean = get_metric_korean_name(metric_type)
    if not df.empty:
        latest_date = df.index[-1].strftime('%Y년 %m월')
        print(f"내구재 {metric_korean} 구성요소별 월간 변화율 ({latest_date})")
    else:
        print(f"내구재 {metric_korean} 구성요소별 월간 변화율")
    
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

def create_custom_horizontal_durable_goods_bar_chart(df, selected_series, chart_type='mom'):
    """
    사용자가 선택한 시리즈만으로 가로 바 차트 생성
    
    Args:
        df: Durable Goods 데이터프레임
        selected_series: 선택할 시리즈 리스트 (예: ['shipments_total', 'orders_total', 'unfilled_total'])
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
        chart_type_label = "전월대비"
    else:  # yoy
        change_data = {}
        for col in available_series:
            yoy_series = calculate_yoy_change(df[col])
            if yoy_series is not None:
                change_data[col] = yoy_series.iloc[-1]
        chart_type_label = "전년동월대비"
    
    if not change_data:
        print("❌ 변화율 계산 실패")
        return None
    
    # 데이터 정렬 (내림차순)
    sorted_data = pd.Series(change_data).sort_values(ascending=True)  # 가로 바차트이므로 ascending=True
    
    # 내구재 카테고리 한국어 라벨
    category_labels = get_durable_goods_korean_labels()
    
    # 라벨 적용
    categories = []
    values = []
    colors = []
    
    for comp, value in sorted_data.items():
        label = category_labels.get(comp, comp)
        categories.append(label)
        values.append(value)
        
        # 특별 색상 규칙
        if 'total' in comp:
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
    
    # 제목 출력
    if not df.empty:
        latest_date = df.index[-1].strftime('%Y년 %m월')
        print(f"내구재 주요 지표 {chart_type_label} 변화율 ({latest_date})")
    else:
        print(f"내구재 주요 지표 {chart_type_label} 변화율")
    
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

# 헬퍼 함수들
def get_metric_korean_name(metric_type):
    """메트릭 타입을 한국어로 변환"""
    metric_names = {
        'shipments': '출하량',
        'orders': '신규주문',
        'unfilled': '미충족 주문',
        'inventory': '재고'
    }
    return metric_names.get(metric_type, metric_type)

def get_durable_goods_korean_labels():
    """내구재 전체 한국어 라벨 매핑"""
    return {
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

print("✅ 내구재 시각화 함수들 정의 완료")

# %%
# === 고급 시각화 함수들 ===

def create_durable_goods_dashboard(df, periods=24):
    """
    내구재 데이터를 위한 커스텀 대시보드 (여러 차트를 서브플롯으로 결합)
    
    Args:
        df: 내구재 데이터프레임
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
            "주요 내구재 지표 트렌드",
            "주문 vs 출하량",
            "코어 자본재 vs 총 내구재",
            "최신 MoM 변화율"
        ],
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"type": "bar"}]]
    )
    
    chart_data = df.tail(periods)
    
    # 1. 주요 내구재 지표 (1,1) - MoM
    durable_indicators = ['shipments_total', 'orders_core_capital', 'unfilled_total']
    for i, indicator in enumerate(durable_indicators):
        if indicator in chart_data.columns:
            mom_data = calculate_mom_change(chart_data[indicator])
            fig.add_trace(
                go.Scatter(x=chart_data.index, y=mom_data,
                          name={'shipments_total': '총출하', 'orders_core_capital': '코어자본재', 'unfilled_total': '미충족주문'}[indicator],
                          line=dict(color=get_kpds_color(i))),
                row=1, col=1
            )
    
    # 2. 주문 vs 출하량 (1,2)
    if 'orders_total' in chart_data.columns and 'shipments_total' in chart_data.columns:
        orders_mom = calculate_mom_change(chart_data['orders_total'])
        shipments_mom = calculate_mom_change(chart_data['shipments_total'])
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=orders_mom,
                      name='총주문 MoM', line=dict(color=get_kpds_color(0))),
            row=1, col=2
        )
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=shipments_mom,
                      name='총출하 MoM', line=dict(color=get_kpds_color(1))),
            row=1, col=2
        )
    
    # 3. 코어 자본재 vs 총 내구재 (2,1) - YoY
    if 'orders_core_capital' in chart_data.columns and 'orders_total' in chart_data.columns:
        core_yoy = calculate_yoy_change(chart_data['orders_core_capital'])
        total_yoy = calculate_yoy_change(chart_data['orders_total'])
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=core_yoy,
                      name='코어자본재 YoY', line=dict(color=get_kpds_color(0))),
            row=2, col=1
        )
        fig.add_trace(
            go.Scatter(x=chart_data.index, y=total_yoy,
                      name='총내구재 YoY', line=dict(color=get_kpds_color(1))),
            row=2, col=1
        )
    
    # 4. 최신 MoM 변화율 바 차트 (2,2)
    series_to_show = ['shipments_total', 'orders_total', 'orders_core_capital', 'unfilled_total']
    available_series = [s for s in series_to_show if s in chart_data.columns]
    
    if available_series:
        korean_names = {
            'shipments_total': '총출하',
            'orders_total': '총주문',
            'orders_core_capital': '코어자본재',
            'unfilled_total': '미충족주문'
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
    
    # 레이아웃 업데이트
    fig.update_layout(
        title_text=f"미국 내구재 종합 대시보드 (최근 {periods//12:.0f}년)",
        title_x=0.5,
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

def create_durable_category_analysis(df, category_type='capital_goods', periods=24):
    """
    내구재 카테고리별 심화 분석
    
    Args:
        df: 내구재 데이터프레임
        category_type: 'capital_goods', 'transport_analysis', 'sector_analysis'
        periods: 분석할 기간 (개월)
    
    Returns:
        plotly Figure
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
    
    # 카테고리별 시리즈 정의
    if category_type == 'capital_goods':
        target_series = ['orders_capital_goods', 'orders_nondef_capital', 'orders_core_capital']
        category_name = '자본재 분석'
    elif category_type == 'transport_analysis':
        target_series = ['shipments_transportation', 'shipments_motor_vehicles', 'shipments_nondef_aircraft']
        category_name = '운송장비 분석'
    elif category_type == 'sector_analysis':
        target_series = ['shipments_machinery', 'shipments_computers_electronics', 'shipments_primary_metals']
        category_name = '업종별 분석'
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
    
    # 한국어 라벨
    korean_labels = get_durable_goods_korean_labels()
    labels_dict = {col: korean_labels.get(col, col) for col in available_series}
    
    fig = df_multi_line_chart(
        df=mom_df,
        columns=available_series,
        title=f"{category_name} MoM 변화율 분석 (최근 {periods//12:.0f}년)",
        ytitle="MoM 변화율 (%)",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

# %%
# === 메인 실행 함수 ===

def run_durable_goods_analysis(update_data=True, start_date='2000-01-01', 
                               charts=['overview', 'sectors', 'orders_vs_shipments', 'horizontal_bar_shipments', 'horizontal_bar_orders', 'dashboard', 'category_analysis'], 
                               smart_update=True):
    """
    완전한 내구재 분석 실행 함수 (스마트 업데이트 지원)
    
    Parameters:
    - update_data: 데이터 업데이트 여부
    - start_date: 데이터 시작 날짜
    - charts: 생성할 차트 목록
    - smart_update: 스마트 업데이트 사용 여부 (최근 3개 데이터 비교)
    
    Returns:
    - dict: 분석 결과 및 차트들
    """
    print("🚀 US Durable Goods 분석 시작")
    print("=" * 50)
    
    # 1) 데이터 로드 또는 업데이트 (스마트 업데이트 적용)
    if update_data:
        # 스마트 업데이트 활성화시 최근 데이터 일치성 확인
        if smart_update:
            print("🔍 스마트 업데이트: 최근 데이터 일치성 확인...")
            consistency_check = check_recent_durable_goods_consistency('durable_goods_data_complete.csv')
            
            # 업데이트 필요 없으면 기존 데이터 사용
            if not consistency_check['need_update']:
                print("🎯 데이터가 최신 상태입니다. 기존 CSV 파일 사용.")
                df = load_durable_goods_data('durable_goods_data_complete.csv')
                if df is None:
                    print("⚠️ CSV 파일 로드 실패, API에서 새로 수집합니다")
                    df = fetch_durable_goods_dataset(DURABLE_GOODS_SERIES, start_date=start_date)
                    if df is not None:
                        save_durable_goods_data(df, 'durable_goods_data_complete.csv')
            else:
                print(f"🔄 업데이트 필요: {consistency_check['reason']}")
                df = fetch_durable_goods_dataset(DURABLE_GOODS_SERIES, start_date=start_date)
                if df is not None:
                    save_durable_goods_data(df, 'durable_goods_data_complete.csv')
        else:
            print("📊 FRED API에서 최신 데이터 수집 중... (스마트 업데이트 비활성화)")
            df = fetch_durable_goods_dataset(DURABLE_GOODS_SERIES, start_date=start_date)
            if df is not None:
                save_durable_goods_data(df, 'durable_goods_data_complete.csv')
    else:
        print("📁 기존 저장된 데이터 로드 중...")
        df = load_durable_goods_data('durable_goods_data_complete.csv')
        if df is None:
            print("⚠️ 기존 데이터가 없어서 새로 수집합니다")
            df = fetch_durable_goods_dataset(DURABLE_GOODS_SERIES, start_date=start_date)
            if df is not None:
                save_durable_goods_data(df, 'durable_goods_data_complete.csv')
    
    if df is None:
        print("❌ 데이터 수집 실패")
        return None
    
    # 2) 기본 통계 생성
    print("\n📈 기본 통계 생성 중...")
    stats = generate_durable_goods_summary_stats(df)
    
    if stats:
        print(f"\n✅ 데이터 요약:")
        metadata = stats['metadata']
        print(f"   - 분석 기간: {metadata['start_date']} ~ {metadata['latest_date']}")
        print(f"   - 총 데이터 포인트: {metadata['data_points']}개월")
        
        # 주요 지표 최신값 출력
        key_indicators = ['shipments_total', 'orders_total', 'orders_core_capital', 'unfilled_total']
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
        print("   - 내구재 개요 (트렌드 분석)")
        figures['overview'] = plot_durable_goods_overview(df)
    
    # 2. 업종별 분석
    if 'sectors' in charts:
        print("   - 업종별 분석")
        figures['sectors'] = plot_durable_goods_sectors(df)
    
    # 3. 주문 vs 출하량 분석
    if 'orders_vs_shipments' in charts:
        print("   - 주문 vs 출하량 비교")
        figures['orders_vs_shipments'] = plot_durable_goods_orders_vs_shipments(df)
    
    # 4. 출하량 가로 바 차트
    if 'horizontal_bar_shipments' in charts:
        print("   - 출하량 가로 바 차트")
        figures['horizontal_bar_shipments'] = create_horizontal_durable_goods_bar_chart(df, metric_type='shipments')
    
    # 5. 주문 가로 바 차트
    if 'horizontal_bar_orders' in charts:
        print("   - 주문 가로 바 차트")
        figures['horizontal_bar_orders'] = create_horizontal_durable_goods_bar_chart(df, metric_type='orders')
    
    # 6. 미충족 주문 가로 바 차트
    if 'horizontal_bar_unfilled' in charts:
        print("   - 미충족 주문 가로 바 차트")
        figures['horizontal_bar_unfilled'] = create_horizontal_durable_goods_bar_chart(df, metric_type='unfilled')
    
    # 7. 재고 가로 바 차트
    if 'horizontal_bar_inventory' in charts:
        print("   - 재고 가로 바 차트")
        figures['horizontal_bar_inventory'] = create_horizontal_durable_goods_bar_chart(df, metric_type='inventory')
    
    # 8. 커스텀 대시보드
    if 'dashboard' in charts:
        print("   - 커스텀 대시보드")
        figures['dashboard'] = create_durable_goods_dashboard(df)
    
    # 9. 카테고리 분석
    if 'category_analysis' in charts:
        print("   - 카테고리별 심화 분석")
        figures['capital_goods'] = create_durable_category_analysis(df, 'capital_goods')
        figures['transport_analysis'] = create_durable_category_analysis(df, 'transport_analysis')
        figures['sector_analysis'] = create_durable_category_analysis(df, 'sector_analysis')
    
    print(f"\n✅ 내구재 분석 완료!")
    print(f"   - 생성된 차트: {len(figures)}개")
    print(f"   - 사용 가능한 지표: {len(df.columns)}개")
    
    return {
        'data': df,
        'stats': stats,
        'figures': figures
    }

# 빠른 테스트용 함수
def quick_durable_goods_test():
    """빠른 내구재 분석 테스트 (최근 5년 데이터만)"""
    print("🧪 내구재 분석 빠른 테스트")
    return run_durable_goods_analysis(
        update_data=True,
        start_date='2019-01-01',
        charts=['overview', 'orders_vs_shipments', 'horizontal_bar_shipments', 'horizontal_bar_orders']
    )

def get_available_durable_series():
    """사용 가능한 내구재 시리즈 목록 출력"""
    print("=== 사용 가능한 US Durable Goods 시리즈 ===")
    print("\n📊 출하량 (Shipments):")
    shipment_series = {k: v for k, v in DURABLE_GOODS_SERIES.items() if k.startswith('shipments_')}
    for key, desc in list(shipment_series.items())[:10]:  # 처음 10개만 표시
        korean_label = get_durable_goods_korean_labels().get(key, key)
        print(f"  '{key}': {korean_label}")
    
    print(f"\n📋 신규 주문 (Orders): {len([k for k in DURABLE_GOODS_SERIES.keys() if k.startswith('orders_')])}개 시리즈")
    print(f"🔄 미충족 주문 (Unfilled): {len([k for k in DURABLE_GOODS_SERIES.keys() if k.startswith('unfilled_')])}개 시리즈")  
    print(f"📦 재고 (Inventory): {len([k for k in DURABLE_GOODS_SERIES.keys() if k.startswith('inventory_')])}개 시리즈")
    
    print(f"\n총 {len(DURABLE_GOODS_SERIES)}개 시리즈 사용 가능")

print("✅ US Durable Goods 분석 도구 완성!")
print("\n🎯 사용법:")
print("   - 전체 분석: result = run_durable_goods_analysis()")
print("   - 빠른 테스트: result = quick_durable_goods_test()")
print("   - 차트 표시: result['figures']['overview'].show()")
print("   - 출하량 바 차트: result['figures']['horizontal_bar_shipments'].show()")
print("   - 주문 바 차트: result['figures']['horizontal_bar_orders'].show()")
print("   - 시리즈 목록: get_available_durable_series()")
print("\n📊 새로운 기능:")
print("   - create_custom_horizontal_durable_goods_bar_chart(): 선택한 시리즈만 표시")
print("   - 예시: create_custom_horizontal_durable_goods_bar_chart(data, ['shipments_total', 'orders_total'])")
print("   - 메트릭별 바 차트: metric_type='shipments|orders|unfilled|inventory'")
print("="*60)

# %%
# 실행 예시 (주석 해제하여 사용)
result = run_durable_goods_analysis()
result['figures']['overview'].show()
result['figures']['horizontal_bar_shipments'].show()

print("✅ 기본 시각화 함수들 정의 완료")
# %%
result['figures']['horizontal_bar_orders'].show()

# %%
