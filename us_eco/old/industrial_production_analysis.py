# %%
"""
FRED API 전용 US Industrial Production & Capacity Utilization 분석 및 시각화 도구
- FRED API를 사용하여 US 산업생산(IP) 및 가동률(CU) 데이터 수집
- 총지수, 제조업, 내구재/비내구재, 업종별 분류 및 분석
- MoM/YoY 기준 시각화 지원
- 스마트 업데이트 기능 (실행 시마다 최신 데이터 확인 및 업데이트)
- CSV 파일 자동 저장 및 업데이트
- 투자은행/이코노미스트 관점의 심층 분석
"""
import datetime
import sys
import warnings
import os
warnings.filterwarnings('ignore')

# 라이브러리 체크
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
    print("✓ pandas 사용 가능")
except ImportError:
    print("⚠️ pandas 라이브러리가 없습니다")
    PANDAS_AVAILABLE = False

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
    print("✓ plotly 사용 가능")
except ImportError:
    print("⚠️ plotly 라이브러리가 없습니다")
    PLOTLY_AVAILABLE = False

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

# KPDS 시각화 라이브러리 불러오기 (선택적)
try:
    sys.path.append('/home/jyp0615')
    from kpds_fig_format_enhanced import *
    KPDS_AVAILABLE = True
    print("✓ KPDS 시각화 포맷 로드됨")
except ImportError:
    print("⚠️ KPDS 시각화 라이브러리가 없습니다")
    KPDS_AVAILABLE = False
    # 기본 색상 정의
    KPDS_COLORS = [
        "rgb(242,27,45)",   # deepred_pds
        "rgb(1,30,81)",     # deepblue_pds  
        "rgb(188,157,126)", # beige_pds
        "rgb(39,101,255)",  # blue_pds
        "rgb(114,113,113)"  # grey_pds
    ]

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

# 색상 매핑 (KPDS 색상 팔레트 사용)
INDUSTRIAL_COLORS = {
    'ip_total': KPDS_COLORS[0],              # deepred_pds
    'ip_manufacturing': KPDS_COLORS[1],      # deepblue_pds  
    'ip_durable_mfg': KPDS_COLORS[2],        # beige_pds
    'ip_nondurable_mfg': KPDS_COLORS[3],     # blue_pds
    'cu_total': KPDS_COLORS[4],              # grey_pds
    'cu_manufacturing': KPDS_COLORS[0],      # deepred_pds (순환)
    'ip_computer_electronic': KPDS_COLORS[1], # deepblue_pds
    'ip_machinery': KPDS_COLORS[2],          # beige_pds
    'ip_motor_vehicles': KPDS_COLORS[3],     # blue_pds
    'ip_primary_metals': KPDS_COLORS[4]      # grey_pds
}

print("✓ Industrial Production & Capacity Utilization 데이터 구조 정의 완료")

# %%
# === 스마트 업데이트 기능 추가 ===

def check_recent_data_consistency(file_path='industrial_production_data_complete.csv', check_count=3):
    """
    최근 N개 데이터의 일치성을 확인하는 함수 (스마트 업데이트)
    
    Args:
        file_path: 기존 CSV 파일 경로
        check_count: 확인할 최근 데이터 개수
    
    Returns:
        dict: 일치성 확인 결과 (need_update, reason, mismatches)
    """
    # 기존 데이터 로드 시도
    existing_df = load_industrial_data(file_path)
    if existing_df is None:
        return {'need_update': True, 'reason': '기존 데이터 없음'}
    
    print(f"📊 최근 {check_count}개 데이터 일치성 확인 중...")
    
    # 주요 시리즈만 체크 (속도 향상)
    key_series = ['ip_total', 'ip_manufacturing', 'cu_total', 'cu_manufacturing']
    available_series = [s for s in key_series if s in existing_df.columns]
    
    if not available_series:
        return {'need_update': True, 'reason': '주요 시리즈 없음'}
    
    mismatches = []
    new_data_available = False
    all_data_identical = True
    
    for col_name in available_series:
        if col_name not in ALL_INDUSTRIAL_SERIES:
            continue
            
        series_id = ALL_INDUSTRIAL_SERIES[col_name]
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
                
                # 값이 다르면 불일치 (0.01 이상 차이)
                if abs(existing_val - api_val) > 0.01:
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
        
        # 큰 차이만 실제 불일치로 간주 (0.1 이상)
        significant_mismatches = [m for m in value_mismatches if m.get('diff', 0) > 0.1]
        if len(significant_mismatches) == 0:
            print("📝 모든 차이가 0.1 미만입니다. 저장 정밀도 차이로 간주하여 업데이트를 건너뜁니다.")
            return {'need_update': False, 'reason': '미세한 정밀도 차이', 'mismatches': mismatches}
        else:
            print(f"🚨 유의미한 차이 발견: {len(significant_mismatches)}개 (0.1 이상)")
            return {'need_update': True, 'reason': '데이터 불일치', 'mismatches': mismatches}
    else:
        print("✅ 최근 데이터가 완전히 일치합니다. 업데이트를 건너뜁니다.")
        return {'need_update': False, 'reason': '데이터 일치', 'mismatches': []}

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

def fetch_industrial_dataset(series_dict, start_date='2000-01-01', end_date=None):
    """
    여러 Industrial Production/Capacity Utilization 시리즈를 한번에 가져와서 데이터프레임으로 반환
    
    Parameters:
    - series_dict: 시리즈 딕셔너리 {name: series_id}
    - start_date: 시작 날짜
    - end_date: 종료 날짜
    
    Returns:
    - pandas.DataFrame: 각 시리즈를 컬럼으로 하는 데이터프레임
    """
    print(f"📊 Industrial Production/Capacity Utilization 데이터 수집 시작 ({len(series_dict)}개 시리즈)")
    
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

def save_industrial_data(df, filename='industrial_production_data.csv'):
    """Industrial Production 데이터를 CSV 파일로 저장"""
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

def load_industrial_data(filename='industrial_production_data.csv'):
    """저장된 Industrial Production 데이터를 로드"""
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
# === Industrial Production 핵심 지표 계산 함수들 ===

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

def generate_industrial_summary_stats(df, recent_months=12):
    """
    Industrial Production 데이터 요약 통계 생성
    
    Parameters:
    - df: Industrial Production 데이터프레임
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
        if col in ALL_INDUSTRIAL_SERIES.keys():
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

print("✅ Industrial Production 핵심 지표 계산 함수 정의 완료")

# %%
# === 다양한 자유도 높은 시각화 함수들 ===

def plot_industrial_overview(df):
    """
    Industrial Production 개요 차트 (개선된 버전)
    """
    print("미국 산업생산 주요 지표 트렌드 (최근 3년, MoM %)")
    if df is None or len(df) == 0:
        print("❌ 시각화할 데이터가 없습니다")
        return None
    
    # 최신 36개월 데이터 (3년 트렌드)
    chart_data = df.tail(36).copy()
    
    # 사용 가능한 컬럼 확인
    available_cols = []
    col_labels = {}
    
    if 'ip_total' in df.columns:
        available_cols.append('ip_total')
        col_labels['ip_total'] = '총 산업생산'
    if 'ip_manufacturing' in df.columns:
        available_cols.append('ip_manufacturing')
        col_labels['ip_manufacturing'] = '제조업'
    if 'ip_durable_mfg' in df.columns:
        available_cols.append('ip_durable_mfg')
        col_labels['ip_durable_mfg'] = '내구재 제조'
    if 'ip_nondurable_mfg' in df.columns:
        available_cols.append('ip_nondurable_mfg')
        col_labels['ip_nondurable_mfg'] = '비내구재 제조'
        
    if not available_cols:
        print("Warning: 표시할 산업생산 지표가 없습니다.")
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

def plot_capacity_utilization_overview(df):
    """
    Capacity Utilization 개요 차트
    """
    print("미국 산업 가동률 주요 지표 트렌드 (최근 3년, Level)")
    if df is None or len(df) == 0:
        print("❌ 시각화할 데이터가 없습니다")
        return None
    
    # 최신 36개월 데이터 (3년 트렌드)
    chart_data = df.tail(36).copy()
    
    # 사용 가능한 컬럼 확인
    available_cols = []
    col_labels = {}
    
    if 'cu_total' in df.columns:
        available_cols.append('cu_total')
        col_labels['cu_total'] = '총 산업 가동률'
    if 'cu_manufacturing' in df.columns:
        available_cols.append('cu_manufacturing')
        col_labels['cu_manufacturing'] = '제조업 가동률'
    if 'cu_durable_mfg' in df.columns:
        available_cols.append('cu_durable_mfg')
        col_labels['cu_durable_mfg'] = '내구재 가동률'
    if 'cu_nondurable_mfg' in df.columns:
        available_cols.append('cu_nondurable_mfg')
        col_labels['cu_nondurable_mfg'] = '비내구재 가동률'
        
    if not available_cols:
        print("Warning: 표시할 가동률 지표가 없습니다.")
        return None
    
    # 가동률은 Level로 표시 (MoM 아닌)
    labels_dict = {col: col_labels[col] for col in available_cols}
    
    fig = df_multi_line_chart(
        df=chart_data[available_cols],
        columns=available_cols,
        ytitle="가동률 (%)",
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_ip_vs_cu_dual_axis(df):
    """
    Industrial Production vs Capacity Utilization 이중축 차트
    """
    print("미국 산업생산 vs 가동률 분석 (최근 2년)")
    if df is None:
        print("❌ 데이터가 없습니다")
        return None
        
    # 최신 24개월 데이터
    chart_data = df.tail(24).copy()
    
    # 컬럼 설정
    ip_cols = []
    cu_cols = []
    ip_labels = {}
    cu_labels = {}
    
    if 'ip_total' in df.columns:
        ip_cols.append('ip_total')
        ip_labels['ip_total'] = '총 산업생산 (MoM)'
        
    if 'cu_total' in df.columns:
        cu_cols.append('cu_total')
        cu_labels['cu_total'] = '총 산업 가동률 (Level)'
    
    if not ip_cols or not cu_cols:
        print("❌ IP 또는 CU 데이터가 없습니다")
        return None
    
    # IP는 MoM 변화율로, CU는 레벨로
    ip_mom_data = {}
    for col in ip_cols:
        ip_mom_data[col] = calculate_mom_change(chart_data[col])
    
    ip_mom_df = pd.DataFrame(ip_mom_data)
    cu_df = chart_data[cu_cols]
    
    # 이중축 차트 생성
    fig = df_dual_axis_chart(
        df=pd.concat([ip_mom_df, cu_df], axis=1),
        left_cols=ip_cols,
        right_cols=cu_cols,
        left_labels=[ip_labels[col] for col in ip_cols],
        right_labels=[cu_labels[col] for col in cu_cols],
        left_title="MoM 변화율 (%)",
        right_title="가동률 (%)",
        width_cm=14,
        height_cm=8
    )
    
    return fig

def plot_industrial_sectors(df):
    """
    Industrial Production 업종별 차트
    """
    print("미국 산업생산 업종별 분석 (최근 2년, MoM %)")
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
        
    # 최신 24개월 데이터
    chart_data = df.tail(24).copy()
    
    # 사용 가능한 컬럼 확인
    available_cols = []
    col_labels = {}
    
    if 'ip_manufacturing' in df.columns:
        available_cols.append('ip_manufacturing')
        col_labels['ip_manufacturing'] = '제조업'
    if 'ip_mining' in df.columns:
        available_cols.append('ip_mining')
        col_labels['ip_mining'] = '광업'
    if 'ip_utilities' in df.columns:
        available_cols.append('ip_utilities')
        col_labels['ip_utilities'] = '유틸리티'
    if 'ip_computer_electronic' in df.columns:
        available_cols.append('ip_computer_electronic')
        col_labels['ip_computer_electronic'] = '컴퓨터/전자'
        
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

def create_horizontal_industrial_bar_chart(df, num_categories=20, metric_type='ip'):
    """
    산업생산/가동률 구성요소별 MoM 변화율 가로 바 차트 생성
    
    Args:
        df: Industrial Production 데이터프레임
        num_categories: 표시할 카테고리 수
        metric_type: 'ip' (산업생산) 또는 'cu' (가동률)
    
    Returns:
        plotly figure
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
    
    # 선택한 메트릭 타입에 따라 시리즈 필터링
    if metric_type == 'ip':
        metric_cols = [col for col in df.columns if col.startswith('ip_')]
        metric_name = '산업생산'
        unit = '%'
    else:  # 'cu'
        metric_cols = [col for col in df.columns if col.startswith('cu_')]
        metric_name = '가동률'
        unit = 'pt'
    
    if not metric_cols:
        print(f"❌ {metric_name} 관련 데이터가 없습니다")
        return None
    
    # MoM 변화율 계산 (가동률의 경우 포인트 변화)
    change_data = {}
    for col in metric_cols:
        if metric_type == 'ip':
            change_series = calculate_mom_change(df[col])
        else:  # cu는 차분 (포인트 변화)
            change_series = df[col] - df[col].shift(1)
        
        if change_series is not None:
            change_data[col] = change_series.iloc[-1]
    
    if not change_data:
        print("❌ 변화율 계산 실패")
        return None
    
    # 데이터 정렬 (내림차순)
    sorted_data = pd.Series(change_data).dropna().sort_values(ascending=True)  # 가로 바차트이므로 ascending=True
    
    # 상위 num_categories개 선택
    if len(sorted_data) > num_categories:
        sorted_data = sorted_data.tail(num_categories)
    
    # 산업분야 한국어 라벨
    category_labels = get_industrial_korean_labels()
    
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
        text=[f'{v:.1f}{unit}' for v in values],
        textposition='outside' if all(v >= 0 for v in values) else 'auto',
        showlegend=False
    ))
    
    # 제목 출력
    if not df.empty:
        latest_date = df.index[-1].strftime('%Y년 %m월')
        change_type = "MoM" if metric_type == 'ip' else "전월대비 pt"
        print(f"{metric_name} 구성요소별 {change_type} 변화 ({latest_date})")
    else:
        print(f"{metric_name} 구성요소별 월간 변화")
    
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

# 헬퍼 함수들
def get_industrial_korean_labels():
    """산업생산/가동률 전체 한국어 라벨 매핑"""
    return {
        # Industrial Production
        'ip_total': '총 산업생산',
        'ip_manufacturing': '제조업',
        'ip_durable_mfg': '내구재 제조',
        'ip_nondurable_mfg': '비내구재 제조',
        'ip_mining': '광업',
        'ip_utilities': '유틸리티',
        'ip_electric_power': '전력생산',
        'ip_natural_gas': '천연가스',
        'ip_consumer_goods': '소비재',
        'ip_consumer_durable': '내구소비재',
        'ip_consumer_nondurable': '비내구소비재',
        'ip_business_equipment': '기업설비',
        'ip_construction_supplies': '건설자재',
        'ip_business_supplies': '기업용 자재',
        'ip_materials': '원자재',
        'ip_durable_materials': '내구 원자재',
        'ip_nondurable_materials': '비내구 원자재',
        'ip_energy_materials': '에너지 원자재',
        
        # Durable Goods IP
        'ip_wood_products': '목재제품',
        'ip_nonmetallic_minerals': '비금속 광물',
        'ip_primary_metals': '1차 금속',
        'ip_fabricated_metals': '금속가공',
        'ip_machinery': '기계',
        'ip_computer_electronic': '컴퓨터/전자',
        'ip_computer': '컴퓨터',
        'ip_communications_equipment': '통신장비',
        'ip_semiconductors': '반도체/부품',
        'ip_electrical_equipment': '전기장비/가전',
        'ip_motor_vehicles': '자동차/부품',
        'ip_aerospace': '항공우주',
        'ip_furniture': '가구',
        'ip_misc_durables': '기타 내구재',
        
        # Nondurable Goods IP
        'ip_food_tobacco': '식료품/담배',
        'ip_textile_mills': '섬유/직물',
        'ip_apparel_leather': '의류/가죽',
        'ip_paper': '제지',
        'ip_printing': '인쇄/지원',
        'ip_petroleum_coal': '석유/석탄',
        'ip_chemicals': '화학제품',
        'ip_plastics_rubber': '플라스틱/고무',
        
        # Capacity Utilization
        'cu_total': '총 산업 가동률',
        'cu_manufacturing': '제조업 가동률',
        'cu_durable_mfg': '내구재 가동률',
        'cu_nondurable_mfg': '비내구재 가동률',
        'cu_mining': '광업 가동률',
        'cu_utilities': '유틸리티 가동률',
        'cu_electric_power': '전력생산 가동률',
        'cu_natural_gas': '천연가스 가동률',
        
        # Durable Goods CU
        'cu_wood_products': '목재제품 가동률',
        'cu_nonmetallic_minerals': '비금속 광물 가동률',
        'cu_primary_metals': '1차 금속 가동률',
        'cu_fabricated_metals': '금속가공 가동률',
        'cu_machinery': '기계 가동률',
        'cu_computer_electronic': '컴퓨터/전자 가동률',
        'cu_computer': '컴퓨터 가동률',
        'cu_communications_equipment': '통신장비 가동률',
        'cu_semiconductors': '반도체 가동률',
        'cu_electrical_equipment': '전기장비 가동률',
        'cu_motor_vehicles': '자동차 가동률',
        'cu_aerospace': '항공우주 가동률',
        'cu_furniture': '가구 가동률',
        'cu_misc_durables': '기타 내구재 가동률',
        
        # Nondurable Goods CU
        'cu_food_tobacco': '식료품/담배 가동률',
        'cu_textile_mills': '섬유/직물 가동률',
        'cu_apparel_leather': '의류/가죽 가동률',
        'cu_paper': '제지 가동률',
        'cu_printing': '인쇄 가동률',
        'cu_petroleum_coal': '석유/석탄 가동률',
        'cu_chemicals': '화학제품 가동률',
        'cu_plastics_rubber': '플라스틱/고무 가동률'
    }

# %%
# === 범용 시계열 및 바 차트 생성 함수들 ===

def create_custom_timeseries_chart(df, series_names=None, chart_type='mom', ytitle=None, period_months=24):
    """
    사용자가 원하는 시리즈를 시계열로 그리는 함수
    
    Args:
        df: Industrial Production 데이터프레임
        series_names: 차트에 표시할 시리즈 리스트 (예: ['ip_total', 'ip_manufacturing'])
        chart_type: 'mom' (전월대비), 'yoy' (전년대비), 'level' (수준)
        ytitle: Y축 제목
        period_months: 표시할 기간 (최근 N개월)
    
    Returns:
        plotly figure
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
    
    if series_names is None:
        series_names = ['ip_total', 'ip_manufacturing']
    
    # 사용 가능한 시리즈만 선택
    available_series = [s for s in series_names if s in df.columns]
    if not available_series:
        print(f"❌ 요청한 시리즈가 없습니다: {series_names}")
        return None
    
    # 최근 N개월 데이터 선택
    chart_data = df.tail(period_months).copy()
    
    # 데이터 변환
    if chart_type == 'mom':
        plot_data = {}
        for col in available_series:
            plot_data[col] = calculate_mom_change(chart_data[col])
        plot_df = pd.DataFrame(plot_data)
        default_ytitle = "MoM 변화율 (%)"
        default_title = "산업생산 - 전월대비 변화율"
    elif chart_type == 'yoy':
        plot_data = {}
        for col in available_series:
            plot_data[col] = calculate_yoy_change(chart_data[col])
        plot_df = pd.DataFrame(plot_data)
        default_ytitle = "YoY 변화율 (%)"
        default_title = "산업생산 - 전년대비 변화율"
    else:  # level
        plot_df = chart_data[available_series]
        default_ytitle = "지수"
        default_title = "산업생산 - 수준"
    
    # 라벨 매핑
    korean_labels = get_industrial_korean_labels()
    labels_dict = {col: korean_labels.get(col, col) for col in available_series}
    
    # 제목 출력
    print_title = default_title + f" (최근 {period_months//12}년)" if period_months >= 12 else default_title
    print(print_title)
    if ytitle is None:
        ytitle = default_ytitle
    
    # 차트 생성
    fig = df_multi_line_chart(
        df=plot_df,
        columns=available_series,
        ytitle=ytitle,
        labels=labels_dict,
        width_cm=14,
        height_cm=8
    )
    
    # 0선 추가 (변화율 차트인 경우)
    if chart_type in ['mom', 'yoy']:
        fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)
    
    return fig

def create_custom_bar_chart(df, series_names=None, chart_type='mom', num_categories=15):
    """
    사용자가 원하는 시리즈를 바 그래프로 그리는 함수 (최신 월 기준)
    
    Args:
        df: Industrial Production 데이터프레임
        series_names: 차트에 표시할 시리즈 리스트
        chart_type: 'mom' (전월대비), 'yoy' (전년대비)
        num_categories: 표시할 카테고리 수
    
    Returns:
        plotly figure
    """
    if df is None or len(df) == 0:
        print("❌ 데이터가 없습니다")
        return None
    
    if series_names is None:
        # 기본값: 모든 IP 시리즈
        series_names = [col for col in df.columns if col.startswith('ip_')]
    
    # 사용 가능한 시리즈만 선택
    available_series = [s for s in series_names if s in df.columns]
    if not available_series:
        print(f"❌ 요청한 시리즈가 없습니다: {series_names}")
        return None
    
    # 변화율 계산
    change_data = {}
    for col in available_series:
        if chart_type == 'mom':
            change_series = calculate_mom_change(df[col])
            unit = '%'
        else:  # yoy
            change_series = calculate_yoy_change(df[col])
            unit = '%'
        
        if change_series is not None and len(change_series.dropna()) > 0:
            change_data[col] = change_series.iloc[-1]
    
    if not change_data:
        print("❌ 변화율 계산 실패")
        return None
    
    # 데이터 정렬 (내림차순)
    sorted_data = pd.Series(change_data).dropna().sort_values(ascending=True)  # 가로 바차트이므로 ascending=True
    
    # 상위 num_categories개 선택
    if len(sorted_data) > num_categories:
        sorted_data = sorted_data.tail(num_categories)
    
    # 한국어 라벨
    korean_labels = get_industrial_korean_labels()
    
    categories = []
    values = []
    colors = []
    
    for series_name, value in sorted_data.items():
        label = korean_labels.get(series_name, series_name)
        categories.append(label)
        values.append(value)
        
        # 색상 설정
        if 'total' in series_name:
            colors.append('#FFA500')  # 전체는 주황색
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
        text=[f'{v:.1f}{unit}' for v in values],
        textposition='outside' if all(v >= 0 for v in values) else 'auto',
        showlegend=False
    ))
    
    # 제목 출력
    if not df.empty:
        latest_date = df.index[-1].strftime('%Y년 %m월')
        change_type = "MoM" if chart_type == 'mom' else "YoY"
        print(f"산업생산 {change_type} 변화 ({latest_date})")
    else:
        print(f"산업생산 {'전월' if chart_type == 'mom' else '전년'}대비 변화")
    
    # 레이아웃 설정
    fig.update_layout(
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=900,
        height=max(500, len(categories) * 25),
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL-1, color="black"),
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            tickformat='.1f',
            range=[min(-1, min(values) * 1.1), max(values) * 1.2]
        ),
        yaxis=dict(
            showline=False,
            tickcolor='white'
        ),
        margin=dict(l=250, r=80, t=80, b=60)
    )
    
    # 격자선 및 0선
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="lightgrey")
    fig.update_yaxes(showgrid=False)
    fig.add_vline(x=0, line_width=2, line_color="black")
    
    return fig

print("✅ 범용 시계열 및 바 차트 생성 함수들 정의 완료")

print("✅ 산업생산 시각화 함수들 정의 완료")

# %%
# === 메인 실행 함수 ===

def run_industrial_analysis(update_data=True, start_date='2000-01-01', 
                           charts=['ip_overview', 'cu_overview', 'ip_vs_cu', 'sectors', 'horizontal_bar_ip', 'horizontal_bar_cu'], 
                           smart_update=True):
    """
    완전한 산업생산/가동률 분석 실행 함수 (스마트 업데이트 지원)
    
    Parameters:
    - update_data: 데이터 업데이트 여부
    - start_date: 데이터 시작 날짜
    - charts: 생성할 차트 목록
    - smart_update: 스마트 업데이트 사용 여부 (최근 3개 데이터 비교)
    
    Returns:
    - dict: 분석 결과 및 차트들
    """
    print("🚀 US Industrial Production & Capacity Utilization 분석 시작")
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
                df = load_industrial_data('industrial_production_data_complete.csv')
                if df is None:
                    print("⚠️ CSV 파일 로드 실패, API에서 새로 수집합니다")
                    df = fetch_industrial_dataset(ALL_INDUSTRIAL_SERIES, start_date=start_date)
                    if df is not None:
                        save_industrial_data(df, 'industrial_production_data_complete.csv')
            else:
                print(f"🔄 업데이트 필요: {consistency_check['reason']}")
                df = fetch_industrial_dataset(ALL_INDUSTRIAL_SERIES, start_date=start_date)
                if df is not None:
                    save_industrial_data(df, 'industrial_production_data_complete.csv')
        else:
            print("📊 FRED API에서 최신 데이터 수집 중... (스마트 업데이트 비활성화)")
            df = fetch_industrial_dataset(ALL_INDUSTRIAL_SERIES, start_date=start_date)
            if df is not None:
                save_industrial_data(df, 'industrial_production_data_complete.csv')
    else:
        print("📁 기존 저장된 데이터 로드 중...")
        df = load_industrial_data('industrial_production_data_complete.csv')
        if df is None:
            print("⚠️ 기존 데이터가 없어서 새로 수집합니다")
            df = fetch_industrial_dataset(ALL_INDUSTRIAL_SERIES, start_date=start_date)
            if df is not None:
                save_industrial_data(df, 'industrial_production_data_complete.csv')
    
    if df is None:
        print("❌ 데이터 수집 실패")
        return None
    
    # 2) 기본 통계 생성
    print("\n📈 기본 통계 생성 중...")
    stats = generate_industrial_summary_stats(df)
    
    if stats:
        print(f"\n✅ 데이터 요약:")
        metadata = stats['metadata']
        print(f"   - 분석 기간: {metadata['start_date']} ~ {metadata['latest_date']}")
        print(f"   - 총 데이터 포인트: {metadata['data_points']}개월")
        
        # 주요 지표 최신값 출력
        key_indicators = ['ip_total', 'ip_manufacturing', 'cu_total', 'cu_manufacturing']
        for indicator in key_indicators:
            if indicator in stats:
                stat = stats[indicator]
                latest = stat['latest_value']
                mom = stat['mom_change']
                yoy = stat['yoy_change']
                
                if latest and mom and yoy:
                    unit = "index" if indicator.startswith('ip_') else "%"
                    print(f"   - {indicator}: {latest:.1f}{unit} (MoM: {mom:+.1f}%, YoY: {yoy:+.1f}%)")
    
    # 3) 차트 생성
    print("\n🎨 차트 생성 중...")
    figures = {}
    
    # 1. IP 개요 차트
    if 'ip_overview' in charts:
        print("   - 산업생산 개요 (트렌드 분석)")
        figures['ip_overview'] = plot_industrial_overview(df)
    
    # 2. CU 개요 차트
    if 'cu_overview' in charts:
        print("   - 산업 가동률 개요")
        figures['cu_overview'] = plot_capacity_utilization_overview(df)
    
    # 3. IP vs CU 이중축 차트
    if 'ip_vs_cu' in charts:
        print("   - 산업생산 vs 가동률 이중축")
        figures['ip_vs_cu'] = plot_ip_vs_cu_dual_axis(df)
    
    # 4. 업종별 분석
    if 'sectors' in charts:
        print("   - 업종별 분석")
        figures['sectors'] = plot_industrial_sectors(df)
    
    # 5. IP 가로 바 차트
    if 'horizontal_bar_ip' in charts:
        print("   - 산업생산 가로 바 차트")
        figures['horizontal_bar_ip'] = create_horizontal_industrial_bar_chart(df, metric_type='ip')
    
    # 6. CU 가로 바 차트
    if 'horizontal_bar_cu' in charts:
        print("   - 가동률 가로 바 차트")
        figures['horizontal_bar_cu'] = create_horizontal_industrial_bar_chart(df, metric_type='cu')
    
    print(f"\n✅ 산업생산/가동률 분석 완료!")
    print(f"   - 생성된 차트: {len(figures)}개")
    print(f"   - 사용 가능한 지표: {len(df.columns)}개")
    
    return {
        'data': df,
        'stats': stats,
        'figures': figures
    }

# 빠른 테스트용 함수
def quick_industrial_test():
    """빠른 산업생산 분석 테스트 (최근 5년 데이터만)"""
    print("🧪 산업생산 분석 빠른 테스트")
    return run_industrial_analysis(
        update_data=True,
        start_date='2019-01-01',
        charts=['ip_overview', 'cu_overview', 'ip_vs_cu', 'horizontal_bar_ip']
    )

def get_available_industrial_series():
    """사용 가능한 산업생산/가동률 시리즈 목록 출력"""
    print("=== 사용 가능한 US Industrial Production & Capacity Utilization 시리즈 ===")
    
    print("\n📈 산업생산(IP) - 주요 지수:")
    ip_main_series = {k: v for k, v in INDUSTRIAL_PRODUCTION_SERIES.items()}
    for i, (key, desc) in enumerate(list(ip_main_series.items())[:8]):  # 처음 8개만 표시
        korean_label = get_industrial_korean_labels().get(key, key)
        print(f"  '{key}': {korean_label}")
    
    print(f"\n📊 가동률(CU) - 주요 지수:")
    cu_main_series = {k: v for k, v in CAPACITY_UTILIZATION_SERIES.items()}
    for i, (key, desc) in enumerate(list(cu_main_series.items())[:8]):  # 처음 8개만 표시
        korean_label = get_industrial_korean_labels().get(key, key)
        print(f"  '{key}': {korean_label}")
    
    print(f"\n🏭 내구재 제조업: IP {len(DURABLE_GOODS_IP_SERIES)}개, CU {len(DURABLE_GOODS_CU_SERIES)}개 시리즈")
    print(f"🛒 비내구재 제조업: IP {len(NONDURABLE_GOODS_IP_SERIES)}개, CU {len(NONDURABLE_GOODS_CU_SERIES)}개 시리즈")
    
    print(f"\n총 {len(ALL_INDUSTRIAL_SERIES)}개 시리즈 사용 가능")

print("✅ US Industrial Production & Capacity Utilization 분석 도구 완성!")
print("\n🎯 사용법:")
print("   - 전체 분석: result = run_industrial_analysis()")
print("   - 스마트 업데이트: result = run_industrial_analysis(smart_update=True)")
print("   - 빠른 테스트: result = quick_industrial_test()")
print("   - 차트 표시: result['figures']['ip_overview'].show()")
print("   - IP 바 차트: result['figures']['horizontal_bar_ip'].show()")
print("   - CU 바 차트: result['figures']['horizontal_bar_cu'].show()")
print("   - 시리즈 목록: get_available_industrial_series()")
print("\n🆕 새로 추가된 기능:")
print("   - 사용자 정의 시계열 차트: create_custom_timeseries_chart(df, ['ip_total', 'ip_manufacturing'])")
print("   - 사용자 정의 바 차트: create_custom_bar_chart(df, ['ip_total', 'ip_durable_mfg'])")
print("   - 스마트 업데이트: check_recent_data_consistency()")
print("=" * 60)

# %%
result = run_industrial_analysis()
result

# %%
result['figures']['horizontal_bar_ip'].show()
# %%
result['figures']['horizontal_bar_cu'].show()

# %%
