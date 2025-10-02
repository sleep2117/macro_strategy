# %%
"""
US Economic Data Utils - 통합 유틸리티 함수들
- BLS API와 FRED API 통합 지원
- 데이터 로드, 저장, 스마트 업데이트
- 시각화 및 분석 공통 함수들
- KPDS 포맷 시각화 지원
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime as dt_datetime, timedelta
import requests
import json
import os
import sys
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# 프로젝트 경로 설정
REPO_ROOT = Path(__file__).resolve().parents[1]
US_ECO_ROOT = REPO_ROOT / "us_eco"
DATA_DIR = US_ECO_ROOT / "data"
EXPORT_ROOT = REPO_ROOT

# KPDS 시각화 라이브러리 불러오기
sys.path.append(str(REPO_ROOT))
from kpds_fig_format_enhanced import *


def ensure_directory(path: Path) -> Path:
    """Ensure directory exists and return the path."""
    path.mkdir(parents=True, exist_ok=True)
    return path


ensure_directory(DATA_DIR)


def data_path(*parts: str) -> Path:
    """Return a path under the shared us_eco/data directory."""
    return DATA_DIR.joinpath(*parts)


def repo_path(*parts: str) -> Path:
    """Return a path under the repository root."""
    return EXPORT_ROOT.joinpath(*parts)

# %%
# === API 설정 클래스 ===

class APIConfig:
    """API 설정 관리 클래스"""
    
    def __init__(self):
        # BLS API 키들 (기본값 - 각 파일에서 덮어쓸 수 있음)
        self.BLS_API_KEY = '56b193612b614cdc9416359fd1c73a74'
        self.BLS_API_KEY2 = '0450ef37363c48b5bedd2ae6fc92dd6e'
        self.BLS_API_KEY3 = 'daf1ca7970b74e81b6a5c7a80a8b8a7f'
        self.CURRENT_BLS_KEY = self.BLS_API_KEY
        
        # FRED API 키 (기본값 - 각 파일에서 덮어쓸 수 있음)
        self.FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'
        
        # 세션 객체들
        self.BLS_SESSION = None
        self.FRED_SESSION = None
        
        # API 사용 가능 여부
        self.BLS_API_AVAILABLE = True
        self.FRED_API_AVAILABLE = True
        
        try:
            import requests
            print("✓ requests 라이브러리 사용 가능")
        except ImportError:
            print("⚠️ requests 라이브러리가 없습니다. 설치하세요: pip install requests")
            self.BLS_API_AVAILABLE = False
            self.FRED_API_AVAILABLE = False

# 전역 API 설정 객체
api_config = APIConfig()

# %%
# === API 초기화 함수들 ===

def initialize_bls_api(api_key=None):
    """
    BLS API 세션 초기화
    
    Args:
        api_key: BLS API 키 (None이면 기본값 사용)
    
    Returns:
        bool: 초기화 성공 여부
    """
    global api_config
    
    if not api_config.BLS_API_AVAILABLE:
        print("⚠️ BLS API 사용 불가 (requests 라이브러리 없음)")
        return False
    
    if api_key:
        api_config.BLS_API_KEY = api_key
        api_config.CURRENT_BLS_KEY = api_key
    
    try:
        api_config.BLS_SESSION = requests.Session()
        print("✓ BLS API 세션 초기화 성공")
        return True
    except Exception as e:
        print(f"⚠️ BLS API 초기화 실패: {e}")
        return False

def initialize_fred_api(api_key=None):
    """
    FRED API 세션 초기화
    
    Args:
        api_key: FRED API 키 (None이면 기본값 사용)
    
    Returns:
        bool: 초기화 성공 여부
    """
    global api_config
    
    if not api_config.FRED_API_AVAILABLE:
        print("⚠️ FRED API 사용 불가 (requests 라이브러리 없음)")
        return False
    
    if api_key:
        api_config.FRED_API_KEY = api_key
    
    if not api_config.FRED_API_KEY or api_config.FRED_API_KEY == 'YOUR_FRED_API_KEY_HERE':
        print("⚠️ FRED API 키가 설정되지 않았습니다.")
        return False
    
    try:
        api_config.FRED_SESSION = requests.Session()
        print("✓ FRED API 세션 초기화 성공")
        return True
    except Exception as e:
        print(f"⚠️ FRED API 초기화 실패: {e}")
        return False

def switch_bls_api_key():
    """BLS API 키를 순환 전환"""
    global api_config
    
    if api_config.CURRENT_BLS_KEY == api_config.BLS_API_KEY:
        api_config.CURRENT_BLS_KEY = api_config.BLS_API_KEY2
        print("🔄 BLS API 키를 KEY2로 전환")
    elif api_config.CURRENT_BLS_KEY == api_config.BLS_API_KEY2:
        api_config.CURRENT_BLS_KEY = api_config.BLS_API_KEY3
        print("🔄 BLS API 키를 KEY3로 전환")
    else:
        api_config.CURRENT_BLS_KEY = api_config.BLS_API_KEY
        print("🔄 BLS API 키를 KEY1로 전환")

# %%
# === 데이터 로드 함수들 ===

BLS_MAX_YEAR_SPAN = 19  # inclusive span → 20년 단위 요청


def _fetch_bls_series_range(series_id, start_year, end_year):
    """단일 BLS API 호출로 주어진 연도 구간을 가져온다."""
    global api_config
    
    if not api_config.BLS_API_AVAILABLE or api_config.BLS_SESSION is None:
        print(f"❌ BLS API 사용 불가 - {series_id}")
        return None

    url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
    headers = {'Content-type': 'application/json'}

    payload = {
        'seriesid': [series_id],
        'startyear': str(start_year),
        'endyear': str(end_year),
        'catalog': False,
        'calculations': False,
        'annualaverage': False,
    }

    if api_config.CURRENT_BLS_KEY:
        payload['registrationkey'] = api_config.CURRENT_BLS_KEY

    try:
        print(f"📊 BLS에서 로딩: {series_id} ({start_year}~{end_year})")
        response = api_config.BLS_SESSION.post(url, data=json.dumps(payload), headers=headers, timeout=30)
        response.raise_for_status()
        
        json_data = response.json()
        
        if json_data.get('status') == 'REQUEST_SUCCEEDED':
            series_data = json_data['Results']['series'][0]['data']
            
            dates = []
            values = []
            
            for item in series_data:
                try:
                    year = int(item['year'])
                    period = item['period']
                    if period.startswith('M'):
                        month = int(period[1:])
                        date = pd.Timestamp(year, month, 1)
                        value = float(item['value'])
                        
                        dates.append(date)
                        values.append(value)
                except (ValueError, KeyError):
                    continue
            
            if dates and values:
                df = pd.DataFrame({'date': dates, 'value': values})
                df = df.sort_values('date')
                series = pd.Series(df['value'].values, index=df['date'], name=series_id)
                print(f"✓ BLS 성공: {series_id} ({len(series)}개 포인트)")
                return series
            print(f"❌ BLS 데이터 없음: {series_id}")
            return None
        else:
            error_msg = json_data.get('message', 'Unknown error')
            print(f"⚠️ BLS API 오류: {error_msg}")
            
            # Daily threshold 초과시 API 키 전환 시도
            if 'daily threshold' in error_msg.lower() or 'daily quota' in error_msg.lower():
                print("📈 Daily threshold 초과 - API 키 전환 시도")
                switch_bls_api_key()
                
                # 새로운 API 키로 재시도
                payload['registrationkey'] = api_config.CURRENT_BLS_KEY
                try:
                    print(f"🔄 새 API 키로 재시도: {series_id}")
                    response = api_config.BLS_SESSION.post(url, data=json.dumps(payload), headers=headers, timeout=30)
                    response.raise_for_status()
                    
                    json_data = response.json()
                    if json_data.get('status') == 'REQUEST_SUCCEEDED':
                        series_data = json_data['Results']['series'][0]['data']
                        dates = []
                        values = []
                        
                        for item in series_data:
                            try:
                                year = int(item['year'])
                                period = item['period']
                                if period.startswith('M'):
                                    month = int(period[1:])
                                    date = pd.Timestamp(year, month, 1)
                                    value = float(item['value'])
                                    
                                    dates.append(date)
                                    values.append(value)
                            except (ValueError, KeyError):
                                continue
                        
                        if dates and values:
                            df = pd.DataFrame({'date': dates, 'value': values})
                            df = df.sort_values('date')
                            series = pd.Series(df['value'].values, index=df['date'], name=series_id)
                            print(f"✓ BLS 재시도 성공: {series_id}")
                            return series
                    
                    print(f"❌ BLS 재시도 실패: {series_id}")
                    return None
                except Exception as retry_e:
                    print(f"❌ BLS 재시도 중 오류: {retry_e}")
                    return None
            
            return None
            
    except Exception as e:
        print(f"❌ BLS 요청 실패: {series_id} - {e}")
        return None


def get_bls_data(series_id, start_year=2020, end_year=None):
    """
    BLS API에서 데이터 가져오기 (20년 제한 회피용 청크 요청).
    """
    if end_year is None:
        end_year = dt_datetime.now().year

    if start_year > end_year:
        print(f"⚠️ 잘못된 연도 범위: {start_year} > {end_year}")
        return None

    chunks = []
    chunk_start = start_year
    while chunk_start <= end_year:
        chunk_end = min(chunk_start + BLS_MAX_YEAR_SPAN, end_year)
        series_chunk = _fetch_bls_series_range(series_id, chunk_start, chunk_end)
        if series_chunk is not None and not series_chunk.empty:
            chunks.append(series_chunk)
        chunk_start = chunk_end + 1

    if not chunks:
        return None

    combined = pd.concat(chunks).sort_index()
    combined = combined[~combined.index.duplicated(keep='last')]
    return combined

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
    global api_config
    
    if not api_config.FRED_API_AVAILABLE or api_config.FRED_SESSION is None:
        print(f"❌ FRED API 사용 불가 - {series_id}")
        return None
    
    if end_date is None:
        end_date = dt_datetime.now().strftime('%Y-%m-%d')
    
    url = f'https://api.stlouisfed.org/fred/series/observations'
    params = {
        'series_id': series_id,
        'api_key': api_config.FRED_API_KEY,
        'file_type': 'json',
        'observation_start': start_date,
        'observation_end': end_date,
        'sort_order': 'asc'
    }
    
    try:
        print(f"📊 FRED에서 로딩: {series_id}")
        response = api_config.FRED_SESSION.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if 'observations' in data:
            observations = data['observations']
            
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
            
    except requests.exceptions.HTTPError as e:
        if 'Bad Request' in str(e):
            print(f"❌ FRED API 오류: 잘못된 시리즈 ID '{series_id}' - 존재하지 않는 시리즈일 수 있습니다")
        else:
            print(f"❌ FRED HTTP 오류: {series_id} - {e}")
        return None
    except Exception as e:
        print(f"❌ FRED 요청 실패: {series_id} - {e}")
        return None

# %%
# === 데이터 저장/로드 함수들 ===

def ensure_data_directory(csv_file_path):
    """데이터 디렉터리 생성 확인"""
    path_obj = Path(csv_file_path)
    data_dir = path_obj.parent
    if not data_dir.exists():
        data_dir.mkdir(parents=True)
        print(f"📁 데이터 디렉터리 생성: {data_dir}")

def save_data_to_csv(data_df, csv_file_path):
    """
    데이터를 CSV에 저장
    
    Args:
        data_df: 저장할 DataFrame
        csv_file_path: CSV 파일 경로
    
    Returns:
        bool: 저장 성공 여부
    """
    if data_df.empty:
        print("⚠️ 저장할 데이터가 없습니다.")
        return False
    
    ensure_data_directory(csv_file_path)
    
    try:
        df_to_save = data_df.copy()
        df_to_save.index.name = 'date'
        df_to_save.to_csv(csv_file_path)
        print(f"💾 데이터 저장 완료: {csv_file_path}")
        return True
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        return False

def load_data_from_csv(csv_file_path):
    """
    CSV에서 데이터 로드
    
    Args:
        csv_file_path: CSV 파일 경로
    
    Returns:
        pandas.DataFrame: 로드된 데이터 (실패시 None)
    """
    if not os.path.exists(csv_file_path):
        print("📂 저장된 CSV 파일이 없습니다.")
        return None
    
    try:
        df = pd.read_csv(csv_file_path, index_col=0, parse_dates=True)
        print(f"📂 CSV 데이터 로드: {len(df)}개 데이터 포인트")
        return df
    except Exception as e:
        print(f"❌ CSV 로드 실패: {e}")
        return None

# %%
# === 데이터 계산 함수들 ===

def calculate_mom_change(data):
    """전월대비 변화량 계산"""
    return data.diff()

def calculate_mom_percent(data):
    """전월대비 변화율 계산 (%)"""
    return (data.pct_change() * 100)

def calculate_yoy_change(data):
    """전년동월대비 변화량 계산"""
    return data.diff(12)

def calculate_yoy_percent(data):
    """전년동월대비 변화율 계산 (%)"""
    return (data.pct_change(12) * 100)

def calculate_average_change(data, months):
    """특정 기간 평균 대비 변화량 계산"""
    avg = data.rolling(window=months).mean()
    return data - avg.shift(1)

def calculate_average_percent_change(data, months):
    """특정 기간 평균 대비 변화율 계산"""
    avg = data.rolling(window=months).mean()
    return ((data / avg.shift(1)) - 1) * 100

# %%
# === 스마트 업데이트 함수들 ===

def check_recent_data_consistency(csv_data, api_data, tolerance=10.0):
    """
    최근 데이터의 일치성 확인
    
    Args:
        csv_data: CSV에서 로드된 데이터
        api_data: API에서 가져온 데이터
        tolerance: 허용 오차
    
    Returns:
        dict: 일치성 확인 결과
    """
    if csv_data is None or csv_data.empty:
        return {'needs_update': True, 'reason': 'CSV 데이터 없음'}
    
    if api_data is None or api_data.empty:
        return {'needs_update': True, 'reason': 'API 데이터 없음'}
    
    try:
        # 최근 3개월 데이터 확인
        csv_data.index = pd.to_datetime(csv_data.index)
        api_data.index = pd.to_datetime(api_data.index)

        csv_latest = csv_data.tail(3)

        csv_latest_date = csv_data.index.max()
        api_latest_date = api_data.index.max()

        if csv_latest_date is not None and api_latest_date is not None:
            if pd.notna(api_latest_date) and pd.notna(csv_latest_date):
                if api_latest_date > csv_latest_date:
                    return {
                        'needs_update': True,
                        'reason': 'API 최신 날짜가 더 최신',
                        'existing_latest': csv_latest_date.strftime('%Y-%m-%d'),
                        'api_latest': api_latest_date.strftime('%Y-%m-%d'),
                        'tolerance': tolerance,
                    }

        # 공통 날짜 찾기
        common_dates = csv_latest.index.intersection(api_data.index)
        
        if len(common_dates) == 0:
            return {'needs_update': True, 'reason': '공통 날짜 없음'}
        
        # 최근 데이터 비교
        inconsistencies = []
        
        for date in common_dates[-3:]:
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
                    
                    # 차이 계산
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
        print(f"   허용 오차: {result['tolerance']:,.1f}")
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
                        print(f"   - {inc['date']} {inc['series']}: CSV={csv_val:,.1f} vs API={api_val:,.1f} (차이: {diff:,.1f})")

# %%
# === 시각화 함수들 (KPDS 포맷) ===

def create_timeseries_chart(data, series_names=None, chart_type='level', 
                           ytitle=None, korean_names=None, title=None):
    """
    시계열 차트 생성 (KPDS 포맷)
    
    Args:
        data: DataFrame 또는 Series
        series_names: 표시할 시리즈 리스트
        chart_type: 'level', 'mom', 'yoy', 'mom_change', 'yoy_change'
        ytitle: Y축 제목
        korean_names: 한국어 이름 매핑 딕셔너리
        title: 차트 제목
    
    Returns:
        plotly figure
    """
    if isinstance(data, pd.Series):
        data = data.to_frame()
    
    if series_names is None:
        series_names = data.columns.tolist()
    
    # 데이터 선택
    if not all(col in data.columns for col in series_names):
        available_cols = [col for col in series_names if col in data.columns]
        if not available_cols:
            print("⚠️ 요청한 시리즈가 없습니다.")
            return None
        series_names = available_cols
    
    plot_data = data[series_names].copy()
    
    # 차트 타입별 데이터 변환
    if chart_type == 'mom':
        plot_data = calculate_mom_percent(plot_data)
        if ytitle is None:
            ytitle = "%"
    elif chart_type == 'yoy':
        plot_data = calculate_yoy_percent(plot_data)
        if ytitle is None:
            ytitle = "%"
    elif chart_type == 'mom_change':
        plot_data = calculate_mom_change(plot_data)
        if ytitle is None:
            ytitle = "천 명"
    elif chart_type == 'yoy_change':
        plot_data = calculate_yoy_change(plot_data)
        if ytitle is None:
            ytitle = "천 명"
    elif ytitle is None:
        ytitle = "천 명"
    
    # 제목 출력
    if title:
        print(title)
    
    if plot_data.empty:
        print("⚠️ 플롯할 데이터가 없습니다.")
        return None
    
    # 시리즈 개수에 따른 차트 함수 선택
    if len(plot_data.columns) == 1:
        # 단일 시리즈
        fig = df_line_chart(plot_data, plot_data.columns[0], ytitle=ytitle)
    else:
        # 다중 시리즈
        if korean_names:
            labels = {col: korean_names.get(col, col) for col in plot_data.columns}
        else:
            labels = {col: col for col in plot_data.columns}
        fig = df_multi_line_chart(plot_data, plot_data.columns.tolist(), ytitle=ytitle, labels=labels)
    
    # 0선 추가 (변화율/변화량 차트인 경우)
    if chart_type in ['mom', 'yoy', 'mom_change', 'yoy_change']:
        fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)
    
    return fig

def create_comparison_chart(data, series_names, periods=[1, 3, 6, 12], 
                           korean_names=None, title=None):
    """
    비교 차트 생성 (KPDS 포맷 가로 바 차트)
    
    Args:
        data: DataFrame
        series_names: 비교할 시리즈 리스트
        periods: 비교할 기간들 (개월 수)
        korean_names: 한국어 이름 매핑
        title: 차트 제목
    
    Returns:
        plotly figure
    """
    if data.empty:
        print("⚠️ 데이터가 없습니다.")
        return None
    
    # 최신 날짜
    latest_date = data.index[-1]
    
    if title:
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
        
        for series in series_names:
            if series not in data.columns:
                continue
            
            series_data = data[series]
            latest_value = series_data.iloc[-1]
            
            if period == 1:
                # 전월 대비
                prev_value = series_data.iloc[-2] if len(series_data) > 1 else latest_value
                change = ((latest_value / prev_value) - 1) * 100
            else:
                # N개월 평균 대비
                avg_value = series_data.iloc[-(period+1):-1].mean()
                change = ((latest_value / avg_value) - 1) * 100
            
            if korean_names:
                korean_name = korean_names.get(series, series)
            else:
                korean_name = series
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

def create_heatmap_chart(data, series_names, months=12, korean_names=None, title=None):
    """
    변화율 히트맵 생성 (KPDS 포맷)
    
    Args:
        data: MoM 변화율 데이터 DataFrame
        series_names: 표시할 시리즈 리스트
        months: 표시할 개월 수 (None이면 전체 데이터)
        korean_names: 한국어 이름 매핑
        title: 차트 제목
    
    Returns:
        plotly figure
    """
    if data.empty:
        print("⚠️ 데이터가 없습니다.")
        return None
    
    # 최근 N개월 데이터 (None이면 전체)
    recent_data = data.tail(months) if months is not None else data
    
    # 시리즈 선택
    available_series = [s for s in series_names if s in recent_data.columns]
    if not available_series:
        print("⚠️ 요청한 시리즈가 없습니다.")
        return None
    
    # 데이터 준비
    heatmap_data = recent_data[available_series].T
    
    # 라벨 변환
    if korean_names:
        y_labels = [korean_names.get(col, col) for col in available_series]
    else:
        y_labels = available_series
    x_labels = [date.strftime('%Y-%m') for date in heatmap_data.columns]
    
    # 제목 출력
    if title:
        print(title)
    
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
        colorbar=dict(title="변화율 (%)", titlefont=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE))
    ))
    
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

def plot_economic_series(data_dict, series_list, chart_type='multi_line', data_type='mom',
                         periods=None, labels=None, left_ytitle=None, right_ytitle=None, 
                         target_date=None, korean_names=None, axis_allocation=None):
    """
    범용 경제 데이터 시각화 함수 - 어떤 경제 데이터든 원하는 시리즈로 다양한 차트 생성
    
    Args:
        data_dict: 데이터 딕셔너리 (예: {'raw_data': df, 'mom_data': df, 'mom_change': df})
        series_list: 시각화할 시리즈 리스트 (예: ['nonfarm_total', 'manufacturing'])
        chart_type: 차트 종류 ('multi_line', 'dual_axis', 'horizontal_bar', 'single_line')
        data_type: 데이터 타입 ('mom', 'raw', 'mom_change', 'yoy', 'yoy_change')
        periods: 표시할 기간 (개월, None이면 전체 데이터)
        labels: 시리즈 라벨 딕셔너리 (None이면 자동)
        left_ytitle: 왼쪽 Y축 제목 (단위만)
        right_ytitle: 오른쪽 Y축 제목 (단위만)
        target_date: 특정 날짜 기준 (예: '2025-06-01', None이면 최신 데이터)
        korean_names: 한국어 이름 매핑 딕셔너리
        axis_allocation: 이중축 차트에서 왼쪽/오른쪽 축에 배치할 시리즈 지정
    
    Returns:
        plotly figure
    """
    if not data_dict or 'raw_data' not in data_dict:
        print("⚠️ 데이터가 로드되지 않았습니다. 먼저 데이터를 로드하세요.")
        return None
    
    # 데이터 타입에 따라 데이터 가져오기
    if data_type == 'mom':
        if 'mom_data' in data_dict:
            data = data_dict['mom_data']
        else:
            data = calculate_mom_percent(data_dict['raw_data'])
        unit = "%"
        desc = "전월대비 변화율"
    elif data_type == 'raw':
        data = data_dict['raw_data']
        unit = "천 명"  # 기본값, 필요시 수정
        desc = "수준"
    elif data_type == 'mom_change':
        if 'mom_change' in data_dict:
            data = data_dict['mom_change']
        else:
            data = calculate_mom_change(data_dict['raw_data'])
        unit = "천 명"
        desc = "전월대비 변화량"
    elif data_type == 'yoy':
        if 'yoy_data' in data_dict:
            data = data_dict['yoy_data']
        else:
            data = calculate_yoy_percent(data_dict['raw_data'])
        unit = "%"
        desc = "전년동월대비 변화율"
    elif data_type == 'yoy_change':
        if 'yoy_change' in data_dict:
            data = data_dict['yoy_change']
        else:
            data = calculate_yoy_change(data_dict['raw_data'])
        unit = "천 명"
        desc = "전년동월대비 변화량"
    else:
        print("❌ 지원하지 않는 data_type입니다. 'mom', 'raw', 'mom_change', 'yoy', 'yoy_change' 중 선택하세요.")
        return None
    
    if data.empty:
        print("❌ 데이터가 없습니다.")
        return None
    
    # 기간 제한 또는 특정 날짜 기준
    if target_date:
        try:
            target_date_parsed = pd.to_datetime(target_date)
            # 해당 날짜 이전의 데이터만 선택
            filtered_data = data[data.index <= target_date_parsed]
            if filtered_data.empty:
                print(f"⚠️ {target_date} 이전의 데이터가 없습니다.")
                return None
            recent_data = filtered_data.tail(periods) if periods is not None else filtered_data
        except:
            print(f"⚠️ 잘못된 날짜 형식입니다: {target_date}. 'YYYY-MM-DD' 형식을 사용하세요.")
            return None
    else:
        recent_data = data.tail(periods) if periods is not None else data
    
    # 사용 가능한 컬럼 확인
    available_cols = [col for col in series_list if col in recent_data.columns]
    axis_allocation = axis_allocation or {}
    
    if not available_cols:
        print("❌ 요청한 시리즈가 데이터에 없습니다.")
        print(f"   사용 가능한 시리즈: {list(recent_data.columns)}")
        return None
    
    # 자동 라벨 생성
    if labels is None:
        if korean_names:
            labels = {col: korean_names.get(col, col) for col in available_cols}
        else:
            labels = {col: col for col in available_cols}
    
    # Y축 제목 자동 설정 (단위만)
    if left_ytitle is None:
        left_ytitle = unit
    
    # 차트 타입별 시각화
    if chart_type == 'multi_line':
        print(f"경제 시리즈 다중 라인 차트 ({desc})")
        fig = df_multi_line_chart(
            df=recent_data,
            columns=available_cols,
            ytitle=left_ytitle,
            labels=labels
        )
    
    elif chart_type == 'single_line' and len(available_cols) == 1:
        print(f"경제 시리즈 단일 라인 차트 ({desc})")
        fig = df_line_chart(
            df=recent_data,
            column=available_cols[0],
            ytitle=left_ytitle,
            label=labels[available_cols[0]]
        )
    
    elif chart_type == 'dual_axis' and len(available_cols) >= 2:
        left_cols = []
        right_cols = []

        if axis_allocation:
            requested_left = axis_allocation.get('left', [])
            requested_right = axis_allocation.get('right', [])

            left_cols = [col for col in requested_left if col in available_cols]
            right_cols = [col for col in requested_right if col in available_cols and col not in left_cols]

            remaining = [col for col in available_cols if col not in left_cols and col not in right_cols]
            for col in remaining:
                if len(left_cols) <= len(right_cols):
                    left_cols.append(col)
                else:
                    right_cols.append(col)

            if left_cols and not right_cols and len(left_cols) > 1:
                right_cols.append(left_cols.pop())
            if right_cols and not left_cols and len(right_cols) > 1:
                left_cols.append(right_cols.pop(0))

        if not left_cols or not right_cols:
            mid = len(available_cols) // 2
            left_cols = available_cols[:mid] if mid > 0 else available_cols[:1]
            right_cols = available_cols[mid:] if mid > 0 else available_cols[1:]

        if not right_cols:
            print("❌ 이중축 차트에는 왼쪽/오른쪽 축에 최소 한 개 이상의 시리즈가 필요합니다.")
            return None

        print(f"경제 시리즈 이중축 차트 ({desc})")
        fig = df_dual_axis_chart(
            df=recent_data,
            left_cols=left_cols,
            right_cols=right_cols,
            left_labels=[labels[col] for col in left_cols],
            right_labels=[labels[col] for col in right_cols],
            left_title=left_ytitle,
            right_title=right_ytitle or left_ytitle
        )
    
    elif chart_type == 'horizontal_bar':
        # 최신값 기준 가로 바 차트
        latest_values = {}
        for col in available_cols:
            latest_val = recent_data[col].dropna().iloc[-1] if not recent_data[col].dropna().empty else 0
            latest_values[labels[col]] = latest_val
        
        # 날짜 정보 표시
        latest_date = recent_data.index[-1].strftime('%Y-%m') if not recent_data.empty else "N/A"
        date_info = f" ({latest_date})" if target_date else ""
        
        print(f"경제 시리즈 가로 바 차트 ({desc}){date_info}")
        fig = create_horizontal_bar_chart(
            data_dict=latest_values,
            positive_color=deepred_pds,
            negative_color=deepblue_pds,
            unit=unit
        )
    
    elif chart_type == 'vertical_bar':
        # 시계열 세로 바 차트
        print(f"경제 시리즈 세로 바 차트 ({desc})")
        fig = create_vertical_bar_chart(
            data=recent_data,
            columns=available_cols,
            labels=labels,
            ytitle=left_ytitle,
            unit=unit
        )
    
    else:
        print("❌ 지원하지 않는 chart_type이거나 시리즈 개수가 맞지 않습니다.")
        print("   - single_line: 1개 시리즈")
        print("   - multi_line: 여러 시리즈")
        print("   - dual_axis: 2개 이상 시리즈")
        print("   - horizontal_bar: 여러 시리즈")
        print("   - vertical_bar: 여러 시리즈")
        return None
    
    # 0선 추가 (변화율/변화량 차트인 경우)
    if data_type in ['mom', 'yoy', 'mom_change', 'yoy_change'] and chart_type not in ['horizontal_bar', 'vertical_bar']:
        if hasattr(fig, 'add_hline'):
            fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)
    
    return fig

def create_horizontal_bar_chart(data_dict, positive_color=None, negative_color=None, unit=""):
    """
    가로 바 차트 생성 (KPDS 포맷)
    
    Args:
        data_dict: 데이터 딕셔너리 {label: value}
        positive_color: 양수 색상
        negative_color: 음수 색상  
        unit: 단위
    
    Returns:
        plotly figure
    """
    if not data_dict:
        print("⚠️ 데이터가 없습니다.")
        return None
    
    # 색상 기본값 설정
    if positive_color is None:
        positive_color = deepred_pds
    if negative_color is None:
        negative_color = deepblue_pds
    
    # 데이터 정렬
    items = list(data_dict.items())
    items.sort(key=lambda x: x[1])  # 값 기준 정렬
    
    categories = [item[0] for item in items]
    values = [item[1] for item in items]
    colors = [positive_color if val >= 0 else negative_color for val in values]
    
    # 바 차트 생성
    fig = go.Figure(data=go.Bar(
        y=categories,
        x=values,
        orientation='h',
        marker_color=colors,
        text=[f'{v:+.1f}{unit}' for v in values],
        textposition='outside',
        showlegend=False
    ))
    
    # x축 범위 설정
    max_val = max(abs(min(values)), max(values)) * 1.2 if values else 1
    fig.update_xaxes(range=[-max_val, max_val])
    
    # 0선 추가
    fig.add_vline(x=0, line_width=1, line_color="black", opacity=0.5)
    
    # KPDS 폰트 설정
    font_family = 'NanumGothic'
    
    # 레이아웃 업데이트
    fig.update_layout(
        height=max(400, len(categories) * 40),
        width=800,
        paper_bgcolor='white',
        plot_bgcolor='white',
        font=dict(family=font_family, size=FONT_SIZE_GENERAL, color="black"),
        xaxis=dict(
            title=unit,
            title_font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE),
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside'
        ),
        yaxis=dict(
            title="",
            tickfont=dict(family=font_family, size=FONT_SIZE_GENERAL)
        )
    )
    
    return fig

def create_vertical_bar_chart(data, columns, labels=None, ytitle="", unit="", stacked=True):
    """
    시계열 세로 바 차트 생성 (KPDS 포맷) - create_gdp_contribution_chart 스타일
    
    Args:
        data: DataFrame (시계열 데이터)
        columns: 표시할 컬럼 리스트
        labels: 컬럼 라벨 딕셔너리 (None이면 자동)
        ytitle: Y축 제목
        unit: 단위
        stacked: True면 누적 막대, False면 그룹 막대
    
    Returns:
        plotly figure
    """
    if data.empty or not columns:
        print("⚠️ 데이터가 없습니다.")
        return None
    
    # 라벨 자동 설정
    if labels is None:
        labels = {col: col for col in columns}
    
    # 사용 가능한 컬럼만 선택
    available_cols = [col for col in columns if col in data.columns]
    if not available_cols:
        print("❌ 요청한 시리즈가 데이터에 없습니다.")
        return None
    
    # 데이터 준비
    chart_data = data[available_cols].dropna()
    if chart_data.empty:
        print("⚠️ 유효한 데이터가 없습니다.")
        return None
    
    # 날짜 인덱스를 표준 datetime으로 변환
    try:
        chart_data.index = pd.to_datetime(chart_data.index)
    except:
        pass  # 이미 datetime이거나 변환 불가능한 경우 그대로 사용
    
    # KPDS 색상 및 폰트 설정
    colors = [deepblue_pds, deepred_pds, beige_pds, blue_pds, grey_pds]
    font_family = 'NanumGothic'
    
    # 세로 막대 차트 생성
    fig = go.Figure()
    
    for i, col in enumerate(available_cols):
        korean_name = labels.get(col, col)
        values = chart_data[col].values
        
        # 막대 추가
        fig.add_trace(go.Bar(
            x=chart_data.index,
            y=values,
            name=korean_name,
            marker_color=colors[i % len(colors)],
            text=[f'{v:+.1f}' if abs(v) >= 0.1 else f'{v:+.2f}' for v in values],
            textposition='inside' if stacked else 'outside',
            textfont=dict(size=10, color='white' if stacked else 'black'),
            showlegend=len(available_cols) > 1
        ))
    
    # 레이아웃 설정 (KPDS 표준 준수)
    fig.update_layout(
        barmode='stack' if stacked else 'group',
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686,  # KPDS 표준 너비
        height=400,  # KPDS 표준 높이
        font=dict(family=font_family, size=FONT_SIZE_GENERAL, color="black"),
        xaxis=dict(
            title=dict(text=None, font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE)),
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            showgrid=False
        ),
        yaxis=dict(
            title="",  # Y축 제목은 annotation으로 대체
            showline=False,
            tickcolor='white',
            tickformat=',',
            showgrid=False
        ),
        legend=dict(
            orientation="h",  # 가로 배열
            yanchor="bottom",
            y=1.05,  # 차트 위쪽
            xanchor="center",
            x=0.5,
            font=dict(family=font_family, size=FONT_SIZE_LEGEND),
            borderwidth=0,
            bordercolor="rgba(0,0,0,0)"
        ) if len(available_cols) > 1 else dict(showlegend=False)
    )
    
    # 표준 날짜 포맷 적용 (다른 차트들과 동일)
    fig = format_date_ticks(fig, '%b-%y', "auto", chart_data.index)
    
    # Y축 제목을 annotation으로 추가
    if ytitle:
        x_pos = calculate_title_position(ytitle, 'left')
        fig.add_annotation(
            text=ytitle,
            xref="paper", yref="paper",
            x=x_pos, y=1.1,
            showarrow=False,
            font=dict(size=FONT_SIZE_ANNOTATION, family=font_family, color="black"),
            align='left'
        )

    # 0선 추가 (기여도나 변화율 데이터인 경우)
    if any('기여' in str(col) or 'contrib' in str(col).lower() or unit == '%' for col in available_cols):
        fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.7)

    # 동적 마진 적용 (다른 차트들과 동일)
    margins = get_dynamic_margins(ytitle1=ytitle)
    fig.update_layout(margin=margins)
    
    return fig

def export_economic_data(data_dict, series_list, data_type='mom', periods=None, 
                        target_date=None, korean_names=None, export_path=None, 
                        file_format='excel'):
    """
    경제 데이터를 엑셀/CSV로 export하는 함수 - plot_economic_series와 동일한 로직
    
    Args:
        data_dict: 데이터 딕셔너리 (예: {'raw_data': df, 'mom_data': df, 'mom_change': df})
        series_list: export할 시리즈 리스트 (예: ['nonfarm_total', 'manufacturing'])
        data_type: 데이터 타입 ('mom', 'raw', 'mom_change', 'yoy', 'yoy_change')
        periods: 표시할 기간 (개월, None이면 전체 데이터)
        target_date: 특정 날짜 기준 (예: '2025-06-01', None이면 최신 데이터)
        korean_names: 한국어 이름 매핑 딕셔너리
        export_path: export할 파일 경로 (None이면 자동 생성)
        file_format: 파일 형식 ('excel', 'csv')
    
    Returns:
        str: export된 파일 경로 (성공시) 또는 None (실패시)
    """
    if not data_dict or 'raw_data' not in data_dict:
        print("⚠️ 데이터가 로드되지 않았습니다. 먼저 데이터를 로드하세요.")
        return None
    
    # 데이터 타입에 따라 데이터 가져오기 (plot_economic_series와 동일한 로직)
    if data_type == 'mom':
        if 'mom_data' in data_dict:
            data = data_dict['mom_data']
        else:
            data = calculate_mom_percent(data_dict['raw_data'])
        unit = "%"
        desc = "전월대비변화율"
    elif data_type == 'raw':
        data = data_dict['raw_data']
        unit = "천명"  # 기본값, 필요시 수정
        desc = "수준"
    elif data_type == 'mom_change':
        if 'mom_change' in data_dict:
            data = data_dict['mom_change']
        else:
            data = calculate_mom_change(data_dict['raw_data'])
        unit = "천명"
        desc = "전월대비변화량"
    elif data_type == 'yoy':
        if 'yoy_data' in data_dict:
            data = data_dict['yoy_data']
        else:
            data = calculate_yoy_percent(data_dict['raw_data'])
        unit = "%"
        desc = "전년동월대비변화율"
    elif data_type == 'yoy_change':
        if 'yoy_change' in data_dict:
            data = data_dict['yoy_change']
        else:
            data = calculate_yoy_change(data_dict['raw_data'])
        unit = "천명"
        desc = "전년동월대비변화량"
    else:
        print("❌ 지원하지 않는 data_type입니다. 'mom', 'raw', 'mom_change', 'yoy', 'yoy_change' 중 선택하세요.")
        return None
    
    if data.empty:
        print("❌ 데이터가 없습니다.")
        return None
    
    # 기간 제한 또는 특정 날짜 기준 (plot_economic_series와 동일한 로직)
    if target_date:
        try:
            target_date_parsed = pd.to_datetime(target_date)
            filtered_data = data[data.index <= target_date_parsed]
            if filtered_data.empty:
                print(f"⚠️ {target_date} 이전의 데이터가 없습니다.")
                return None
            export_data = filtered_data.tail(periods) if periods is not None else filtered_data
        except:
            print(f"⚠️ 잘못된 날짜 형식입니다: {target_date}. 'YYYY-MM-DD' 형식을 사용하세요.")
            return None
    else:
        export_data = data.tail(periods) if periods is not None else data
    
    # 사용 가능한 컬럼 확인
    available_cols = [col for col in series_list if col in export_data.columns]
    
    if not available_cols:
        print("❌ 요청한 시리즈가 데이터에 없습니다.")
        print(f"   사용 가능한 시리즈: {list(export_data.columns)}")
        return None
    
    # 데이터 선택 및 컬럼명 변경
    final_data = export_data[available_cols].copy()
    
    # 한국어 컬럼명으로 변경
    if korean_names:
        column_mapping = {}
        for col in available_cols:
            korean_name = korean_names.get(col, col)
            if unit and unit != "천명":  # 단위가 있고 천명이 아닌 경우 (%, 달러 등)
                column_mapping[col] = f"{korean_name} ({unit})"
            else:
                column_mapping[col] = korean_name
        final_data = final_data.rename(columns=column_mapping)
    
    # 날짜 인덱스를 컬럼으로 변경
    final_data.reset_index(inplace=True)
    final_data.rename(columns={final_data.columns[0]: '날짜'}, inplace=True)
    
    # export 파일 경로 자동 생성
    if export_path is None:
        timestamp = dt_datetime.now().strftime('%Y%m%d_%H%M%S')
        series_str = '_'.join(available_cols[:3])  # 처음 3개 시리즈만 파일명에 포함
        if len(available_cols) > 3:
            series_str += f"_등{len(available_cols)}개"

        period_str = ""
        if periods is not None:
            period_str = f"_{periods}개월"
        elif target_date:
            period_str = f"_{target_date}까지"

        export_dir = repo_path('us_eco', 'exports')
        export_dir.mkdir(parents=True, exist_ok=True)
        if file_format == 'excel':
            export_path = export_dir / f"{desc}_{series_str}{period_str}_{timestamp}.xlsx"
        else:
            export_path = export_dir / f"{desc}_{series_str}{period_str}_{timestamp}.csv"
    else:
        export_path = Path(export_path)

    # exports 디렉터리 생성
    ensure_data_directory(export_path)
    
    try:
        # 파일 저장
        if file_format == 'excel':
            # 엑셀 저장 (with styling)
            try:
                with pd.ExcelWriter(export_path, engine='openpyxl') as writer:
                    final_data.to_excel(writer, sheet_name='데이터', index=False)
                    
                    # 워크시트 스타일링
                    worksheet = writer.sheets['데이터']
                    
                    # 헤더 스타일링
                    from openpyxl.styles import Font, PatternFill, Alignment
                    header_font = Font(bold=True, color='FFFFFF')
                    header_fill = PatternFill(start_color='366092', end_color='366092', fill_type='solid')
                    
                    for cell in worksheet[1]:
                        cell.font = header_font
                        cell.fill = header_fill
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                    
                    # 컬럼 너비 자동 조정
                    for column in worksheet.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 50)
                        worksheet.column_dimensions[column_letter].width = adjusted_width
                
                print(f"📊 엑셀 파일로 export 완료: {export_path}")
                
            except ImportError:
                # openpyxl이 없으면 기본 엑셀 저장
                final_data.to_excel(export_path, index=False)
                print(f"📊 엑셀 파일로 export 완료 (기본 형식): {export_path}")
        
        else:  # CSV
            final_data.to_csv(export_path, index=False, encoding='utf-8-sig')  # 한글 지원
            print(f"📄 CSV 파일로 export 완료: {export_path}")
        
        # 데이터 정보 출력
        print(f"   📅 기간: {final_data['날짜'].iloc[0].strftime('%Y-%m-%d') if not final_data.empty else 'N/A'} ~ {final_data['날짜'].iloc[-1].strftime('%Y-%m-%d') if not final_data.empty else 'N/A'}")
        print(f"   📈 시리즈: {len(available_cols)}개")
        print(f"   📊 데이터 포인트: {len(final_data)}개")
        print(f"   🔢 데이터 타입: {desc}")
        
        return export_path
        
    except Exception as e:
        print(f"❌ 파일 저장 실패: {e}")
        return None

# %%
# === 분석 함수들 ===

def analyze_latest_trends(data, series_names, korean_names=None):
    """
    최신 트렌드 분석
    
    Args:
        data: 원본 데이터 DataFrame
        series_names: 분석할 시리즈 리스트
        korean_names: 한국어 이름 매핑
    
    Returns:
        dict: 분석 결과
    """
    if data.empty:
        print("⚠️ 데이터가 없습니다.")
        return None
    
    latest_date = data.index[-1]
    mom_data = calculate_mom_percent(data)
    mom_change = calculate_mom_change(data)
    
    print(f"\n📊 최신 트렌드 분석 ({latest_date.strftime('%Y년 %m월')})")
    print("="*60)
    
    results = {
        'latest_date': latest_date,
        'series_analysis': {}
    }
    
    for series in series_names:
        if series not in data.columns:
            continue
        
        current_value = data[series].iloc[-1]
        mom_change_val = mom_change[series].iloc[-1]
        mom_percent_val = mom_data[series].iloc[-1]
        
        korean_name = korean_names.get(series, series) if korean_names else series
        
        print(f"\n{korean_name}:")
        print(f"   현재값: {current_value:,.1f}")
        print(f"   전월대비: {mom_change_val:+.1f} ({mom_percent_val:+.1f}%)")
        
        results['series_analysis'][series] = {
            'korean_name': korean_name,
            'current_value': current_value,
            'mom_change': mom_change_val,
            'mom_percent': mom_percent_val
        }
    
    return results

# %%
# === 통합 데이터 로드 함수 ===

def load_economic_data(series_dict, data_source='BLS', csv_file_path=None,
                      start_date='2020-01-01', smart_update=True, force_reload=False,
                      tolerance=10.0):
    """
    경제 데이터 통합 로드 함수 (스마트 업데이트 지원)
    
    Args:
        series_dict: 시리즈 매핑 딕셔너리 {name: id}
        data_source: 'BLS' 또는 'FRED'
        csv_file_path: CSV 파일 경로
        start_date: 시작 날짜
        smart_update: 스마트 업데이트 사용 여부
        force_reload: 강제 재로드 여부
        tolerance: 일치성 확인 허용 오차
    
    Returns:
        dict: 로드된 데이터와 메타정보
    """
    print(f"🚀 {data_source} 데이터 로딩 시작...")
    print("="*50)
    
    # API 초기화
    if data_source == 'BLS':
        if not initialize_bls_api():
            print("❌ BLS API 초기화 실패")
            return None
    elif data_source == 'FRED':
        if not initialize_fred_api():
            print("❌ FRED API 초기화 실패")
            return None
    else:
        print("❌ 지원하지 않는 데이터 소스")
        return None
    
    # 스마트 업데이트 로직
    needs_api_call = True
    consistency_result = None
    
    if smart_update and not force_reload and csv_file_path:
        print("🤖 스마트 업데이트 모드 활성화")
        
        # CSV에서 데이터 로드 시도
        csv_data = load_data_from_csv(csv_file_path)
        
        if csv_data is not None and not csv_data.empty:
            # 최신 몇 개 데이터만 API로 가져와서 비교
            print("🔍 최근 데이터 일치성 확인 중...")
            
            # 대표 시리즈 하나만 확인 (빠른 체크)
            main_series = list(series_dict.keys())[0]
            main_series_id = series_dict[main_series]
            
            if data_source == 'BLS':
                recent_start_year = dt_datetime.now().year - 1
                recent_data = get_bls_data(main_series_id, recent_start_year)
            else:  # FRED
                recent_start = (dt_datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
                recent_data = get_fred_data(main_series_id, recent_start)
            
            if recent_data is not None:
                # 임시 DataFrame 생성
                temp_api_data = pd.DataFrame({main_series: recent_data})
                
                # 일치성 확인
                consistency_result = check_recent_data_consistency(csv_data, temp_api_data, tolerance)
                print_consistency_results(consistency_result)
                
                needs_api_call = consistency_result['needs_update']
                
                if not needs_api_call:
                    print("✅ 최근 데이터가 일치함 - API 호출 건너뛰기")
                    return {
                        'raw_data': csv_data,
                        'mom_data': calculate_mom_percent(csv_data),
                        'mom_change': calculate_mom_change(csv_data),
                        'yoy_data': calculate_yoy_percent(csv_data),
                        'yoy_change': calculate_yoy_change(csv_data),
                        'load_info': {
                            'loaded': True,
                            'load_time': dt_datetime.now(),
                            'start_date': start_date,
                            'series_count': len(csv_data.columns),
                            'data_points': len(csv_data),
                            'source': f'CSV (스마트 업데이트)',
                            'consistency_check': consistency_result
                        }
                    }
                else:
                    print("📡 데이터 불일치 감지 - 전체 API 호출 진행")
    
    # API를 통한 전체 데이터 로드
    if needs_api_call:
        print(f"📊 {data_source} API를 통한 데이터 수집...")
        
        raw_data_dict = {}
        
        for series_name, series_id in series_dict.items():
            if data_source == 'BLS':
                start_year = int(start_date[:4])
                series_data = get_bls_data(series_id, start_year)
            else:  # FRED
                series_data = get_fred_data(series_id, start_date)
            
            if series_data is not None and len(series_data) > 0:
                raw_data_dict[series_name] = series_data
            else:
                print(f"❌ 데이터 로드 실패: {series_name}")
        
        if len(raw_data_dict) == 0:
            print("❌ 로드된 시리즈가 없습니다.")
            return None
        
        # DataFrame 생성
        raw_data = pd.DataFrame(raw_data_dict)
        
        # 파생 데이터 계산
        mom_data = calculate_mom_percent(raw_data)
        mom_change = calculate_mom_change(raw_data)
        yoy_data = calculate_yoy_percent(raw_data)
        yoy_change = calculate_yoy_change(raw_data)
        
        # CSV에 저장
        if csv_file_path:
            save_data_to_csv(raw_data, csv_file_path)
        
        print(f"\n✅ 데이터 로딩 완료! 시리즈: {len(raw_data_dict)}개")
        
        return {
            'raw_data': raw_data,
            'mom_data': mom_data,
            'mom_change': mom_change,
            'yoy_data': yoy_data,
            'yoy_change': yoy_change,
            'load_info': {
                'loaded': True,
                'load_time': dt_datetime.now(),
                'start_date': start_date,
                'series_count': len(raw_data_dict),
                'data_points': len(raw_data),
                'source': f'{data_source} API (전체 로드)',
                'consistency_check': consistency_result
            }
        }
    
    return None

# %%
# === 그룹별 스마트 업데이트 함수들 ===

def check_group_consistency(existing_data, group_name, main_series_dict, data_source='FRED', tolerance=10.0):
    """
    특정 그룹의 데이터 일치성 확인
    
    Args:
        existing_data: 기존 데이터 DataFrame
        group_name: 그룹 이름 (예: 'philadelphia', 'chicago')
        main_series_dict: 그룹의 메인 시리즈 딕셔너리 {name: fred_id}
        data_source: 데이터 소스 ('FRED' 또는 'BLS')
        tolerance: 허용 오차
    
    Returns:
        dict: 일치성 확인 결과
    """
    if existing_data.empty:
        return {
            'need_update': True,
            'reason': f'{group_name} 그룹 기존 데이터 없음',
            'existing_date': None,
            'api_date': None
        }
    
    # 그룹의 메인 지표 확인
    if not main_series_dict:
        return {
            'need_update': True,
            'reason': f'{group_name} 그룹 메인 시리즈 없음',
            'existing_date': None,
            'api_date': None
        }
    
    main_series_name = list(main_series_dict.keys())[0]
    main_series_id = list(main_series_dict.values())[0]
    
    print(f"🔍 {group_name} 그룹 일치성 확인 중...")
    print(f"   메인 시리즈: {main_series_name} ({main_series_id})")
    
    # 기존 데이터에서 그룹 시리즈 확인
    group_columns = [col for col in existing_data.columns if col.startswith(f"{group_name}_")]
    
    if not group_columns:
        print(f"   ⚠️ 기존 데이터에 {group_name} 그룹 없음")
        return {
            'need_update': True,
            'reason': f'{group_name} 그룹 기존 데이터 없음',
            'existing_date': None,
            'api_date': None
        }
    
    # 메인 시리즈 확인
    if main_series_name not in existing_data.columns:
        print(f"   ⚠️ 메인 시리즈 없음: {main_series_name}")
        return {
            'need_update': True,
            'reason': f'{group_name} 메인 시리즈 없음',
            'existing_date': None,
            'api_date': None
        }
    
    # 기존 데이터 최신 날짜
    existing_series = existing_data[main_series_name].dropna()
    if existing_series.empty:
        print(f"   ⚠️ 메인 시리즈 데이터 비어있음")
        return {
            'need_update': True,
            'reason': f'{group_name} 메인 시리즈 비어있음',
            'existing_date': None,
            'api_date': None
        }
    
    existing_latest_date = existing_series.index[-1]
    print(f"   기존 데이터 최신: {existing_latest_date.strftime('%Y-%m')}")
    
    # 최근 3개월 데이터만 API에서 확인
    try:
        recent_start = (dt_datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        
        if data_source == 'FRED':
            recent_api_data = get_fred_data(main_series_id, recent_start)
        elif data_source == 'BLS':
            recent_start_year = dt_datetime.now().year - 1
            recent_api_data = get_bls_data(main_series_id, recent_start_year)
        else:
            print(f"   ❌ 지원하지 않는 데이터 소스: {data_source}")
            return {
                'need_update': True,
                'reason': f'지원하지 않는 데이터 소스: {data_source}',
                'existing_date': existing_latest_date,
                'api_date': None
            }
        
        if recent_api_data is None or recent_api_data.empty:
            print(f"   ⚠️ API 데이터 조회 실패")
            return {
                'need_update': True,
                'reason': f'{group_name} API 조회 실패',
                'existing_date': existing_latest_date,
                'api_date': None
            }
        
        api_latest_date = recent_api_data.index[-1]
        print(f"   API 데이터 최신: {api_latest_date.strftime('%Y-%m')}")
        
        # 날짜 비교 (월 단위)
        existing_month = existing_latest_date.to_period('M')
        api_month = api_latest_date.to_period('M')
        
        if api_month > existing_month:
            print(f"   🆕 {group_name} 새로운 데이터: {existing_latest_date.strftime('%Y-%m')} → {api_latest_date.strftime('%Y-%m')}")
            return {
                'need_update': True,
                'reason': f'{group_name} 새로운 데이터 ({api_latest_date.strftime("%Y-%m")})',
                'existing_date': existing_latest_date,
                'api_date': api_latest_date
            }
        
        # 값 비교
        common_dates = existing_series.index.intersection(recent_api_data.index)
        if len(common_dates) > 0:
            for date in common_dates[-3:]:  # 최근 3개 확인
                existing_val = existing_series[date]
                api_val = recent_api_data[date]
                
                if pd.notna(existing_val) and pd.notna(api_val):
                    diff = abs(existing_val - api_val)
                    if diff > tolerance:
                        print(f"   🚨 {group_name} 값 불일치 ({date.strftime('%Y-%m')}): {existing_val:.1f} vs {api_val:.1f}")
                        return {
                            'need_update': True,
                            'reason': f'{group_name} 값 불일치 ({date.strftime("%Y-%m")})',
                            'existing_date': existing_latest_date,
                            'api_date': api_latest_date
                        }
        
        print(f"   ✅ {group_name} 데이터 일치")
        return {
            'need_update': False,
            'reason': f'{group_name} 데이터 일치',
            'existing_date': existing_latest_date,
            'api_date': api_latest_date
        }
        
    except Exception as e:
        print(f"   ❌ {group_name} 일치성 확인 오류: {e}")
        return {
            'need_update': True,
            'reason': f'{group_name} 확인 오류: {str(e)}',
            'existing_date': existing_latest_date,
            'api_date': None
        }

def update_group_data(group_name, group_series_dict, existing_data, data_source='FRED', start_date='2020-01-01'):
    """
    특정 그룹의 데이터만 업데이트
    
    Args:
        group_name: 그룹 이름
        group_series_dict: 그룹 시리즈 딕셔너리 {name: id}
        existing_data: 기존 데이터 DataFrame
        data_source: 데이터 소스
        start_date: 시작 날짜
    
    Returns:
        pandas.DataFrame: 업데이트된 전체 데이터
    """
    print(f"🔄 {group_name} 그룹 데이터 업데이트 중...")
    
    # 그룹 데이터 새로 수집
    new_group_data = {}
    
    for series_name, series_id in group_series_dict.items():
        try:
            if data_source == 'FRED':
                series_data = get_fred_data(series_id, start_date)
            elif data_source == 'BLS':
                start_year = int(start_date[:4])
                series_data = get_bls_data(series_id, start_year)
            else:
                print(f"   ❌ 지원하지 않는 데이터 소스: {data_source}")
                continue
                
            if series_data is not None and len(series_data) > 0:
                new_group_data[series_name] = series_data
                print(f"   ✓ {series_name}: {len(series_data.dropna())}개 포인트")
            else:
                print(f"   ❌ {series_name}: 데이터 없음")
                
        except Exception as e:
            print(f"   ❌ {series_name} 업데이트 실패: {e}")
            continue
    
    if not new_group_data:
        print(f"   ❌ {group_name} 그룹 업데이트 실패 - 새 데이터 없음")
        return existing_data
    
    # 새 그룹 데이터를 DataFrame으로 변환
    new_group_df = pd.DataFrame(new_group_data)
    
    # 기존 데이터에서 해당 그룹의 실제 시리즈 컬럼 제거 (중복 방지)
    group_series_names = list(group_series_dict.keys())
    existing_group_columns = [col for col in existing_data.columns if col in group_series_names]
    updated_data = existing_data.drop(columns=existing_group_columns)
    
    # 새 그룹 데이터 병합
    updated_data = updated_data.join(new_group_df, how='outer')
    
    print(f"   ✅ {group_name} 그룹 업데이트 완료: {len(new_group_data)}개 시리즈")
    
    return updated_data

def load_economic_data_grouped(series_groups, data_source='FRED', csv_file_path=None,
                              start_date='2020-01-01', smart_update=True, force_reload=False,
                              tolerance=10.0):
    """
    그룹별 경제 데이터 스마트 업데이트 로드 함수
    
    Args:
        series_groups: 그룹별 시리즈 딕셔너리 
                      {group_name: {series_name: series_id}}
        data_source: 'BLS' 또는 'FRED'
        csv_file_path: CSV 파일 경로
        start_date: 시작 날짜
        smart_update: 스마트 업데이트 사용 여부
        force_reload: 강제 재로드 여부
        tolerance: 일치성 확인 허용 오차
    
    Returns:
        dict: 로드된 데이터와 메타정보
    """
    print(f"🚀 그룹별 {data_source} 데이터 로딩 시작...")
    print(f"   그룹 수: {len(series_groups)}")
    print(f"   그룹명: {list(series_groups.keys())}")
    print("="*50)
    
    # API 초기화
    if data_source == 'BLS':
        if not initialize_bls_api():
            print("❌ BLS API 초기화 실패")
            return None
    elif data_source == 'FRED':
        if not initialize_fred_api():
            print("❌ FRED API 초기화 실패")
            return None
    else:
        print("❌ 지원하지 않는 데이터 소스")
        return None
    
    # 스마트 업데이트 로직
    if smart_update and not force_reload and csv_file_path:
        print("🤖 그룹별 스마트 업데이트 모드 활성화")
        
        # CSV에서 기존 데이터 로드
        existing_data = load_data_from_csv(csv_file_path)
        
        if existing_data is not None and not existing_data.empty:
            print("📂 기존 CSV 데이터 로드 완료")
            
            # 각 그룹별 일치성 확인
            groups_to_update = []
            group_consistency_results = {}
            
            for group_name, group_series in series_groups.items():
                # 그룹의 메인 시리즈 추출 (첫 번째 시리즈를 메인으로)
                main_series_dict = {
                    list(group_series.keys())[0]: list(group_series.values())[0]
                }
                
                consistency_result = check_group_consistency(
                    existing_data, group_name, main_series_dict, data_source, tolerance
                )
                
                group_consistency_results[group_name] = consistency_result
                
                if consistency_result['need_update']:
                    groups_to_update.append(group_name)
                    print(f"   📝 {group_name}: 업데이트 필요 - {consistency_result['reason']}")
                else:
                    print(f"   ✅ {group_name}: 데이터 일치")
            
            # 업데이트가 필요한 그룹만 처리
            if not groups_to_update:
                print("✅ 모든 그룹 데이터 일치 - API 호출 건너뛰기")
                return {
                    'raw_data': existing_data,
                    'mom_data': calculate_mom_percent(existing_data),
                    'mom_change': calculate_mom_change(existing_data),
                    'yoy_data': calculate_yoy_percent(existing_data),
                    'yoy_change': calculate_yoy_change(existing_data),
                    'load_info': {
                        'loaded': True,
                        'load_time': dt_datetime.now(),
                        'start_date': start_date,
                        'series_count': len(existing_data.columns),
                        'data_points': len(existing_data),
                        'source': f'CSV (그룹별 스마트 업데이트)',
                        'groups_checked': list(series_groups.keys()),
                        'groups_updated': [],
                        'consistency_results': group_consistency_results
                    }
                }
            
            # 필요한 그룹만 업데이트
            print(f"📡 {len(groups_to_update)}개 그룹 개별 업데이트: {groups_to_update}")
            
            updated_data = existing_data.copy()
            
            for group_name in groups_to_update:
                group_series = series_groups[group_name]
                updated_data = update_group_data(
                    group_name, group_series, updated_data, data_source, start_date
                )
            
            # CSV에 저장
            if csv_file_path:
                save_data_to_csv(updated_data, csv_file_path)
            
            print(f"\n✅ 그룹별 부분 업데이트 완료! ({len(groups_to_update)}개 그룹 업데이트)")
            
            return {
                'raw_data': updated_data,
                'mom_data': calculate_mom_percent(updated_data),
                'mom_change': calculate_mom_change(updated_data),
                'yoy_data': calculate_yoy_percent(updated_data),
                'yoy_change': calculate_yoy_change(updated_data),
                'load_info': {
                    'loaded': True,
                    'load_time': dt_datetime.now(),
                    'start_date': start_date,
                    'series_count': len(updated_data.columns),
                    'data_points': len(updated_data),
                    'source': f'{data_source} API (그룹별 부분 업데이트)',
                    'groups_checked': list(series_groups.keys()),
                    'groups_updated': groups_to_update,
                    'consistency_results': group_consistency_results
                }
            }
        else:
            print("⚠️ CSV 로드 실패 - 전체 로드로 진행")
    
    # 전체 로드 (스마트 업데이트 실패하거나 비활성화된 경우)
    print("📊 전체 그룹 데이터 로드 진행")
    
    # 모든 시리즈를 하나의 딕셔너리로 통합
    all_series = {}
    for group_name, group_series in series_groups.items():
        for series_name, series_id in group_series.items():
            all_series[series_name] = series_id
    
    # 기존 load_economic_data 함수 호출
    result = load_economic_data(
        series_dict=all_series,
        data_source=data_source,
        csv_file_path=csv_file_path,
        start_date=start_date,
        smart_update=False,  # 이미 그룹별 체크를 했으므로 비활성화
        force_reload=force_reload,
        tolerance=tolerance
    )
    
    if result:
        result['load_info']['groups_checked'] = list(series_groups.keys())
        result['load_info']['groups_updated'] = list(series_groups.keys())
        result['load_info']['source'] = f'{data_source} API (전체 로드)'
    
    return result

# %%
# === 사용 예시 ===

print("✅ US Economic Data Utils 로드 완료!")
print("\n=== 주요 함수들 ===")
print("1. API 초기화:")
print("   - initialize_bls_api(api_key=None)")
print("   - initialize_fred_api(api_key=None)")
print()
print("2. 데이터 로드:")
print("   - get_bls_data(series_id, start_year, end_year)")
print("   - get_fred_data(series_id, start_date, end_date)")
print("   - load_economic_data(series_dict, data_source, csv_file_path, ...)")
print()
print("3. 데이터 저장/로드:")
print("   - save_data_to_csv(data_df, csv_file_path)")
print("   - load_data_from_csv(csv_file_path)")
print()
print("4. 데이터 계산:")
print("   - calculate_mom_percent/change(data)")
print("   - calculate_yoy_percent/change(data)")
print()
print("5. 시각화 (KPDS 포맷):")
print("   - create_timeseries_chart(data, series_names, chart_type, ...)")
print("   - create_comparison_chart(data, series_names, periods, ...)")
print("   - create_heatmap_chart(data, series_names, months, ...)")
print("   🔥 plot_economic_series(data_dict, series_list, chart_type, data_type, ...)")
print("      └─ 가장 강력한 범용 시각화 함수!")
print("      └─ 차트 타입: 'multi_line', 'single_line', 'dual_axis', 'horizontal_bar', 'vertical_bar'")
print("      └─ 데이터 타입: 'mom', 'raw', 'mom_change', 'yoy', 'yoy_change'")
print("      └─ 기간 설정, 특정 날짜 기준 시각화 지원")
print("   🔥 export_economic_data(data_dict, series_list, data_type, ...)")
print("      └─ 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("      └─ 한국어 컬럼명, 스타일링, 자동 경로 생성")
print()
print("6. 분석:")
print("   - analyze_latest_trends(data, series_names, korean_names)")
print()
print("7. 스마트 업데이트:")
print("   - check_recent_data_consistency(csv_data, api_data, tolerance)")
print()
print("🎯 사용 예시:")
print("   # 전체 데이터 시각화 (기본)")
print("   plot_economic_series(data_dict, ['series1', 'series2'], 'multi_line', 'mom')")
print("   plot_economic_series(data_dict, ['series1', 'series2'], 'dual_axis', 'yoy')")
print("   # 기간 제한 시각화")
print("   plot_economic_series(data_dict, ['series1'], 'single_line', 'raw', periods=24)")
print("   # 특정 날짜 기준")
print("   plot_economic_series(data_dict, ['series1'], 'single_line', 'mom', target_date='2024-06-01')")
print("   # 시계열 세로 바 차트 (기여도 분석에 최적)")
print("   plot_economic_series(data_dict, ['consumption', 'investment'], 'vertical_bar', 'mom')")
print("   # 데이터 export (엑셀)")
print("   export_economic_data(data_dict, ['series1', 'series2'], 'mom')")
print("   # 데이터 export (CSV, 최근 24개월)")
print("   export_economic_data(data_dict, ['series1'], 'raw', periods=24, file_format='csv')")
