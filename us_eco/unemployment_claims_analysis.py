"""
미국 실업급여 신청 건수 데이터 수집, 분석 및 시각화
====================================================

수집 시리즈:
- ICSA: Initial Claims (seasonally adjusted) - 초회신청 계절조정
- ICNSA: Initial Claims (not seasonally adjusted) - 초회신청 비계절조정
- CCSA: Continued Claims (seasonally adjusted) - 계속신청 계절조정  
- CCNSA: Continued Claims (not seasonally adjusted) - 계속신청 비계절조정

분석 방법:
- 계절조정 시리즈: 이코노미스트/투자은행 스타일 분석
- 비계절조정 시리즈: 5년 비교 차트 (create_five_year_comparison_chart)
"""

from typing import Any

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import requests
import warnings
warnings.filterwarnings('ignore')

# KPDS 시각화 포맷 import
import sys
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

from us_eco_utils import load_economic_data

# FRED API 키 설정
FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'  # FRED API Key
FRED_BASE_URL = 'https://api.stlouisfed.org/fred'

print("=" * 60)
print("미국 실업급여 신청 건수 데이터 분석 시작")
print("=" * 60)

# 통합 대시보드용 메타데이터
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/unemployment_claims_data.csv'

UNEMPLOYMENT_CLAIMS_SERIES = {
    'ICSA': 'ICSA',
    'ICNSA': 'ICNSA',
    'CCSA': 'CCSA',
    'CCNSA': 'CCNSA',
}

UNEMPLOYMENT_CLAIMS_KOREAN_NAMES = {
    'ICSA': '초회신청(SA)',
    'ICNSA': '초회신청(NSA)',
    'CCSA': '계속신청(SA)',
    'CCNSA': '계속신청(NSA)',
}

UNEMPLOYMENT_CLAIMS_DATA: dict[str, Any] = {
    'raw_data': pd.DataFrame(),
    'mom_data': pd.DataFrame(),
    'mom_change': pd.DataFrame(),
    'yoy_data': pd.DataFrame(),
    'yoy_change': pd.DataFrame(),
    'extra_data': pd.DataFrame(),
    'load_info': {
        'loaded': False,
        'load_time': None,
        'start_date': None,
        'series_count': 0,
        'data_points': 0,
        'source': 'cache',
    },
}

ALL_SERIES = UNEMPLOYMENT_CLAIMS_SERIES


def load_unemployment_claims_data(start_date='2000-01-01', smart_update=True, force_reload=False) -> bool:
    """통합 로더와 CSV 캐시를 활용한 실업급여 데이터 적재"""

    global UNEMPLOYMENT_CLAIMS_DATA

    result = load_economic_data(
        series_dict=UNEMPLOYMENT_CLAIMS_SERIES,
        data_source='FRED',
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
    )

    if result:
        UNEMPLOYMENT_CLAIMS_DATA = result
        return True

    return False

# =============================================================================
# 1. 데이터 수집 (FRED API 사용)
# =============================================================================

def fetch_fred_series(series_id, start_date, end_date):
    """FRED API에서 단일 시리즈 데이터 수집"""
    url = f"{FRED_BASE_URL}/series/observations"
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'observation_start': start_date,
        'observation_end': end_date
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if 'observations' not in data:
            print(f"❌ {series_id}: 데이터가 없습니다.")
            return None
        
        # 데이터 파싱
        observations = data['observations']
        dates = []
        values = []
        
        for obs in observations:
            if obs['value'] != '.':  # FRED에서 결측치는 '.'로 표시
                dates.append(pd.to_datetime(obs['date']))
                values.append(float(obs['value']))
        
        if len(dates) == 0:
            print(f"❌ {series_id}: 유효한 데이터가 없습니다.")
            return None
        
        series = pd.Series(values, index=dates, name=series_id)
        print(f"✅ {series_id}: {len(series)}개 데이터 포인트 수집 완료")
        return series
        
    except requests.exceptions.RequestException as e:
        print(f"❌ {series_id} API 요청 실패: {e}")
        return None
    except Exception as e:
        print(f"❌ {series_id} 데이터 처리 실패: {e}")
        return None

def collect_fred_data():
    """FRED API에서 실업급여 신청 건수 데이터 수집"""
    
    # 수집 기간 설정 (2000년부터 현재까지)
    start_date = "2000-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    # 시리즈 정보
    series_info = {
        'ICSA': 'Initial Claims (Seasonally Adjusted)',
        'ICNSA': 'Initial Claims (Not Seasonally Adjusted)', 
        'CCSA': 'Continued Claims (Seasonally Adjusted)',
        'CCNSA': 'Continued Claims (Not Seasonally Adjusted)'
    }
    
    print("🔄 FRED API에서 데이터 수집 중...")
    
    # 데이터 수집을 위한 딕셔너리
    all_data = {}
    
    try:
        # 각 시리즈별로 데이터 수집
        for series_key, series_description in series_info.items():
            print(f"📊 {series_key} ({series_description}) 데이터 수집...")
            
            series_data = fetch_fred_series(series_key, start_date, end_date)
            if series_data is not None:
                all_data[series_key] = series_data
            else:
                print(f"⚠️ {series_key} 데이터 수집 실패 - 건너뜁니다.")
        
        if len(all_data) == 0:
            print("❌ 수집된 데이터가 없습니다.")
            return None, None
        
        print(f"✅ {len(all_data)}개 시리즈 데이터 수집 완료")
        
        # 데이터프레임으로 결합
        df = pd.DataFrame(all_data)
        
        # 결측치 제거 및 정렬
        df = df.dropna().sort_index()
        
        print(f"📊 최종 데이터: {len(df)}개 관측치, 기간: {df.index[0].strftime('%Y-%m-%d')} ~ {df.index[-1].strftime('%Y-%m-%d')}")
        
        # 메타데이터 저장
        metadata = {
            'series': series_info,
            'source': 'Federal Reserve Economic Data (FRED)',
            'collection_date': datetime.now().isoformat(),
            'start_date': start_date,
            'end_date': end_date,
            'frequency': 'Weekly (Thursday)',
            'units': {
                'ICSA': 'Number of people',
                'ICNSA': 'Number of people', 
                'CCSA': 'Number of people',
                'CCNSA': 'Number of people'
            },
            'api_info': {
                'base_url': FRED_BASE_URL,
                'series_collected': list(all_data.keys()),
                'data_points_per_series': {k: len(v) for k, v in all_data.items()}
            }
        }
        
        return df, metadata
        
    except Exception as e:
        print(f"❌ 데이터 수집 중 오류 발생: {e}")
        return None, None

# =============================================================================
# 2. 데이터 저장 및 로드
# =============================================================================

def save_data(df, metadata, base_path="/home/jyp0615/us_eco/data"):
    """수집된 데이터를 CSV와 JSON으로 저장"""
    
    try:
        # CSV 저장
        csv_path = os.path.join(base_path, "unemployment_claims_data.csv")
        df.to_csv(csv_path)
        print(f"📁 데이터 저장 완료: {csv_path}")
        
        # 메타데이터 JSON 저장
        json_path = os.path.join(base_path, "unemployment_claims_data_meta.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        print(f"📁 메타데이터 저장 완료: {json_path}")
        
        return True
        
    except Exception as e:
        print(f"❌ 데이터 저장 중 오류: {e}")
        return False

def load_data(base_path="/home/jyp0615/us_eco/data"):
    """저장된 데이터를 로드"""
    
    try:
        # CSV 로드
        csv_path = os.path.join(base_path, "unemployment_claims_data.csv")
        df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        
        # 메타데이터 로드
        json_path = os.path.join(base_path, "unemployment_claims_data_meta.json")
        with open(json_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        print(f"📂 데이터 로드 완료: {len(df)}개 관측치")
        return df, metadata
        
    except Exception as e:
        print(f"❌ 데이터 로드 실패: {e}")
        return None, None

# =============================================================================
# 3. 5년 비교 데이터 포맷 생성 (비계절조정 시리즈용)
# =============================================================================

def create_five_year_format(df, column):
    """
    5년 비교 차트용 데이터 포맷 생성 (주간 기준)
    주간 데이터를 연도별로 주차별(1~52주)로 비교 가능하게 변환
    """
    
    print(f"🔄 {column} 5년 비교 데이터 포맷 생성 중 (주간 기준)...")
    
    try:
        # 원본 주간 데이터 사용
        series = df[column].copy()
        
        # 연도와 주차 추출
        series_with_period = series.copy()
        series_with_period.index = pd.to_datetime(series_with_period.index)
        
        # 최근 5년 데이터만 선택
        current_year = datetime.datetime.now().year
        start_year = current_year - 4
        mask = series_with_period.index.year >= start_year
        recent_data = series_with_period[mask]
        
        # 주차별로 pivot (1~52주)
        weekly_data = []
        for year in range(start_year, current_year + 1):
            year_data = recent_data[recent_data.index.year == year]
            year_series = pd.Series(index=range(1, 53), name=str(year))  # 1~52주
            
            # 각 주차별로 데이터 매핑
            for i, (date, value) in enumerate(year_data.items()):
                week_of_year = date.isocalendar()[1]  # ISO 주차 (1~53)
                if week_of_year <= 52:  # 52주까지만
                    year_series.loc[week_of_year] = value
            
            weekly_data.append(year_series)
        
        # 데이터프레임 생성
        five_year_df = pd.concat(weekly_data, axis=1)
        
        # 통계 계산 (5년 평균, 최소, 최대)
        five_year_df['5년평균'] = five_year_df.mean(axis=1, skipna=True)
        five_year_df['Min'] = five_year_df.iloc[:, :-1].min(axis=1, skipna=True)
        five_year_df['Min~Max'] = five_year_df.iloc[:, :-2].max(axis=1, skipna=True)
        
        print(f"✅ {column} 5년 비교 데이터 포맷 생성 완료 (주간 기준)")
        return five_year_df
        
    except Exception as e:
        print(f"❌ {column} 5년 비교 데이터 포맷 생성 실패: {e}")
        return None

# =============================================================================
# 4. 경제 분석 함수들
# =============================================================================

def calculate_economic_indicators(df):
    """주요 경제 지표 계산"""
    
    print("📈 경제 지표 계산 중...")
    
    indicators = {}
    
    try:
        # 최신 데이터 포인트
        latest_date = df.index[-1]
        latest_data = df.iloc[-1]
        
        # 4주 이동평균 (노이즈 제거)
        ma4_data = df.rolling(window=4).mean().iloc[-1]
        
        # 전주 대비 변화
        wow_change = df.iloc[-1] - df.iloc[-2]
        wow_pct = (wow_change / df.iloc[-2] * 100)
        
        # 전년 동기 대비 (52주 전)
        if len(df) >= 52:
            yoy_change = df.iloc[-1] - df.iloc[-53]
            yoy_pct = (yoy_change / df.iloc[-53] * 100)
        else:
            yoy_change = pd.Series([np.nan] * len(df.columns), index=df.columns)
            yoy_pct = pd.Series([np.nan] * len(df.columns), index=df.columns)
        
        # 코로나19 이전 평균 대비 (2019년 평균)
        pre_covid_avg = df[(df.index.year == 2019)].mean()
        vs_precovid = latest_data - pre_covid_avg
        vs_precovid_pct = (vs_precovid / pre_covid_avg * 100)
        
        indicators = {
            'latest_date': latest_date,
            'latest_values': latest_data,
            'ma4_values': ma4_data,
            'wow_change': wow_change,
            'wow_pct': wow_pct,
            'yoy_change': yoy_change, 
            'yoy_pct': yoy_pct,
            'vs_precovid': vs_precovid,
            'vs_precovid_pct': vs_precovid_pct
        }
        
        print("✅ 경제 지표 계산 완료")
        return indicators
        
    except Exception as e:
        print(f"❌ 경제 지표 계산 실패: {e}")
        return None

def analyze_trends(df):
    """트렌드 분석"""
    
    print("📊 트렌드 분석 중...")
    
    trends = {}
    
    try:
        for column in df.columns:
            series = df[column].dropna()
            
            # 최근 3개월 트렌드 (12주)
            recent_12w = series.tail(12)
            if len(recent_12w) >= 2:
                # 선형 회귀로 트렌드 계산
                x = np.arange(len(recent_12w))
                coeffs = np.polyfit(x, recent_12w.values, 1)
                trend_slope = coeffs[0]
                
                if trend_slope > 500:
                    trend_direction = "강한 상승"
                elif trend_slope > 100:
                    trend_direction = "상승"
                elif trend_slope > -100:
                    trend_direction = "보합"
                elif trend_slope > -500:
                    trend_direction = "하락"
                else:
                    trend_direction = "강한 하락"
            else:
                trend_slope = 0
                trend_direction = "분석불가"
            
            # 변동성 계산 (최근 12주 표준편차)
            volatility = recent_12w.std() if len(recent_12w) > 1 else 0
            
            trends[column] = {
                'trend_slope': trend_slope,
                'trend_direction': trend_direction,
                'volatility': volatility,
                'recent_12w_avg': recent_12w.mean(),
                'recent_12w_min': recent_12w.min(),
                'recent_12w_max': recent_12w.max()
            }
        
        print("✅ 트렌드 분석 완료")
        return trends
        
    except Exception as e:
        print(f"❌ 트렌드 분석 실패: {e}")
        return None

# =============================================================================
# 5. 시각화 함수들
# =============================================================================

def create_sa_analysis_charts(df):
    """계절조정 시리즈 분석 차트 생성 (이코노미스트/투자은행 스타일)"""
    
    print("📊 계절조정 시리즈 차트 생성 중...")
    
    try:
        # 1. 초회신청과 계속신청 이중축 차트
        print("1️⃣ 초회신청 vs 계속신청 이중축 차트")
        print("미국 실업급여 신청 건수 (계절조정)")
        df_dual_axis_chart(
            df, 
            left_cols=['ICSA'], 
            right_cols=['CCSA'],
            left_labels=['초회신청(SA)'], 
            right_labels=['계속신청(SA)'],
            left_title="명", 
            right_title="명",
            xtitle=None
        )
        
        # 2. 4주 이동평균 비교 (노이즈 제거) - 이중축
        print("2️⃣ 4주 이동평균 트렌드 차트 (이중축)")
        print("실업급여 신청 건수 4주 이동평균 (노이즈 제거)")
        df_ma4 = df[['ICSA', 'CCSA']].rolling(window=4).mean()
        df_dual_axis_chart(
            df_ma4,
            left_cols=['ICSA'],
            right_cols=['CCSA'],
            left_labels=['초회신청 4주MA'],
            right_labels=['계속신청 4주MA'],
            left_title="명",
            right_title="명"
        )
        
        # 3. 전년동기대비 변화율
        print("3️⃣ 전년동기대비 변화율 차트")
        print("실업급여 신청 건수 전년동기대비 변화율")
        df_yoy = df[['ICSA', 'CCSA']].pct_change(periods=52) * 100
        df_multi_line_chart(
            df_yoy,
            columns=['ICSA', 'CCSA'], 
            labels={'ICSA': '초회신청 YoY', 'CCSA': '계속신청 YoY'},
            ytitle="%"
        )
        
        # 4. 최근 2년 상세 트렌드
        print("4️⃣ 최근 2년 상세 트렌드")
        print("최근 2년 실업급여 신청 건수 상세 트렌드")
        recent_2y = df[df.index >= (datetime.datetime.now() - timedelta(days=730))]
        df_dual_axis_chart(
            recent_2y,
            left_cols=['ICSA'],
            right_cols=['CCSA'], 
            left_labels=['초회신청'],
            right_labels=['계속신청'],
            left_title="명",
            right_title="명"
        )
        
        print("✅ 계절조정 시리즈 차트 생성 완료")
        
    except Exception as e:
        print(f"❌ 계절조정 시리즈 차트 생성 실패: {e}")

def create_nsa_comparison_charts(df):
    """비계절조정 시리즈 5년 비교 차트 생성"""
    
    print("📊 비계절조정 시리즈 5년 비교 차트 생성 중...")
    
    try:
        # ICNSA 5년 비교
        print("1️⃣ 초회신청(비계절조정) 5년 비교")
        print("초회신청 실업급여 5년 비교 (비계절조정)")
        icnsa_five_year = create_five_year_format(df, 'ICNSA')
        if icnsa_five_year is not None:
            create_five_year_comparison_chart(
                icnsa_five_year,
                y_title="명",
                x_axis_type='week',
                recent_years=3
            )
        
        # CCNSA 5년 비교  
        print("2️⃣ 계속신청(비계절조정) 5년 비교")
        print("계속신청 실업급여 5년 비교 (비계절조정)")
        ccnsa_five_year = create_five_year_format(df, 'CCNSA')
        if ccnsa_five_year is not None:
            create_five_year_comparison_chart(
                ccnsa_five_year,
                y_title="명", 
                x_axis_type='week',
                recent_years=3
            )
        
        print("✅ 비계절조정 시리즈 5년 비교 차트 생성 완료")
        
    except Exception as e:
        print(f"❌ 비계절조정 시리즈 차트 생성 실패: {e}")

def create_comparative_analysis_charts(df):
    """비교 분석 차트 생성"""
    
    print("📊 비교 분석 차트 생성 중...")
    
    try:
        # 1. 계절조정 vs 비계절조정 비교 (초회신청)
        print("1️⃣ 초회신청: 계절조정 vs 비계절조정")
        print("초회신청 실업급여: 계절조정 vs 비계절조정")
        df_multi_line_chart(
            df,
            columns=['ICSA', 'ICNSA'],
            labels={'ICSA': '계절조정', 'ICNSA': '비계절조정'},
            ytitle="명"
        )
        
        # 2. 계절조정 vs 비계절조정 비교 (계속신청)
        print("2️⃣ 계속신청: 계절조정 vs 비계절조정")
        print("계속신청 실업급여: 계절조정 vs 비계절조정")
        df_multi_line_chart(
            df,
            columns=['CCSA', 'CCNSA'], 
            labels={'CCSA': '계절조정', 'CCNSA': '비계절조정'},
            ytitle="명"
        )
        
        # 3. 계절성 분석 (차이 시각화)
        print("3️⃣ 계절성 분석")
        print("실업급여 신청의 계절성 (NSA - SA)")
        df_seasonal = pd.DataFrame()
        df_seasonal['초회신청_계절성'] = df['ICNSA'] - df['ICSA']
        df_seasonal['계속신청_계절성'] = df['CCNSA'] - df['CCSA']
        
        df_multi_line_chart(
            df_seasonal,
            columns=['초회신청_계절성', '계속신청_계절성'],
            ytitle="명"
        )
        
        print("✅ 비교 분석 차트 생성 완료")
        
    except Exception as e:
        print(f"❌ 비교 분석 차트 생성 실패: {e}")

# =============================================================================
# 6. 보고서 생성
# =============================================================================

def generate_analysis_report(df, indicators, trends):
    """분석 보고서 생성"""
    
    print("\n" + "="*60)
    print("📋 미국 실업급여 신청 건수 분석 보고서")
    print("="*60)
    
    if indicators is None or trends is None:
        print("❌ 분석 데이터가 없어 보고서를 생성할 수 없습니다.")
        return
    
    # 1. 최신 현황
    print(f"\n📊 최신 현황 ({indicators['latest_date'].strftime('%Y-%m-%d')})")
    print("-" * 40)
    for series in ['ICSA', 'ICNSA', 'CCSA', 'CCNSA']:
        latest_val = indicators['latest_values'][series]
        wow_change = indicators['wow_change'][series]
        wow_pct = indicators['wow_pct'][series]
        
        series_name = {
            'ICSA': '초회신청(SA)', 
            'ICNSA': '초회신청(NSA)',
            'CCSA': '계속신청(SA)',
            'CCNSA': '계속신청(NSA)'
        }[series]
        
        print(f"{series_name:12}: {latest_val:>8,.0f}명 "
              f"(전주대비 {wow_change:+6,.0f}명, {wow_pct:+5.1f}%)")
    
    # 2. 트렌드 분석
    print(f"\n📈 트렌드 분석 (최근 12주)")
    print("-" * 40)
    for series in ['ICSA', 'ICNSA', 'CCSA', 'CCNSA']:
        trend_info = trends[series]
        series_name = {
            'ICSA': '초회신청(SA)', 
            'ICNSA': '초회신청(NSA)',
            'CCSA': '계속신청(SA)',
            'CCNSA': '계속신청(NSA)'
        }[series]
        
        print(f"{series_name:12}: {trend_info['trend_direction']:8} "
              f"(기울기: {trend_info['trend_slope']:+6.0f})")
    
    # 3. 주요 포인트
    print(f"\n🎯 주요 포인트")
    print("-" * 40)
    
    # 초회신청 vs 계속신청 비교
    icsa_latest = indicators['latest_values']['ICSA']
    ccsa_latest = indicators['latest_values']['CCSA']
    claims_ratio = ccsa_latest / icsa_latest
    
    print(f"• 계속신청/초회신청 비율: {claims_ratio:.1f}배")
    print(f"• 계속신청 수준: {'높음' if claims_ratio > 7 else '보통' if claims_ratio > 5 else '낮음'}")
    
    # 전년 동기 대비
    if not np.isnan(indicators['yoy_pct']['ICSA']):
        yoy_icsa = indicators['yoy_pct']['ICSA']
        yoy_ccsa = indicators['yoy_pct']['CCSA']
        print(f"• 초회신청 전년대비: {yoy_icsa:+.1f}%")
        print(f"• 계속신청 전년대비: {yoy_ccsa:+.1f}%")
    
    # 계절성 분석
    icsa_vs_icnsa = abs(indicators['latest_values']['ICNSA'] - indicators['latest_values']['ICSA'])
    seasonality_impact = icsa_vs_icnsa / indicators['latest_values']['ICSA'] * 100
    print(f"• 현재 계절성 영향: {seasonality_impact:.1f}%")
    
    # 4. 경제적 해석
    print(f"\n💡 경제적 해석")
    print("-" * 40)
    
    # 초회신청 수준 평가
    if icsa_latest < 300000:
        claims_assessment = "양호 - 노동시장이 건강한 상태"
    elif icsa_latest < 400000:
        claims_assessment = "보통 - 노동시장이 안정적"
    elif icsa_latest < 500000:
        claims_assessment = "주의 - 노동시장에 압력 증가"
    else:
        claims_assessment = "우려 - 노동시장 악화 신호"
    
    print(f"• 초회신청 수준 평가: {claims_assessment}")
    
    # 트렌드 종합 평가
    icsa_trend = trends['ICSA']['trend_direction']
    ccsa_trend = trends['CCSA']['trend_direction']
    
    if '하락' in icsa_trend and '하락' in ccsa_trend:
        trend_assessment = "긍정적 - 실업급여 신청이 감소 추세"
    elif '상승' in icsa_trend or '상승' in ccsa_trend:
        trend_assessment = "부정적 - 실업급여 신청이 증가 추세"
    else:
        trend_assessment = "중립적 - 실업급여 신청이 보합 상태"
    
    print(f"• 트렌드 종합 평가: {trend_assessment}")
    
    print(f"\n📅 보고서 생성 시간: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

# =============================================================================
# 7. 메인 실행 함수
# =============================================================================

def main():
    """메인 분석 실행"""
    
    # 데이터 수집 또는 로드
    df, metadata = load_data()
    
    if df is None:
        print("💾 기존 데이터가 없어 새로 수집합니다...")
        df, metadata = collect_fred_data()
        
        if df is not None:
            save_data(df, metadata)
        else:
            print("❌ 데이터 수집 실패로 분석을 중단합니다.")
            return
    
    print(f"\n📊 데이터 확인")
    print(f"기간: {df.index[0].strftime('%Y-%m-%d')} ~ {df.index[-1].strftime('%Y-%m-%d')}")
    print(f"관측치: {len(df)}개")
    print(f"시리즈: {', '.join(df.columns)}")
    
    # 경제 지표 계산
    indicators = calculate_economic_indicators(df)
    trends = analyze_trends(df)
    
    # 분석 보고서 생성
    generate_analysis_report(df, indicators, trends)
    
    # 시각화
    print(f"\n🎨 시각화 생성 중...")
    
    # 1. 계절조정 시리즈 분석
    create_sa_analysis_charts(df)
    
    # 2. 비계절조정 시리즈 5년 비교
    create_nsa_comparison_charts(df)
    
    # 3. 비교 분석
    create_comparative_analysis_charts(df)
    
    print(f"\n✅ 모든 분석 완료!")
    print("="*60)

# 실행
if __name__ == "__main__":
    main()
