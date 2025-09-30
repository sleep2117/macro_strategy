# %%
"""
BLS 고용 데이터 분석 (리팩토링 버전)
- us_eco_utils를 사용한 통합 구조
- 시리즈 정의와 분석 로직만 포함
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sys
import warnings
warnings.filterwarnings('ignore')

# 통합 유틸리티 함수 불러오기
from us_eco_utils import *

# %%
# === BLS API 키 설정 ===
api_config.BLS_API_KEY = '56b193612b614cdc9416359fd1c73a74'
api_config.BLS_API_KEY2 = '0450ef37363c48b5bedd2ae6fc92dd6e'
api_config.BLS_API_KEY3 = 'daf1ca7970b74e81b6a5c7a80a8b8a7f'

# %%
# === 고용 시리즈 정의 ===

EMPLOYMENT_SERIES = {
    # === ALL EMPLOYEES (천명 단위) ===
    # Core Employment Indicators
    'nonfarm_total': 'CES0000000001',  # Total nonfarm
    'private_total': 'CES0500000001',  # Total private
    'goods_producing': 'CES0600000001',  # Goods-producing
    'service_providing': 'CES0700000001',  # Service-providing
    'private_service': 'CES0800000001',  # Private service-providing
    'government': 'CES9000000001',  # Government
    
    # Goods-Producing Sectors
    'mining_logging': 'CES1000000001',  # Mining and logging
    'construction': 'CES2000000001',  # Construction
    'manufacturing': 'CES3000000001',  # Manufacturing
    'durable_goods': 'CES3100000001',  # Durable goods
    'nondurable_goods': 'CES3200000001',  # Nondurable goods
    
    # Service-Providing Sectors
    'trade_transport_utilities': 'CES4000000001',  # Trade, transportation, and utilities
    'wholesale_trade': 'CES4142000001',  # Wholesale trade
    'retail_trade': 'CES4200000001',  # Retail trade
    'transport_warehouse': 'CES4300000001',  # Transportation and warehousing
    'utilities': 'CES4422000001',  # Utilities
    'information': 'CES5000000001',  # Information
    'financial': 'CES5500000001',  # Financial activities
    'professional_business': 'CES6000000001',  # Professional and business services
    'education_health': 'CES6500000001',  # Private education and health services
    'leisure_hospitality': 'CES7000000001',  # Leisure and hospitality
    'other_services': 'CES8000000001',  # Other services
    
    # === AVERAGE WEEKLY HOURS ===
    'hours_private': 'CES0500000002',  # Total private
    'hours_goods': 'CES0600000002',  # Goods-producing
    'hours_private_service': 'CES0800000002',  # Private service-providing
    'hours_mining': 'CES1000000002',  # Mining and logging
    'hours_construction': 'CES2000000002',  # Construction
    'hours_manufacturing': 'CES3000000002',  # Manufacturing
    'hours_durable': 'CES3100000002',  # Durable goods
    'hours_nondurable': 'CES3200000002',  # Nondurable goods
    'hours_trade': 'CES4000000002',  # Trade, transportation, and utilities
    'hours_wholesale': 'CES4142000002',  # Wholesale trade
    'hours_retail': 'CES4200000002',  # Retail trade
    'hours_transport': 'CES4300000002',  # Transportation and warehousing
    'hours_utilities': 'CES4422000002',  # Utilities
    'hours_information': 'CES5000000002',  # Information
    'hours_financial': 'CES5500000002',  # Financial activities
    'hours_professional': 'CES6000000002',  # Professional and business services
    'hours_education_health': 'CES6500000002',  # Private education and health services
    'hours_leisure': 'CES7000000002',  # Leisure and hospitality
    'hours_other': 'CES8000000002',  # Other services
    
    # === AVERAGE HOURLY EARNINGS ===
    'earnings_private': 'CES0500000003',  # Total private
    'earnings_goods': 'CES0600000003',  # Goods-producing
    'earnings_private_service': 'CES0800000003',  # Private service-providing
    'earnings_mining': 'CES1000000003',  # Mining and logging
    'earnings_construction': 'CES2000000003',  # Construction
    'earnings_manufacturing': 'CES3000000003',  # Manufacturing
    'earnings_durable': 'CES3100000003',  # Durable goods
    'earnings_nondurable': 'CES3200000003',  # Nondurable goods
    'earnings_trade': 'CES4000000003',  # Trade, transportation, and utilities
    'earnings_wholesale': 'CES4142000003',  # Wholesale trade
    'earnings_retail': 'CES4200000003',  # Retail trade
    'earnings_transport': 'CES4300000003',  # Transportation and warehousing
    'earnings_utilities': 'CES4422000003',  # Utilities
    'earnings_information': 'CES5000000003',  # Information
    'earnings_financial': 'CES5500000003',  # Financial activities
    'earnings_professional': 'CES6000000003',  # Professional and business services
    'earnings_education_health': 'CES6500000003',  # Private education and health services
    'earnings_leisure': 'CES7000000003',  # Leisure and hospitality
    'earnings_other': 'CES8000000003',  # Other services
    
    # === AVERAGE WEEKLY EARNINGS ===
    'weekly_earnings_private': 'CES0500000011',  # Total private
    'weekly_earnings_goods': 'CES0600000011',  # Goods-producing
    'weekly_earnings_private_service': 'CES0800000011',  # Private service-providing
    'weekly_earnings_mining': 'CES1000000011',  # Mining and logging
    'weekly_earnings_construction': 'CES2000000011',  # Construction
    'weekly_earnings_manufacturing': 'CES3000000011',  # Manufacturing
    'weekly_earnings_durable': 'CES3100000011',  # Durable goods
    'weekly_earnings_nondurable': 'CES3200000011',  # Nondurable goods
    'weekly_earnings_trade': 'CES4000000011',  # Trade, transportation, and utilities
    'weekly_earnings_wholesale': 'CES4142000011',  # Wholesale trade
    'weekly_earnings_retail': 'CES4200000011',  # Retail trade
    'weekly_earnings_transport': 'CES4300000011',  # Transportation and warehousing
    'weekly_earnings_utilities': 'CES4422000011',  # Utilities
    'weekly_earnings_information': 'CES5000000011',  # Information
    'weekly_earnings_financial': 'CES5500000011',  # Financial activities
    'weekly_earnings_professional': 'CES6000000011',  # Professional and business services
    'weekly_earnings_education_health': 'CES6500000011',  # Private education and health services
    'weekly_earnings_leisure': 'CES7000000011',  # Leisure and hospitality
    'weekly_earnings_other': 'CES8000000011',  # Other services
}

# 한국어 이름 매핑
EMPLOYMENT_KOREAN_NAMES = {
    # 고용 지표
    'nonfarm_total': '전체 비농업 고용',
    'private_total': '민간부문 고용',
    'goods_producing': '재화생산 부문',
    'service_providing': '서비스제공 부문',
    'private_service': '민간 서비스 부문',
    'government': '정부부문',
    
    # 재화생산 섹터
    'mining_logging': '광업·벌목업',
    'construction': '건설업',
    'manufacturing': '제조업',
    'durable_goods': '내구재',
    'nondurable_goods': '비내구재',
    
    # 서비스 섹터
    'trade_transport_utilities': '무역·운송·공공서비스',
    'wholesale_trade': '도매업',
    'retail_trade': '소매업',
    'transport_warehouse': '운송·창고업',
    'utilities': '공공서비스',
    'information': '정보산업',
    'financial': '금융업',
    'professional_business': '전문·비즈니스 서비스',
    'education_health': '교육·의료 서비스',
    'leisure_hospitality': '레저·숙박업',
    'other_services': '기타 서비스',
    
    # 근로시간 지표
    'hours_private': '민간부문 주당 근로시간',
    'hours_goods': '재화생산 주당 근로시간',
    'hours_private_service': '민간서비스 주당 근로시간',
    'hours_mining': '광업 주당 근로시간',
    'hours_construction': '건설업 주당 근로시간',
    'hours_manufacturing': '제조업 주당 근로시간',
    'hours_durable': '내구재 주당 근로시간',
    'hours_nondurable': '비내구재 주당 근로시간',
    'hours_trade': '무역·운송 주당 근로시간',
    'hours_wholesale': '도매업 주당 근로시간',
    'hours_retail': '소매업 주당 근로시간',
    'hours_transport': '운송업 주당 근로시간',
    'hours_utilities': '공공서비스 주당 근로시간',
    'hours_information': '정보산업 주당 근로시간',
    'hours_financial': '금융업 주당 근로시간',
    'hours_professional': '전문서비스 주당 근로시간',
    'hours_education_health': '교육·의료 주당 근로시간',
    'hours_leisure': '레저·숙박 주당 근로시간',
    'hours_other': '기타서비스 주당 근로시간',
    
    # 시간당 임금 지표
    'earnings_private': '민간부문 시급',
    'earnings_goods': '재화생산 시급',
    'earnings_private_service': '민간서비스 시급',
    'earnings_mining': '광업 시급',
    'earnings_construction': '건설업 시급',
    'earnings_manufacturing': '제조업 시급',
    'earnings_durable': '내구재 시급',
    'earnings_nondurable': '비내구재 시급',
    'earnings_trade': '무역·운송 시급',
    'earnings_wholesale': '도매업 시급',
    'earnings_retail': '소매업 시급',
    'earnings_transport': '운송업 시급',
    'earnings_utilities': '공공서비스 시급',
    'earnings_information': '정보산업 시급',
    'earnings_financial': '금융업 시급',
    'earnings_professional': '전문서비스 시급',
    'earnings_education_health': '교육·의료 시급',
    'earnings_leisure': '레저·숙박 시급',
    'earnings_other': '기타서비스 시급',
    
    # 주당 임금 지표
    'weekly_earnings_private': '민간부문 주급',
    'weekly_earnings_goods': '재화생산 주급',
    'weekly_earnings_private_service': '민간서비스 주급',
    'weekly_earnings_mining': '광업 주급',
    'weekly_earnings_construction': '건설업 주급',
    'weekly_earnings_manufacturing': '제조업 주급',
    'weekly_earnings_durable': '내구재 주급',
    'weekly_earnings_nondurable': '비내구재 주급',
    'weekly_earnings_trade': '무역·운송 주급',
    'weekly_earnings_wholesale': '도매업 주급',
    'weekly_earnings_retail': '소매업 주급',
    'weekly_earnings_transport': '운송업 주급',
    'weekly_earnings_utilities': '공공서비스 주급',
    'weekly_earnings_information': '정보산업 주급',
    'weekly_earnings_financial': '금융업 주급',
    'weekly_earnings_professional': '전문서비스 주급',
    'weekly_earnings_education_health': '교육·의료 주급',
    'weekly_earnings_leisure': '레저·숙박 주급',
    'weekly_earnings_other': '기타서비스 주급'
}

# %%
# === 전역 변수 ===

# CSV 파일 경로
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/CES_employ_data_refactored.csv'

# 전역 데이터 저장소
CES_EMPLOY_DATA = {}

# %%
# === 데이터 로드 함수 ===

def load_ces_employ_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """
    CES 고용 데이터 로드 (통합 함수 사용)
    
    Args:
        start_date: 시작 날짜
        smart_update: 스마트 업데이트 사용 여부
        force_reload: 강제 재로드 여부
    
    Returns:
        bool: 로드 성공 여부
    """
    global CES_EMPLOY_DATA
    
    # 통합 함수 사용하여 데이터 로드
    result = load_economic_data(
        series_dict=EMPLOYMENT_SERIES,
        data_source='BLS',
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=1000.0  # 고용 데이터는 큰 수치이므로 1000천명 허용
    )
    
    if result:
        CES_EMPLOY_DATA = result
        print_load_info()
        return True
    else:
        print("❌ CES 고용 데이터 로드 실패")
        return False

def print_load_info():
    """로드 정보 출력"""
    if not CES_EMPLOY_DATA:
        print("❌ 데이터가 로드되지 않음")
        return
    
    info = CES_EMPLOY_DATA['load_info']
    print(f"📊 로드된 데이터 정보:")
    print(f"   시리즈 개수: {info['series_count']}")
    print(f"   데이터 포인트: {info['data_points']}")
    print(f"   시작 날짜: {info['start_date']}")
    print(f"   로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   데이터 소스: {info.get('source', 'API')}")
    
    if CES_EMPLOY_DATA['raw_data'] is not None and not CES_EMPLOY_DATA['raw_data'].empty:
        date_range = f"{CES_EMPLOY_DATA['raw_data'].index[0].strftime('%Y-%m')} ~ {CES_EMPLOY_DATA['raw_data'].index[-1].strftime('%Y-%m')}"
        print(f"   데이터 기간: {date_range}")

# %%
# === 시각화 함수들 ===

def create_ces_employ_timeseries(series_names=None, chart_type='level'):
    """
    고용 시계열 차트 생성
    
    Args:
        series_names: 표시할 시리즈 리스트
        chart_type: 'level', 'mom', 'yoy'
    
    Returns:
        plotly figure
    """
    if not CES_EMPLOY_DATA:
        print("⚠️ 먼저 load_ces_employ_data()를 실행하세요.")
        return None
    
    if series_names is None:
        series_names = ['nonfarm_total']
    
    # 차트 타입별 데이터 선택
    if chart_type == 'level':
        data = CES_EMPLOY_DATA['raw_data']
        ytitle = "천 명"
        title = f"미국 고용 현황 - {', '.join([EMPLOYMENT_KOREAN_NAMES.get(s, s) for s in series_names])}"
    elif chart_type == 'mom':
        data = CES_EMPLOY_DATA['mom_data']
        ytitle = "%"
        title = f"미국 고용 전월대비 변화율 - {', '.join([EMPLOYMENT_KOREAN_NAMES.get(s, s) for s in series_names])}"
    elif chart_type == 'yoy':
        data = CES_EMPLOY_DATA['yoy_data']
        ytitle = "%"
        title = f"미국 고용 전년동월대비 변화율 - {', '.join([EMPLOYMENT_KOREAN_NAMES.get(s, s) for s in series_names])}"
    else:
        print("❌ 지원하지 않는 차트 타입")
        return None
    
    # 통합 함수 사용
    return create_timeseries_chart(
        data=data,
        series_names=series_names,
        chart_type='level',  # 이미 변환된 데이터를 사용하므로 level로 설정
        ytitle=ytitle,
        korean_names=EMPLOYMENT_KOREAN_NAMES,
        title=title
    )

def create_ces_employ_comparison(series_names=None, periods=[1, 3, 6, 12]):
    """
    고용 비교 차트 생성
    
    Args:
        series_names: 비교할 시리즈 리스트
        periods: 비교할 기간들
    
    Returns:
        plotly figure
    """
    if not CES_EMPLOY_DATA:
        print("⚠️ 먼저 load_ces_employ_data()를 실행하세요.")
        return None
    
    if series_names is None:
        # 주요 섹터들만 선택
        series_names = ['construction', 'manufacturing', 'trade_transport_utilities', 
                       'information', 'financial', 'professional_business', 
                       'education_health', 'leisure_hospitality']
    
    latest_date = CES_EMPLOY_DATA['raw_data'].index[-1]
    title = f"미국 고용 섹터별 변화율 비교 ({latest_date.strftime('%Y년 %m월')})"
    
    # 통합 함수 사용
    return create_comparison_chart(
        data=CES_EMPLOY_DATA['raw_data'],
        series_names=series_names,
        periods=periods,
        korean_names=EMPLOYMENT_KOREAN_NAMES,
        title=title
    )

def create_ces_employ_heatmap(series_names=None, months=12):
    """
    고용 변화율 히트맵 생성
    
    Args:
        series_names: 표시할 시리즈 리스트
        months: 표시할 개월 수
    
    Returns:
        plotly figure
    """
    if not CES_EMPLOY_DATA:
        print("⚠️ 먼저 load_ces_employ_data()를 실행하세요.")
        return None
    
    if series_names is None:
        # 주요 섹터들만 선택
        series_names = ['construction', 'manufacturing', 'trade_transport_utilities', 
                       'information', 'financial', 'professional_business', 
                       'education_health', 'leisure_hospitality']
    
    title = "미국 고용 섹터별 월별 변화율 히트맵 (%)"
    
    # 통합 함수 사용
    return create_heatmap_chart(
        data=CES_EMPLOY_DATA['mom_data'],
        series_names=series_names,
        months=months,
        korean_names=EMPLOYMENT_KOREAN_NAMES,
        title=title
    )

# %%
# === 분석 함수 ===

def analyze_ces_employ_trends():
    """
    고용 트렌드 분석
    
    Returns:
        dict: 분석 결과
    """
    if not CES_EMPLOY_DATA:
        print("⚠️ 먼저 load_ces_employ_data()를 실행하세요.")
        return None
    
    # 전체 시리즈 분석
    all_series = list(EMPLOYMENT_SERIES.keys())
    
    # 통합 함수 사용
    return analyze_latest_trends(
        data=CES_EMPLOY_DATA['raw_data'],
        series_names=all_series,
        korean_names=EMPLOYMENT_KOREAN_NAMES
    )

# %%
# === 통합 실행 함수 ===

def run_ces_employ_analysis(start_date='2020-01-01', smart_update=True, force_reload=False):
    """
    완전한 고용 분석 실행
    
    Args:
        start_date: 데이터 시작 날짜
        smart_update: 스마트 업데이트 사용 여부
        force_reload: 강제 재로드 여부
    
    Returns:
        dict: 생성된 차트들과 분석 결과
    """
    print("🚀 미국 고용 데이터 분석 시작")
    print("="*50)
    
    # 1. 데이터 로드
    print("\n1️⃣ 데이터 로딩")
    success = load_ces_employ_data(start_date=start_date, smart_update=smart_update, force_reload=force_reload)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    # 2. 분석 실행
    print("\n2️⃣ 트렌드 분석")
    analysis_results = analyze_ces_employ_trends()
    
    # 3. 시각화 생성
    print("\n3️⃣ 시각화 생성")
    
    results = {
        'analysis': analysis_results,
        'charts': {}
    }
    
    try:
        # 전체 고용 시계열
        print("   📈 전체 고용 시계열...")
        results['charts']['total_timeseries'] = create_ces_employ_timeseries(['nonfarm_total'], 'level')
        
        # 전월대비 변화율 시계열
        print("   📊 전월대비 변화율...")
        results['charts']['mom_timeseries'] = create_ces_employ_timeseries(['nonfarm_total'], 'mom')
        
        # 섹터별 비교
        print("   🏢 섹터별 비교...")
        results['charts']['sector_comparison'] = create_ces_employ_comparison()
        
        # 히트맵
        print("   🗺️ 변화율 히트맵...")
        results['charts']['heatmap'] = create_ces_employ_heatmap()
        
        # 주요 섹터 시계열
        print("   📈 주요 섹터 시계열...")
        major_sectors = ['manufacturing', 'construction', 'professional_business']
        results['charts']['major_sectors'] = create_ces_employ_timeseries(major_sectors, 'mom')
        
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
# === 범용 시각화 함수 (plot_economic_series 사용) ===

def plot_ces_employ_series_advanced(series_list, chart_type='multi_line', data_type='mom',
                                    periods=None, target_date=None):
    """
    범용 고용 시각화 함수 - plot_economic_series를 사용한 고급 시각화
    
    Args:
        series_list: 시각화할 시리즈 리스트
        chart_type: 'multi_line', 'single_line', 'dual_axis', 'horizontal_bar'
        data_type: 'mom', 'raw', 'mom_change', 'yoy', 'yoy_change'
        periods: 표시할 기간 (개월, None이면 전체 데이터)
        target_date: 특정 날짜 기준 (예: '2025-06-01')
    
    Returns:
        plotly figure
    """
    if not CES_EMPLOY_DATA:
        print("⚠️ 먼저 load_ces_employ_data()를 실행하세요.")
        return None
    
    return plot_economic_series(
        data_dict=CES_EMPLOY_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=EMPLOYMENT_KOREAN_NAMES
    )

def export_ces_employ_data(series_list, data_type='mom', periods=None, target_date=None,
                          export_path=None, file_format='excel'):
    """
    고용 데이터를 엑셀/CSV로 export하는 함수
    
    Args:
        series_list: export할 시리즈 리스트
        data_type: 'mom', 'raw', 'mom_change', 'yoy', 'yoy_change'
        periods: 표시할 기간 (개월, None이면 전체 데이터)
        target_date: 특정 날짜 기준 (예: '2025-06-01')
        export_path: export할 파일 경로 (None이면 자동 생성)
        file_format: 파일 형식 ('excel', 'csv')
    
    Returns:
        str: export된 파일 경로 (성공시) 또는 None (실패시)
    """
    if not CES_EMPLOY_DATA:
        print("⚠️ 먼저 load_ces_employ_data()를 실행하세요.")
        return None
    
    return export_economic_data(
        data_dict=CES_EMPLOY_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=EMPLOYMENT_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 사용 예시 ===

print("\n=== 리팩토링된 CES 고용 데이터 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_ces_employ_data()  # 스마트 업데이트 활성화")
print("   load_ces_employ_data(smart_update=False)  # 전체 재로드")
print("   load_ces_employ_data(force_reload=True)  # 강제 재로드")
print()
print("2. 기본 시계열 차트:")
print("   create_ces_employ_timeseries(['nonfarm_total'], 'level')  # 고용 수준")
print("   create_ces_employ_timeseries(['nonfarm_total'], 'mom')    # 전월대비")
print("   create_ces_employ_timeseries(['nonfarm_total'], 'yoy')    # 전년동월대비")
print()
print("3. 🔥 범용 시각화 함수 (가장 강력한 함수!):")
print("   # 다중 라인 차트 (전체 데이터)")
print("   plot_ces_employ_series_advanced(['manufacturing', 'construction', 'financial'], 'multi_line', 'mom')")
print("   # 이중축 차트 (전체 데이터)")
print("   plot_ces_employ_series_advanced(['nonfarm_total', 'manufacturing'], 'dual_axis', 'raw')")
print("   # 가로 바 차트 (최신값 기준)")
print("   plot_ces_employ_series_advanced(['construction', 'manufacturing', 'financial'], 'horizontal_bar', 'mom')")
print("   # 최근 24개월만 보기")
print("   plot_ces_employ_series_advanced(['nonfarm_total'], 'single_line', 'mom', periods=24)")
print("   # 특정 날짜 기준 차트")
print("   plot_ces_employ_series_advanced(['nonfarm_total'], 'single_line', 'mom', target_date='2024-06-01')")
print("   # 임금 데이터 시각화")
print("   plot_ces_employ_series_advanced(['earnings_private', 'earnings_manufacturing'], 'multi_line', 'yoy')")
print("   # 근로시간 데이터 시각화")
print("   plot_ces_employ_series_advanced(['hours_private', 'hours_manufacturing'], 'multi_line', 'raw')")
print()
print("4. 비교 차트:")
print("   create_ces_employ_comparison()  # 섹터별 비교")
print()
print("5. 히트맵:")
print("   create_ces_employ_heatmap()  # 변화율 히트맵")
print()
print("6. 통합 분석:")
print("   run_ces_employ_analysis()  # 전체 분석 및 시각화")
print()
print("7. 트렌드 분석:")
print("   analyze_ces_employ_trends()  # 상세 트렌드 분석")
print()
print("8. 🔥 데이터 Export (새로운 기능!):")
print("   # 엑셀로 export (전체 데이터)")
print("   export_ces_employ_data(['nonfarm_total', 'manufacturing'], 'mom')")
print("   # CSV로 export (최근 24개월)")
print("   export_ces_employ_data(['nonfarm_total'], 'raw', periods=24, file_format='csv')")
print("   # 특정 날짜까지만")
print("   export_ces_employ_data(['manufacturing'], 'yoy', target_date='2024-06-01')")
print("   # 커스텀 경로 지정")
print("   export_ces_employ_data(['nonfarm_total'], 'mom', export_path='/home/jyp0615/my_data.xlsx')")
print("   # 임금 데이터 export")
print("   export_ces_employ_data(['earnings_private', 'earnings_manufacturing'], 'yoy')")
print("   # 근로시간 데이터 export")
print("   export_ces_employ_data(['hours_private', 'hours_manufacturing'], 'raw')")
print()
print("✅ plot_ces_employ_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화 가능!")
print("✅ export_ces_employ_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수를 사용합니다.")
print("✅ 고용, 임금, 근로시간 데이터를 통합 지원합니다!")

# %%
# 사용 가능한 시리즈 표시
def show_available_series():
    """사용 가능한 고용 시리즈 표시"""
    print("\n=== 사용 가능한 고용 시리즈 ===")
    
    for key, series_id in EMPLOYMENT_SERIES.items():
        korean_name = EMPLOYMENT_KOREAN_NAMES.get(key, key)
        print(f"  '{key}': {korean_name} ({series_id})")

show_available_series()

# %%
# 기본 실행 (스마트 업데이트 활성화)
# 주석 해제하여 자동 실행 가능
run_ces_employ_analysis(smart_update=True)

# %%
plot_ces_employ_series_advanced()