# %%
"""
ADP 고용 데이터 분석 (리팩토링 버전)
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
# === FRED API 키 설정 ===
api_config.FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'

# %%
# === ADP 시리즈 정의 ===

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

# CSV 파일 경로
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/adp_employ_data_refactored.csv'

# 전역 데이터 저장소
ADP_DATA = {}

# %%
# === 데이터 로드 함수 ===

def load_adp_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """
    ADP 데이터 로드 (통합 함수 사용)
    
    Args:
        start_date: 시작 날짜
        smart_update: 스마트 업데이트 사용 여부
        force_reload: 강제 재로드 여부
    
    Returns:
        bool: 로드 성공 여부
    """
    global ADP_DATA
    
    # 통합 함수 사용하여 데이터 로드
    result = load_economic_data(
        series_dict=ALL_ADP_SERIES,
        data_source='FRED',
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=1000.0  # ADP 데이터는 큰 수치이므로 1000천명 허용
    )
    
    if result:
        ADP_DATA = result
        print_load_info()
        return True
    else:
        print("❌ ADP 데이터 로드 실패")
        return False

def print_load_info():
    """로드 정보 출력"""
    if not ADP_DATA:
        print("❌ 데이터가 로드되지 않음")
        return
    
    info = ADP_DATA['load_info']
    print(f"📊 로드된 데이터 정보:")
    print(f"   시리즈 개수: {info['series_count']}")
    print(f"   데이터 포인트: {info['data_points']}")
    print(f"   시작 날짜: {info['start_date']}")
    print(f"   로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   데이터 소스: {info.get('source', 'API')}")
    
    if ADP_DATA['raw_data'] is not None and not ADP_DATA['raw_data'].empty:
        date_range = f"{ADP_DATA['raw_data'].index[0].strftime('%Y-%m')} ~ {ADP_DATA['raw_data'].index[-1].strftime('%Y-%m')}"
        print(f"   데이터 기간: {date_range}")

# %%
# === 시각화 함수들 ===

def create_adp_timeseries(series_names=None, chart_type='level'):
    """
    ADP 시계열 차트 생성
    
    Args:
        series_names: 표시할 시리즈 리스트
        chart_type: 'level', 'mom', 'yoy'
    
    Returns:
        plotly figure
    """
    if not ADP_DATA:
        print("⚠️ 먼저 load_adp_data()를 실행하세요.")
        return None
    
    if series_names is None:
        series_names = ['total']
    
    # 차트 타입별 데이터 선택
    if chart_type == 'level':
        data = ADP_DATA['raw_data']
        ytitle = "천 명"
        title = f"ADP 고용 현황 - {', '.join([ADP_KOREAN_NAMES.get(s, s) for s in series_names])}"
    elif chart_type == 'mom':
        data = ADP_DATA['mom_data']
        ytitle = "%"
        title = f"ADP 고용 전월대비 변화율 - {', '.join([ADP_KOREAN_NAMES.get(s, s) for s in series_names])}"
    elif chart_type == 'yoy':
        data = ADP_DATA['yoy_data']
        ytitle = "%"
        title = f"ADP 고용 전년동월대비 변화율 - {', '.join([ADP_KOREAN_NAMES.get(s, s) for s in series_names])}"
    else:
        print("❌ 지원하지 않는 차트 타입")
        return None
    
    # 통합 함수 사용
    return create_timeseries_chart(
        data=data,
        series_names=series_names,
        chart_type='level',  # 이미 변환된 데이터를 사용하므로 level로 설정
        ytitle=ytitle,
        korean_names=ADP_KOREAN_NAMES,
        title=title
    )

def create_adp_size_comparison(periods=[1, 3, 6, 12]):
    """
    ADP 사업 규모별 비교 차트 생성
    
    Args:
        periods: 비교할 기간들
    
    Returns:
        plotly figure
    """
    if not ADP_DATA:
        print("⚠️ 먼저 load_adp_data()를 실행하세요.")
        return None
    
    size_series = list(ADP_SIZE_SERIES.keys())
    latest_date = ADP_DATA['raw_data'].index[-1]
    title = f"ADP 고용 사업 규모별 변화율 비교 ({latest_date.strftime('%Y년 %m월')})"
    
    # 통합 함수 사용
    return create_comparison_chart(
        data=ADP_DATA['raw_data'],
        series_names=size_series,
        periods=periods,
        korean_names=ADP_KOREAN_NAMES,
        title=title
    )

def create_adp_industry_comparison(periods=[1, 3, 6, 12]):
    """
    ADP 산업별 비교 차트 생성
    
    Args:
        periods: 비교할 기간들
    
    Returns:
        plotly figure
    """
    if not ADP_DATA:
        print("⚠️ 먼저 load_adp_data()를 실행하세요.")
        return None
    
    industry_series = list(ADP_INDUSTRY_SERIES.keys())
    latest_date = ADP_DATA['raw_data'].index[-1]
    title = f"ADP 고용 산업별 변화율 비교 ({latest_date.strftime('%Y년 %m월')})"
    
    # 통합 함수 사용
    return create_comparison_chart(
        data=ADP_DATA['raw_data'],
        series_names=industry_series,
        periods=periods,
        korean_names=ADP_KOREAN_NAMES,
        title=title
    )

def create_adp_heatmap(category='all', months=12):
    """
    ADP 변화율 히트맵 생성
    
    Args:
        category: 'all', 'size', 'industry'
        months: 표시할 개월 수
    
    Returns:
        plotly figure
    """
    if not ADP_DATA:
        print("⚠️ 먼저 load_adp_data()를 실행하세요.")
        return None
    
    if category == 'size':
        series_names = list(ADP_SIZE_SERIES.keys())
        title = "ADP 고용 사업 규모별 월별 변화율 히트맵 (%)"
    elif category == 'industry':
        series_names = list(ADP_INDUSTRY_SERIES.keys())
        title = "ADP 고용 산업별 월별 변화율 히트맵 (%)"
    else:  # all
        series_names = list(ALL_ADP_SERIES.keys())
        title = "ADP 고용 전체 월별 변화율 히트맵 (%)"
    
    # 통합 함수 사용
    return create_heatmap_chart(
        data=ADP_DATA['mom_data'],
        series_names=series_names,
        months=months,
        korean_names=ADP_KOREAN_NAMES,
        title=title
    )

# %%
# === 분석 함수 ===

def analyze_adp_trends():
    """
    ADP 트렌드 분석
    
    Returns:
        dict: 분석 결과
    """
    if not ADP_DATA:
        print("⚠️ 먼저 load_adp_data()를 실행하세요.")
        return None
    
    # 전체 시리즈 분석
    all_series = list(ALL_ADP_SERIES.keys())
    
    # 통합 함수 사용
    result = analyze_latest_trends(
        data=ADP_DATA['raw_data'],
        series_names=all_series,
        korean_names=ADP_KOREAN_NAMES
    )
    
    # ADP 특화 분석 추가
    if result:
        print("\n📈 ADP 특화 분석:")
        
        # 사업 규모별 분석
        print("\n🏢 사업 규모별 고용 변화 (전월대비)")
        size_changes = []
        for size_key in ADP_SIZE_SERIES.keys():
            if size_key in result['series_analysis']:
                data = result['series_analysis'][size_key]
                size_changes.append((data['korean_name'], data['mom_change'], data['mom_percent']))
        
        size_changes.sort(key=lambda x: x[1], reverse=True)
        for name, change, percent in size_changes:
            print(f"   {name}: {change:+.0f}천명 ({percent:+.1f}%)")
        
        # 산업별 분석
        print("\n🏭 산업별 고용 변화 (전월대비)")
        industry_changes = []
        for ind_key in ADP_INDUSTRY_SERIES.keys():
            if ind_key in result['series_analysis']:
                data = result['series_analysis'][ind_key]
                industry_changes.append((data['korean_name'], data['mom_change'], data['mom_percent']))
        
        industry_changes.sort(key=lambda x: x[1], reverse=True)
        
        print("\n   상위 증가 산업:")
        for name, change, percent in industry_changes[:3]:
            print(f"   - {name}: {change:+.0f}천명 ({percent:+.1f}%)")
        
        print("\n   하위 증가/감소 산업:")
        for name, change, percent in industry_changes[-3:]:
            print(f"   - {name}: {change:+.0f}천명 ({percent:+.1f}%)")
    
    return result

# %%
# === 통합 실행 함수 ===

def run_adp_analysis(start_date='2020-01-01', smart_update=True, force_reload=False):
    """
    완전한 ADP 분석 실행
    
    Args:
        start_date: 데이터 시작 날짜
        smart_update: 스마트 업데이트 사용 여부
        force_reload: 강제 재로드 여부
    
    Returns:
        dict: 생성된 차트들과 분석 결과
    """
    print("🚀 ADP Employment 분석 시작")
    print("="*50)
    
    # 1. 데이터 로드
    print("\n1️⃣ 데이터 로딩")
    success = load_adp_data(start_date=start_date, smart_update=smart_update, force_reload=force_reload)
    
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
        results['charts']['total_timeseries'] = create_adp_timeseries(['total'], 'level')
        
        # 전월대비 변화율 시계열
        print("   📊 전월대비 변화율...")
        results['charts']['mom_timeseries'] = create_adp_timeseries(['total'], 'mom')
        
        # 사업 규모별 비교
        print("   🏢 사업 규모별 비교...")
        results['charts']['size_comparison'] = create_adp_size_comparison([1, 3, 6, 12])
        
        # 산업별 비교
        print("   🏭 산업별 비교...")
        results['charts']['industry_comparison'] = create_adp_industry_comparison([1, 3, 6, 12])
        
        # 히트맵
        print("   🗺️ 변화율 히트맵...")
        results['charts']['heatmap'] = create_adp_heatmap('all', 12)
        
        # 사업 규모별 시계열
        print("   📈 사업 규모별 시계열...")
        size_series = [k for k in ADP_SIZE_SERIES.keys() if k != 'total']
        results['charts']['size_timeseries'] = create_adp_timeseries(size_series[:3], 'mom')
        
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

def plot_adp_series_advanced(series_list, chart_type='multi_line', data_type='mom',
                            periods=None, target_date=None):
    """
    범용 ADP 시각화 함수 - plot_economic_series를 사용한 고급 시각화
    
    Args:
        series_list: 시각화할 시리즈 리스트
        chart_type: 'multi_line', 'single_line', 'dual_axis', 'horizontal_bar'
        data_type: 'mom', 'raw', 'mom_change', 'yoy', 'yoy_change'
        periods: 표시할 기간 (개월, None이면 전체 데이터)
        target_date: 특정 날짜 기준 (예: '2025-06-01')
    
    Returns:
        plotly figure
    """
    if not ADP_DATA:
        print("⚠️ 먼저 load_adp_data()를 실행하세요.")
        return None
    
    return plot_economic_series(
        data_dict=ADP_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=ADP_KOREAN_NAMES
    )

def export_adp_data(series_list, data_type='mom', periods=None, target_date=None,
                   export_path=None, file_format='excel'):
    """
    ADP 데이터를 엑셀/CSV로 export하는 함수
    
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
    if not ADP_DATA:
        print("⚠️ 먼저 load_adp_data()를 실행하세요.")
        return None
    
    return export_economic_data(
        data_dict=ADP_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=ADP_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 사용 예시 ===

print("\n=== 리팩토링된 ADP 고용 데이터 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_adp_data()  # 스마트 업데이트 활성화")
print("   load_adp_data(smart_update=False)  # 전체 재로드")
print()
print("2. 기본 시계열 차트:")
print("   create_adp_timeseries(['total'], 'level')  # 고용 수준")
print("   create_adp_timeseries(['total'], 'mom')    # 전월대비")
print("   create_adp_timeseries(['total'], 'yoy')    # 전년동월대비")
print()
print("3. 범용 시각화 함수 (🔥 가장 강력한 함수!):")
print("   # 사업 규모별 다중 라인 차트 (전체 데이터)")
print("   plot_adp_series_advanced(['size_1_19', 'size_20_49', 'size_50_249'], 'multi_line', 'mom')")
print("   # 산업별 이중축 차트 (전체 데이터)")
print("   plot_adp_series_advanced(['manufacturing', 'construction'], 'dual_axis', 'raw')")
print("   # 가로 바 차트 (최신값 기준)")
print("   plot_adp_series_advanced(['construction', 'manufacturing', 'financial'], 'horizontal_bar', 'mom')")
print("   # 최근 24개월만 보기")
print("   plot_adp_series_advanced(['total'], 'single_line', 'mom', periods=24)")
print("   # 특정 날짜 기준 차트")
print("   plot_adp_series_advanced(['total'], 'single_line', 'mom', target_date='2024-06-01')")
print("   # 전체 vs 규모별 비교 (전체 데이터)")
print("   plot_adp_series_advanced(['total', 'size_1_19', 'size_500_plus'], 'multi_line', 'yoy')")
print()
print("4. 비교 차트:")
print("   create_adp_size_comparison()     # 사업 규모별 비교")
print("   create_adp_industry_comparison() # 산업별 비교")
print()
print("5. 히트맵:")
print("   create_adp_heatmap('size')     # 사업 규모별 히트맵")
print("   create_adp_heatmap('industry') # 산업별 히트맵")
print("   create_adp_heatmap('all')      # 전체 히트맵")
print()
print("6. 통합 분석:")
print("   run_adp_analysis()  # 전체 분석 및 시각화")
print()
print("7. 트렌드 분석:")
print("   analyze_adp_trends()  # 상세 트렌드 분석")
print()
print("8. 데이터 Export (🔥 새로운 기능!):")
print("   # 엑셀로 export (전체 데이터)")
print("   export_adp_data(['total', 'size_1_19', 'manufacturing'], 'mom')")
print("   # CSV로 export (최근 24개월)")
print("   export_adp_data(['construction', 'financial'], 'raw', periods=24, file_format='csv')")
print("   # 특정 날짜까지만")
print("   export_adp_data(['total'], 'yoy', target_date='2024-06-01')")
print("   # 커스텀 경로 지정")
print("   export_adp_data(['total'], 'mom', export_path='/home/jyp0615/adp_data.xlsx')")
print()
print("✅ plot_adp_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화 가능!")
print("✅ export_adp_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수를 사용합니다.")

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
# 기본 실행 (스마트 업데이트 활성화)
if __name__ != "__main__":  # Jupyter 환경에서만 실행
    run_adp_analysis(smart_update=True)

# %%
run_adp_analysis()
# %%
# %%
plot_adp_series_advanced(['total', 'size_1_19'], 'multi_line', 'mom', target_date='2024-06-01')
# %%
plot_adp_series_advanced(['total', 'size_1_19'], 'horizontal_bar', 'mom_change', target_date='2024-06-01')

# %%
export_adp_data(['total', 'size_1_19'], 'mom', periods=24, export_path='/home/jyp0615/adp_data.xlsx')