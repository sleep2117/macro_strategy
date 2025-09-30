# 🔧 US Economic Data 신규 파일 생성 프롬프트 
(시리즈 정보 파일 기반 자동 코드 생성)

이 프롬프트는 시리즈 정보 파일(예: fhfa.md)을 분석해서
리팩토링된 구조로 새로운 분석 파일을 자동 생성하는 가이드입니다.

### 📋 입력 요구사항:
1. **시리즈 정보 파일**: `/home/jyp0615/us_eco/structure/[데이터명].md`
2. **데이터 소스**: FRED, BLS, DBnomics 중 하나 명시
3. **지표 특성**: 단위, 변화율 계산 방식, 허용 오차 등

---

## 🔍 1단계: 시리즈 정보 파일 분석

### 필수 정보 추출:
```markdown
- **발행기관**: [기관명]
- **지표명**: [지표명]  
- **빈도**: [Monthly/Quarterly/Annual]
- **단위**: [단위 정보]
- **데이터 시작**: [YYYY년 MM월부터]
- **데이터 소스**: [FRED/BLS/DBnomics]
```

### 시리즈 테이블 분석:
```markdown
|지역/카테고리|시리즈명|시리즈 ID|추가 정보|
|---|---|---|---|
|[분류1]|[시리즈명1]|[ID1]|[SA/NSA/기타]|
|[분류2]|[시리즈명2]|[ID2]|[SA/NSA/기타]|
```

---

## 🏗️ 2단계: 파일 구조 생성

### A. 헤더 섹션 (필수):
```python
# %%
"""
[지표명] 데이터 분석 (리팩토링 버전)
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
```

### B. API 키 설정 (데이터 소스별):
```python
# %%
# === [데이터 소스] API 키 설정 ===

# BLS 데이터인 경우:
api_config.BLS_API_KEY = '...'
api_config.BLS_API_KEY2 = '...'
api_config.BLS_API_KEY3 = '...'

# FRED 데이터인 경우:
# api_config.FRED_API_KEY = '...'

# DBnomics 데이터인 경우:
# import dbnomics (별도 API 키 불필요)
```

---

## 📊 3단계: 시리즈 정의 생성 규칙

### A. 시리즈 딕셔너리 생성:
**🚨 중요: 올바른 구조 사용 필수!**

```python
# %%
# === [지표명] 시리즈 정의 ===

# 메인 시리즈 딕셔너리 (올바른 구조: 시리즈_이름: API_ID)
[데이터명]_SERIES = {
    '[시리즈_이름1]': '[API_ID1]',     # [한글명1]
    '[시리즈_이름2]': '[API_ID2]',     # [한글명2]
    '[시리즈_이름3]': '[API_ID3]',     # [한글명3]
    # ... 모든 시리즈
}

# DBnomics인 경우 특별 구조:
[데이터명]_SERIES = {
    '[시리즈_이름1]': ('Provider', 'dataset', 'series'),
    '[시리즈_이름2]': ('Provider', 'dataset', 'series'),
}
```

### B. 시리즈 이름 생성 규칙:
```python
# 생성 규칙:
# 1. 소문자 + 언더스코어 사용
# 2. 의미 있는 약어 사용
# 3. SA/NSA는 접미사로 구분

# 예시:
national_sa     # National Seasonally Adjusted
national_nsa    # National Not Seasonally Adjusted
northeast_sa    # Northeast Seasonally Adjusted
new_england_sa  # New England Seasonally Adjusted
middle_atl_sa   # Middle Atlantic Seasonally Adjusted
```

### C. 한국어 이름 매핑:
```python
# 한국어 이름 매핑 (기존 것 그대로 유지)
[데이터명]_KOREAN_NAMES = {
    '[시리즈_이름1]': '[완전한 한국어명1]',
    '[시리즈_이름2]': '[완전한 한국어명2]',
    '[시리즈_이름3]': '[완전한 한국어명3]',
    # ... 모든 시리즈
}

# 한국어명 생성 규칙:
# 1. 지역명 + 지표명 + (계절조정 여부)
# 2. 예: "전국 주택가격지수 (계절조정)", "뉴잉글랜드 주택가격지수 (원계열)"
```

### D. 카테고리 분류:
```python
# 카테고리 분류 (기존 것 그대로 유지)
[데이터명]_CATEGORIES = {
    '주요 지표': {
        'National': ['national_sa', 'national_nsa'],
        'Regional Summary': ['northeast_sa', 'south_sa', 'midwest_sa', 'west_sa']
    },
    '세부 지역': {
        'Northeast': ['new_england_sa', 'middle_atl_sa'],
        'South': ['south_atl_sa', 'east_south_central_sa', 'west_south_central_sa'],
        'Midwest': ['east_north_central_sa', 'west_north_central_sa'],
        'West': ['mountain_sa', 'pacific_sa']
    },
    '계절조정별': {
        'Seasonally Adjusted': ['[모든 _sa 시리즈들]'],
        'Not Seasonally Adjusted': ['[모든 _nsa 시리즈들]']
    }
}
```

---

## 🛠️ 4단계: 핵심 함수 생성

### A. 전역 변수 설정:
```python
# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/[파일명]_data_refactored.csv'
[데이터명]_DATA = {}
```

### B. 데이터 로드 함수:
```python
# %%
# === 데이터 로드 함수 ===
def load_[데이터명]_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 [데이터명] 데이터 로드"""
    global [데이터명]_DATA

    # 데이터 소스별 분기
    data_source = '[FRED/BLS/DBnomics]'  # 실제 소스로 교체
    tolerance = [10.0/1000.0]  # 지표별 적절한 허용 오차
    
    # DBnomics가 아닌 경우 us_eco_utils 사용
    if data_source != 'DBnomics':
        result = load_economic_data(
            series_dict=[데이터명]_SERIES,
            data_source=data_source,
            csv_file_path=CSV_FILE_PATH,
            start_date=start_date,
            smart_update=smart_update,
            force_reload=force_reload,
            tolerance=tolerance
        )

        if result:
            [데이터명]_DATA = result
            print_load_info()
            return True
        else:
            print("❌ [데이터명] 데이터 로드 실패")
            return False
    
    # DBnomics인 경우 별도 구현
    else:
        # [DBnomics 전용 로드 로직 구현]
        pass

def print_load_info():
    """[데이터명] 데이터 로드 정보 출력"""
    if not [데이터명]_DATA or 'load_info' not in [데이터명]_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = [데이터명]_DATA['load_info']
    print(f"\n📊 [데이터명] 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in [데이터명]_DATA and not [데이터명]_DATA['raw_data'].empty:
        latest_date = [데이터명]_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")
```

### C. 범용 시각화 함수:
```python
# %%
# === 범용 시각화 함수 ===
def plot_[데이터명]_series_advanced(series_list, chart_type='multi_line', 
                                    data_type='raw', periods=None, target_date=None,
                                    left_ytitle=None, right_ytitle=None):
    """범용 [데이터명] 시각화 함수 - plot_economic_series 활용"""
    if not [데이터명]_DATA:
        print("⚠️ 먼저 load_[데이터명]_data()를 실행하세요.")
        return None

    # 단위별 기본 Y축 제목 설정
    if data_type == 'mom':
        default_ytitle = "%"
    elif data_type == 'yoy':
        default_ytitle = "%"  
    else:  # raw
        default_ytitle = "[단위]"  # 실제 단위로 교체 (예: "지수", "천 명", "%" 등)

    return plot_economic_series(
        data_dict=[데이터명]_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        left_ytitle=left_ytitle or default_ytitle,
        right_ytitle=right_ytitle or default_ytitle,
        korean_names=[데이터명]_KOREAN_NAMES
    )
```

### D. 데이터 Export 함수:
```python
# %%
# === 데이터 Export 함수 ===
def export_[데이터명]_data(series_list, data_type='raw', periods=None, 
                           target_date=None, export_path=None, file_format='excel'):
    """[데이터명] 데이터 export 함수 - export_economic_data 활용"""
    if not [데이터명]_DATA:
        print("⚠️ 먼저 load_[데이터명]_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=[데이터명]_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=[데이터명]_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )
```

---

## 🔧 5단계: 데이터 접근 및 유틸리티 함수

### A. 데이터 접근 함수들 (표준):
```python
# %%
# === 데이터 접근 함수들 ===

def clear_[데이터명]_data():
    """[데이터명] 데이터 초기화"""
    global [데이터명]_DATA
    [데이터명]_DATA = {}
    print("🗑️ [데이터명] 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not [데이터명]_DATA or 'raw_data' not in [데이터명]_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_[데이터명]_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return [데이터명]_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in [데이터명]_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return [데이터명]_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not [데이터명]_DATA or 'mom_data' not in [데이터명]_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_[데이터명]_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return [데이터명]_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in [데이터명]_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return [데이터명]_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not [데이터명]_DATA or 'yoy_data' not in [데이터명]_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_[데이터명]_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return [데이터명]_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in [데이터명]_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return [데이터명]_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not [데이터명]_DATA or 'raw_data' not in [데이터명]_DATA:
        return []
    return list([데이터명]_DATA['raw_data'].columns)
```

### B. 유틸리티 함수들 (표준):
```python
# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 [데이터명] 시리즈 표시"""
    print("=== 사용 가능한 [데이터명] 시리즈 ===")
    
    for series_name, series_id in [데이터명]_SERIES.items():
        korean_name = [데이터명]_KOREAN_NAMES.get(series_name, series_name)
        print(f"  '{series_name}': {korean_name} ({series_id})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in [데이터명]_CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            for series_name in series_list:
                korean_name = [데이터명]_KOREAN_NAMES.get(series_name, series_name)
                api_id = [데이터명]_SERIES.get(series_name, series_name)
                print(f"    - {series_name}: {korean_name} ({api_id})")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not [데이터명]_DATA or 'load_info' not in [데이터명]_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': [데이터명]_DATA['load_info']['loaded'],
        'series_count': [데이터명]_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': [데이터명]_DATA['load_info']
    }
```

---

## 📋 6단계: 사용 예시 및 마무리

### A. 사용 예시 (필수):
```python
# %%
# === 사용 예시 ===

print("=== 리팩토링된 [데이터명] 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_[데이터명]_data()  # 스마트 업데이트")
print("   load_[데이터명]_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_[데이터명]_series_advanced(['series1', 'series2'], 'multi_line', 'raw')")
print("   plot_[데이터명]_series_advanced(['series1'], 'horizontal_bar', 'yoy', left_ytitle='%')")
print("   plot_[데이터명]_series_advanced(['series1'], 'single_line', 'mom', periods=24, left_ytitle='%')")
print("   plot_[데이터명]_series_advanced(['series1', 'series2'], 'dual_axis', 'raw', left_ytitle='[단위1]', right_ytitle='[단위2]')")
print()
print("3. 🔥 데이터 Export:")
print("   export_[데이터명]_data(['series1', 'series2'], 'raw')")
print("   export_[데이터명]_data(['series1'], 'mom', periods=24, file_format='csv')")
print("   export_[데이터명]_data(['series1'], 'yoy', target_date='2024-06-01')")
print()
print("✅ plot_[데이터명]_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_[데이터명]_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
```

---

## 🚨 중요 체크리스트

### ✅ 필수 확인사항:

1. **시리즈 딕셔너리 구조 정확성**:
   - ✅ `{'시리즈_이름': 'API_ID'}` 구조 사용
   - ❌ `{'API_ID': '시리즈_설명'}` 구조 사용 금지

2. **데이터 소스별 적응**:
   - ✅ FRED: `load_economic_data()` 사용
   - ✅ BLS: `load_economic_data()` 사용  
   - ✅ DBnomics: 별도 구현 필요

3. **Y축 제목 파라미터**:
   - ✅ `left_ytitle=None, right_ytitle=None` 필수
   - ✅ 단위별 적절한 기본값 설정

4. **허용 오차 설정**:
   - ✅ 고용 데이터: `1000.0`
   - ✅ 일반 지표: `10.0`  
   - ✅ 가격 지수: `10.0`

5. **한국어 매핑 완성도**:
   - ✅ 모든 시리즈에 대한 한국어명
   - ✅ 지역명 + 지표명 + (계절조정 여부)

---

## 📝 파일명 규칙

### 생성할 파일명:
- `/home/jyp0615/us_eco/[데이터명]_analysis_refactor.py`
- 예: `fhfa_hpi_analysis_refactor.py`

### 변수명 규칙:
- 데이터명: `FHFA_HPI`, `JOLTS_EMPLOY` 등 (대문자 + 언더스코어)
- 함수명: `load_fhfa_hpi_data()` 등 (소문자 + 언더스코어)

---

## 🎯 최종 목표

이 프롬프트를 따라 생성된 파일은:

✅ **통합된 구조**: us_eco_utils 기반  
✅ **깔끔한 코드**: 중복 함수 완전 제거  
✅ **완전한 기능**: 로드, 시각화, Export 모두 지원  
✅ **표준화된 API**: 모든 파일이 동일한 함수 구조  
✅ **자동화 지원**: 스마트 업데이트 및 CSV 관리  
✅ **강력한 시각화**: plot_xxx_series_advanced() 범용 함수  

### 💡 참고 완성 예시:
- **완전 수정 완료**: `/home/jyp0615/us_eco/JOLTS_employ_refactor.py`
- **완전 수정 완료**: `/home/jyp0615/us_eco/PPI_analysis_refactor.py`  
- **완전 수정 완료**: `/home/jyp0615/us_eco/import_price_refactor.py`
- **DBnomics 전용**: `/home/jyp0615/us_eco/ism_pmi_refactor.py`

이 프롬프트로 생성된 모든 파일은 일관된 구조와 강력한 기능을 제공합니다!