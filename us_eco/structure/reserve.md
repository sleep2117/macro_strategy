## H.4.1 시리즈 ID - 수요일 시점(Wednesday Level) 통일 버전

### 1. **핵심 시리즈 - 수요일 시점 버전**

#### A. 공급 요인 (Factors Supplying Reserve Funds)

|시리즈 ID|시리즈명|설명|중요도|
|---|---|---|---|
|**WALCL**|All: Total Assets|연준 총자산|⭐⭐⭐⭐⭐|
|**WSHOSHO**|Securities Held Outright|보유 증권 총액|⭐⭐⭐⭐⭐|
|**WUTSHO**|U.S. Treasury Securities|미 국채 보유액|⭐⭐⭐⭐|
|**WSHOMCB**|Mortgage-Backed Securities|MBS 보유액|⭐⭐⭐⭐|
|**WLCFLL**|Liquidity and Credit Facilities: Loans|유동성 지원 대출|⭐⭐⭐|
|**WUDSHO**|Unamortized Discounts|미상각 할인|⭐⭐|
|**WUPSHO**|Unamortized Premiums|미상각 프리미엄|⭐⭐|
|**WORAL**|Other: Repurchase Agreements|환매조건부채권|⭐⭐⭐|

#### B. 흡수 요인 (Factors Absorbing Reserve Funds)

|시리즈 ID|시리즈명|설명|중요도|
|---|---|---|---|
|**WCURCIR**|Currency in Circulation|유통 화폐|⭐⭐⭐⭐|
|**WLRRAL**|Reverse Repurchase Agreements|역환매조건부채권 (RRP)|⭐⭐⭐⭐⭐|
|**WDTGAL**|Treasury General Account|재무부 일반계정 (TGA)|⭐⭐⭐⭐⭐|
|**WDFOL**|Foreign Official Deposits|외국 공식 예금|⭐⭐⭐|
|**WDSOPDL**|Deposits: Other|기타 예금|⭐⭐|
|**WOTHLB**|Other Liabilities and Capital|기타 부채 및 자본|⭐⭐⭐|

#### C. 결과 지표 (Resulting Balance) - 수요일 시점

|시리즈 ID|시리즈명|설명|중요도|
|---|---|---|---|
|**WRBWFRBL**|Reserve Balances with Federal Reserve Banks: Wednesday Level|준비금 잔액 (수요일)|⭐⭐⭐⭐⭐|

### 2. **추가 중요 시리즈 - 수요일 시점**

#### D. 연준 대출 프로그램

|시리즈 ID|시리즈명|설명|
|---|---|---|
|**WLCFLPCL**|Primary Credit|1차 신용|
|**WLCFLSCL**|Secondary Credit|2차 신용|
|**WLCFLPDCF**|Primary Dealer Credit Facility|PD 신용 지원|
|**WBTFPL**|Bank Term Funding Program|은행 기간 자금 프로그램|

#### E. 세부 증권 보유

|시리즈 ID|시리즈명|설명|
|---|---|---|
|**WUTB1L**|U.S. Treasury Bills|단기 국채|
|**WUTN2L**|U.S. Treasury Notes (2-10Y)|중기 국채|
|**WUTB3L**|U.S. Treasury Bonds (>10Y)|장기 국채|
|**WFADSL**|Federal Agency Debt Securities|연방기관 채권|

#### F. 총계 시리즈

|시리즈 ID|시리즈명|설명|
|---|---|---|
|**WTFSRFL**|Total Factors Supplying Reserve Funds|준비금 공급 요인 총계|
|**WTFORARFL**|Total Factors Absorbing Reserve Funds (Other than Reserve Balances)|준비금 흡수 요인 총계|

### 3. **수요일 시점 WRESBAL 계산 공식**

python

```python
# 수요일 시점 준비금 잔액 계산
WRBWFRBL = WTFSRFL - WTFORARFL

# 또는 상세 계산
WRBWFRBL = WALCL - WCURCIR - WLRRAL - WDTGAL - WDFOL - WDSOPDL - WOTHLB

# 간단 유동성 공식 (수요일 시점)
Liquidity_Wednesday = WALCL - WDTGAL - WLRRAL
```

### 4. **수요일 시점 데이터 수집 코드**

python

```python
# 수요일 시점 핵심 시리즈
wednesday_series = {
    # 자산 측면
    'total_assets': 'WALCL',
    'securities_outright': 'WSHOSHO',
    'treasury_securities': 'WUTSHO',
    'mbs': 'WSHOMCB',
    'loans': 'WLCFLL',
    
    # 부채 측면
    'currency': 'WCURCIR',
    'rrp': 'WLRRAL',
    'tga': 'WDTGAL',
    'foreign_deposits': 'WDFOL',
    'other_deposits': 'WDSOPDL',
    'other_liabilities': 'WOTHLB',
    
    # 결과
    'reserve_balances': 'WRBWFRBL',
    
    # 총계
    'total_supply': 'WTFSRFL',
    'total_absorb': 'WTFORARFL'
}
```

### 5. **주간 평균 → 수요일 시점 변환 매핑**

|주간 평균 (Week Average)|수요일 시점 (Wednesday Level)|
|---|---|
|WRESBAL|WRBWFRBL|
|WTREGEN|WDTGAL|
|WLRRA|WLRRAL|
|WDFO|WDFOL|
|WDSOPD|WDSOPDL|
|WTFSRF|WTFSRFL|
|WTFORAR|WTFORARFL|

### 6. **실시간 모니터링용 대시보드 구성**

python

```python
# 수요일 시점 데이터 모니터링
realtime_dashboard = {
    'primary_indicators': [
        'WRBWFRBL',  # 준비금 잔액
        'WALCL',     # 총자산
        'WLRRAL',    # RRP
        'WDTGAL'     # TGA
    ],
    
    'secondary_indicators': [
        'WSHOSHO',   # 증권 보유
        'WCURCIR',   # 유통 화폐
        'WLCFLL',    # 대출
        'WDFOL'      # 외국 예금
    ],
    
    'calculated_metrics': [
        'WALCL - WDTGAL - WLRRAL',  # 기본 유동성
        'WRBWFRBL + WDTGAL',         # 순 유동성
        'WLRRAL / WALCL * 100'       # RRP 비율
    ]
}
```

### 7. **데이터 업데이트 시점**

- **발표일**: 매주 목요일 오후 4:30 ET
- **기준일**: 직전 수요일 종료 시점
- **지연**: 1영업일 (수요일 데이터 → 목요일 발표)

수요일 시점 데이터를 사용하면 더 정확한 시점 분석이 가능하며, 특히 일간 변동성이 큰 시기에는 주간 평균보다 더 유용한 정보를 제공합니다.

### 3. **WRESBAL 계산 방법**

WRESBAL은 연준 시스템의 실제 유동성을 나타내는 핵심 지표입니다:

```
WRESBAL = Total Factors Supplying Reserve Funds 
          - Total Factors Absorbing Reserve Funds (WRESBAL 제외)

또는 간단히:

WRESBAL ≈ WALCL - WCURCIR - WLRRAL - WDTGAL - WDFOL - WOTHLB
```

### 4. **유동성 분석을 위한 주요 공식**

#### A. 기본 유동성 공식

```
Basic Liquidity = WALCL - TGA - RRP
                = WALCL - WDTGAL - WLRRAL
```

#### B. 정밀 유동성 공식 (WRESBAL 활용)

```
Precise Liquidity = WRESBAL
```

#### C. 순 유동성 공식

```
Net Liquidity = WRESBAL + TGA
              = WRESBAL + WDTGAL
```

### 5. **분석 방법론**

#### A. 트렌드 분석

1. **WRESBAL 추이 모니터링**
    - 증가 = 시장 유동성 증가 → 위험자산 우호적
    - 감소 = 긴축적 환경 → 위험자산 부정적
2. **RRP 수준 관찰**
    - 높은 RRP = 과잉 유동성이 연준에 파킹
    - RRP 감소 = 유동성이 시장으로 유입
3. **TGA 변동 추적**
    - TGA 증가 = 유동성 흡수 (부정적)
    - TGA 감소 = 유동성 공급 (긍정적)

#### B. 상관관계 분석

python

```python
# 주요 상관관계 분석 대상
- WRESBAL vs S&P 500
- WALCL vs 나스닥
- (WALCL - TGA - RRP) vs 비트코인
- RRP 변화율 vs 채권 수익률
```

#### C. 선행지표로서의 활용

1. **QT/QE 모니터링**
    - WALCL 변화 = QT/QE 진행 상황
    - WSHOSHO 변화 = 증권 매입/매도
2. **정책 변화 예측**
    - WRESBAL/GDP 비율 = 통화정책 여력
    - RRP 사용률 = 단기금리 압력

### 6. **실전 활용 팁**

#### A. 데이터 수집 자동화

python

```python
# FRED API를 통한 주요 시리즈 수집
key_series = [
    'WALCL',    # Total Assets
    'WRESBAL',  # Reserve Balances
    'WLRRAL',   # RRP (Wednesday Level)
    'WDTGAL',   # TGA (Wednesday Level)
    'WSHOSHO',  # Securities Held Outright
    'WCURCIR'   # Currency in Circulation
]
```

#### B. 복합 지표 생성

python

```python
# 유동성 지표 계산
liquidity_indicators = {
    'basic_liquidity': 'WALCL - WDTGAL - WLRRAL',
    'net_liquidity': 'WRESBAL + WDTGAL',
    'excess_reserves': 'WRESBAL - required_reserves',
    'rrp_ratio': 'WLRRAL / WALCL'
}
```

#### C. 알림 설정 기준

- WRESBAL < $3T: 유동성 경고
- RRP > $2T: 과잉 유동성
- TGA > $500B: 재정 압박
- WALCL 주간 변화 > ±$50B: 정책 변화

### 7. **주의사항 및 한계**

1. **시차 문제**
    - Wednesday Level은 수요일 데이터가 목요일 발표
    - Week Average는 추가 지연
2. **계절성**
    - 분기말/연말 창구 조작(window dressing)
    - 세금 납부 시즌 TGA 급증
3. **특수 상황**
    - 위기 시 특별 유동성 프로그램 가동
    - 정책 변경 시 구조적 변화

### 8. **추천 모니터링 대시보드 구성**

```
Primary Panel:
- WRESBAL (일간/주간 차트)
- WALCL vs S&P 500
- RRP & TGA 누적 차트

Secondary Panel:
- WRESBAL 변화율
- RRP/WALCL 비율
- Net Liquidity 추이

Alert Panel:
- WRESBAL 임계값 돌파
- RRP 급변
- TGA 이상 변동
```

이 H.4.1 데이터는 연준의 통화정책 실행과 시장 유동성을 실시간으로 파악할 수 있는 가장 중요한 지표입니다. 특히 WRESBAL은 많은 기관투자자들이 시장 방향성 예측에 활용하는 핵심 지표입니다.
