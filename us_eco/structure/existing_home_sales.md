## Existing Home Sales (기존 주택 판매 관련 지표) - 전체 시리즈 목록

### 시리즈 설명

- **발행기관**: National Association of Realtors (NAR)
- **릴리즈명**: Existing Home Sales
- **빈도**: Monthly (월별)
- **최신 업데이트**: 2025년 7월 23일 (2025년 6월 데이터)

### 1. **판매량 지표 (Sales Volume) - 가격 제외**

#### National Level

|지표명|시리즈 ID|단위|조정|설명|
|---|---|---|---|---|
|**All Homes**|||||
|Existing Home Sales|EXHOSLUSM495S|Number of Units|SA|전체 기존 주택 판매량|
|Existing Home Sales: Housing Inventory|HOSINVUSM495N|Number of Units|NSA|판매 가능 재고|
|Existing Home Sales: Months Supply|HOSSUPUSM673N|Months' Supply|NSA|재고 소진 개월수|
|**Single-Family Homes**|||||
|Existing Single-Family Home Sales|EXSFHSUSM495S|Number of Units|SA|단독주택 판매량|
|Existing Single-Family Home Sales: Housing Inventory|HSFINVUSM495N|Number of Units|SA|단독주택 재고|
|Existing Single-Family Home Sales: Months Supply|HSFSUPUSM673N|Months' Supply|NSA|단독주택 재고 소진 개월수|

#### Regional Level - Sales Volume

|지역|All Homes 시리즈 ID|Single-Family 시리즈 ID|단위|조정|
|---|---|---|---|---|
|**판매량 (Sales)**|||||
|Northeast|EXHOSLUSNEM495S|EXSFHSUSNEM495S|Number of Units|SA|
|Midwest|EXHOSLUSMWM495S|EXSFHSUSMWM495S|Number of Units|SA|
|South|EXHOSLUSSOM495S|EXSFHSUSSOM495S|Number of Units|SA|
|West|EXHOSLUSWTM495S|EXSFHSUSWTM495S|Number of Units|SA|

### 2. **가격 지표 (Price Indicators) - 참고용**

#### National Level - Median Prices

|지표명|시리즈 ID|단위|조정|
|---|---|---|---|
|Median Sales Price of Existing Homes|HOSMEDUSM052N|Dollars|NSA|
|Median Sales Price of Existing Single-Family Homes|HSFMEDUSM052N|Dollars|NSA|

#### Regional Level - Median Prices

|지역|All Homes 시리즈 ID|Single-Family 시리즈 ID|단위|조정|
|---|---|---|---|---|
|Northeast|HOSMEDUSNEM052N|HSFMEDUSNEM052N|Dollars|NSA|
|Midwest|HOSMEDUSMWM052N|HSFMEDUSMWM052N|Dollars|NSA|
|South|HOSMEDUSSOM052N|HSFMEDUSSOM052N|Dollars|NSA|
|West|HOSMEDUSWTM052N|HSFMEDUSWTM052N|Dollars|NSA|

### 3. **단종된 시리즈 (DISCONTINUED) - Mean Prices**

|지표명|시리즈 ID|단위|단종일|
|---|---|---|---|
|**National**||||
|Mean Sales Price of Existing Homes|HOSAVGUSM052N|Dollars|2022-07-20|
|Mean Sales Price of Existing Single-Family Homes|HSFAVGUSM052N|Dollars|2022-07-20|
|**Regional - All Homes**||||
|Northeast|HOSAVGUSNEM052N|Dollars|2022-07-20|
|Midwest|HOSAVGUSMWM052N|Dollars|2022-07-20|
|South|HOSAVGUSSOM052N|Dollars|2022-07-20|
|West|HOSAVGUSWTM052N|Dollars|2022-07-20|
|**Regional - Single-Family**||||
|Northeast|HSFAVGUSNEM052N|Dollars|2022-07-20|
|Midwest|HSFAVGUSMWM052N|Dollars|2022-07-20|
|South|HSFAVGUSSOM052N|Dollars|2022-07-20|
|West|HSFAVGUSWTM052N|Dollars|2022-07-20|

### 시리즈 ID 패턴 설명

1. **판매량 (Sales Volume)**:
    - EXHOSLUS[지역코드]M495S
    - EXSFHSUS[지역코드]M495S (Single-Family)
2. **재고 (Inventory)**:
    - HOSINVUSM495N (All Homes)
    - HSFINVUSM495N (Single-Family)
3. **재고 소진 개월수 (Months Supply)**:
    - HOSSUPUSM673N (All Homes)
    - HSFSUPUSM673N (Single-Family)
4. **가격 (Prices)**:
    - HOSMEDUS[지역코드]M052N (Median, All Homes)
    - HSFMEDUS[지역코드]M052N (Median, Single-Family)
5. **지역 코드**:
    - NE = Northeast
    - MW = Midwest
    - SO = South
    - WT = West

### 주요 지표 설명

1. **Existing Home Sales**: 기존 주택(중고 주택)의 월별 판매 건수
2. **Housing Inventory**: 현재 시장에 나와 있는 판매 가능한 주택 수
3. **Months Supply**: 현재 판매 속도로 재고가 모두 소진되는데 걸리는 개월 수 (재고/월판매량)
4. **Median Sales Price**: 판매된 주택 가격의 중간값
5. **Mean Sales Price**: 판매된 주택 가격의 평균값 (2022년 단종)

### 요약 통계

- **활성 시리즈 수**: 26개
- **단종 시리즈 수**: 10개
- **총 시리즈 수**: 36개
- **데이터 카테고리**:
    - 판매량 관련: 10개
    - 재고 관련: 2개
    - 재고 소진율: 2개
    - 가격 관련: 12개 (활성) + 10개 (단종)
- **지역 구분**: National + 4개 Census Regions (Northeast, Midwest, South, West)
- **주택 유형**: All Homes, Single-Family Homes

이 데이터는 미국 부동산 시장의 건전성과 활동성을 평가하는 핵심 지표들입니다.