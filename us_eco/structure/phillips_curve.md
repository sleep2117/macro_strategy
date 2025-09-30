🎯 필립스 커브 필수 데이터
1. 실업률 (이미 확인됨)

시리즈 ID: UNRATE
제목: Unemployment Rate
단위: Percent
빈도: Monthly, Seasonally Adjusted

2. 인플레이션율 데이터 옵션들
A. CPI 기반 인플레이션
헤드라인 CPI 인플레이션:

시리즈 ID: CPIAUCSL (지수) → 연간 변화율로 변환 필요
제목: Consumer Price Index for All Urban Consumers: All Items in U.S. City Average Consumer Price Index for All Urban Consumers: All Items in U.S. City Average (CPIAUCSL) | FRED | St. Louis Fed
단위: Index (1982-84=100)
빈도: Monthly, Not Seasonally Adjusted

코어 CPI 인플레이션:

시리즈 ID: CPILFESL (지수) → 연간 변화율로 변환 필요
제목: Consumer Price Index for All Urban Consumers: All Items Less Food and Energy in U.S. City Average Consumer Price Index for All Urban Consumers: All Items Less Food and Energy in U.S. City Average (CPILFESL) | FRED | St. Louis Fed
단위: Index (1982-84=100)
빈도: Monthly, Not Seasonally Adjusted

B. PCE 기반 인플레이션 (연준 선호 지표)
헤드라인 PCE 인플레이션:

시리즈 ID: PCEPI (지수) → 연간 변화율로 변환 필요
제목: Personal Consumption Expenditures: Chain-type Price Index Personal Consumption Expenditures: Chain-type Price Index (PCEPI) | FRED | St. Louis Fed
단위: Index (2017=100)
빈도: Monthly, Seasonally Adjusted

코어 PCE 인플레이션:

시리즈 ID: PCEPILFE (지수) → 연간 변화율로 변환 필요
제목: Personal Consumption Expenditures Excluding Food and Energy (Chain-Type Price Index) Personal Consumption Expenditures Excluding Food and Energy (Chain-Type Price Index) (PCEPILFE) | FRED | St. Louis Fed
단위: Index (2017=100)
빈도: Monthly, Seasonally Adjusted

C. 이미 변화율로 계산된 시리즈들
트림 평균 PCE 인플레이션:

시리즈 ID: PCETRIM1M158SFRBDAL
제목: Trimmed Mean PCE Inflation Rate Trimmed Mean PCE Inflation Rate (PCETRIM1M158SFRBDAL) | FRED | St. Louis Fed
단위: Percent Change from Year Ago
빈도: Monthly, Seasonally Adjusted

스티키 프라이스 코어 CPI:

시리즈 ID: CORESTICKM159SFRBATL
제목: Sticky Price Consumer Price Index less Food and Energy Sticky Price Consumer Price Index less Food and Energy (CORESTICKM159SFRBATL) | FRED | St. Louis Fed
단위: Percent Change from Year Ago
빈도: Monthly, Seasonally Adjusted

📊 필립스 커브 작성 방법
필립스 커브는 다음과 같이 그립니다:

X축: 실업률 (UNRATE)
Y축: 인플레이션율 (위 시리즈 중 선택)
각 점: 특정 시점의 (실업률, 인플레이션율) 좌표

🔧 데이터 처리 주의사항

지수를 변화율로 변환: CPIAUCSL, CPILFESL, PCEPI, PCEPILFE는 지수이므로 연간 변화율로 변환해야 합니다:
인플레이션율 = ((현재값 / 12개월 전 값) - 1) × 100

이미 변화율인 시리즈: PCETRIM1M158SFRBDAL, CORESTICKM159SFRBATL는 이미 연간 변화율로 계산되어 있습니다.

💡 추천 조합
가장 일반적인 필립스 커브:

X축: UNRATE (실업률)
Y축: CPIAUCSL을 연간 변화율로 변환한 헤드라인 인플레이션

연준 스타일 필립스 커브:

X축: UNRATE (실업률)
Y축: PCEPILFE를 연간 변화율로 변환한 코어 PCE 인플레이션

이 데이터들로 경기순환에 따른 인플레이션과 실업률의 관계를 분석할 수 있습니다.