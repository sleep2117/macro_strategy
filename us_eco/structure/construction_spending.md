## Construction Spending (건설 지출) - 전체 시리즈 목록

### 릴리즈 정보

- **발행기관**: U.S. Census Bureau
- **릴리즈명**: Construction Spending (Value of Construction Put in Place)
- **최신 업데이트**: 2025년 8월 1일 (2025년 6월 데이터)
- **총 시리즈 수**: 96개
- **빈도**: Monthly (월별)
- **단위**: Millions of Dollars
- **데이터 시작**: 대부분 2002년, 일부 1993년

### 1. **주요 분류 체계**

Construction Spending은 3개 차원으로 분류됩니다:

1. **소유 구분**: Total, Private, Public
2. **건설 유형**: Residential, Nonresidential
3. **산업/용도**: 16개 세부 카테고리

### 2. **계층별 주요 시리즈**

#### A. 최상위 총계 (Total Construction)

|시리즈명|SA|NSA|% Change|시작년도|
|---|---|---|---|---|
|Total Construction|TTLCONS|TTLCON|MPCTXXXXS|1993|
|Total Private|TLPRVCONS|TLPRVCON|MPCVXXXXS|1993|
|Total Public|TLPBLCONS|TLPBLCON|MPCPXXXXS|1993|

#### B. 주거/비주거 구분

|유형|Total SA|Total NSA|Private SA|Private NSA|Public SA|Public NSA|
|---|---|---|---|---|---|---|
|**Residential**|TLRESCONS|TLRESCON|PRRESCONS|PRRESCON|PBRESCONS|PBRESCON|
|**Nonresidential**|TLNRESCONS|TLNRESCON|PNRESCONS|PNRESCON|PBNRESCONS|PBNRESCON|

### 3. **산업/용도별 세부 카테고리 (16개)**

|카테고리|Total SA|Total NSA|Private SA|Private NSA|Public SA|Public NSA|
|---|---|---|---|---|---|---|
|**1. Manufacturing**|TLMFGCONS|TLMFGCON|PRMFGCONS|PRMFGCON|-|-|
|**2. Commercial**|TLCOMCONS|TLCOMCON|PRCOMCONS|PRCOMCON|PBCOMCONS|PBCOMCON|
|**3. Office**|TLOFCONS|TLOFCON|PROFCONS|PROFCON|PBOFCONS|PBOFCON|
|**4. Lodging**|TLLODGCONS|TLLODGCON|PLODGCONS|PLODGCON|-|-|
|**5. Health Care**|TLHLTHCONS|TLHLTHCON|PRHLTHCONS|PRHLTHCON|PBHLTHCONS|PBHLTHCON|
|**6. Educational**|TLEDUCONS|TLEDUCON|PREDUCONS|PREDUCON|PBEDUCONS|PBEDUCON|
|**7. Religious**|TLRELCONS|TLRELCON|PRRELCONS|PRRELCON|-|-|
|**8. Public Safety**|TLPSCONS|TLPSCON|-|-|PBPSCONS|PBPSCON|
|**9. Amusement and Recreation**|TLAMUSCONS|TLAMUSCON|PRAMUSCONS|PRAMUSCON|PBAMUSCONS|PBAMUSCON|
|**10. Transportation**|TLTRANSCONS|TLTRANSCON|PRTRANSCONS|PRTRANSCON|PBTRANSCONS|PBTRANSCON|
|**11. Communication**|TLCMUCONS|TLCMUCON|PRCMUCONS|PRCMUCON|-|-|
|**12. Power**|TLPWRCONS|TLPWRCON|PRPWRCONS|PRPWRCON|PBPWRCONS|PBPWRCON|
|**13. Highway and Street**|TLHWYCONS|TLHWYCON|-|-|PBHWYCONS|PBHWYCON|
|**14. Sewage and Waste Disposal**|TLSWDCONS|TLSWDCON|-|-|PBSWGCONS|PBSWGCON|
|**15. Water Supply**|TLWSCONS|TLWSCON|-|-|PBWSCONS|PBWSCON|
|**16. Conservation and Development**|TLCADCONS|TLCADCON|-|-|PBCADCONS|PBCADCON|

### 4. **Percent Change 시리즈 (MPC 코드)**

각 주요 시리즈에는 전월 대비 변화율 시리즈가 있습니다:

- **Total**: MPCTXXXXS (Total), MPCVXXXXS (Private), MPCPXXXXS (Public)
- **Residential**: MPCT00XXS (Total), MPCV00XXS (Private), MPCP00XXS (Public)
- **Nonresidential**: MPCTNRXXS (Total), MPCVNRXXS (Private), MPCPNRXXS (Public)
- **각 카테고리별**: MPCT01XXS ~ MPCT15XXS (Total), MPCV01XXS ~ MPCV15XXS (Private), MPCP01XXS ~ MPCP15XXS (Public)

### 5. **시리즈 ID 패턴 분석**

```
기본 패턴:
[소유구분][카테고리][조정]

소유구분:
- TL: Total (전체)
- PR: Private (민간)
- PB: Public (공공)

카테고리 코드:
- RES: Residential
- NRES: Nonresidential
- MFG: Manufacturing
- COM: Commercial
- OF: Office
- LODG: Lodging
- HLTH: Health Care
- EDU: Educational
- REL: Religious
- PS: Public Safety
- AMUS: Amusement and Recreation
- TRANS: Transportation
- CMU: Communication
- PWR: Power
- HWY: Highway and Street
- SWD/SWG: Sewage and Waste Disposal
- WS: Water Supply
- CAD: Conservation and Development

조정:
- CONS: Seasonally Adjusted Annual Rate
- CON: Not Seasonally Adjusted
- S: Percent Change (Seasonally Adjusted)
```

### 6. **주요 특징 및 활용**

1. **데이터 특성**:
    - 모든 시리즈가 SA, NSA, % Change 버전 제공
    - 달러 단위 (Millions of Dollars)
    - SA는 연율 환산 (Annual Rate)
    - 대부분 2002년부터, 일부 1993년부터
2. **활용 분야**:
    - **경제 지표**: GDP 구성요소, 경기 선행지표
    - **부동산 시장**: 건설 활동 수준
    - **산업 분석**: 섹터별 투자 동향
    - **정책 분석**: 공공 vs 민간 투자 비교
3. **주요 관심 지표**:
    - **Total Construction**: 전체 건설 활동
    - **Residential vs Nonresidential**: 주택 vs 상업/산업 건설
    - **Manufacturing**: 리쇼어링 지표
    - **Infrastructure**: Highway, Water, Sewage 등

이 데이터는 미국 경제의 건설 투자 활동을 가장 포괄적으로 추적하는 공식 통계입니다.