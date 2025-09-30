import re

# 파일 읽기
with open('/home/jyp0615/world_indices copy.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 티커 패턴 추출 (index와 etf)
index_pattern = r"'index':\s*'([^']+)'"
etf_pattern = r"'etf':\s*'([^']+)'"
alternatives_pattern = r"'alternatives':\s*\[([^\]]+)\]"

# 모든 티커 수집
all_tickers = set()

# Index 티커들
index_matches = re.findall(index_pattern, content)
for ticker in index_matches:
    if ticker != 'None' and ticker.strip():
        all_tickers.add(ticker)

# ETF 티커들
etf_matches = re.findall(etf_pattern, content)
for ticker in etf_matches:
    if ticker != 'None' and ticker.strip():
        all_tickers.add(ticker)

# Alternative 티커들
alt_matches = re.findall(alternatives_pattern, content)
for alt_group in alt_matches:
    # 따옴표로 둘러싸인 티커들 추출
    alt_tickers = re.findall(r"'([^']+)'", alt_group)
    for ticker in alt_tickers:
        if ticker.strip():
            all_tickers.add(ticker)

# 결과 출력
print('현재 투자 유니버스의 모든 티커 목록:')
print('='*50)
sorted_tickers = sorted(list(all_tickers))
for i, ticker in enumerate(sorted_tickers, 1):
    print(f'{i:2d}. {ticker}')
    
print(f'\n총 {len(sorted_tickers)}개의 티커')

# 티커를 파일에 저장
with open('/home/jyp0615/ticker_list.txt', 'w') as f:
    for ticker in sorted_tickers:
        f.write(f'{ticker}\n')

print('\n티커 목록이 ticker_list.txt에 저장되었습니다.')