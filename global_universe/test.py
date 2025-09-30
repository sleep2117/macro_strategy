# %%
from pykrx import stock

tickers = stock.get_index_ticker_list(market='KRX')
for ticker in stock.get_index_ticker_list():
    print(ticker, stock.get_index_ticker_name(ticker))
# %%
df = stock.get_index_fundamental("20250101", "20250908", "1008")
print(df.head())
# %%
df.tail()
# %%
