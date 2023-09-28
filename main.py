import pandas as pd
import requests
from bs4 import BeautifulSoup 
import pytz
from datetime import datetime 
import yfinance
import threading
from utils import load_pickle, save_pickle

def get_sp500_tickers():
    res = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    soup = BeautifulSoup(res.content, 'html.parser')  
    table = soup.find_all('table')[0]
    df = pd.read_html(str(table))
    tickers = list(df[0].Symbol)
    return tickers

tickers = get_sp500_tickers()

#print(ticker[:5])  


def get_history(ticker, period_start, period_end, granularity="1d"):
    df = yfinance.Ticker(ticker).history(start=period_start, end=period_end, interval=granularity).reset_index()
    df = df.rename(columns={
        "Date":"datetime",
        "Open":"open",
        "High":"high",
        "Low":"low",
        "Close":"close",
        "Volume":"volume"
    })
    if df.empty:
        return pd.DataFrame()
    df["datetime"] = df["datetime"].dt.tz_localize(pytz.utc)
    df = df.drop(columns=['Dividends', 'Stock Splits'])
    df = df.set_index("datetime", drop=True)
    #index datetime OHLC
    return df


# for ticker in tickers:
#     get_history(ticker, period_start, period_end)



def get_histories(tickers, period_start, period_end, granularity="1d"):
    dfs = [None]*len(tickers)
    def _helper(i):
        df = get_history(
            tickers[i],
            period_start[i],
            period_end[i],
            granularity=granularity
        )
        dfs[i] = df
    threads = [threading.Thread(target=_helper, args=(i, )) for i in range(len(tickers))]
    [thread.start() for thread in threads]
    [thread.join() for thread in threads]
    tickers = [tickers[i] for i in range(len(tickers)) if not dfs[i].empty]
    dfs = [df for df in dfs if not df.empty]
    return tickers, dfs

def get_ticker_dfs(start, end):
    try:
        tickers, ticker_dfs = load_pickle("dataset.obj")
    except Exception as err:
        tickers = get_sp500_tickers()
        starts=[start]*len(tickers)
        ends=[end]*len(tickers)
        tickers, dfs = get_histories(tickers, starts, ends, granularity="1d")
        ticker_dfs = {ticker:df for ticker, df in zip(tickers, dfs)}
        save_pickle("dataset.obj", (tickers, ticker_dfs))

    return tickers, ticker_dfs

period_start = datetime(2010, 1, 1, tzinfo=pytz.utc)
period_end = datetime.now(pytz.utc)

tickers, ticker_dfs = get_ticker_dfs(period_start, period_end)
#print(ticker_dfs)