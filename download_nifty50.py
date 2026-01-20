"""requirements:
# pip install yfinance pandas requests
"""

import datetime as dt
import os
import time
from typing import List, Tuple

import pandas as pd
import requests
import yfinance as yf


WIKI_URL = "https://en.wikipedia.org/wiki/NIFTY_50"
OUTPUT_DIR = "nifty50_csv"
SLEEP_SECONDS = 0.5


def fetch_nifty50_tickers() -> Tuple[List[str], str]:
    """Return list of Yahoo Finance tickers and a source label."""
    try:
        response = requests.get(WIKI_URL, timeout=20)
        response.raise_for_status()
        tables = pd.read_html(response.text)
        table = next(
            tbl for tbl in tables if "Symbol" in tbl.columns or "Symbol" in tbl.head(1).to_dict()
        )
        if "Symbol" not in table.columns:
            table.columns = [str(col).strip() for col in table.columns]
        symbols = table["Symbol"].dropna().astype(str).str.strip().unique().tolist()
        tickers = [f"{symbol}.NS" for symbol in symbols]
        if not tickers:
            raise ValueError("No tickers parsed from Wikipedia")
        return tickers, "wikipedia"
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to fetch tickers from Wikipedia: {exc}")
        hardcoded = [
            "ADANIENT.NS",
            "ADANIPORTS.NS",
            "APOLLOHOSP.NS",
            "ASIANPAINT.NS",
            "AXISBANK.NS",
            "BAJAJ-AUTO.NS",
            "BAJFINANCE.NS",
            "BAJAJFINSV.NS",
            "BPCL.NS",
            "BHARTIARTL.NS",
            "BRITANNIA.NS",
            "CIPLA.NS",
            "COALINDIA.NS",
            "DIVISLAB.NS",
            "DRREDDY.NS",
            "EICHERMOT.NS",
            "GRASIM.NS",
            "HCLTECH.NS",
            "HDFCBANK.NS",
            "HDFCLIFE.NS",
            "HEROMOTOCO.NS",
            "HINDALCO.NS",
            "HINDUNILVR.NS",
            "ICICIBANK.NS",
            "ITC.NS",
            "INDUSINDBK.NS",
            "INFY.NS",
            "JSWSTEEL.NS",
            "KOTAKBANK.NS",
            "LTIM.NS",
            "LT.NS",
            "M&M.NS",
            "MARUTI.NS",
            "NESTLEIND.NS",
            "NTPC.NS",
            "ONGC.NS",
            "POWERGRID.NS",
            "RELIANCE.NS",
            "SBIN.NS",
            "SBILIFE.NS",
            "SUNPHARMA.NS",
            "TCS.NS",
            "TATACONSUM.NS",
            "TATAMOTORS.NS",
            "TATASTEEL.NS",
            "TECHM.NS",
            "TITAN.NS",
            "ULTRACEMCO.NS",
            "UPL.NS",
            "WIPRO.NS",
        ]
        return hardcoded, "hardcoded"


def build_nse_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/",
            "Connection": "keep-alive",
        }
    )
    session.get("https://www.nseindia.com", timeout=20)
    return session


def fetch_nse_delivery_data(
    session: requests.Session, ticker: str, start_date: pd.Timestamp, end_date: pd.Timestamp
) -> pd.DataFrame:
    symbol = ticker.replace(".NS", "")
    url = "https://www.nseindia.com/api/historical/cm/equity"
    params = {
        "symbol": symbol,
        "series": "[\"EQ\"]",
        "from": start_date.strftime("%d-%m-%Y"),
        "to": end_date.strftime("%d-%m-%Y"),
    }
    response = session.get(url, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    records = payload.get("data", [])
    if not records:
        return pd.DataFrame()
    frame = pd.DataFrame(records)
    frame["Date"] = pd.to_datetime(frame["CH_TIMESTAMP"], format="%d-%b-%Y")
    frame = frame.rename(
        columns={
            "CH_DELIVERY_QTY": "Deliverable Volume",
            "CH_DELIVERY_PERC": "Deliverable %",
            "CH_OPEN_INT": "OI",
        }
    )
    frame = frame[["Date", "Deliverable Volume", "Deliverable %", "OI"]]
    return frame


def download_ticker_data(
    ticker: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    nse_session: requests.Session,
) -> pd.DataFrame:
    data = yf.download(
        ticker,
        start=start_date,
        end=end_date,
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if data.empty:
        return data
    data = data.reset_index()
    data = data.rename(columns={"Adj Close": "Adj Close"})
    try:
        nse_data = fetch_nse_delivery_data(nse_session, ticker, start_date, end_date)
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to fetch NSE delivery data for {ticker}: {exc}")
        nse_data = pd.DataFrame()
    if nse_data.empty:
        data["Deliverable Volume"] = pd.NA
        data["Deliverable %"] = pd.NA
        data["OI"] = pd.NA
    else:
        data = data.merge(nse_data, on="Date", how="left")
        data["Deliverable Volume"] = data["Deliverable Volume"].astype("Int64")
        data["Deliverable %"] = data["Deliverable %"].astype("Float64")
        data["OI"] = data["OI"].astype("Int64")
    data = data[
        [
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
            "Adj Close",
            "Volume",
            "Deliverable Volume",
            "Deliverable %",
            "OI",
        ]
    ]
    return data


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    tickers, source = fetch_nifty50_tickers()
    print(f"Using {len(tickers)} tickers from {source}.")

    end_date = pd.Timestamp(dt.date.today())
    start_date = end_date - pd.DateOffset(years=5)
    success = 0
    failed = []
    nse_session = build_nse_session()

    for ticker in tickers:
        print(f"Downloading {ticker}...")
        try:
            data = download_ticker_data(ticker, start_date, end_date, nse_session)
            if data.empty:
                print(f"No data for {ticker}; skipping.")
                failed.append(ticker)
            else:
                output_path = os.path.join(OUTPUT_DIR, f"{ticker.replace('.', '_')}.csv")
                data.to_csv(output_path, index=False)
                print(f"Saved {ticker} to {output_path}")
                success += 1
        except Exception as exc:  # noqa: BLE001
            print(f"Failed {ticker}: {exc}")
            failed.append(ticker)
        time.sleep(SLEEP_SECONDS)

    print("\nSummary")
    print(f"Successful downloads: {success}")
    print(f"Failed downloads: {len(failed)}")
    if failed:
        print("Failed tickers:")
        print(", ".join(failed))


if __name__ == "__main__":
    main()
