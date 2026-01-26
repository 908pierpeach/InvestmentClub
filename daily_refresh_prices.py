# daily_refresh_prices.py
import os
import sys
import time
import datetime as dt

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import yfinance as yf

DBURL = os.environ.get("DBURL")
if not DBURL:
    raise RuntimeError("Missing DBURL env var (set it in GitHub Actions secrets).")

# Ensure SSL for Neon if user forgot it (harmless if already present)
if "sslmode=" not in DBURL and "neon.tech" in DBURL:
    joiner = "&" if "?" in DBURL else "?"
    DBURL = DBURL + f"{joiner}sslmode=require"

PRICE_TABLE = os.environ.get("PRICE_TABLE", "ic_prices")
TICKER_TABLE = os.environ.get("TICKER_TABLE", "dim_ticker_sector")
DEFAULT_START = dt.date(2015, 1, 1)

# If you only want complete daily bars, use "yesterday".
# Set USE_YESTERDAY=1 in env to enable.
USE_YESTERDAY = os.environ.get("USE_YESTERDAY", "0") == "1"


def connect():
    # Neon can be sensitive to connection settings. Keep it simple.
    return psycopg2.connect(DBURL)


def get_tickers(cur):
    cur.execute(f"""
        SELECT DISTINCT ticker
        FROM {TICKER_TABLE}
        WHERE ticker IS NOT NULL
          AND upper(ticker) <> 'CASH'
        ORDER BY 1;
    """)
    return [r[0] for r in cur.fetchall()]


def last_loaded_dates(cur, tickers):
    cur.execute(f"""
        SELECT ticker, MAX(px_date)::date AS last_date
        FROM {PRICE_TABLE}
        WHERE ticker = ANY(%s)
        GROUP BY ticker;
    """, (tickers,))
    out = {t: None for t in tickers}
    for t, d in cur.fetchall():
        out[t] = d
    return out


def clamp_start_date(start_date: dt.date) -> dt.date:
    """Prevent future-date requests and reduce weekend silliness."""
    today = dt.date.today()
    cutoff = today - dt.timedelta(days=1) if USE_YESTERDAY else today

    # Never request beyond cutoff (today or yesterday)
    if start_date > cutoff:
        start_date = cutoff

    # If cutoff is weekend and we clamped to it, step back to Friday
    while start_date.weekday() >= 5:  # 5 Sat, 6 Sun
        start_date -= dt.timedelta(days=1)

    return start_date


def fetch_prices_yf(ticker: str, start_date: dt.date, max_retries: int = 3) -> pd.DataFrame:
    """
    Fetch adjusted close prices from Yahoo.
    We request through tomorrow because Yahoo end is effectively exclusive.
    """
    start_date = clamp_start_date(start_date)

    # If start_date is still in the future relative to now (shouldn't happen), bail.
    today = dt.date.today()
    if start_date > today:
        return pd.DataFrame()

    end_date = (today + dt.timedelta(days=1)).isoformat()

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            df = yf.download(
                ticker,
                start=start_date.isoformat(),
                end=end_date,
                auto_adjust=False,
                progress=False,
                actions=False,
                threads=False
            )
            if df is None or df.empty:
                return pd.DataFrame()

            df = df.reset_index()
            df.rename(columns={"Date": "px_date"}, inplace=True)

            # yfinance returns "Adj Close" with that exact spacing/case
            if "Adj Close" not in df.columns:
                return pd.DataFrame()

            df = df[["px_date", "Adj Close"]].copy()
            df.columns = ["px_date", "adj_close"]

            df["ticker"] = ticker
            df["px_date"] = pd.to_datetime(df["px_date"]).dt.date
            df["adj_close"] = pd.to_numeric(df["adj_close"], errors="coerce")
            df = df.dropna(subset=["px_date", "adj_close"])

            # Extra safety: never keep future dates
            cutoff = today - dt.timedelta(days=1) if USE_YESTERDAY else today
            df = df[df["px_date"] <= cutoff]

            return df

        except Exception as e:
            last_err = e
            # backoff
            time.sleep(1.5 * attempt)

    raise RuntimeError(f"yfinance failed after {max_retries} tries: {last_err}")


def upsert_prices(cur, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    rows = list(df[["ticker", "px_date", "adj_close"]].itertuples(index=False, name=None))

    sql = f"""
        INSERT INTO {PRICE_TABLE} (ticker, px_date, adj_close)
        VALUES %s
        ON CONFLICT (ticker, px_date) DO UPDATE SET
            adj_close = EXCLUDED.adj_close;
    """
    execute_values(cur, sql, rows, page_size=2000)
    return len(rows)


def main():
    run_ts = dt.datetime.now().isoformat(timespec="seconds")
    print(f"RUN_START {run_ts} | table={PRICE_TABLE} | ticker_table={TICKER_TABLE} | use_yesterday={USE_YESTERDAY}")

    with connect() as conn:
        conn.autocommit = False
        cur = conn.cursor()

        tickers = get_tickers(cur)
        if not tickers:
            print(f"No tickers found in {TICKER_TABLE}. Nothing to do.")
            return

        last = last_loaded_dates(cur, tickers)
        total = 0
        failures = 0

        for t in tickers:
            try:
                last_date = last.get(t)
                raw_start = (last_date + dt.timedelta(days=1)) if last_date else DEFAULT_START
                start_date = clamp_start_date(raw_start)

                print(f">> {t}: last={last_date} start={start_date}")
                df = fetch_prices_yf(t, start_date)

                rows = upsert_prices(cur, df)
                conn.commit()
                total += rows

                print(f"OK {t}: upserted {rows} rows")

            except Exception as e:
                conn.rollback()
                failures += 1
                print(f"ERR {t}: {type(e).__name__}: {e}", file=sys.stderr)

        print(f"RUN_END total_rows={total} failures={failures}")

        # Fail the job if lots of tickers fail (optional)
        # If you want Actions to go red when something is wrong:
        if failures > 0 and failures == len(tickers):
            raise RuntimeError("All tickers failed. Something is wrong with network/DB/credentials.")


if __name__ == "__main__":
    main()