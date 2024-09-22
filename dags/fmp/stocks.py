import common.generic as generic
import pandas as pd
import datetime as dt
import requests
import os
import fmp.settings as s
import common.utils as utils
from io import StringIO
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_date, date_add, to_date, max as max_
import psycopg2
from common.utils import write_to_postgres

import psycopg2
import os


FMP_KEY = os.environ["FMP_KEY"]


def get_distinct_tickers():
    # Database connection parameters
    conn_params = {
        "dbname": os.environ.get("MARTY_DB_NAME"),
        "user": os.environ.get("MARTY_DB_USR"),
        "password": os.environ.get("MARTY_DB_PW"),
        "host": os.environ.get("MARTY_DB_HOST"),
        "port": os.environ.get("MARTY_DB_PORT", "5432"),
    }

    try:
        # Connect to the database
        conn = psycopg2.connect(**conn_params)
        # Create a cursor object
        cursor = conn.cursor()

        # SQL to fetch distinct ticker symbols
        query = "SELECT DISTINCT symbol FROM watchlist;"

        # Execute the query
        cursor.execute(query)

        # Fetch all distinct ticker symbols
        tickers = cursor.fetchall()

        # Convert list of tuples to list of strings
        tickers = [ticker[0] for ticker in tickers]

        # Close the cursor and the connection
        cursor.close()
        conn.close()

        return tickers
    except Exception as e:
        print(f"An error occurred: {e}")
        return []


def collect_full_price(ds: dt.date, ticker: str, buffer: bool = True):
    """
    Collect full price for a ticker and save to data-lake.
    Save the full file to easily query all info for a ticker.
    Make a subset of the given day to construct a full
    snapshot of all tickers on the day.
    Date is converted for airflow.

    Inputs: Date and ticker to collect.
    """
    print(ticker)
    ds_start = dt.date(1980, 1, 1)
    r = requests.get(
        s.HISTORICAL_PRICE_FULL.format(DSS=ds_start, DSE=ds, TICKER=ticker, API=FMP_KEY)
    )
    data = r.json()
    if "historical" not in data.keys():
        return False
    df = pd.DataFrame(pd.DataFrame(data["historical"]))
    df["symbol"] = ticker
    df_typed = utils.format_data(df, s.price_full_types)
    df_typed_end = df_typed.loc[df_typed["date"] == ds].copy()
    ## save
    if buffer:
        df_typed_end.to_parquet(
            s.buffer_historical_daily_price_full + f"/{ds}/{ticker}.parquet",
            index=False,
        )
    df_typed.to_parquet(
        s.historical_ticker_price_full + f"/{ticker}.parquet", index=False
    )
    return True


def collect_thirty_minute_price(ds: dt.date, ticker: str):
    """
    Collect full price for a ticker and save to data-lake.
    Save the full file to easily query all info for a ticker.
    Make a subset of the given day to construct a full
    snapshot of all tickers on the day.
    Date is converted for airflow.

    Inputs: Date and ticker to collect.
    """
    ds_start = dt.date(1980, 1, 1)
    r = requests.get(
        s.HISTORICAL_THIRTY_MINUTE_PRICE.format(
            DSS=ds_start, DSE=ds, TICKER=ticker, API=FMP_KEY
        )
    )
    data = r.json()
    if len(data) == 0:
        return False
    df = pd.DataFrame(data)
    df["symbol"] = ticker
    df_typed = utils.format_data(df, s.thirty_minute_price_types)
    df_typed_end = df_typed.loc[df_typed["date"].apply(lambda r: r.date()) == ds].copy()
    ## save
    df_typed_end.to_parquet(
        s.buffer_historical_thirty_minute_price + f"/{ds}/{ticker}.parquet", index=False
    )
    df_typed.to_parquet(
        s.historical_thirty_minute_ticker_price + f"/{ticker}.parquet", index=False
    )
    return True


def collect_peers(ds: dt.date):
    r = requests.get(s.STOCK_PEERS.format(API=FMP_KEY))
    data = r.content.decode("utf8")
    df = pd.read_csv(StringIO(data))
    df["peers"] = df["peers"].apply(lambda r: r.split(","))
    if not os.path.exists(s.stock_peers + f"/{ds}"):
        os.makedirs(s.stock_peers + f"/{ds}")
    df.to_parquet(s.stock_peers + f"/{ds}/stock_peers.parquet")
    df.to_parquet(s.stock_peers + f"/latest/stock_peers.parquet")


## custom functions using the generics


def collect_dcf(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.DCF,
        "buffer_dir": s.buffer_dcf,
        "dl_ticker_dir": s.dcf_ticker,
        "dtypes": s.dcf_types,
    }
    utils.format_buffer(ds, s.buffer_dcf, yesterday=utils.strbool(yesterday))
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.dcf,
        buffer_loc=s.buffer_dcf,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-dcf-collection",
        **config_,
    )


def collect_rating(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.HISTORICAL_RATING,
        "buffer_dir": s.buffer_historical_rating,
        "dl_ticker_dir": s.historical_rating_ticker,
        "dtypes": s.rating_types,
    }
    utils.format_buffer(
        ds, s.buffer_historical_rating, yesterday=utils.strbool(yesterday)
    )
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.historical_rating,
        buffer_loc=s.buffer_historical_rating,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-rating-collection",
        **config_,
    )


def collect_enterprise_values_annual(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.ENTERPRISE_VALUES_ANNUAL,
        "buffer_dir": s.buffer_enterprise_values_annual,
        "dl_ticker_dir": s.enterprise_values_annual,
        "dtypes": s.enterprise_values_types,
    }
    utils.format_buffer(
        ds, s.buffer_enterprise_values_annual, yesterday=utils.strbool(yesterday)
    )
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.enterprise_values_annual,
        buffer_loc=s.buffer_enterprise_values_annual,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-enterprise-values-annual-collection",
        **config_,
    )


def collect_enterprise_values_quarter(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.ENTERPRISE_VALUES_QUARTER,
        "buffer_dir": s.buffer_enterprise_values_quarter,
        "dl_ticker_dir": s.enterprise_values_quarter,
        "dtypes": s.enterprise_values_types,
    }
    utils.format_buffer(
        ds, s.buffer_enterprise_values_quarter, yesterday=utils.strbool(yesterday)
    )
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.enterprise_values_quarter,
        buffer_loc=s.buffer_enterprise_values_quarter,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-enterprise-value-quarter-collection",
        **config_,
    )


def collect_grade(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.GRADE,
        "buffer_dir": s.buffer_grade,
        "dl_ticker_dir": s.grade_ticker,
        "dtypes": s.grade_types,
    }
    utils.format_buffer(ds, s.buffer_grade, yesterday=utils.strbool(yesterday))
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.grade,
        buffer_loc=s.buffer_grade,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-grade-collection",
        **config_,
    )


def collect_sentiment(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.SOCIAL_SENTIMENT,
        "buffer_dir": s.buffer_social_sentiment,
        "dl_ticker_dir": s.social_sentiment_ticker,
        "dtypes": s.social_sentiment_types,
    }
    utils.format_buffer(
        ds, s.buffer_social_sentiment, yesterday=utils.strbool(yesterday)
    )
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.social_sentiment,
        buffer_loc=s.buffer_social_sentiment,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-sentiment-collection",
        **config_,
    )


def collect_analyst_estimates(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.ANALYST_ESTIMATES,
        "buffer_dir": s.buffer_analyst_estimates,
        "dl_ticker_dir": s.analyst_estimates_ticker,
        "dtypes": s.analyst_estimates_types,
    }
    utils.format_buffer(
        ds, s.buffer_analyst_estimates, yesterday=utils.strbool(yesterday)
    )
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.analyst_estimates,
        buffer_loc=s.buffer_analyst_estimates,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-analyst-estimates-collection",
        **config_,
    )


def collect_analyst_estimates_quarter(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.ANALYST_ESTIMATES_Q,
        "buffer_dir": s.buffer_analyst_estimates_quarter,
        "dl_ticker_dir": s.analyst_estimates_quarter_ticker,
        "dtypes": s.analyst_estimates_types,
    }
    utils.format_buffer(
        ds, s.buffer_analyst_estimates_quarter, yesterday=utils.strbool(yesterday)
    )
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.analyst_estimates_quarter,
        buffer_loc=s.buffer_analyst_estimates_quarter,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-analyst-estimates-quarter-collection",
        **config_,
    )


def collect_balance_sheets():
    config_ = {
        "add_ticker": False,
        "url": s.BALANCE_SHEET,
        "dl_ticker_dir": s.fmp_a_bs_ticker,
        "dtypes": s.balance_sheet_types,
    }
    generic.collect_generic_distributed(
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-balance-sheet-collection",
        **config_,
    )


def collect_balance_sheets_quarter():
    config_ = {
        "add_ticker": False,
        "url": s.BALANCE_SHEET_Q,
        "dl_ticker_dir": s.fmp_q_bs_ticker,
        "dtypes": s.balance_sheet_types,
    }
    generic.collect_generic_distributed(
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-balance-sheet-quarter-collection",
        **config_,
    )


def collect_cash_flow():
    config_ = {
        "add_ticker": False,
        "url": s.CASH_FLOW,
        "dl_ticker_dir": s.fmp_a_cf_ticker,
        "dtypes": s.cash_flow_types,
    }
    generic.collect_generic_distributed(
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-cash-flow-collection",
        **config_,
    )


def collect_cash_flow_quarter():
    config_ = {
        "add_ticker": False,
        "url": s.CASH_FLOW_Q,
        "dl_ticker_dir": s.fmp_q_cf_ticker,
        "dtypes": s.cash_flow_types,
    }
    generic.collect_generic_distributed(
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-cash-flow-quarter-collection",
        **config_,
    )


def collect_income():
    config_ = {
        "add_ticker": False,
        "url": s.INCOME_STATEMENT,
        "dl_ticker_dir": s.fmp_a_i_ticker,
        "dtypes": s.income_types,
    }
    generic.collect_generic_distributed(
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-income-collection",
        **config_,
    )


def collect_income_quarter():
    config_ = {
        "add_ticker": False,
        "url": s.INCOME_STATEMENT_Q,
        "dl_ticker_dir": s.fmp_q_i_ticker,
        "dtypes": s.income_types,
    }
    generic.collect_generic_distributed(
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-income-quarter-collection",
        **config_,
    )


def collect_key_metrics(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.KEY_METRICS,
        "buffer_dir": s.buffer_fmp_a_km,
        "dl_ticker_dir": s.fmp_a_km_ticker,
        "dtypes": s.key_metrics_types,
    }
    utils.format_buffer(ds, s.buffer_fmp_a_km, yesterday=utils.strbool(yesterday))
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.fmp_a_km,
        buffer_loc=s.buffer_fmp_a_km,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-key-metrics-collection",
        **config_,
    )


def collect_key_metrics_quarter(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.KEY_METRICS_Q,
        "buffer_dir": s.buffer_fmp_a_km,
        "dl_ticker_dir": s.fmp_a_km_ticker,
        "dtypes": s.key_metrics_types,
    }
    utils.format_buffer(ds, s.buffer_fmp_a_km, yesterday=utils.strbool(yesterday))
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.fmp_a_km,
        buffer_loc=s.buffer_fmp_a_km,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-key-metrics-quarter-collection",
        **config_,
    )


def collect_ratios(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.RATIOS,
        "buffer_dir": s.buffer_fmp_a_r,
        "dl_ticker_dir": s.fmp_a_r_ticker,
        "dtypes": s.ratios_types,
    }
    utils.format_buffer(ds, s.buffer_fmp_a_r, yesterday=utils.strbool(yesterday))
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.fmp_a_r,
        buffer_loc=s.buffer_fmp_a_r,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-ratios-collection",
        **config_,
    )


def collect_ratios_quarter(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.RATIOS_Q,
        "buffer_dir": s.buffer_fmp_q_r,
        "dl_ticker_dir": s.fmp_q_r_ticker,
        "dtypes": s.ratios_types,
    }
    utils.format_buffer(ds, s.buffer_fmp_q_r, yesterday=utils.strbool(yesterday))
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.fmp_q_r,
        buffer_loc=s.buffer_fmp_q_r,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-ratios-quarter-collection",
        **config_,
    )


def collect_earnings_surprises(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.EARNINGS_SURPRISES,
        "buffer_dir": s.buffer_earnings_surprises,
        "dl_ticker_dir": s.earnings_surprises_ticker,
        "dtypes": s.earnings_surprises_types,
    }
    utils.format_buffer(
        ds, s.buffer_earnings_surprises, yesterday=utils.strbool(yesterday)
    )
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.earnings_surprises,
        buffer_loc=s.buffer_earnings_surprises,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-earnings-surprises-collection",
        **config_,
    )


def collect_insider_trading(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.SOCIAL_SENTIMENT,
        "buffer_dir": s.buffer_insider_trading,
        "dl_ticker_dir": s.insider_trading_ticker,
        "dtypes": s.insider_trading_types,
    }
    utils.format_buffer(
        ds, s.buffer_insider_trading, yesterday=utils.strbool(yesterday)
    )
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.insider_trading,
        buffer_loc=s.buffer_insider_trading,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-insider-trading-collection",
        **config_,
    )


def collect_stock_news():
    config_ = {
        "add_ticker": False,
        "url": s.STOCK_NEWS,
        "dl_ticker_dir": s.stock_news_ticker,
        "dtypes": s.stock_news_types,
    }
    generic.collect_generic_distributed(
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-stock-news-collection",
        **config_,
    )


def collect_press_releases():
    config_ = {
        "add_ticker": False,
        "url": s.PRESS_RELEASE,
        "dl_ticker_dir": s.press_releases_ticker,
        "dtypes": s.press_release_types,
    }
    generic.collect_generic_distributed(
        distribute_through=generic.collect_generic_page,
        spark_app="daily-press-releases-collection",
        **config_,
    )


def collect_earnings_call_transcript(year: float = None):
    if year == None:
        year = dt.date.today().year
    config_ = {
        "add_ticker": False,
        "url": s.EARNINGS_CALL_TRANSCRIPT,
        "dl_ticker_dir": s.earning_call_transcript,
        "dtypes": s.earning_call_transcript_types,
        "year": year,
    }
    generic.collect_generic_distributed(
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-earnings-call-transcript-collection",
        **config_,
    )


def collect_earnings_calendar():
    config_ = {
        "add_ticker": False,
        "url": s.EARNINGS_CALENDAR,
        "dl_ticker_dir": s.earning_calendar,
        "dtypes": s.earning_calendar_types,
    }
    generic.collect_generic_distributed(
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-earnings-calendar",
        **config_,
    )


def collect_end_of_day_prices(ds: dt.date, yesterday: bool = True):
    """
    - Read full stream and write to data lake.
        - historical_daily_price_full_raw
    - Subset stream by the tickers found in the daily "to-collect" file.
    - Write subset.
        - historical_daily_price_full
    """
    ds = pd.to_datetime(ds).date()
    if utils.strbool(yesterday):
        ds = ds - dt.timedelta(1)
    ## Define shit
    url = f"https://financialmodelingprep.com/api/v4/batch-request-end-of-day-prices?date={ds}&apikey={FMP_KEY}"

    # locations
    raw_dir = f"{s.historical_daily_price_full_raw}/{ds}/"
    subset_dir = f"{s.historical_daily_price_full}/{ds}/"
    raw_fn = f"{raw_dir}/data.parquet"
    subset_fn = f"{subset_dir}/data.parquet"

    ## Make request and unpack
    r = requests.get(url)
    data = [
        z[0].split(",")
        for z in [
            y.split("\t") for y in [x for x in r.content.decode("utf-8").split("\r\n")]
        ]
    ]
    df_raw = pd.DataFrame(data[1:], columns=data[0])
    df_raw = utils.format_data(df_raw, s.eod_price_types)

    ## Read "to-collect" list
    to_collect = pd.read_parquet(f"{s.to_collect}/{ds}.parquet")

    ## Subset
    df_subset = df_raw.loc[df_raw["symbol"].isin(to_collect["symbol"])]

    ## Write data
    # Make location
    utils.mk_dir(raw_dir)
    utils.mk_dir(subset_dir)
    df_raw.to_parquet(raw_fn)
    df_subset.to_parquet(subset_fn)


def append_price(ticker: str, new_data: pd.DataFrame):
    """
    Open historical ticker price file,
    append new data, and deduplicate.

    Input: Ticker to append.
    """
    ticker_fn = f"{s.historical_ticker_price_full}/{ticker}.parquet"
    if os.path.exists(ticker_fn):
        new_data = (
            pd.read_parquet(ticker_fn)
            .append(new_data)
            .drop_duplicates(["symbol", "date"])
            .sort_values("date", ascending=False)
        )
    new_data.to_parquet(ticker_fn, index=True)


def distribute_append_price(ds: dt.date, yesterday: bool = True):
    """
    Read daily prices of tickers I'm monitoring.
    Prep the dataframe to distribute through spark.
    Append daily price to full ticker price history
    and deduplicate.

    Input: Date to deconstruct stream.
    """
    ds = pd.to_datetime(ds).date()
    if utils.strbool(yesterday):
        ds = ds - dt.timedelta(1)
    ## prep list to distribute through append function
    # add missing columns to record schema evolution
    subset_dir = f"{s.historical_daily_price_full}/{ds}/"
    subset_fn = f"{subset_dir}/data.parquet"
    df_distribute = pd.read_parquet(subset_fn)
    for c in set(s.price_full_types.keys()) - set(df_distribute.columns):
        df_distribute[c] = None
    df_distribute = utils.format_data(df_distribute, s.price_full_types)

    cols = df_distribute.columns.to_list()
    distribute_list = [
        {"ticker": x[0], "new_data": pd.DataFrame([x[1]], columns=cols)}
        for x in zip(
            df_distribute["symbol"].tolist(), df_distribute[cols].values.tolist()
        )
    ]

    ## distribute append
    spark = SparkSession.builder.appName(
        f"pivot-daily-price-to-ticker-{ds}"
    ).getOrCreate()
    sc = spark.sparkContext
    sc.parallelize(distribute_list).map(lambda r: append_price(**r)).collect()
    sc.stop()
    spark.stop()


def collect_watchlist_daily_price(ds: dt.date):
    """
    Collect daily price for tickers on TDA watchlist.
    """
    ds = pd.to_datetime(ds).date()
    # wl = Watchlist()
    # wl.refresh_watchlist(extended=True)
    # watchlist = wl.df["symbol"]
    watchlist = get_distinct_tickers()
    [collect_full_price(ds, ticker, buffer=False) for ticker in watchlist]


def fetch_current_price(symbol):
    url = s.CURRENT_PRICE.format(TICKER=symbol, API=FMP_KEY)
    try:
        r = requests.get(url)
        data = r.json()
        pd.DataFrame(data)

        if data and isinstance(data, list):
            data = data[0]  # Assuming the first item is the relevant one
            df = pd.DataFrame([data])
            df["timestamp"] = dt.datetime.utcfromtimestamp(df["timestamp"])
            df["date"] = df["timestamp"].apply(lambda r: r.date())
            # Format and type the data
            typed_df = utils.format_data(df, s.current_price_types)
            return typed_df
        else:
            print(f"No data found for {symbol}")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return pd.DataFrame()


def update_watchlist_prices():
    """
    Get realtime price of stock
    """
    # Get the current time
    now = dt.datetime.now()
    # Define the cut-off time as 9:30 AM
    cut_off_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
    if now < cut_off_time:
        return  # do not append yet

    # wl = Watchlist()
    # wl.refresh_watchlist(extended=False)
    # symbols = wl.df["symbol"]
    symbols = get_distinct_tickers()

    dfs = []
    # Establish a database connection
    conn = psycopg2.connect(
        dbname=os.environ.get("MARTY_DB_NAME"),
        user=os.environ.get("MARTY_DB_USR"),
        password=os.environ.get("MARTY_DB_PW"),
        host=os.environ.get("MARTY_DB_HOST"),
        port=os.environ.get("MARTY_DB_PORT", "5432"),
    )
    cur = conn.cursor()
    for symbol in symbols:
        current_data = fetch_current_price(symbol)
        if current_data.empty:
            print(f"No data found for {symbol}")
            continue
        dfs.append(current_data)  # save to local

        # Prepare data for database operations
        data = {
            "open": current_data.loc[0, "open"],
            "high": current_data.loc[0, "dayHigh"],
            "low": current_data.loc[0, "dayLow"],
            "close": current_data.loc[0, "price"],
            "adj_close": current_data.loc[0, "price"],
            "volume": current_data.loc[0, "volume"],
            "unadjusted_volume": current_data.loc[0, "volume"],
            "change": current_data.loc[0, "change"],
            "change_percent": current_data.loc[0, "changesPercentage"],
            "vwap": (
                current_data.loc[0, "dayLow"]
                + current_data.loc[0, "dayHigh"]
                + current_data.loc[0, "price"]
            )
            / 3,  # Simplified VWAP calculation
            "label": current_data.loc[0, "name"],
            "change_over_time": None,
        }

        # Check if the entry exists for today
        cur.execute(
            "SELECT 1 FROM symbol_daily_price WHERE symbol = %s AND date = CURRENT_DATE;",
            (symbol,),
        )
        exists = cur.fetchone()

        if exists:
            # Update existing row
            update_query = """
            UPDATE symbol_daily_price
            SET open = %(open)s, high = %(high)s, low = %(low)s, close = %(close)s,
                adj_close = %(adj_close)s, volume = %(volume)s, unadjusted_volume = %(unadjusted_volume)s,
                change = %(change)s, change_percent = %(change_percent)s, vwap = %(vwap)s, label = %(label)s,
                change_over_time = %(change_over_time)s
            WHERE symbol = %(symbol)s AND date = CURRENT_DATE;
            """
            cur.execute(update_query, {**data, "symbol": symbol})
        else:
            # Insert new row
            insert_query = """
            INSERT INTO symbol_daily_price
                (date, symbol, open, high, low, close, adj_close, volume, unadjusted_volume, change, 
                 change_percent, vwap, label, change_over_time)
            VALUES
                (CURRENT_DATE, %(symbol)s, %(open)s, %(high)s, %(low)s, %(close)s, %(adj_close)s, 
                 %(volume)s, %(unadjusted_volume)s, %(change)s, %(change_percent)s, %(vwap)s, 
                 %(label)s, %(change_over_time)s);
            """
            cur.execute(insert_query, {**data, "symbol": symbol})

        conn.commit()

    df = pd.concat(dfs, ignore_index=True)
    df.to_parquet(s.current_price + "/data.parquet", index=False)

    # Close the connection
    cur.close()
    conn.close()


def append_current_to_historical():
    # Get the current time
    now = dt.datetime.now()
    # Define the cut-off time as 9:30 AM
    cut_off_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
    if now < cut_off_time:
        return  # do not append yet

    # wl = Watchlist()
    # wl.refresh_watchlist(extended=False)
    # symbols = wl.df["symbol"]

    symbols = get_distinct_tickers()
    current_price_df = pd.read_parquet(s.current_price + "/data.parquet")
    current_price_df = current_price_df.rename(
        columns={"dayLow": "low", "dayHigh": "high"}
    )
    current_price_df.loc[:, "close"] = current_price_df.loc[:, "price"]
    current_price_df.loc[:, "adjClose"] = current_price_df.loc[:, "price"]
    current_price_df.loc[:, "date"] = pd.to_datetime(current_price_df["date"]).apply(
        lambda r: r.date()
    )
    current_price_df = current_price_df[
        [
            "symbol",
            "date",
            "low",
            "change",
            "close",
            "volume",
            "adjClose",
            "high",
            "open",
        ]
    ]

    for symbol in symbols:
        tmp_df = current_price_df.loc[current_price_df["symbol"] == symbol]
        max_date = tmp_df["date"].max()
        historical_price_full = pd.read_parquet(
            s.historical_ticker_price_full + f"/{symbol}.parquet"
        )
        historical_price_full.loc[:, "date"] = pd.to_datetime(
            historical_price_full["date"]
        ).apply(lambda r: r.date())
        historical_price_full = historical_price_full.loc[
            historical_price_full["date"] < max_date
        ]
        historical_price_full = (
            historical_price_full.append(tmp_df)
            .sort_values("date", ascending=False)
            .drop_duplicates("date")
        )
        historical_price_full.to_parquet(
            s.historical_ticker_price_full + f"/{symbol}.parquet", index=False
        )


def load_and_process_earnings_data():
    spark = (
        SparkSession.builder.appName("Load Earnings Data")
        .config("spark.jars.packages", os.environ.get("POSTGRES_JAR"))
        .getOrCreate()
    )

    # Database configuration
    db_url = f"jdbc:postgresql://{os.environ.get('MARTY_DB_HOST')}:{os.environ.get('MARTY_DB_PORT', '5432')}/{os.environ.get('MARTY_DB_NAME')}"
    db_user = os.environ.get("MARTY_DB_USR")
    db_password = os.environ.get("MARTY_DB_PW")

    # Load watchlist data
    watchlist_df = (
        spark.read.format("jdbc")
        .option("url", db_url)
        .option("dbtable", "watchlist")
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # Get distinct symbols
    distinct_symbols = (
        watchlist_df.select("symbol").distinct().rdd.flatMap(lambda x: x).collect()
    )

    # Load earnings data
    earnings_data = spark.read.parquet(s.earning_calendar)
    earnings_data = earnings_data.select(
        "date",
        "symbol",
        "eps",
        "epsEstimated",
        "time",
        "revenue",
        "revenueEstimated",
        "updatedFromDate",
        "fiscalDateEnding",
    ).toDF(
        "date",
        "symbol",
        "eps",
        "eps_estimated",
        "time",
        "revenue",
        "revenue_estimated",
        "updated_from_date",
        "fiscal_date_ending",
    )

    # Filter data based on watchlist
    filtered_earnings_data = earnings_data.filter(
        earnings_data.symbol.isin(distinct_symbols)
    )

    # Overwrite filtered data to PostgreSQL
    write_to_postgres(
        filtered_earnings_data,
        "earnings_calendar",
        mode="overwrite",
    )

    spark.stop()


def load_and_update_ticker_data():
    """
    Write historical daily price to postgres
    """
    spark = (
        SparkSession.builder.appName("Load Daily Price Data")
        .config("spark.jars.packages", os.environ.get("POSTGRES_JAR"))
        .getOrCreate()
    )

    # Get distinct symbols
    distinct_symbols_df = utils.get_max_date(tbl="symbol_daily_price", date_column="date")

    # Process each symbol
    for i in distinct_symbols_df.index:
        symbol = distinct_symbols_df.loc[i, "symbol"]
        max_date = distinct_symbols_df.loc[i, "max_date"]
        
        # Load data from .parquet files
        file_path = f"{s.historical_ticker_price_full}/{symbol}.parquet"
        if not os.path.exists(file_path):
            continue
        pd_df = pd.read_parquet(file_path)
        stock_data_df = spark.createDataFrame(pd_df)
        # stock_data_df = spark.read.parquet(file_path)
        # Expected: timestamp, Found: INT64. I cant figure out how to resolve

        # Ensure the 'date' column is of type DATE
        stock_data_df = stock_data_df.withColumn("date", to_date(col("date")))

        # Rename the columns
        stock_data_df = (
            stock_data_df.withColumnRenamed("adjClose", "adj_close")
            .withColumnRenamed("unadjustedVolume", "unadjusted_volume")
            .withColumnRenamed("changePercent", "change_percent")
            .withColumnRenamed("changeOverTime", "change_over_time")
        )

        # Filter data to only include entries from the last 6 years
        min_date = date_add(current_date(), -6 * 365)  # 6 years back from today
        filtered_data_df = stock_data_df.filter((col("date") >= min_date) & (col("date") > max_date))

        # Write only new entries to PostgreSQL
        if filtered_data_df.count() > 0:
            write_to_postgres(
                filtered_data_df,
                "symbol_daily_price",
                mode="append",
            )
    spark.stop()

def load_and_update_ticker_news():
    spark = SparkSession.builder \
        .appName("Load News Data") \
        .config("spark.jars.packages", os.environ.get("POSTGRES_JAR")) \
        .getOrCreate()

    # Database configuration
    db_url = f"jdbc:postgresql://{os.environ.get('MARTY_DB_HOST')}:{os.environ.get('MARTY_DB_PORT', '5432')}/{os.environ.get('MARTY_DB_NAME')}"
    db_properties = {"user": os.environ.get("MARTY_DB_USR"), "password": os.environ.get("MARTY_DB_PW"), "driver": "org.postgresql.Driver"}

    # Get distinct symbols from utility function
    distinct_symbols = utils.get_distinct_tickers()

    for symbol in distinct_symbols:
        file_path = f"{s.stock_news_ticker}/{symbol}.parquet"
        if not os.path.exists(file_path):
            continue

        # Load data from Parquet via Python (to handle types correctly)
        pd_df = pd.read_parquet(file_path)
        new_data_df = spark.createDataFrame(pd_df)

        # Rename columns appropriately
        new_data_df = new_data_df.withColumnRenamed("publishedDate", "published_date")

        # Repartition the DataFrame to optimize Spark execution
        new_data_df = new_data_df.repartition(200)  # Adjust the number of partitions as necessary

        # Read existing news data for the symbol from PostgreSQL, fetching only necessary columns
        existing_data_df = spark.read.format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", f"(SELECT symbol, published_date, url FROM ticker_news WHERE symbol = '{symbol}') AS subquery") \
            .options(**db_properties) \
            .load()
        
        # Repartition the existing data DataFrame as well
        existing_data_df = existing_data_df.repartition(200)

        # Determine the latest date in the existing data
        max_date = existing_data_df.select(max_("published_date")).collect()[0][0]
        if max_date is None:
            max_date = dt.datetime.now() - pd.DateOffset(years=1)  # Fallback to last year if no data exists

        # Filter new data to include only entries more recent than the max_date
        new_data_df = new_data_df.filter(col("published_date") > max_date)

        # Find the data that does not exist in the database
        new_entries_df = new_data_df.join(existing_data_df, ["symbol", "published_date", "url"], "left_anti")

        # Write only new entries to PostgreSQL
        if new_entries_df.count() > 0:
            write_to_postgres(
                new_entries_df,
                "ticker_news",
                mode="append",
            )

    spark.stop()

