import common.generic as generic
import pandas as pd
import datetime as dt
import requests
import os
import fmp.settings as s
import common.utils as utils
from io import StringIO
from pyspark.sql import SparkSession

FMP_KEY = os.environ["FMP_KEY"]


def collect_full_price(ds: dt.date, ticker: str, buffer: bool = True):
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


def collect_balance_sheets(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.BALANCE_SHEET,
        "buffer_dir": s.buffer_fmp_a_bs,
        "dl_ticker_dir": s.fmp_a_bs_ticker,
        "dtypes": s.balance_sheet_types,
    }
    utils.format_buffer(ds, s.buffer_fmp_a_bs, yesterday=utils.strbool(yesterday))
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.fmp_a_bs,
        buffer_loc=s.buffer_fmp_a_bs,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-balance-sheet-collection",
        **config_,
    )


def collect_balance_sheets_quarter(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.BALANCE_SHEET_Q,
        "buffer_dir": s.buffer_fmp_q_bs,
        "dl_ticker_dir": s.fmp_q_bs_ticker,
        "dtypes": s.balance_sheet_types,
    }
    utils.format_buffer(ds, s.buffer_fmp_q_bs, yesterday=utils.strbool(yesterday))
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.fmp_q_bs,
        buffer_loc=s.buffer_fmp_q_bs,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-balance-sheet-quarter-collection",
        **config_,
    )


def collect_cash_flow(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.CASH_FLOW,
        "buffer_dir": s.buffer_fmp_a_cf,
        "dl_ticker_dir": s.fmp_a_cf_ticker,
        "dtypes": s.cash_flow_types,
    }
    utils.format_buffer(ds, s.buffer_fmp_a_cf, yesterday=utils.strbool(yesterday))
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.fmp_a_cf,
        buffer_loc=s.buffer_fmp_a_cf,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-cash-flow-collection",
        **config_,
    )


def collect_cash_flow_quarter(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.CASH_FLOW_Q,
        "buffer_dir": s.buffer_fmp_q_cf,
        "dl_ticker_dir": s.fmp_q_cf_ticker,
        "dtypes": s.cash_flow_types,
    }
    utils.format_buffer(ds, s.buffer_fmp_q_cf, yesterday=utils.strbool(yesterday))
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.fmp_q_cf,
        buffer_loc=s.buffer_fmp_q_cf,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-cash-flow-quarter-collection",
        **config_,
    )


def collect_income(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.INCOME_STATEMENT,
        "buffer_dir": s.buffer_fmp_a_i,
        "dl_ticker_dir": s.fmp_a_i_ticker,
        "dtypes": s.income_types,
    }
    utils.format_buffer(ds, s.buffer_fmp_a_i, yesterday=utils.strbool(yesterday))
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.fmp_a_i,
        buffer_loc=s.buffer_fmp_a_i,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-income-collection",
        **config_,
    )


def collect_income_quarter(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.INCOME_STATEMENT_Q,
        "buffer_dir": s.buffer_fmp_q_i,
        "dl_ticker_dir": s.fmp_q_i_ticker,
        "dtypes": s.income_types,
    }
    utils.format_buffer(ds, s.buffer_fmp_q_i, yesterday=utils.strbool(yesterday))
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.fmp_q_i,
        buffer_loc=s.buffer_fmp_q_i,
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


def collect_stock_news(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.STOCK_NEWS,
        "buffer_dir": s.buffer_stock_news,
        "dl_ticker_dir": s.stock_news_ticker,
        "dtypes": s.stock_news_types,
        "date_col": "publishedDate",
    }
    utils.format_buffer(ds, s.buffer_stock_news, yesterday=utils.strbool(yesterday))
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.stock_news,
        buffer_loc=s.buffer_stock_news,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-stock-news-collection",
        **config_,
    )


def collect_press_releases(ds: dt.date, yesterday: bool = True):
    config_ = {
        "add_ticker": False,
        "url": s.PRESS_RELEASE,
        "buffer_dir": s.buffer_press_releases,
        "dl_ticker_dir": s.press_releases_ticker,
        "dtypes": s.press_release_types,
    }
    utils.format_buffer(ds, s.buffer_press_releases, yesterday=utils.strbool(yesterday))
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.press_releases,
        buffer_loc=s.buffer_press_releases,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-press-releases-collection",
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
    watch_list = utils.get_watchlist(extend=True) + ["^VIX"]
    [collect_full_price(ds, ticker, buffer=False) for ticker in watch_list]
