## spark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

import common.generic as generic
from typing import Callable
import pandas as pd
import datetime as dt
import requests
import os
import fmp.settings as s
import common.utils as utils
from io import StringIO

FMP_KEY = os.environ["FMP_KEY"]


def collect_full_price(ds: dt.date, ticker: str):
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
    df_typed_end.to_parquet(
        s.buffer_historical_daily_price_full + f"/{ds}/{ticker}.parquet", index=False
    )
    df_typed.to_parquet(
        s.historical_ticker_price_full + f"/{ticker}.parquet", index=False
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
    utils.format_buffer(ds, s.buffer_dcf, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_historical_rating, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_enterprise_values_annual, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_enterprise_values_quarter, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_grade, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_social_sentiment, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_analyst_estimates, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_analyst_estimates_quarter, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_fmp_a_bs, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_fmp_q_bs, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_fmp_a_cf, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_fmp_q_cf, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_fmp_a_i, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_fmp_q_i, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_fmp_a_km, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_fmp_a_km, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_fmp_a_r, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_fmp_q_r, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_earnings_surprises, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_insider_trading, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_stock_news, yesterday=yesterday)
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
    utils.format_buffer(ds, s.buffer_press_releases, yesterday=yesterday)
    generic.collect_generic_distributed(
        get_distribution_list=utils.get_to_collect,
        dl_loc=s.press_releases,
        buffer_loc=s.buffer_press_releases,
        distribute_through=generic.collect_generic_ticker,
        spark_app="daily-press-releases-collection",
        **config_,
    )
