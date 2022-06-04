import pandas as pd
import numpy as np
import datetime as dt
import requests
import os
import yahoo.settings as s
import common.utils as utils
import glob
from time import sleep

## spark
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf


def collect_insights(ticker: str):
    """
    General FMP collection for tickers.

    Inputs:
        - ticker: ticker
    """
    ds = dt.datetime.now().date()
    ## request data
    url = s.INSIGHTS.format(TICKER=ticker)
    headers = {"x-api-key": os.environ["YAHOO_KEY"]}
    r = requests.get(url, headers=headers)
    if r.status_code == 429:
        return ticker
    elif r.status_code == 404:
        return False
    data = r.json()
    if len(data) == 0:
        return False
    ## unpack into three datasets
    json_result = r.json()["finance"]["result"]
    if "reports" in json_result.keys():
        reports = pd.DataFrame(json_result["reports"])
    else:
        reports = pd.DataFrame()
    if "companySnapshot" in json_result.keys():
        company_cols = {
            "innovativeness": "company_innovativeness",
            "hiring": "company_hiring",
            "sustainability": "company_sustainability",
            "insiderSentiments": "company_insider_sentiments",
            "earningsReports": "company_earnings_reports",
            "dividends": "company_dividends",
        }

        sector_cols = {
            "innovativeness": "sector_innovativeness",
            "hiring": "sector_hiring",
            "sustainability": "sector_sustainability",
            "insiderSentiments": "sector_insiderSentiments",
            "earningsReports": "sector_earningsReports",
            "dividends": "sector_dividends",
        }
        data = []
        if "sectorInfo" in json_result["companySnapshot"].keys():
            data.append(
                pd.DataFrame(
                    [json_result["companySnapshot"]["sectorInfo"]],
                    columns=["sector_info"],
                )
            )
        if "company" in json_result["companySnapshot"].keys():
            data.append(
                pd.DataFrame([json_result["companySnapshot"]["company"]]).rename(
                    columns=company_cols
                )
            )
        if "sector" in json_result["companySnapshot"].keys():
            data.append(
                pd.DataFrame([json_result["companySnapshot"]["company"]]).rename(
                    columns=sector_cols
                )
            )
        company_shapshot = pd.concat(data, axis=1)
    else:
        company_shapshot = pd.DataFrame()

    if "instrumentInfo" in json_result.keys():
        evnets_cols = {
            "provider": "technical_evensts_provider",
            "shortTerm": "short_term",
            "midTerm": "mid_term",
            "longTerm": "long_term",
        }
        key_cols = {"provider": "key_technicals_provider", "stopLoss": "stop_loss"}
        valuation_cols = {
            "provider": "valuation_provider",
            "relativeValue": "relative_value",
        }
        recommendation_cols = {
            "targetPrice": "target_price",
            "provider": "recommendation_provider",
        }
        data = []
        if "technicalEvents" in json_result["instrumentInfo"].keys():
            data.append(
                pd.DataFrame([json_result["instrumentInfo"]["technicalEvents"]]).rename(
                    columns=evnets_cols
                )
            )
        if "keyTechnicals" in json_result["instrumentInfo"].keys():
            data.append(
                pd.DataFrame([json_result["instrumentInfo"]["keyTechnicals"]]).rename(
                    columns=key_cols
                )
            )
        if "valuation" in json_result["instrumentInfo"].keys():
            data.append(
                pd.DataFrame([json_result["instrumentInfo"]["valuation"]]).rename(
                    columns=valuation_cols
                )
            )
        if "recommendation" in json_result["instrumentInfo"].keys():
            data.append(
                pd.DataFrame([json_result["instrumentInfo"]["recommendation"]]).rename(
                    columns=recommendation_cols
                )
            )
        instrument_info = pd.concat(data, axis=1)
    else:
        instrument_info = pd.DataFrame()
    ## add ticker
    reports["ticker"] = ticker
    company_shapshot["ticker"] = ticker
    instrument_info["ticker"] = ticker
    ## add date
    reports["date"] = ds
    company_shapshot["date"] = ds
    instrument_info["date"] = ds
    ## type
    reports_typed = utils.format_data(reports, s.reports_types)
    company_shapshot_typed = utils.format_data(
        company_shapshot, s.company_shapshot_types
    )
    instrument_info_typed = utils.format_data(instrument_info, s.instrument_info_types)
    ## save buffer for daily
    reports_typed.to_parquet(s.buffer_reports + f"/{ds}/{ticker}.parquet", index=False)
    company_shapshot_typed.to_parquet(
        s.buffer_company_shapshot + f"/{ds}/{ticker}.parquet", index=False
    )
    instrument_info_typed.to_parquet(
        s.buffer_instrument_info + f"/{ds}/{ticker}.parquet", index=False
    )
    ## append previously collected
    if os.path.isfile(s.reports_ticker + "/{ticker}.parquet"):
        reports_typed = reports_typed.append(
            pd.read_parquet(s.reports_ticker + "/{ticker}.parquet"), ignore_index=True
        )
    if os.path.isfile(s.company_shapshot_ticker + "/{ticker}.parquet"):
        company_shapshot_typed = company_shapshot_typed.append(
            pd.read_parquet(s.company_shapshot_ticker + "/{ticker}.parquet"),
            ignore_index=True,
        )
    if os.path.isfile(s.instrument_info_ticker + "/{ticker}.parquet"):
        instrument_info_typed = instrument_info_typed.append(
            pd.read_parquet(s.instrument_info_ticker + "/{ticker}.parquet"),
            ignore_index=True,
        )
    ## save full file
    reports_typed.to_parquet(s.reports_ticker + f"/{ticker}.parquet", index=False)
    company_shapshot_typed.to_parquet(
        s.company_shapshot_ticker + f"/{ticker}.parquet", index=False
    )
    instrument_info_typed.to_parquet(
        s.instrument_info_ticker + f"/{ticker}.parquet", index=False
    )
    return True


def collect_insights_distributed():
    """Distribute collection of insights from Yahoo's API."""
    ## format buffer
    ds = dt.datetime.now().date()
    utils.format_buffer(ds, s.buffer_reports, yesterday=False)
    utils.format_buffer(ds, s.buffer_company_shapshot, yesterday=False)
    utils.format_buffer(ds, s.buffer_instrument_info, yesterday=False)
    collection_list = utils.get_high_volume(ds)
    print("{} to collect.\n\n".format(len(collection_list)))
    while len(collection_list) > 0:
        print("attempting to collect")
        spark = SparkSession.builder.appName(f"collect-yahoo-insights").getOrCreate()
        sc = spark.sparkContext
        return_list = (
            sc.parallelize(collection_list).map(lambda r: collect_insights(r)).collect()
        )
        return_df = pd.DataFrame(return_list, columns=["return"])
        bad_response_df = return_df.loc[~return_df["return"].isin([True, False])]
        collection_list = bad_response_df["return"].tolist()
        sc.stop()
        spark.stop()
        if len(collection_list) > 0:
            sleep(60)

    ## reports
    if len(glob.glob(s.buffer_reports + f"/{ds}/*.parquet")) > 0:
        ## migrate from buffer to data-lake
        spark = SparkSession.builder.appName(f"coalesce-reports").getOrCreate()
        (
            spark.read.format("parquet")
            .option("path", s.buffer_reports + f"/{ds}/*.parquet")
            .load()
            .write.format("parquet")
            .mode("overwrite")
            .save(s.reports_daily + f"/{ds}", header=True)
        )
        spark.stop()
    utils.clear_buffer(s.buffer_reports.split("/data/buffer")[1])
    ## shapshots
    if len(glob.glob(s.buffer_company_shapshot + f"/{ds}/*.parquet")) > 0:
        ## migrate from buffer to data-lake
        spark = SparkSession.builder.appName(
            f"coalesce-company-snapshots"
        ).getOrCreate()
        (
            spark.read.format("parquet")
            .option("path", s.buffer_company_shapshot + f"/{ds}/*.parquet")
            .load()
            .write.format("parquet")
            .mode("overwrite")
            .save(s.company_shapshot_daily + f"/{ds}", header=True)
        )
        spark.stop()
    utils.clear_buffer(s.buffer_company_shapshot.split("/data/buffer")[1])
    ## instrument info
    if len(glob.glob(s.buffer_instrument_info + f"/{ds}/*.parquet")) > 0:
        ## migrate from buffer to data-lake
        spark = SparkSession.builder.appName(f"coalesce-intrument-info").getOrCreate()
        (
            spark.read.format("parquet")
            .option("path", s.buffer_instrument_info + f"/{ds}/*.parquet")
            .load()
            .write.format("parquet")
            .mode("overwrite")
            .save(s.instrument_info_daily + f"/{ds}", header=True)
        )
        spark.stop()
    utils.clear_buffer(s.buffer_instrument_info.split("/data/buffer")[1])
