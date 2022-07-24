import common.generic as generic
from time import sleep
import pandas as pd
import datetime as dt
import requests
import os
import fmp.settings as s
import common.utils as utils

## spark
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf

FMP_KEY = os.environ["FMP_KEY"]


def collect_sector_performance():
    """Collect all sector performance data."""
    url = s.SECTORS_PERFORMANCE.format(API=FMP_KEY)
    r = requests.get(url)
    data = r.json()
    if len(data) == 0:
        False
    df = pd.DataFrame(data)
    df_typed = utils.format_data(df, s.sectors_performance_types)
    df_typed.to_parquet(s.historical_sectors_performance)
    return True


def collect_sector_pe(ds: dt.date, yesterday: bool = True):
    """Collect sector price earnings ration by date."""
    ds = pd.to_datetime(ds).date()
    if utils.strbool(yesterday):
        ds = ds - dt.timedelta(1)
    config_ = {
        "ds": ds,
        "url": s.SECTOR_PE.format(DS=ds, API=FMP_KEY),
        "dl_dir": s.sector_pe,
        "dtypes": s.sector_pe_types,
    }
    return generic.collect_generic_daily(**config_)


def collect_industry_pe(ds: dt.date, yesterday: bool = True):
    """Collect sector price earnings ration by date."""
    ds = pd.to_datetime(ds).date()
    if utils.strbool(yesterday):
        ds = ds - dt.timedelta(1)
    config_ = {
        "ds": ds,
        "url": s.INDUSTRY_PE.format(DS=ds, API=FMP_KEY),
        "dl_dir": s.industry_pe,
        "dtypes": s.industry_pe_types,
    }
    return generic.collect_generic_daily(**config_)


def collect_treasury_rate(ds: dt.date, yesterday: bool = True):
    """Collect sector price earnings ration by date."""
    ds = pd.to_datetime(ds).date()
    if utils.strbool(yesterday):
        ds = ds - dt.timedelta(1)
    config_ = {
        "ds": ds,
        "url": s.TREASURY.format(DSS=ds, DSE=ds, API=FMP_KEY),
        "dl_dir": s.treasury_rates,
        "dtypes": s.treasury_rates_types,
    }
    return generic.collect_generic_daily(**config_)


def collect_economic_indicator(ei: str, ei_dir: str, ei_types: dict):
    """Collect economic indicator data."""
    ds = dt.datetime.now().date()
    url = s.ECONOMIC_INDICATORS.format(INDICATOR=ei, DSE=ds, API=FMP_KEY)
    r = requests.get(url)
    if r.status_code == 429:
        return ei
    data = r.json()
    if len(data) == 0:
        False
    df = pd.DataFrame(data)
    df["economic_indicator"] = ei
    df_typed = utils.format_data(df, ei_types)
    df_typed.to_parquet(f"{ei_dir}/data.parquet", index=False)
    return True


def collect_all_economic_indicators():
    ei_list = [
        ["GDP", s.gdp, s.gdp_types],
        ["realGDP", s.real_gdp, s.real_gdp_types],
        ["nominalPotentialGDP", s.nominal_potential_gdp, s.nominal_potential_gdp_types],
        ["realGDPPerCapita", s.real_gdp_per_capita, s.real_gdp_per_capita_types],
        ["federalFunds", s.federal_funds, s.federal_funds_types],
        ["CPI", s.cpi, s.cpi_types],
        ["inflationRate", s.inflation_rate, s.inflation_rate_types],
        ["inflation", s.inflation, s.inflation_types],
        ["retailSales", s.retail_sales, s.retail_sales_types],
        ["consumerSentiment", s.consumer_sentiment, s.consumer_sentiment_types],
        ["durableGoods", s.durable_goods, s.durable_goods_types],
        ["unemploymentRate", s.unemployment_rate, s.unemployment_rate_types],
        ["totalNonfarmPayroll", s.total_nonfarm_payroll, s.total_nonfarm_payroll_types],
        ["initialClaims", s.initial_claims, s.initial_claims_types],
        [
            "industrialProductionTotalIndex",
            s.industrial_production_total_index,
            s.industrial_production_total_index_types,
        ],
        [
            "newPrivatelyOwnedHousingUnitsStartedTotalUnits",
            s.new_privately_owned_housing_units_started_total_units,
            s.new_privately_owned_housing_units_started_total_units_types,
        ],
        ["totalVehicleSales", s.total_vehicle_sales, s.total_vehicle_sales_types],
        ["retailMoneyFunds", s.retail_money_funds, s.retail_money_funds_types],
        [
            "smoothedUSRecessionProbabilities",
            s.smoothed_us_recession_probabilities,
            s.smoothed_us_recession_probabilities_types,
        ],
        [
            "3MonthOr90DayRatesAndYieldsCertificatesOfDeposit",
            s.ci_3_month_or_90_day_rates_and_yields_certificates_of_deposit,
            s.ci_3_month_or_90_day_rates_and_yields_certificates_of_deposit_types,
        ],
        [
            "commercialBankInterestRateOnCreditCardPlansAllAccounts",
            s.commercial_bank_interest_rate_on_credit_card_plans_all_accounts,
            s.commercial_bank_interest_rate_on_credit_card_plans_all_accounts_types,
        ],
        [
            "30YearFixedRateMortgageAverage",
            s.ci_30_year_fixed_rate_mortgage_average,
            s.ci_30_year_fixed_rate_mortgage_average_types,
        ],
        [
            "15YearFixedRateMortgageAverage",
            s.ci_15_year_fixed_rate_mortgage_average,
            s.ci_15_year_fixed_rate_mortgage_average_types,
        ],
    ]
    while len(ei_list) > 0:
        return_list = []
        for ei, ei_dir, ei_types in ei_list:
            return_list.append(collect_economic_indicator(ei, ei_dir, ei_types))
        return_df = pd.DataFrame(return_list, columns=["return"])
        bad_response_df = return_df.loc[~return_df["return"].isin([True, False])]
        ei_list = bad_response_df["return"].tolist()
