import common.generic as generic
from time import sleep
import pandas as pd
import datetime as dt
import requests
import os
import fmp.settings as s
import common.utils as utils
import glob

## spark
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf

FMP_KEY = os.environ["FMP_KEY"]

## Collection
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


## Put together
def merge_signals():
    """
    Put necessary macro signals into one dataset and write to DL.
    This will be used to observe the health of the economy
    as well as for ML models.

    """
    ## Load Economic Calendar
    # Many signals use this
    econ_cal_df = pd.concat(
        [pd.read_parquet(x) for x in glob.glob(f"{s.economic_calendar}/*")]
    )
    econ_cal_df["date"] = pd.to_datetime(econ_cal_df["date"]).apply(lambda r: r.date())
    econ_cal_df = (
        econ_cal_df.loc[
            ((econ_cal_df["country"] == "US") & (econ_cal_df["actual"].notnull()))
        ]
        .sort_values("date")
        .drop_duplicates(["event", "date"], ignore_index=True)
    )

    ## GPD
    with open(f"{os.environ['DL_DIR']}/ycharts/raw/dgp.txt", "r") as file:
        data = file.read()

    gpd_df = pd.DataFrame(
        [x.split("\t") for x in data.split("\n")],
        columns=["release_date", "gdp_change"],
    )
    gpd_df["release_date"] = pd.to_datetime(gpd_df["release_date"]).apply(
        lambda r: r.date()
    )
    gpd_df = gpd_df.sort_values("release_date", ascending=False)
    gpd_df["gdp_change"] = gpd_df["gdp_change"].apply(lambda r: float(r.strip("%")))
    gpd_df["gdp_date"] = gpd_df["release_date"]
    gpd_df.to_parquet(f"{os.environ['DL_DIR']}/ycharts/clean/dgp.parquet")

    ## Employment figures
    use_cols = ["date", "value"]
    rename_cols = {"date": "unemployment_date", "value": "unemployment_rate"}
    unemployment_rate_df = pd.read_parquet(
        f"{s.unemployment_rate}/data.parquet", columns=use_cols
    ).rename(columns=rename_cols)
    unemployment_rate_df["release_date"] = unemployment_rate_df[
        "unemployment_date"
    ].apply(lambda r: r + dt.timedelta(weeks=4))
    rename_cols = {
        "date": "release_date",
        "actual": "adp_employment_actual",
        "previous": "adp_employment_previous",
        "change": "adp_employment_change",
    }
    employment_rate = (
        econ_cal_df.loc[
            econ_cal_df["event"].apply(lambda r: "ADP Employment Change (" in r)
        ]
        .sort_values("date")
        .drop_duplicates(["date", "actual"], ignore_index=True)[
            ["date", "event", "actual", "previous", "change"]
        ]
        .rename(columns=rename_cols)
    )
    index = employment_rate.loc[
        employment_rate["event"].apply(lambda r: len(r.split("("))) == 2
    ].index
    fixed_dates = [
        pd.to_datetime("{} 1 {}".format(x[0].split("(")[1].split(")")[0], x[1].year))
        for x in employment_rate.loc[index, ["event", "release_date"]].values
    ]
    employment_rate.loc[index, "adp_employment_date"] = pd.Series(
        fixed_dates, index=index
    )
    fix_year_index = employment_rate.loc[
        employment_rate["release_date"] < employment_rate["adp_employment_date"]
    ].index
    employment_rate.loc[fix_year_index, "adp_employment_date"] = employment_rate.loc[
        fix_year_index, "adp_employment_date"
    ].apply(lambda r: r - dt.timedelta(days=365))
    employment_rate = employment_rate.drop(columns=["event"]).sort_values(
        "adp_employment_date", ignore_index=True
    )
    full_employement_df = (
        pd.DataFrame(
            [x.date() for x in pd.date_range("2020-01-01", dt.datetime.now().date())],
            columns=["release_date"],
        )
        .merge(unemployment_rate_df, how="left", on="release_date")
        .sort_values("release_date", ascending=False)
        .merge(employment_rate, how="left", on="release_date")
        .sort_values(["release_date", "adp_employment_date"], ascending=False)
        .fillna(method="bfill")
    )
    clean_employement_df = full_employement_df.drop_duplicates(
        "release_date", ignore_index=True
    )

    ## Industrial Production
    rename_cols = {
        "date": "release_date",
        "actual": "industrial_production_actual",
        "previous": "industrial_production_previous",
        "change": "industrial_production_change",
    }
    industrial_production_df = (
        econ_cal_df.loc[
            econ_cal_df["event"].apply(lambda r: "ADP Employment Change (" in r)
        ]
        .sort_values("date")
        .drop_duplicates(["date", "actual"], ignore_index=True)[
            ["date", "event", "actual", "previous", "change"]
        ]
        .rename(columns=rename_cols)
    )
    index = industrial_production_df.loc[
        industrial_production_df["event"].apply(lambda r: len(r.split("("))) == 2
    ].index
    fixex_dates = [
        pd.to_datetime("{} 1 {}".format(x[0].split("(")[1].split(")")[0], x[1].year))
        for x in industrial_production_df.loc[index, ["event", "release_date"]].values
    ]
    industrial_production_df.loc[index, "industrial_production_date"] = pd.Series(
        fixex_dates, index=index
    )
    fix_year_index = industrial_production_df.loc[
        industrial_production_df["release_date"]
        < industrial_production_df["industrial_production_date"]
    ].index
    industrial_production_df.loc[
        fix_year_index, "industrial_production_date"
    ] = industrial_production_df.loc[
        fix_year_index, "industrial_production_date"
    ].apply(
        lambda r: r - dt.timedelta(days=365)
    )
    industrial_production_df = industrial_production_df.drop(
        columns=["event"]
    ).sort_values("industrial_production_date", ignore_index=True)

    ## Consumer Spending
    rename_cols = {
        "date": "consumer_spending_date",
        "actual": "consumer_spending_actual",
        "previous": "consumer_spending_previous",
        "change": "consumer_spending_change",
    }
    consumer_spending_df = (
        econ_cal_df.loc[
            econ_cal_df["event"].apply(
                lambda r: r.split(" (")[0] == "Real Consumer Spending QoQ"
            )
        ]
        .sort_values("date")
        .drop_duplicates(["date", "actual"], ignore_index=True)[
            ["date", "actual", "previous", "change"]
        ]
        .rename(columns=rename_cols)
    )
    consumer_spending_df["release_date"] = consumer_spending_df[
        "consumer_spending_date"
    ].apply(lambda r: r + dt.timedelta(weeks=4))

    ## Consumer Sentiment
    rename_cols = {
        "date": "release_date",
        "actual": "consumer_sentiment_actual",
        "previous": "consumer_sentiment_previous",
        "change": "consumer_sentiment_change",
    }
    consumer_sentiment_df = (
        econ_cal_df.loc[
            econ_cal_df["event"].apply(lambda r: "ADP Employment Change (" in r)
        ]
        .sort_values("date")
        .drop_duplicates(["date", "actual"], ignore_index=True)[
            ["date", "event", "actual", "previous", "change"]
        ]
        .rename(columns=rename_cols)
    )
    index = consumer_sentiment_df.loc[
        consumer_sentiment_df["event"].apply(lambda r: len(r.split("("))) == 2
    ].index
    fixed_dates = [
        pd.to_datetime("{} 1 {}".format(x[0].split("(")[1].split(")")[0], x[1].year))
        for x in consumer_sentiment_df.loc[index, ["event", "release_date"]].values
    ]
    consumer_sentiment_df.loc[index, "consumer_sentiment_date"] = pd.Series(
        fixed_dates, index=index
    )
    fix_year_index = consumer_sentiment_df.loc[
        consumer_sentiment_df["release_date"]
        < consumer_sentiment_df["consumer_sentiment_date"]
    ].index
    consumer_sentiment_df.loc[
        fix_year_index, "consumer_sentiment_date"
    ] = consumer_sentiment_df.loc[fix_year_index, "consumer_sentiment_date"].apply(
        lambda r: r - dt.timedelta(days=365)
    )
    consumer_sentiment_df = consumer_sentiment_df.drop(columns=["event"]).sort_values(
        "consumer_sentiment_date", ignore_index=True
    )

    ## Inflation
    rename_cols = {
        "date": "release_date",
        "actual": "core_inflation_actual",
        "previous": "core_inflation_previous",
        "change": "core_inflation_change",
    }
    core_inflation_df = (
        econ_cal_df.loc[
            econ_cal_df["event"].apply(
                lambda r: r.split(" (")[0] == "Core Inflation Rate YoY"
            )
        ]
        .sort_values("date")
        .drop_duplicates(["date", "actual"], ignore_index=True)[
            ["date", "event", "actual", "previous", "change"]
        ]
        .rename(columns=rename_cols)
    )

    index = core_inflation_df.loc[
        core_inflation_df["event"].apply(lambda r: len(r.split("("))) == 2
    ].index
    fixed_dates = [
        pd.to_datetime("{} 1 {}".format(x[0].split("(")[1].split(")")[0], x[1].year))
        for x in core_inflation_df.loc[index, ["event", "release_date"]].values
    ]
    core_inflation_df.loc[index, "core_inflation_date"] = pd.Series(
        fixed_dates, index=index
    )
    fix_year_index = core_inflation_df.loc[
        core_inflation_df["release_date"] < core_inflation_df["core_inflation_date"]
    ].index
    core_inflation_df.loc[
        fix_year_index, "core_inflation_date"
    ] = core_inflation_df.loc[fix_year_index, "core_inflation_date"].apply(
        lambda r: r - dt.timedelta(days=365)
    )
    core_inflation_df = core_inflation_df.drop(columns=["event"]).sort_values(
        "core_inflation_date", ignore_index=True
    )
    ##
    rename_cols = {
        "date": "release_date",
        "actual": "inflation_rate_actual",
        "previous": "inflation_rate_previous",
        "change": "inflation_rate_change",
    }
    inflation_rate_df = (
        econ_cal_df.loc[
            econ_cal_df["event"].apply(
                lambda r: r.split(" (")[0] == "Inflation Rate YoY"
            )
        ]
        .sort_values("date")
        .drop_duplicates(["date", "actual"], ignore_index=True)[
            ["date", "event", "actual", "previous", "change"]
        ]
        .rename(columns=rename_cols)
    )

    index = inflation_rate_df.loc[
        inflation_rate_df["event"].apply(lambda r: len(r.split("("))) == 2
    ].index
    fixed_dates = [
        pd.to_datetime("{} 1 {}".format(x[0].split("(")[1].split(")")[0], x[1].year))
        for x in inflation_rate_df.loc[index, ["event", "release_date"]].values
    ]
    inflation_rate_df.loc[index, "inflation_rate_date"] = pd.Series(
        fixed_dates, index=index
    )
    fix_year_index = inflation_rate_df.loc[
        inflation_rate_df["release_date"] < inflation_rate_df["inflation_rate_date"]
    ].index
    inflation_rate_df.loc[
        fix_year_index, "inflation_rate_date"
    ] = inflation_rate_df.loc[fix_year_index, "inflation_rate_date"].apply(
        lambda r: r - dt.timedelta(days=365)
    )
    inflation_rate_df = inflation_rate_df.drop(columns=["event"]).sort_values(
        "inflation_rate_date", ignore_index=True
    )
    ##
    rename_cols = {
        "date": "release_date",
        "actual": "consumer_inflation_expectations_actual",
        "previous": "consumer_inflation_expectations_previous",
        "change": "consumer_inflation_expectations_change",
    }
    consumer_inflation_expectations_df = (
        econ_cal_df.loc[
            econ_cal_df["event"].apply(
                lambda r: r.split(" (")[0] == "Consumer Inflation Expectations"
            )
        ]
        .sort_values("date")
        .drop_duplicates(["date", "actual"], ignore_index=True)[
            ["date", "event", "actual", "previous", "change"]
        ]
        .rename(columns=rename_cols)
    )

    index = consumer_inflation_expectations_df.loc[
        consumer_inflation_expectations_df["event"].apply(lambda r: len(r.split("(")))
        == 2
    ].index
    fixed_dates = [
        pd.to_datetime("{} 1 {}".format(x[0].split("(")[1].split(")")[0], x[1].year))
        for x in consumer_inflation_expectations_df.loc[
            index, ["event", "release_date"]
        ].values
    ]
    consumer_inflation_expectations_df.loc[
        index, "consumer_inflation_expectations_date"
    ] = pd.Series(fixed_dates, index=index)
    fix_year_index = consumer_inflation_expectations_df.loc[
        consumer_inflation_expectations_df["release_date"]
        < consumer_inflation_expectations_df["consumer_inflation_expectations_date"]
    ].index
    consumer_inflation_expectations_df.loc[
        fix_year_index, "consumer_inflation_expectations_date"
    ] = consumer_inflation_expectations_df.loc[
        fix_year_index, "consumer_inflation_expectations_date"
    ].apply(
        lambda r: r - dt.timedelta(days=365)
    )
    consumer_inflation_expectations_df = consumer_inflation_expectations_df.drop(
        columns=["event"]
    ).sort_values("consumer_inflation_expectations_date", ignore_index=True)

    ## Home Sales
    rename_cols = {
        "date": "release_date",
        "actual": "new_home_sales_actual",
        "previous": "new_home_sales_previous",
        "change": "new_home_sales_change",
    }
    new_home_sales_df = (
        econ_cal_df.loc[
            econ_cal_df["event"].apply(lambda r: r.split(" (")[0]) == "New Home Sales"
        ]
        .loc[econ_cal_df["event"] != "New Home Sales (MoM)"]
        .sort_values("date")
        .drop_duplicates(["date", "actual"], ignore_index=True)[
            ["date", "event", "actual", "previous", "change"]
        ]
        .rename(columns=rename_cols)
    )

    index = new_home_sales_df.loc[
        new_home_sales_df["event"].apply(lambda r: len(r.split("("))) == 2
    ].index
    fixed_dates = [
        pd.to_datetime("{} 1 {}".format(x[0].split("(")[1].split(")")[0], x[1].year))
        for x in new_home_sales_df.loc[index, ["event", "release_date"]].values
    ]
    new_home_sales_df.loc[index, "new_home_sales_date"] = pd.Series(
        fixed_dates, index=index
    )
    fix_year_index = new_home_sales_df.loc[
        new_home_sales_df["release_date"] < new_home_sales_df["new_home_sales_date"]
    ].index
    new_home_sales_df.loc[
        fix_year_index, "new_home_sales_date"
    ] = new_home_sales_df.loc[fix_year_index, "new_home_sales_date"].apply(
        lambda r: r - dt.timedelta(days=365)
    )
    new_home_sales_df = new_home_sales_df.drop(columns=["event"]).sort_values(
        "new_home_sales_date", ignore_index=True
    )
    ##
    rename_cols = {
        "date": "release_date",
        "actual": "existing_home_sales_actual",
        "previous": "existing_home_sales_previous",
        "change": "existing_home_sales_change",
    }
    existing_home_sales_df = (
        econ_cal_df.loc[
            econ_cal_df["event"].apply(lambda r: r.split(" (")[0])
            == "Existing Home Sales"
        ]
        .loc[econ_cal_df["event"] != "Existing Home Sales (MoM)"]
        .sort_values("date")
        .drop_duplicates(["date", "actual"], ignore_index=True)[
            ["date", "event", "actual", "previous", "change"]
        ]
        .rename(columns=rename_cols)
    )

    index = existing_home_sales_df.loc[
        existing_home_sales_df["event"].apply(lambda r: len(r.split("("))) == 2
    ].index
    fixed_dates = [
        pd.to_datetime("{} 1 {}".format(x[0].split("(")[1].split(")")[0], x[1].year))
        for x in existing_home_sales_df.loc[index, ["event", "release_date"]].values
    ]
    existing_home_sales_df.loc[index, "existing_home_sales_date"] = pd.Series(
        fixed_dates, index=index
    )
    fix_year_index = existing_home_sales_df.loc[
        existing_home_sales_df["release_date"]
        < existing_home_sales_df["existing_home_sales_date"]
    ].index
    existing_home_sales_df.loc[
        fix_year_index, "existing_home_sales_date"
    ] = existing_home_sales_df.loc[fix_year_index, "existing_home_sales_date"].apply(
        lambda r: r - dt.timedelta(days=365)
    )
    existing_home_sales_df = existing_home_sales_df.drop(columns=["event"]).sort_values(
        "existing_home_sales_date", ignore_index=True
    )

    ## Building permits
    rename_cols = {
        "date": "release_date",
        "actual": "building_permits_actual",
        "previous": "building_permits_previous",
        "change": "building_permits_change",
    }
    building_permits_df = (
        econ_cal_df.loc[
            econ_cal_df["event"].apply(lambda r: r.split(" (")[0]) == "Building Permits"
        ]
        .loc[econ_cal_df["event"] != "Building Permits (MoM)"]
        .sort_values("date")
        .drop_duplicates(["date", "actual"], ignore_index=True)[
            ["date", "event", "actual", "previous", "change"]
        ]
        .rename(columns=rename_cols)
    )

    index = building_permits_df.loc[
        building_permits_df["event"].apply(lambda r: len(r.split("("))) == 2
    ].index
    fixed_dates = [
        pd.to_datetime("{} 1 {}".format(x[0].split("(")[1].split(")")[0], x[1].year))
        for x in building_permits_df.loc[index, ["event", "release_date"]].values
    ]
    building_permits_df.loc[index, "building_permits_date"] = pd.Series(
        fixed_dates, index=index
    )
    fix_year_index = building_permits_df.loc[
        building_permits_df["release_date"]
        < building_permits_df["building_permits_date"]
    ].index
    building_permits_df.loc[
        fix_year_index, "building_permits_date"
    ] = building_permits_df.loc[fix_year_index, "building_permits_date"].apply(
        lambda r: r - dt.timedelta(days=365)
    )
    building_permits_df = building_permits_df.drop(columns=["event"]).sort_values(
        "building_permits_date", ignore_index=True
    )

    ## Construction Spending
    rename_cols = {
        "date": "release_date",
        "actual": "construction_spending_actual",
        "previous": "construction_spending_previous",
        "change": "construction_spending_change",
    }
    construction_spending_df = (
        econ_cal_df.loc[
            econ_cal_df["event"].apply(lambda r: "Construction Spending" in r)
        ]
        .loc[econ_cal_df["event"] != "Construction Spending (MoM)"]
        .sort_values("date")
        .drop_duplicates(["date", "actual"], ignore_index=True)[
            ["date", "event", "actual", "previous", "change"]
        ]
        .rename(columns=rename_cols)
    )
    index = construction_spending_df.loc[
        construction_spending_df["event"].apply(lambda r: len(r.split("("))) == 2
    ].index
    fixed_dates = [
        pd.to_datetime("{} 1 {}".format(x[0].split("(")[1].split(")")[0], x[1].year))
        for x in construction_spending_df.loc[index, ["event", "release_date"]].values
    ]
    construction_spending_df.loc[index, "construction_spending_date"] = pd.Series(
        fixed_dates, index=index
    )
    fix_year_index = construction_spending_df.loc[
        construction_spending_df["release_date"]
        < construction_spending_df["construction_spending_date"]
    ].index
    construction_spending_df.loc[
        fix_year_index, "construction_spending_date"
    ] = construction_spending_df.loc[
        fix_year_index, "construction_spending_date"
    ].apply(
        lambda r: r - dt.timedelta(days=365)
    )
    construction_spending_df = construction_spending_df.drop(
        columns=["event"]
    ).sort_values("construction_spending_date", ignore_index=True)

    ## Manufacturing Demand
    rename_cols = {
        "date": "release_date",
        "actual": "manufacturing_demand_actual",
        "previous": "manufacturing_demand_previous",
        "change": "manufacturing_demand_change",
    }
    manufacturing_demand_df = (
        econ_cal_df.loc[
            econ_cal_df["event"].apply(lambda r: "ISM Manufacturing PMI" in r)
        ]
        .sort_values("date")
        .drop_duplicates(["date", "actual"], ignore_index=True)[
            ["date", "event", "actual", "previous", "change"]
        ]
        .rename(columns=rename_cols)
    )

    index = manufacturing_demand_df.loc[
        manufacturing_demand_df["event"].apply(lambda r: len(r.split("("))) == 2
    ].index
    fixed_dates = [
        pd.to_datetime("{} 1 {}".format(x[0].split("(")[1].split(")")[0], x[1].year))
        for x in manufacturing_demand_df.loc[index, ["event", "release_date"]].values
    ]
    manufacturing_demand_df.loc[index, "manufacturing_demand_date"] = pd.Series(
        fixed_dates, index=index
    )
    fix_year_index = manufacturing_demand_df.loc[
        manufacturing_demand_df["release_date"]
        < manufacturing_demand_df["manufacturing_demand_date"]
    ].index
    manufacturing_demand_df.loc[
        fix_year_index, "manufacturing_demand_date"
    ] = manufacturing_demand_df.loc[fix_year_index, "manufacturing_demand_date"].apply(
        lambda r: r - dt.timedelta(days=365)
    )
    manufacturing_demand_df = manufacturing_demand_df.drop(
        columns=["event"]
    ).sort_values("manufacturing_demand_date", ignore_index=True)

    ## Retail Sales
    rename_cols = {
        "date": "release_date",
        "actual": "retail_sales_actual",
        "previous": "retail_sales_previous",
        "change": "retail_sales_change",
    }
    retail_sales_df = (
        econ_cal_df.loc[econ_cal_df["event"].apply(lambda r: "Retail Sales MoM" in r)]
        .sort_values("date")
        .drop_duplicates(["date", "actual"], ignore_index=True)[
            ["date", "event", "actual", "previous", "change"]
        ]
        .rename(columns=rename_cols)
    )
    index = retail_sales_df.loc[
        retail_sales_df["event"].apply(lambda r: len(r.split("("))) == 2
    ].index
    fixed_dates = [
        pd.to_datetime("{} 1 {}".format(x[0].split("(")[1].split(")")[0], x[1].year))
        for x in retail_sales_df.loc[index, ["event", "release_date"]].values
    ]
    retail_sales_df.loc[index, "retail_sales_date"] = pd.Series(
        fixed_dates, index=index
    )
    fix_year_index = retail_sales_df.loc[
        retail_sales_df["release_date"] < retail_sales_df["retail_sales_date"]
    ].index
    retail_sales_df.loc[fix_year_index, "retail_sales_date"] = retail_sales_df.loc[
        fix_year_index, "retail_sales_date"
    ].apply(lambda r: r - dt.timedelta(days=365))
    retail_sales_df = retail_sales_df.drop(columns=["event"]).sort_values(
        "retail_sales_date", ignore_index=True
    )

    ## Interest Rate
    rename_cols = {
        "date": "release_date",
        "actual": "interest_rate_decision_actual",
        "previous": "interest_rate_decision_previous",
        "change": "interest_rate_decision_change",
    }
    interest_rate_decision_df = (
        econ_cal_df.loc[
            econ_cal_df["event"].apply(lambda r: "interest rate decision" in r.lower())
        ]
        .sort_values("date")
        .drop_duplicates(["date", "actual"], ignore_index=True)[
            ["date", "actual", "previous", "change"]
        ]
        .rename(columns=rename_cols)
    )
    interest_rate_decision_df[
        "interest_rate_decision_date"
    ] = interest_rate_decision_df["release_date"]

    ## Full macro signals
    BASE_DATE_DF = pd.DataFrame(
        [x.date() for x in pd.date_range("2021-01-01", dt.datetime.now().date())],
        columns=["release_date"],
    )
    maco_signals_df = (
        BASE_DATE_DF.merge(gpd_df, how="left", on="release_date")
        .merge(clean_employement_df, how="left", on="release_date")
        .merge(industrial_production_df, how="left", on="release_date")
        .merge(consumer_spending_df, how="left", on="release_date")
        .merge(consumer_sentiment_df, how="left", on="release_date")
        .merge(core_inflation_df, how="left", on="release_date")
        .merge(inflation_rate_df, how="left", on="release_date")
        .merge(consumer_inflation_expectations_df, how="left", on="release_date")
        .merge(new_home_sales_df, how="left", on="release_date")
        .merge(existing_home_sales_df, how="left", on="release_date")
        .merge(building_permits_df, how="left", on="release_date")
        .merge(construction_spending_df, how="left", on="release_date")
        .merge(manufacturing_demand_df, how="left", on="release_date")
        .merge(retail_sales_df, how="left", on="release_date")
        .merge(interest_rate_decision_df, how="left", on="release_date")
        .fillna(method="ffill")
        .dropna()
    )
    maco_signals_df.to_parquet(f"{s.full_macro}/data.parquet", index=False)
