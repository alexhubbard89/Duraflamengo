import datetime as dt
import os

## paths
DL_DIR = os.environ["DL_DIR"]
BASE_TICKER_FN = DL_DIR + "/seed-data/nasdaq_screener_1628807233734.csv"
avg_price = DL_DIR + "/derived-measurements/avg-price-vol"
# market constituents
fmp_constituents = DL_DIR + "/fmp/market-constituents"
## Stock peers
stock_peers = DL_DIR + "/fmp/stock-peers"
## Company profile for sector and industry
buffer_company_profile = DL_DIR + "/buffer/fmp/company-profile"
company_profile = DL_DIR + "/fmp/company-profile"
company_profile_ticker = DL_DIR + "/fmp/company-profile-ticker"
## Calendars
economic_calendar = DL_DIR + "/fmp/economic-calendar"
ipo_calendar_confirmed = DL_DIR + "/fmp/ipo-calendar-confirmed"
earnings_calendar_confirmed = DL_DIR + "/fmp/earnings-calendar-confirmed"
stock_split_calendar = DL_DIR + "/fmp/stock-split-calendar"
## Tickers for daily collection
to_collect = DL_DIR + "/fmp/to-collect"
## Delisted
delisted_companies = DL_DIR + "/fmp/delisted-companies/latest.parquet"
## Current Price
current_price = DL_DIR + "/fmp/current-price-ticker"
## Historical price
historical_daily_price_full = DL_DIR + "/fmp/historical-daily-price-full"
historical_daily_price_full_raw = DL_DIR + "/fmp/historical-daily-price-full-raw"
historical_ticker_price_full = DL_DIR + "/fmp/historical-ticker-price-full"
## Historical price minute
ticker_minute_price = DL_DIR + "/fmp/ticker-minute-price"
historical_thirty_minute_price = DL_DIR + "/fmp/historical-thirty-minute-price"
historical_thirty_minute_ticker_price = (
    DL_DIR + "/fmp/historical-thirty-minute-ticker-price"
)
## Discounted cash flow
dcf = DL_DIR + "/fmp/dcf"
dcf_ticker = DL_DIR + "/fmp/dcf-ticker"
## Rating
historical_rating = DL_DIR + "/fmp/historical-rating"
historical_rating_ticker = DL_DIR + "/fmp/historical-rating-ticker"
## Enterprise Values
buffer_enterprise_values_quarter = DL_DIR + "/buffer/fmp/enterprise-values-quarter"
enterprise_values_annual = DL_DIR + "/fmp/enterprise-values-annual"
enterprise_values_quarter = DL_DIR + "/fmp/enterprise-values-quarter"
enterprise_values_annual_ticker = DL_DIR + "/fmp/enterprise-values-annual-ticker"
enterprise_values_quarter_ticker = DL_DIR + "/fmp/enterprise-values-quarter-ticker"
## Stock Grade
buffer_grade = DL_DIR + "/buffer/fmp/grade"
grade = DL_DIR + "/fmp/grade"
grade_ticker = DL_DIR + "/fmp/grade-ticker"
## Social Sentiment
buffer_social_sentiment = DL_DIR + "/buffer/fmp/social-sentiment"
social_sentiment = DL_DIR + "/fmp/social-sentiment"
social_sentiment_ticker = DL_DIR + "/fmp/social-sentiment-ticker"
## Analyst Estimates
buffer_analyst_estimates = DL_DIR + "/buffer/fmp/analyst-estimates"
analyst_estimates = DL_DIR + "/fmp/analyst-estimates"
analyst_estimates_ticker = DL_DIR + "/fmp/analyst-estimates-ticker"
buffer_analyst_estimates_quarter = DL_DIR + "/buffer/fmp/analyst-estimates-quarter"
analyst_estimates_quarter = DL_DIR + "/fmp/analyst-estimates-quarter"
analyst_estimates_quarter_ticker = DL_DIR + "/fmp/analyst-estimates-quarter-ticker"
# annual earnings buffer
buffer_fmp_a_bs = DL_DIR + "/buffer/fmp/financial-statements-annual/balance-sheet"
buffer_fmp_a_cf = DL_DIR + "/buffer/fmp/financial-statements-annual/cash-flow"
buffer_fmp_a_i = DL_DIR + "/buffer/fmp/financial-statements-annual/income"
buffer_fmp_a_km = DL_DIR + "/buffer/fmp/financial-statements-annual/key-metrics"
buffer_fmp_a_r = DL_DIR + "/buffer/fmp/financial-statements-annual/ratios"
# quarterly earnings buffer
buffer_fmp_q_bs = DL_DIR + "/buffer/fmp/financial-statements-quarter/balance-sheet"
buffer_fmp_q_cf = DL_DIR + "/buffer/fmp/financial-statements-quarter/cash-flow"
buffer_fmp_q_i = DL_DIR + "/buffer/fmp/financial-statements-quarter/income"
buffer_fmp_q_km = DL_DIR + "/buffer/fmp/financial-statements-quarter/key-metrics"
buffer_fmp_q_r = DL_DIR + "/buffer/fmp/financial-statements-quarter/ratios"
# annual earnings
fmp_a_bs = DL_DIR + "/fmp/financial-statements-annual/balance-sheet"
fmp_a_cf = DL_DIR + "/fmp/financial-statements-annual/cash-flow"
fmp_a_i = DL_DIR + "/fmp/financial-statements-annual/income"
fmp_a_km = DL_DIR + "/fmp/financial-statements-annual/key-metrics"
fmp_a_r = DL_DIR + "/fmp/financial-statements-annual/ratios"
# quarterly earnings
fmp_q_bs = DL_DIR + "/fmp/financial-statements-quarter/balance-sheet"
fmp_q_cf = DL_DIR + "/fmp/financial-statements-quarter/cash-flow"
fmp_q_i = DL_DIR + "/fmp/financial-statements-quarter/income"
fmp_q_km = DL_DIR + "/fmp/financial-statements-quarter/key-metrics"
fmp_q_r = DL_DIR + "/fmp/financial-statements-quarter/ratios"
# annual earnings ticker
fmp_a_bs_ticker = DL_DIR + "/fmp/financial-statements-annual-ticker/balance-sheet"
fmp_a_cf_ticker = DL_DIR + "/fmp/financial-statements-annual-ticker/cash-flow"
fmp_a_i_ticker = DL_DIR + "/fmp/financial-statements-annual-ticker/income"
fmp_a_km_ticker = DL_DIR + "/fmp/financial-statements-annual-ticker/key-metrics"
fmp_a_r_ticker = DL_DIR + "/fmp/financial-statements-annual-ticker/ratios"
# quarterly earnings ticker
fmp_q_bs_ticker = DL_DIR + "/fmp/financial-statements-quarter-ticker/balance-sheet"
fmp_q_cf_ticker = DL_DIR + "/fmp/financial-statements-quarter-ticker/cash-flow"
fmp_q_i_ticker = DL_DIR + "/fmp/financial-statements-quarter-ticker/income"
fmp_q_km_ticker = DL_DIR + "/fmp/financial-statements-quarter-ticker/key-metrics"
fmp_q_r_ticker = DL_DIR + "/fmp/financial-statements-quarter-ticker/ratios"
## Stock Grade
buffer_earnings_surprises = DL_DIR + "/buffer/fmp/earnings-surprises"
earnings_surprises = DL_DIR + "/fmp/earnings-surprises"
earnings_surprises_ticker = DL_DIR + "/fmp/earnings-surprises-ticker"
## Insider Trading
buffer_insider_trading = DL_DIR + "/buffer/fmp/insider-trading"
insider_trading = DL_DIR + "/fmp/insider-trading"
insider_trading_ticker = DL_DIR + "/fmp/insider-trading-ticker"
## Stock News
buffer_stock_news = DL_DIR + "/buffer/fmp/stock-news"
stock_news = DL_DIR + "/fmp/stock-news"
stock_news_ticker = DL_DIR + "/fmp/stock-news-ticker"
## Press Release
buffer_press_releases = DL_DIR + "/buffer/fmp/press-releases"
press_releases = DL_DIR + "/fmp/press-releases"
press_releases_ticker = DL_DIR + "/fmp/press-releases-ticker"
## Sector Market Performance
sector_price_earning_ratio = DL_DIR + "/fmp/sector-price-earning-ratio/data.parquet"
#### Macro Economic Section ####
## Sector and industry PE
sector_pe = DL_DIR + "/fmp/sector-pe"
industry_pe = DL_DIR + "/fmp/industry-pe"
## Treasury Rates
treasury_rates = DL_DIR + "/fmp/treasury-rates"
## Economic Indicators
economic_indicators = DL_DIR + "/fmp/economic-indicators"
gdp = economic_indicators + "/gdp"
real_gdp = economic_indicators + "/real-gdp"
nominal_potential_gdp = economic_indicators + "/nominal-potential-gdp"
real_gdp_per_capita = economic_indicators + "/real-gdp-per-capita"
federal_funds = economic_indicators + "/federal-funds"
cpi = economic_indicators + "/cpi"
inflation_rate = economic_indicators + "/inflation-rate"
inflation = economic_indicators + "/inflation"
retail_sales = economic_indicators + "/retail-sales"
consumer_sentiment = economic_indicators + "/consumer-sentiment"
durable_goods = economic_indicators + "/durable-goods"
unemployment_rate = economic_indicators + "/unemployment-rate"
total_nonfarm_payroll = economic_indicators + "/total-nonfarm-payroll"
initial_claims = economic_indicators + "/initial-claims"
industrial_production_total_index = (
    economic_indicators + "/industrial-production-total-index"
)
new_privately_owned_housing_units_started_total_units = (
    economic_indicators + "/new-privately-owned-housing-units-started-total-units"
)
total_vehicle_sales = economic_indicators + "/total-vehicle-sales"
retail_money_funds = economic_indicators + "/retail-money-funds"
smoothed_us_recession_probabilities = (
    economic_indicators + "/smoothed-us-recession-probabilities"
)
ci_3_month_or_90_day_rates_and_yields_certificates_of_deposit = (
    economic_indicators
    + "/ci-3-month-or-90-day-rates-and-yields-certificates-of-deposit"
)
commercial_bank_interest_rate_on_credit_card_plans_all_accounts = (
    economic_indicators
    + "/commercial-bank-interest-rate-on-credit-card-plans-all-accounts"
)
ci_30_year_fixed_rate_mortgage_average = (
    economic_indicators + "/ci-30-year-fixed-rate-mortgage-average"
)
ci_15_year_fixed_rate_mortgage_average = (
    economic_indicators + "/ci-15-year-fixed-rate-mortgage-average"
)
## Stitched together macro signals
full_macro = DL_DIR + "/fmp/full-macro-signals"

## intraday stuff
buffer_historical_chart_1_min = (
    f"{os.environ['DL_DIR']}/buffer/fmp/historical-chart/1-min"
)
buffer_historical_chart_15_min = (
    f"{os.environ['DL_DIR']}/buffer/fmp/historical-chart/15-min"
)
buffer_technical_indicator_1_min_sma_9 = (
    f"{os.environ['DL_DIR']}/buffer/fmp/technical_indicator/1-min/sma/window_9"
)
buffer_technical_indicator_1_min_sma_50 = (
    f"{os.environ['DL_DIR']}/buffer/fmp/technical_indicator/1-min/sma/window_50"
)
buffer_technical_indicator_15_min_sma_9 = (
    f"{os.environ['DL_DIR']}/buffer/fmp/technical_indicator/15-min/sma/window_9"
)
buffer_technical_indicator_15_min_sma_50 = (
    f"{os.environ['DL_DIR']}/buffer/fmp/technical_indicator/15-min/sma/window_50"
)
buffer_technical_indicator_1_min_rsi = (
    f"{os.environ['DL_DIR']}/buffer/fmp/technical_indicator/1-min/rsi"
)
buffer_technical_indicator_15_min_rsi = (
    f"{os.environ['DL_DIR']}/buffer/fmp/technical_indicator/15-min/rsi"
)
historical_chart_1_min = f"{os.environ['DL_DIR']}/fmp/historical-chart/1-min"
historical_chart_15_min = f"{os.environ['DL_DIR']}/fmp/historical-chart/15-min"
technical_indicator_1_min_sma_9 = (
    f"{os.environ['DL_DIR']}/fmp/technical_indicator/1-min/sma/window_9"
)
technical_indicator_1_min_sma_50 = (
    f"{os.environ['DL_DIR']}/fmp/technical_indicator/1-min/sma/window_50"
)
technical_indicator_15_min_sma_9 = (
    f"{os.environ['DL_DIR']}/fmp/technical_indicator/15-min/sma/window_9"
)
technical_indicator_15_min_sma_50 = (
    f"{os.environ['DL_DIR']}/fmp/technical_indicator/15-min/sma/window_50"
)
technical_indicator_1_min_rsi = (
    f"{os.environ['DL_DIR']}/fmp/technical_indicator/1-min/rsi"
)
technical_indicator_15_min_rsi = (
    f"{os.environ['DL_DIR']}/fmp/technical_indicator/15-min/rsi"
)
shares_float = f"{os.environ['DL_DIR']}/fmp/shares_float"
earning_calendar = f"{os.environ['DL_DIR']}/fmp/earning_calendar"
earning_call_transcript = f"{os.environ['DL_DIR']}/fmp/earnings-transcript"


## urls
# base
FMP = "https://financialmodelingprep.com/api"
FULL_END_SUFFIX = "from=1980-01-01&to={DSE}&apikey={API}"
START_END_SUFFIX = "from={DSS}&to={DSE}&apikey={API}"
TL_SUFFIX = "{TICKER}?limit=100000&apikey={API}"
NEWS_SUFFIX = "tickers={TICKER}&limit=100000&apikey={API}"
PAGE_COUNT_SUFFIX = "{TICKER}?page={PAGE}&apikey={API}"
DS_E_SUFFIX = "date={DS}&exchange=NYSE&apikey={API}"
# full urls
SP_CONSTITUENCY = FMP + "/v3/sp500_constituent?apikey="
NASDAQ_CONSTITUENCY = FMP + "/v3/nasdaq_constituent?apikey="
DOWJONES_CONSTITUENCY = FMP + "/v3/dowjones_constituent?apikey="
STOCK_PEERS = FMP + "/v4/stock_peers_bulk?apikey={API}"
COMPANY_PROFILE = FMP + "/v3/profile/{TICKER}?apikey={API}"
ECONOMIC_CAL = FMP + "/v3/economic_calendar?" + START_END_SUFFIX
IPO_CONFIRMED_CAL = FMP + "/v4/ipo-calendar-confirmed?" + START_END_SUFFIX
EARNINGS_CONFIRMED_CAL = FMP + "/v4/earning-calendar-confirmed?" + START_END_SUFFIX
SPLIT_CAL = FMP + "/v3/stock_split_calendar?" + START_END_SUFFIX
DELISTED_COMPANIES = FMP + "/v3/delisted-companies?page={PAGE}&apikey={API}"
CURRENT_PRICE = FMP + "/v3/quote/{TICKER}?apikey={API}"
HISTORICAL_PRICE_FULL = (
    FMP
    + "/v3/historical-price-full/{TICKER}?serietype=bar&from={DSS}&to={DSE}&apikey={API}"
)
HISTORICAL_THIRTY_MINUTE_PRICE = (
    FMP
    + "/v3/historical-chart/30min/{TICKER}?serietype=bar&from={DSS}&to={DSE}&apikey={API}"
)
DCF = FMP + "/v3/historical-daily-discounted-cash-flow/" + TL_SUFFIX
HISTORICAL_RATING = FMP + "/v3/historical-rating/" + TL_SUFFIX
ENTERPRISE_VALUES_ANNUAL = FMP + "/v3/enterprise-values/" + TL_SUFFIX
ENTERPRISE_VALUES_QUARTER = (
    FMP + "/v3/enterprise-values/" + TL_SUFFIX + "&period=quarter"
)
GRADE = FMP + "/v3/grade/" + TL_SUFFIX
SOCIAL_SENTIMENT = FMP + "/v4/historical/social-sentiment?" + TL_SUFFIX
ANALYST_ESTIMATES = FMP + "/v3/analyst-estimates/" + TL_SUFFIX
ANALYST_ESTIMATES_Q = ANALYST_ESTIMATES + "&period=quarter"

INCOME_STATEMENT = FMP + "/v3/income-statement/" + TL_SUFFIX
BALANCE_SHEET = FMP + "/v3/balance-sheet-statement/" + TL_SUFFIX
CASH_FLOW = FMP + "/v3/cash-flow-statement/" + TL_SUFFIX
KEY_METRICS = FMP + "/v3/key-metrics/" + TL_SUFFIX
RATIOS = FMP + "/v3/ratios/" + TL_SUFFIX
INCOME_STATEMENT_Q = INCOME_STATEMENT + "&period=quarter"
BALANCE_SHEET_Q = BALANCE_SHEET + "&period=quarter"
CASH_FLOW_Q = CASH_FLOW + "&period=quarter"
KEY_METRICS_Q = KEY_METRICS + "&period=quarter"
RATIOS_Q = RATIOS + "&period=quarter"
EARNINGS_SURPRISES = FMP + "/v3/earnings-surprises/" + TL_SUFFIX
INSIDER_TRADING = FMP + "/v4/insider-trading?" + TL_SUFFIX
STOCK_NEWS = FMP + "/v3/stock_news?" + NEWS_SUFFIX
PRESS_RELEASE = FMP + "/v3/press-releases/" + PAGE_COUNT_SUFFIX
SECTOR_PRICE_EARNING_RATIO = (
    FMP + "/v4/sector_price_earning_ratio?date={DATE}&exchange=NYSE&apikey={API}"
)
SECTOR_PE = FMP + "/v4/sector_price_earning_ratio?" + DS_E_SUFFIX
INDUSTRY_PE = FMP + "/v4/industry_price_earning_ratio?" + DS_E_SUFFIX
TREASURY = FMP + "/v4/treasury?" + START_END_SUFFIX
ECONOMIC_INDICATORS = FMP + "/v4/economic?name={INDICATOR}&" + FULL_END_SUFFIX
ONE_MINUTE_INTRADAY_PRICE = (
    FMP
    + "/v3/historical-chart/1min/{TICKER}?from={START_DATE}&to={END_DATE}&apikey={API}"
)
FIFTEEN_MINUTE_INTRADAY_PRICE = FMP + "/v3/historical-chart/15min/{TICKER}?apikey={API}"
SMA_9_ONE_MINUTE_TECHNICAL_INDICATOR = (
    FMP + "/v3/technical_indicator/1min/{TICKER}?period=9&type=sma&apikey={API}"
)
SMA_50_ONE_MINUTE_TECHNICAL_INDICATOR = (
    FMP + "/v3/technical_indicator/1min/{TICKER}?period=50&type=sma&apikey={API}"
)
RSI_ONE_MINUTE_TECHNICAL_INDICATOR = (
    FMP + "/v3/technical_indicator/1min/{TICKER}?period=9&type=rsi&apikey={API}"
)
SMA_9_FIFTEEN_MINUTE_TECHNICAL_INDICATOR = (
    FMP + "/v3/technical_indicator/15min/{TICKER}?period=9&type=sma&apikey={API}"
)
SMA_50_FIFTEEN_MINUTE_TECHNICAL_INDICATOR = (
    FMP + "/v3/technical_indicator/15min/{TICKER}?period=50&type=sma&apikey={API}"
)
RSI_FIFTEEN_MINUTE_TECHNICAL_INDICATOR = (
    FMP + "/v3/technical_indicator/15min/{TICKER}?period=9&type=sma&apikey={API}"
)

SHARES_FLOAT = FMP + "/v4/shares_float?symbol={TICKER}&apikey={API}"
EARNINGS_CALENDAR = (
    FMP + "/v3/historical/earning_calendar/{TICKER}?limit=1000&apikey={API}"
)

EARNINGS_CALL_TRANSCRIPT = (
    FMP + "/v4/batch_earning_call_transcript/{TICKER}?year={YEAR}&apikey={API}"
)

## variable inputs
calendar_collection_list = [
    (ECONOMIC_CAL, economic_calendar),
    (IPO_CONFIRMED_CAL, ipo_calendar_confirmed),
    (EARNINGS_CONFIRMED_CAL, earnings_calendar_confirmed),
    (SPLIT_CAL, stock_split_calendar),
]

## types
income_types = {
    "date": dt.date,
    "symbol": str,
    "reportedCurrency": str,
    "cik": str,
    "fillingDate": dt.date,
    "acceptedDate": dt.datetime,
    "calendarYear": float,
    "period": str,
    "revenue": float,
    "costOfRevenue": float,
    "grossProfit": float,
    "grossProfitRatio": float,
    "researchAndDevelopmentExpenses": float,
    "generalAndAdministrativeExpenses": float,
    "sellingAndMarketingExpenses": float,
    "sellingGeneralAndAdministrativeExpenses": float,
    "otherExpenses": float,
    "operatingExpenses": float,
    "costAndExpenses": float,
    "interestIncome": float,
    "interestExpense": float,
    "depreciationAndAmortization": float,
    "ebitda": float,
    "ebitdaratio": float,
    "operatingIncome": float,
    "operatingIncomeRatio": float,
    "totalOtherIncomeExpensesNet": float,
    "incomeBeforeTax": float,
    "incomeBeforeTaxRatio": float,
    "incomeTaxExpense": float,
    "netIncome": float,
    "netIncomeRatio": float,
    "eps": float,
    "epsdiluted": float,
    "weightedAverageShsOut": float,
    "weightedAverageShsOutDil": float,
    "link": str,
    "finalLink": str,
}

balance_sheet_types = {
    "date": dt.date,
    "symbol": str,
    "reportedCurrency": str,
    "cik": str,
    "fillingDate": dt.date,
    "acceptedDate": dt.datetime,
    "calendarYear": float,
    "period": str,
    "cashAndCashEquivalents": float,
    "shortTermInvestments": float,
    "cashAndShortTermInvestments": float,
    "netReceivables": float,
    "inventory": float,
    "otherCurrentAssets": float,
    "totalCurrentAssets": float,
    "propertyPlantEquipmentNet": float,
    "goodwill": float,
    "intangibleAssets": float,
    "goodwillAndIntangibleAssets": float,
    "longTermInvestments": float,
    "taxAssets": float,
    "otherNonCurrentAssets": float,
    "totalNonCurrentAssets": float,
    "otherAssets": float,
    "totalAssets": float,
    "accountPayables": float,
    "shortTermDebt": float,
    "taxPayables": float,
    "deferredRevenue": float,
    "otherCurrentLiabilities": float,
    "totalCurrentLiabilities": float,
    "longTermDebt": float,
    "deferredRevenueNonCurrent": float,
    "deferredTaxLiabilitiesNonCurrent": float,
    "otherNonCurrentLiabilities": float,
    "totalNonCurrentLiabilities": float,
    "otherLiabilities": float,
    "capitalLeaseObligations": float,
    "totalLiabilities": float,
    "preferredStock": float,
    "commonStock": float,
    "retainedEarnings": float,
    "accumulatedOtherComprehensiveIncomeLoss": float,
    "othertotalStockholdersEquity": float,
    "totalStockholdersEquity": float,
    "totalLiabilitiesAndStockholdersEquity": float,
    "minorityInterest": float,
    "totalEquity": float,
    "totalLiabilitiesAndTotalEquity": float,
    "totalInvestments": float,
    "totalDebt": float,
    "netDebt": float,
    "link": str,
    "finalLink": str,
}

cash_flow_types = {
    "date": dt.date,
    "symbol": str,
    "reportedCurrency": str,
    "cik": str,
    "fillingDate": dt.date,
    "acceptedDate": dt.datetime,
    "calendarYear": float,
    "period": str,
    "netIncome": float,
    "depreciationAndAmortization": float,
    "deferredIncomeTax": float,
    "stockBasedCompensation": float,
    "changeInWorkingCapital": float,
    "accountsReceivables": float,
    "inventory": float,
    "accountsPayables": float,
    "otherWorkingCapital": float,
    "otherNonCashItems": float,
    "netCashProvidedByOperatingActivities": float,
    "investmentsInPropertyPlantAndEquipment": float,
    "acquisitionsNet": float,
    "purchasesOfInvestments": float,
    "salesMaturitiesOfInvestments": float,
    "otherInvestingActivites": float,
    "netCashUsedForInvestingActivites": float,
    "debtRepayment": float,
    "commonStockIssued": float,
    "commonStockRepurchased": float,
    "dividendsPaid": float,
    "otherFinancingActivites": float,
    "netCashUsedProvidedByFinancingActivities": float,
    "effectOfForexChangesOnCash": float,
    "netChangeInCash": float,
    "cashAtEndOfPeriod": float,
    "cashAtBeginningOfPeriod": float,
    "operatingCashFlow": float,
    "capitalExpenditure": float,
    "freeCashFlow": float,
    "link": str,
    "finalLink": str,
}

ratios_types = {
    "symbol": str,
    "date": dt.date,
    "period": str,
    "currentRatio": float,
    "quickRatio": float,
    "cashRatio": float,
    "daysOfSalesOutstanding": float,
    "daysOfInventoryOutstanding": float,
    "operatingCycle": float,
    "daysOfPayablesOutstanding": float,
    "cashConversionCycle": float,
    "grossProfitMargin": float,
    "operatingProfitMargin": float,
    "pretaxProfitMargin": float,
    "netProfitMargin": float,
    "effectiveTaxRate": float,
    "returnOnAssets": float,
    "returnOnEquity": float,
    "returnOnCapitalEmployed": float,
    "netIncomePerEBT": float,
    "ebtPerEbit": float,
    "ebitPerRevenue": float,
    "debtRatio": float,
    "debtEquityRatio": float,
    "longTermDebtToCapitalization": float,
    "totalDebtToCapitalization": float,
    "interestCoverage": float,
    "cashFlowToDebtRatio": float,
    "companyEquityMultiplier": float,
    "receivablesTurnover": float,
    "payablesTurnover": float,
    "inventoryTurnover": float,
    "fixedAssetTurnover": float,
    "assetTurnover": float,
    "operatingCashFlowPerShare": float,
    "freeCashFlowPerShare": float,
    "cashPerShare": float,
    "payoutRatio": float,
    "operatingCashFlowSalesRatio": float,
    "freeCashFlowOperatingCashFlowRatio": float,
    "cashFlowCoverageRatios": float,
    "shortTermCoverageRatios": float,
    "capitalExpenditureCoverageRatio": float,
    "dividendPaidAndCapexCoverageRatio": float,
    "dividendPayoutRatio": float,
    "priceBookValueRatio": float,
    "priceToBookRatio": float,
    "priceToSalesRatio": float,
    "priceEarningsRatio": float,
    "priceToFreeCashFlowsRatio": float,
    "priceToOperatingCashFlowsRatio": float,
    "priceCashFlowRatio": float,
    "priceEarningsToGrowthRatio": float,
    "priceSalesRatio": float,
    "dividendYield": float,
    "enterpriseValueMultiple": float,
    "priceFairValue": float,
}

key_metrics_types = {
    "symbol": str,
    "date": dt.date,
    "period": str,
    "revenuePerShare": float,
    "netIncomePerShare": float,
    "operatingCashFlowPerShare": float,
    "freeCashFlowPerShare": float,
    "cashPerShare": float,
    "bookValuePerShare": float,
    "tangibleBookValuePerShare": float,
    "shareholdersEquityPerShare": float,
    "interestDebtPerShare": float,
    "marketCap": float,
    "enterpriseValue": float,
    "peRatio": float,
    "priceToSalesRatio": float,
    "pocfratio": float,
    "pfcfRatio": float,
    "pbRatio": float,
    "ptbRatio": float,
    "evToSales": float,
    "enterpriseValueOverEBITDA": float,
    "evToOperatingCashFlow": float,
    "evToFreeCashFlow": float,
    "earningsYield": float,
    "freeCashFlowYield": float,
    "debtToEquity": float,
    "debtToAssets": float,
    "netDebtToEBITDA": float,
    "currentRatio": float,
    "interestCoverage": float,
    "incomeQuality": float,
    "dividendYield": float,
    "payoutRatio": float,
    "salesGeneralAndAdministrativeToRevenue": float,
    "researchAndDdevelopementToRevenue": float,
    "intangiblesToTotalAssets": float,
    "capexToOperatingCashFlow": float,
    "capexToRevenue": float,
    "capexToDepreciation": float,
    "stockBasedCompensationToRevenue": float,
    "grahamNumber": float,
    "roic": float,
    "returnOnTangibleAssets": float,
    "grahamNetNet": float,
    "workingCapital": float,
    "tangibleAssetValue": float,
    "netCurrentAssetValue": float,
    "investedCapital": float,
    "averageReceivables": float,
    "averagePayables": float,
    "averageInventory": float,
    "daysSalesOutstanding": float,
    "daysPayablesOutstanding": float,
    "daysOfInventoryOnHand": float,
    "receivablesTurnover": float,
    "payablesTurnover": float,
    "inventoryTurnover": float,
    "roe": float,
    "capexPerShare": float,
}

price_full_types = {
    "date": dt.date,
    "open": float,
    "high": float,
    "low": float,
    "close": float,
    "adjClose": float,
    "volume": float,
    "unadjustedVolume": float,
    "change": float,
    "changePercent": float,
    "vwap": float,
    "label": str,
    "changeOverTime": float,
    "symbol": str,
}

intraday_price_types = {
    "date": dt.datetime,
    "open": float,
    "high": float,
    "low": float,
    "close": float,
    "volume": float,
    "symbol": str,
}

technial_rsi_types = {
    "date": dt.datetime,
    "open": float,
    "high": float,
    "low": float,
    "close": float,
    "volume": float,
    "rsi": float,
    "symbol": str,
}

technial_sma_types = {
    "date": dt.datetime,
    "open": float,
    "high": float,
    "low": float,
    "close": float,
    "volume": float,
    "sma": float,
    "symbol": str,
}

eod_price_types = {
    "symbol": str,
    "date": dt.date,
    "open": float,
    "low": float,
    "high": float,
    "close": float,
    "adjClose": float,
    "volume": float,
}

dcf_types = {"symbol": str, "date": dt.date, "dcf": float}

rating_types = {
    "symbol": str,
    "date": dt.date,
    "rating": str,
    "ratingScore": float,
    "ratingRecommendation": str,
    "ratingDetailsDCFScore": float,
    "ratingDetailsDCFRecommendation": str,
    "ratingDetailsROEScore": float,
    "ratingDetailsROERecommendation": str,
    "ratingDetailsROAScore": float,
    "ratingDetailsROARecommendation": str,
    "ratingDetailsDEScore": float,
    "ratingDetailsDERecommendation": str,
    "ratingDetailsPEScore": float,
    "ratingDetailsPERecommendation": str,
    "ratingDetailsPBScore": float,
    "ratingDetailsPBRecommendation": str,
}

enterprise_values_types = {
    "symbol": str,
    "date": dt.date,
    "stockPrice": float,
    "numberOfShares": float,
    "marketCapitalization": float,
    "minusCashAndCashEquivalents": float,
    "addTotalDebt": float,
    "enterpriseValue": float,
}

grade_types = {
    "symbol": str,
    "date": dt.date,
    "gradingCompany": str,
    "previousGrade": str,
    "newGrade": str,
}

social_sentiment_types = {
    "date": dt.date,
    "symbol": str,
    "stocktwitsPosts": float,
    "twitterPosts": float,
    "stocktwitsComments": float,
    "twitterComments": float,
    "stocktwitsLikes": float,
    "twitterLikes": float,
    "stocktwitsImpressions": float,
    "twitterImpressions": float,
    "stocktwitsSentiment": float,
    "twitterSentiment": float,
}

analyst_estimates_types = {
    "symbol": str,
    "date": dt.date,
    "estimatedRevenueLow": float,
    "estimatedRevenueHigh": float,
    "estimatedRevenueAvg": float,
    "estimatedEbitdaLow": float,
    "estimatedEbitdaHigh": float,
    "estimatedEbitdaAvg": float,
    "estimatedEbitLow": float,
    "estimatedEbitHigh": float,
    "estimatedEbitAvg": float,
    "estimatedNetIncomeLow": float,
    "estimatedNetIncomeHigh": float,
    "estimatedNetIncomeAvg": float,
    "estimatedSgaExpenseLow": float,
    "estimatedSgaExpenseHigh": float,
    "estimatedSgaExpenseAvg": float,
    "estimatedEpsAvg": float,
    "estimatedEpsHigh": float,
    "estimatedEpsLow": float,
    "numberAnalystEstimatedRevenue": float,
    "numberAnalystsEstimatedEps": float,
}

earnings_surprises_types = {
    "date": dt.date,
    "symbol": str,
    "actualEarningResult": float,
    "estimatedEarning": float,
}

insider_trading_types = {
    "symbol": str,
    "filingDate": dt.datetime,
    "transactionDate": dt.date,
    "reportingCik": str,
    "transactionType": str,
    "securitiesOwned": float,
    "companyCik": str,
    "reportingName": str,
    "typeOfOwner": str,
    "acquistionOrDisposition": str,
    "formType": str,
    "securitiesTransacted": float,
    "price": float,
    "securityName": str,
    "link": str,
}

stock_news_types = {
    "symbol": str,
    "publishedDate": dt.datetime,
    "title": str,
    "image": str,
    "site": str,
    "text": str,
    "url": str,
}

press_release_types = {"symbol": str, "date": dt.datetime, "title": str, "text": str}

sector_price_earning_ratio_types = {
    "date": dt.date,
    "sector": str,
    "exchange": str,
    "pe": float,
}

sector_pe_types = {"date": dt.date, "sector": str, "exchange": str, "pe": float}

industry_pe_types = {"date": dt.date, "industry": str, "exchange": str, "pe": float}

treasury_rates_types = {
    "date": dt.date,
    "month1": float,
    "month2": float,
    "month3": float,
    "month6": float,
    "year1": float,
    "year2": float,
    "year3": float,
    "year5": float,
    "year7": float,
    "year10": float,
    "year20": float,
    "year30": float,
}

economic_indicators_types = {"date": dt.date, "value": float, "indicator": str}

company_profile_types = {
    "symbol": str,
    "price": float,
    "beta": float,
    "volAvg": float,
    "mktCap": float,
    "lastDiv": float,
    "range": str,
    "changes": float,
    "companyName": str,
    "currency": str,
    "cik": str,
    "isin": str,
    "cusip": str,
    "exchange": str,
    "exchangeShortName": str,
    "industry": str,
    "website": str,
    "description": str,
    "ceo": str,
    "sector": str,
    "country": str,
    "fullTimeEmployees": str,
    "phone": str,
    "address": str,
    "city": str,
    "state": str,
    "zip": str,
    "dcfDiff": float,
    "dcf": float,
    "image": str,
    "ipoDate": str,
    "defaultImage": bool,
    "isEtf": bool,
    "isActivelyTrading": bool,
    "isAdr": bool,
    "isFund": bool,
}

#### Macro Economic Indicators ####
gdp_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

real_gdp_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

nominal_potential_gdp_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

real_gdp_per_capita_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

federal_funds_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

cpi_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

inflation_rate_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

inflation_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

retail_sales_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

consumer_sentiment_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

durable_goods_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

unemployment_rate_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

total_nonfarm_payroll_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

initial_claims_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

industrial_production_total_index_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

new_privately_owned_housing_units_started_total_units_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

total_vehicle_sales_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

retail_money_funds_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

smoothed_us_recession_probabilities_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

ci_3_month_or_90_day_rates_and_yields_certificates_of_deposit_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

commercial_bank_interest_rate_on_credit_card_plans_all_accounts_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

ci_30_year_fixed_rate_mortgage_average_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

ci_15_year_fixed_rate_mortgage_average_types = {
    "economic_indicator": str,
    "date": dt.date,
    "value": float,
}

shares_float_types = {
    "symbol": str,
    "date": dt.date,
    "freeFloat": float,
    "floatShares": float,
    "outstandingShares": float,
    "source": str,
}

earning_calendar_types = {
    "date": dt.date,
    "symbol": str,
    "eps": float,
    "epsEstimated": float,
    "time": str,  ## time zone
    "revenue": float,
    "revenueEstimated": float,
    "updatedFromDate": dt.date,
    "fiscalDateEnding": dt.date,
}

earning_call_transcript_types = {
    "symbol": str,
    "quarter": float,
    "year": float,
    "date": dt.datetime,
    "content": str,
}

current_price_types = {
    "symbol": str,
    "name": str,
    "price": float,
    "changesPercentage": float,
    "change": float,
    "dayLow": float,
    "dayHigh": float,
    "yearHigh": float,
    "yearLow": float,
    "marketCap": float,
    "priceAvg50": float,
    "priceAvg200": float,
    "exchange": str,
    "volume": float,
    "avgVolume": float,
    "open": float,
    "previousClose": float,
    "eps": float,
    "pe": float,
    "earningsAnnouncement": str,
    "sharesOutstanding": float,
    "timestamp": dt.datetime,
    "date": dt.date,
}
