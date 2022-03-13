import datetime as dt
import os

## paths
DL_DIR = os.environ["DL_DIR"]
BASE_TICKER_FN = DL_DIR + "/seed-data/nasdaq_screener_1628807233734.csv"
avg_price = DL_DIR + "/derived-measurements/avg-price-vol"
# annual earnings buffer
buffer_fmp_q_bs = DL_DIR + "/buffer/fmp-earnings/quarter/balance-sheet"
buffer_fmp_q_cf = DL_DIR + "/buffer/fmp-earnings/quarter/cash-flow"
buffer_fmp_q_i = DL_DIR + "/buffer/fmp-earnings/quarter/income"
buffer_fmp_q_km = DL_DIR + "/buffer/fmp-earnings/quarter/key-metrics"
buffer_fmp_q_r = DL_DIR + "/buffer/fmp-earnings/quarter/ratios"
# quarterly earnings buffer
buffer_fmp_a_bs = DL_DIR + "/buffer/fmp-earnings/annual/balance-sheet"
buffer_fmp_a_cf = DL_DIR + "/buffer/fmp-earnings/annual/cash-flow"
buffer_fmp_a_i = DL_DIR + "/buffer/fmp-earnings/annual/income"
buffer_fmp_a_km = DL_DIR + "/buffer/fmp-earnings/annual/key-metrics"
buffer_fmp_a_r = DL_DIR + "/buffer/fmp-earnings/annual/ratios"
# annual earnings
fmp_q_bs = DL_DIR + "/fmp/earnings/quarter/balance-sheet"
fmp_q_cf = DL_DIR + "/fmp/earnings/quarter/cash-flow"
fmp_q_i = DL_DIR + "/fmp/earnings/quarter/income"
fmp_q_km = DL_DIR + "/fmp/earnings/quarter/key-metrics"
fmp_q_r = DL_DIR + "/fmp/earnings/quarter/ratios"
# quarterly earnings
fmp_a_bs = DL_DIR + "/fmp/earnings/annual/balance-sheet"
fmp_a_cf = DL_DIR + "/fmp/earnings/annual/cash-flow"
fmp_a_i = DL_DIR + "/fmp/earnings/annual/income"
fmp_a_km = DL_DIR + "/fmp/earnings/annual/key-metrics"
fmp_a_r = DL_DIR + "/fmp/earnings/annual/ratios"
# market constituents
fmp_constituents = DL_DIR + "/fmp/market-constituents"
## sector and industry PE
sector_pe = DL_DIR + "/fmp/sector-pe"
industry_pe = DL_DIR + "/fmp/industry-pe"
## Stock peers
stock_peers = DL_DIR + "/fmp/stock-peers"
## Key stats for sector and industry
key_stats = DL_DIR + "/fmp/key-stats"
## Calendars
economic_calendar = DL_DIR + "/fmp/economic-calendar"
ipo_calendar_confirmed = DL_DIR + "/fmp/ipo-calendar-confirmed"
earnings_calendar_confirmed = DL_DIR + "/fmp/earnings-calendar-confirmed"
stock_split_calendar = DL_DIR + "/fmp/stock-split-calendar"
## Tickers for daily collection
to_collect = DL_DIR + "/fmp/to-collect"
## Delisted
delisted_companies = DL_DIR + "/fmp/delisted-companies/latest.parquet"


## urls
FMP = "https://financialmodelingprep.com/api"
SP_CONSTITUENCY = FMP + "/v3/sp500_constituent?apikey="
NASDAQ_CONSTITUENCY = FMP + "/v3/nasdaq_constituent?apikey="
DOWJONES_CONSTITUENCY = FMP + "/v3/dowjones_constituent?apikey="
PE_SECTOR = FMP + "/v4/sector_price_earning_ratio?date={DS}&exchange=NYSE&apikey={API}"
PE_INDUSTRY = (
    FMP + "/v4/industry_price_earning_ratio?date={DS}&exchange=NYSE&apikey={API}"
)
STOCK_PEERS = FMP + "/v4/stock_peers?symbol={TICKER}&apikey={API}"
KEY_STATS = FMP + "/v3/profile/{TICKER}?apikey={API}"
ECONOMIC_CAL = FMP + "/v3/economic_calendar?from={DSS}&to={DSE}&apikey={API}"
IPO_CONFIRMED_CAL = FMP + "/v4/ipo-calendar-confirmed?from={DSS}&to={DSE}&apikey={API}"
EARNINGS_CONFIRMED_CAL = (
    FMP + "/v4/earning-calendar-confirmed?from={DSS}&to={DSE}&apikey={API}"
)
SPLIT_CAL = FMP + "/v3/stock_split_calendar?from={DSS}&to={DSE}&apikey={API}"
DELISTED_COMPANIES = FMP + "/v3/delisted-companies?page={PAGE}&apikey={API}"

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
    "acceptedDate": dt.date,
    "calendarYear": float,
    "period": str,
    "revenue": float,
    "costOfRevenue": float,
    "grossProfit": float,
    "grossProfitRatio": float,
    "ResearchAndDevelopmentExpenses": float,
    "GeneralAndAdministrativeExpenses": float,
    "SellingAndMarketingExpenses": float,
    "sellingGeneralAndAdministrativeExpenses": float,
    "otherExpenses": float,
    "operatingExpenses": float,
    "costAndExpenses": float,
    "interestExpense": float,
    "depreciationAndAmortization": float,
    "EBITDA": float,
    "EBITDARatio": float,
    "operatingIncome": float,
    "operatingIncomeRatio": float,
    "totalOtherIncomeExpensesNet": float,
    "incomeBeforeTax": float,
    "incomeBeforeTaxRatio": float,
    "incomeTaxExpense": float,
    "netIncome": float,
    "netIncomeRatio": float,
    "EPS": float,
    "EPSDiluted": float,
    "weightedAverageShsOut": float,
    "weightedAverageShsOutDil": float,
    "link": str,
    "finalLink": str,
}

balance_sheet_types = {
    "symbol": str,
    "date": dt.date,
    "reportedCurrency": str,
    "cik": str,
    "fillingDate": dt.date,
    "acceptedDate": dt.date,
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
    "deferrredTaxLiabilitiesNonCurrent": float,
    "otherNonCurrentLiabilities": float,
    "totalNonCurrentLiabilities": float,
    "otherLiabilities": float,
    "totalLiabilities": float,
    "commonStock": float,
    "retainedEarnings": float,
    "accumulatedOtherComprehensiveIncomeLoss": float,
    "othertotalStockholdersEquity": float,
    "totalStockholdersEquity": float,
    "totalLiabilitiesAndStockholdersEquity": float,
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
    "acceptedDate": dt.date,
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
    "netCashProvidedByOperatingActivites": float,
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
