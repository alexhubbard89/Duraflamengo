import datetime as dt
import os

## GLOBAL
DL_DIR = os.environ["DL_DIR"]
YAHOO_KEY = os.environ["YAHOO_KEY"]

#### PATHS
avg_price = DL_DIR + "/derived-measurements/avg-price-vol"
company_shapshot_ticker = DL_DIR + "/yahoo-api/ticker/company-shapshot"
instrument_info_ticker = DL_DIR + "/yahoo-api/ticker/instrument-info"
reports_ticker = DL_DIR + "/yahoo-api/ticker/reports"
company_shapshot_daily = DL_DIR + "/yahoo-api/daily/company-shapshot"
instrument_info_daily = DL_DIR + "/yahoo-api/daily/instrument-info"
reports_daily = DL_DIR + "/yahoo-api/daily/reports"
buffer_company_shapshot = DL_DIR + "/buffer/yahoo-api/company-shapshot"
buffer_instrument_info = DL_DIR + "/buffer/yahoo-api/instrument-info"
buffer_reports = DL_DIR + "/buffer/yahoo-api/reports"

#### URLS
HEADERS = {"x-api-key": YAHOO_KEY}
INSIGHTS = "https://yfapi.net/ws/insights/v1/finance/insights?symbol={TICKER}"

#### DATA TYPES
reports_types = {
    "ticker": str,
    "date": dt.date,
    "id": str,
    "title": str,
    "provider": str,
    "publishedOn": dt.datetime,
    "summary": str,
}

company_shapshot_types = {
    "ticker": str,
    "date": dt.date,
    "sector_info": str,
    "company_innovativeness": float,
    "company_hiring": float,
    "company_sustainability": float,
    "company_insider_sentiments": float,
    "company_earnings_reports": float,
    "company_dividends": float,
    "sector_innovativeness": float,
    "sector_hiring": float,
    "sector_sustainability": float,
    "sector_insiderSentiments": float,
    "sector_earningsReports": float,
    "sector_dividends": float,
}

instrument_info_types = {
    "ticker": str,
    "date": dt.date,
    "technical_evensts_provider": str,
    "short_term": str,
    "mid_term": str,
    "long_term": str,
    "key_technicals_provider": str,
    "support": float,
    "resistance": float,
    "stop_loss": float,
    "color": float,
    "description": str,
    "discount": str,
    "relative_value": str,
    "valuation_provider": str,
    "target_price": float,
    "recommendation_provider": str,
    "rating": str,
}
