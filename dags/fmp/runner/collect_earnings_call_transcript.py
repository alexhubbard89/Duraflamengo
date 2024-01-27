from fmp import stocks
import datetime as dt

if __name__ == "__main__":
    year = dt.date.today().year
    stocks.collect_earnings_call_transcript(year)
    stocks.collect_earnings_call_transcript(year + 1)
