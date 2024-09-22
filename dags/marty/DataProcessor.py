import pandas as pd
import datetime as dt
from marty.Watchlist import Watchlist
import fmp.settings as fmp_s
import derived_metrics.settings as der_s


class DataProcessor:
    def __init__(self, ticker, end_date, lookback=90, cutoff=14, str_lb=14):
        """
        Initialize the DataProcessor class with the given parameters.
        :param ticker: The stock ticker symbol as a string.
        :param end_date: The end date for data filtering as a string in 'YYYY-MM-DD' format.
        :param lookback: The lookback period for the analysis. Default is 90 days.
        :param cutoff: The cutoff for filtering data. Default is 14 days.
        """
        self.watchlist = Watchlist()
        self.ticker = ticker
        self.set_end_date(end_date)
        self.lookback = lookback
        self.cutoff = cutoff
        self.str_lb = str_lb
        self.data = None
        self.load_data()
        self.prepare_technicals_string()

    @staticmethod
    def find_candle_patterns(row):
        candle_cols = [
            "CDL2CROWS",
            "CDL3BLACKCROWS",
            "CDL3INSIDE",
            "CDL3LINESTRIKE",
            "CDL3OUTSIDE",
            "CDL3STARSINSOUTH",
            "CDL3WHITESOLDIERS",
            "CDLABANDONEDBABY",
            "CDLADVANCEBLOCK",
            "CDLBELTHOLD",
            "CDLBREAKAWAY",
            "CDLCLOSINGMARUBOZU",
            "CDLCONCEALBABYSWALL",
            "CDLCOUNTERATTACK",
            "CDLDARKCLOUDCOVER",
            "CDLDOJI",
            "CDLDOJISTAR",
            "CDLDRAGONFLYDOJI",
            "CDLENGULFING",
            "CDLEVENINGDOJISTAR",
            "CDLEVENINGSTAR",
            "CDLGAPSIDESIDEWHITE",
            "CDLGRAVESTONEDOJI",
            "CDLHAMMER",
            "CDLHANGINGMAN",
            "CDLHARAMI",
            "CDLHARAMICROSS",
            "CDLHIGHWAVE",
            "CDLHIKKAKE",
            "CDLHIKKAKEMOD",
            "CDLHOMINGPIGEON",
            "CDLIDENTICAL3CROWS",
            "CDLINNECK",
            "CDLINVERTEDHAMMER",
            "CDLKICKING",
            "CDLKICKINGBYLENGTH",
            "CDLLADDERBOTTOM",
            "CDLLONGLEGGEDDOJI",
            "CDLLONGLINE",
            "CDLMARUBOZU",
            "CDLMATCHINGLOW",
            "CDLMATHOLD",
            "CDLMORNINGDOJISTAR",
            "CDLMORNINGSTAR",
            "CDLONNECK",
            "CDLPIERCING",
            "CDLRICKSHAWMAN",
            "CDLRISEFALL3METHODS",
            "CDLSEPARATINGLINES",
            "CDLSHOOTINGSTAR",
            "CDLSHORTLINE",
            "CDLSPINNINGTOP",
            "CDLSTALLEDPATTERN",
            "CDLSTICKSANDWICH",
            "CDLTAKURI",
            "CDLTASUKIGAP",
            "CDLTHRUSTING",
            "CDLTRISTAR",
            "CDLUNIQUE3RIVER",
            "CDLUPSIDEGAP2CROWS",
            "CDLXSIDEGAP3METHODS",
        ]
        # Filter the columns with non-zero values and get their names
        patterns = [col for col in candle_cols if row[col] != 0]
        # Join the pattern names with a comma
        return ", ".join(patterns)

    def set_end_date(self, end_date):
        """
        Sets the end_date attribute. Converts string input to datetime.date.
        """
        if isinstance(end_date, str):
            try:
                # Attempt to convert string to datetime.date
                self.end_date = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
            except ValueError as e:
                raise ValueError(
                    f"Invalid date string: {end_date}. Please use 'YYYY-MM-DD' format."
                ) from e
        elif isinstance(end_date, dt.date):
            self.end_date = end_date
        else:
            raise TypeError(
                "end_date must be a datetime.date object or a string in 'YYYY-MM-DD' format."
            )

    def load_data(self):
        """
        Load data from specified sources, merging technical analysis and historical price data.
        """
        # technical_analysis_path = (
        #     f"{der_s.enriched_technical_analysis}/{self.ticker}.parquet"
        # )

        # # Load technical analysis and historical price data, and merge them
        # # tech_df = pd.read_parquet(technical_analysis_path)
        tech_df = pd.read_csv("/Users/alexanderhubbard/Downloads/AAL_test.csv")
        tech_df["date"] = pd.to_datetime(tech_df["date"])
        vix_df = pd.read_parquet(f"{fmp_s.historical_ticker_price_full}/^VIX.parquet")[
            ["date", "close"]
        ].rename(columns={"close": "vix_close"})
        # Add earnings
        full_earnings_data = pd.read_parquet(fmp_s.earning_calendar).drop_duplicates(
            ignore_index=True
        )
        full_earnings_data["date"] = pd.to_datetime(full_earnings_data["date"]).apply(
            lambda r: r.date()
        )
        self.ticker_earnings_data = full_earnings_data.loc[
            full_earnings_data["symbol"] == self.ticker, ["date"]
        ].drop_duplicates()
        self.ticker_earnings_data["earnings"] = True
        # Add watchlist status
        prospecting_status = self.watchlist.prospecting_notes_df.loc[
            (
                (self.watchlist.prospecting_notes_df["active"] == True)
                & (self.watchlist.prospecting_notes_df["symbol"] == self.ticker)
            ),
            "contract_type",
        ].tolist()
        if len(prospecting_status) > 0:
            self.prospecting_status = prospecting_status[0]
        else:
            self.prospecting_status = "N/A"
        # Join
        vix_df["date"] = pd.to_datetime(vix_df["date"])
        self.ticker_earnings_data["date"] = pd.to_datetime(
            self.ticker_earnings_data["date"]
        )
        self.data = pd.merge(tech_df, vix_df, on="date", how="inner").merge(
            self.ticker_earnings_data, how="left", on="date"
        )
        # self.filter_data()
        # # Find candlestick patterns
        # self.data["candle_patterns_found"] = self.data.apply(
        #     self.find_candle_patterns, axis=1
        # )
        # Add earnings distance
        self.find_next_earnings()
        # self.find_earnings_distance()

    def filter_data(self):
        """
        Filter the loaded data based on the end_date and lookback period.
        """
        start_date = self.end_date - dt.timedelta(days=self.lookback)
        self.data = self.data[
            (self.data["date"] >= start_date) & (self.data["date"] <= self.end_date)
        ]

    def find_next_earnings(self):
        self.next_earnings = self.ticker_earnings_data.loc[
            self.ticker_earnings_data["date"] > self.data["date"].max(), "date"
        ].min()

    def find_earnings_distance(self):
        earnings_index = self.data.loc[self.data["earnings"] == True].index
        self.data.loc[earnings_index, "next_earnings_date"] = self.data.loc[
            earnings_index, "date"
        ]
        self.data.loc[max(self.data.index), "next_earnings_date"] = self.next_earnings
        self.data["next_earnings_date"] = self.data["next_earnings_date"].fillna(
            method="bfill"
        )
        try:
            self.data["days_next_earnings"] = (
                self.data["next_earnings_date"] - self.data["date"]
            ).apply(lambda r: r.days)
        except:
            self.data["days_next_earnings"] = -1

    def prepare_technicals_string(self):
        """
        Prepares the technicals data into string format for display.
        """
        # Assuming `self.data` is already filtered and contains the last `self.cutoff` days data
        tech_lb_data = self.data  # self.data.tail(self.cutoff)

        # Preparing the display dictionary
        data_to_display = {
            "Date": [x.strftime("%Y-%m-%d") for x in tech_lb_data["date"]],
            "Open": tech_lb_data["open"].tolist(),
            "High": tech_lb_data["high"].tolist(),
            "Low": tech_lb_data["low"].tolist(),
            "Close": tech_lb_data["close"].tolist(),
            "Volume": tech_lb_data["volume"].tolist(),
            "RSI": tech_lb_data.get(
                "rsi", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "MACD": tech_lb_data.get(
                "macd", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "MACD Signal": tech_lb_data.get(
                "macdSignal", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "MACD Hist": tech_lb_data.get(
                "macdHist", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "ADX Signal": tech_lb_data.get(
                "adx_signal", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "Minus DI Signal": tech_lb_data.get(
                "minus_di_signal", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "Upper BB": tech_lb_data.get(
                "upper_band_bb", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "Middle BB": tech_lb_data.get(
                "middle_band_bb", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "Lower BB": tech_lb_data.get(
                "lower_band_bb", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "Senkou Span A": tech_lb_data.get(
                "senkou_span_a", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "Senkou Span B": tech_lb_data.get(
                "senkou_span_b", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "Tenkan-sen": tech_lb_data.get(
                "tenkan_sen", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "Kijun-sn": tech_lb_data.get(
                "kijun_sen", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "Chikou Span": tech_lb_data.get(
                "chikou_span", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "Close AVG 10": tech_lb_data.get(
                "close_avg_10", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "Close AVG 20": tech_lb_data.get(
                "close_avg_20", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "Close AVG 50": tech_lb_data.get(
                "close_avg_50", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "Close AVG 100": tech_lb_data.get(
                "close_avg_100", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "Close AVG 200": tech_lb_data.get(
                "close_avg_200", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "Candle Stick Patterns Found": tech_lb_data.get(
                "candle_patterns_found", pd.Series(["Not Plotted"] * self.str_lb)
            ).tolist(),
            "Fibonacci Level 0": tech_lb_data["fibonacci_level_0"].tolist(),
            "Fibonacci Level 0.236": tech_lb_data["fibonacci_level_0.236"].tolist(),
            "Fibonacci Level 0.382": tech_lb_data["fibonacci_level_0.382"].tolist(),
            "Fibonacci Level 0.5": tech_lb_data["fibonacci_level_0.5"].tolist(),
            "Fibonacci Level 0.618": tech_lb_data["fibonacci_level_0.618"].tolist(),
            "Fibonacci Level 0.786": tech_lb_data["fibonacci_level_0.786"].tolist(),
            "Fibonacci Level 1": tech_lb_data["fibonacci_level_1"].tolist(),
            "Average True Range (ATR) Conversion Period": tech_lb_data[
                "atr_conversion_period"
            ].tolist(),
            "Average True Range (ATR) Base Period": tech_lb_data[
                "atr_base_period"
            ].tolist(),
            "Average True Range (ATR) Leading Span B Period": tech_lb_data[
                "atr_leading_span_b_period"
            ].tolist(),
        }

        # Constructing the string
        technicals_input = f"Last {self.cutoff} Days' Data\n"
        for key, values in data_to_display.items():
            technicals_input += f"{key}: {' | '.join(map(str, values))}\n"

        # Add last earnings date and next earnings date
        if self.data[self.data["earnings"] == True].empty:
            last_earnings_date = "Last earnings date not found"
        else:
            last_earnings_date = str(
                self.data[self.data["earnings"] == True]["date"].max()
            )

        next_earnings_date = (
            str(self.next_earnings)
            if self.next_earnings
            else "Next earnings date not found"
        )
        technicals_input += f"Last Earnings Date: {last_earnings_date}\nNext Earnings Date: {next_earnings_date}\n"

        # Save the string to an attribute
        self.technicals_input = technicals_input
