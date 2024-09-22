import pandas as pd
import datetime as dt
import json
import time
import os

from marty.Watchlist import Watchlist
from marty.DataProcessor import DataProcessor
from marty.NewsSummarizer import NewsSummarizer

from openai import OpenAI


class MartyTechnicalAnalysis:
    def __init__(self, model_id="asst_iYc8OavK1Ps5sK64B3AoyMAK"):
        """
        Initializes the MartyTechnicalAnalysis class with necessary configurations.
        """
        self.fn = os.environ["DL_DIR"] + "/marty/technical-analysis/full_data.csv"
        self.api_key = os.environ["OPEN_AI_KEY"]
        self.model_id = model_id
        self.client = OpenAI(api_key=self.api_key)
        self.watchlist = Watchlist()
        self.watchlist.refresh_watchlist(extended=False)
        self.data_processor = DataProcessor
        self.news_summarizer = NewsSummarizer

        self.SINGLE_TECHNICALS_PROMPT = """Objective:
        Your task is to conduct an in-depth technical analysis of a single ticker from your watchlist over the last 14 days. This analysis aims to dissect the ticker's market behavior, examining its technical signals, patterns, and trends in detail. The goal is to provide a comprehensive report that not only decodes the ticker's current market position but also predicts its short-term movements with a high degree of precision.

        Report Format:
        - Title: In-Depth Analysis of [Ticker Name]
        - Structure: The report will maintain a detailed and analytical tone, treating the ticker as the focal point of a thorough market investigation. Your analysis will uncover and interpret significant technical signals and patterns, presenting the findings in a clear, concise, and engaging manner.
        - Accuracy: Ensuring the reliability of your insights is crucial. The analysis must be deeply rooted in the data, making sure that the conclusions are sound, timely, and reflective of the ticker's current and potential market conditions.
        - The output will be displayed using markdown. Please format accordingly.
        - Put a summary at the top. No need for an overview to explain the analysis.
        - Include a score within the summary. It should be very to easy to tell where to spend my attention on further analysis.
        
        Output Requirements:
        - Data Deliverable: A comprehensive textual report that offers a detailed analysis of the ticker, including its categorization based on the technical analysis and a prediction of its short-term market behavior.
        - Report Date: State the last date of the report as the analysis date.
        - Technical Analysis Summary: A concise summary at the end of the report that categorizes the ticker as CALL, PUT, MONITOR, or HOLD, based on the detailed analysis. This summary should also include a forecast of the ticker's potential movement direction and magnitude.
        - If the analysis results in a CALL or PUT classification, please include a score, with explained rationale to help identify which potential trades I should prioritize for further analysis.
        
        Evaluation Criteria for the Ticker:
        - CALLs: Indicators such as prices breaking above resistance, bullish crossovers, low RSI, and increasing trading volume suggest a bullish outlook.
        - PUTs: Indicators such as prices breaking below support, bearish crossovers, high RSI, and decreasing trading volume suggest a bearish outlook.
        - MONITOR: The ticker is on the verge of critical levels or exhibits patterns indicating that a significant move is imminent, but the direction remains uncertain.
        - HOLD: The ticker shows no discernible trend or its signals are mixed, suggesting no immediate action is recommended.
        
        Task for Marty:
        - Detailed Categorization: Place the ticker into the appropriate category that reflects its current and predicted technical posture, based on a meticulous analysis of the provided indicators.
        - In-Depth Analysis: Provide a comprehensive rationale for the categorization, integrating technical insights with a narrative that makes the analysis insightful, actionable, and understandable.
        -- If your evaluation points you to a trade. Please mark the entry, exit, and stop loss points.
        - Pattern Identification: Vigilantly identify and underscore recurring patterns or notable anomalies that could inform future trades, contribute to a recommendation system, or guide more detailed analysis in the future.
        -- Tell me what patterns mean.
        --------
        """

    def generate_technical_analysis_report(self, prompt_input):
        """
        Generates a technical analysis report using the OpenAI API and saves the output.
        """
        thread = self.client.beta.threads.create()
        message = self.client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=prompt_input,
        )
        run = self.client.beta.threads.runs.create(
            thread_id=thread.id, assistant_id=self.model_id
        )

        generating = True
        while generating:
            run_status = self.client.beta.threads.runs.retrieve(
                thread_id=thread.id, run_id=run.id
            )

            if run_status.status == "completed":
                break
                generating = False
            elif run_status.status in ["cancelling", "cancelled", "failed", "expired"]:
                raise Exception(f"Thread {thread.id} ran into an issue")

            time.sleep(2)
        response = self.client.beta.threads.messages.list(thread_id=thread.id)

        # Open the file for writing
        json_data = json.loads(response.model_dump_json())["data"][0]["content"][0][
            "text"
        ]["value"]

        return json_data

    def report_exists(self, ticker, date):
        """
        Checks if a report for the given ticker and date already exists.
        """
        existing_reports = pd.read_csv(f"{self.fn}")
        return not existing_reports[
            (existing_reports["ticker"] == ticker) & (existing_reports["date"] == date)
        ].empty

    def make_report(
        self, ticker, date, lookback=14, include_news=False, override=False
    ):
        """
        Generates a technical analysis report if one does not already exist or if override is True.
        """
        if self.report_exists(ticker, date) and not override:
            print("Report already exists. Set override=True to regenerate.")
            return

        # load and format data
        data_processor = self.data_processor(ticker, date, cutoff=lookback)
        data_processor.load_data()
        ticker_prompt = (
            self.SINGLE_TECHNICALS_PROMPT
            + f"""
        Ticker: {ticker}
        {data_processor.technicals_input}
        """
        )

        # Do not use this yet
        # if include_news:
        #     date = pd.to_datetime(date).date()
        #     start_date = date - dt.timedelta(lookback)
        #     headlines = self.news_summarizer(
        #         date_min=start_date,
        #         date_max=date,
        #         symbol_list=[ticker],
        #         watchlist=False,
        #         model=4,
        #     )
        #     headlines.news_df = headlines.news_df[["date", "title", "text"]]
        #     news_txt = "\nRecent News:\n"
        #     for i in headlines.news_df.index:
        #         title = headlines.news_df.loc[i, "title"]
        #         txt = headlines.news_df.loc[i, "text"]
        #         date_txt = headlines.news_df.loc[i, "date"]
        #         news_txt += f"{date_txt} - Title: {title}\n\tSummary: {txt}\n\n"
        #     ticker_prompt += news_txt

        #     marty_technical_analysis = self.generate_technical_analysis_report(
        #         ticker_prompt
        #     )

        marty_technical_analysis = self.generate_technical_analysis_report(
            ticker_prompt
        )

        # Directly load the existing dataframe, append the new data, and save.
        full_df = pd.read_csv(f"{self.fn}")
        new_data = pd.DataFrame(
            {
                "ticker": [ticker],
                "date": [date],
                "input_length": [lookback],
                "marty_anlysis": [marty_technical_analysis],
                "generated_at": [dt.datetime.now()],
            }
        )

        full_df = full_df.append(new_data, ignore_index=True)
        full_df["generated_at"] = pd.to_datetime(full_df["generated_at"])
        full_df.sort_values("generated_at", ascending=False, inplace=True)
        full_df.reset_index(drop=True, inplace=True)
        full_df.to_csv(f"{self.fn}", index=False)
