import pandas as pd
import datetime as dt
import json
import time
from common import utils
import marty.settings as ms
import os
from openai import OpenAI

DL_DIR = os.environ["DL_DIR"]


class NewsSummarizer:
    def __init__(
        self,
        symbol_list=[],
        date_min=dt.date(2023, 12, 31),
        date_max=dt.date(2023, 12, 31),
        watchlist=True,
        model=4,
    ):
        # Initialize NewsSummarizer specific attributes
        self.symbol_list = symbol_list
        self.date_min = date_min
        self.date_max = date_max
        if watchlist:
            self.symbol_list = list(set(["QQQ"] + utils.get_watchlist(extend=True)))
        # load news
        self.load_news()
        self.format_news()
        # add prompt
        self.prompt = """
        Your objective is to generate comprehensive summaries of news articles with a focus on five key aspects. Please note that you should exclusively use the provided file for generating these summaries:

        1. Headlines: Highlight significant market-moving announcements found in the provided content.
        2. Earnings: Summarize recent earnings outcomes for relevant companies mentioned in the content.
        3. Topic Impact: Identify major market themes or topics and assess their impact on the market.
        4. Insights: Bring attention to noteworthy trends and anomalies discovered in the content.
        5. Market Conditions: Your main task is to classify the current market state (or business cycle) using SRA. Clearly identify whether it's a Bull Market, Bear Market, Growth Market, Value Market, or in transition. If it's a transition, specify what it's transitioning from and to. Back your classification with relevant factors, ensuring a comprehensive overview.
        Additionally, include any relevant sentiment indications, comparative analyses, and predictive insights where applicable. 
        It's essential to avoid repeating information and ensure that each section is exhaustively analyzed. 
        When discussing specific stocks, please include their ticker names. 
        The goal is to provide truthful and comprehensive responses to all the specified sections.
        """
        # set GPT
        if model == 4:
            self.marty_model = "asst_RcNliS9k9CrL1DX3KHmLmQ8O"
            self.summarization_date_dir = "watchlist-news-summary-gpt4"
        else:  # 3.5
            self.marty_model = "asst_eg70rVoPKcsAQu5iJ5ETstHc"
            self.summarization_date_dir = "watchlist-news-summary"

    def load_news(self):
        dfs = []
        for symbol in self.symbol_list:
            try:
                tmp_df = pd.read_parquet(
                    DL_DIR + f"/fmp/stock-news-ticker/{symbol}.parquet"
                )
                tmp_df["date"] = tmp_df["publishedDate"].apply(lambda r: r.date())
                tmp_df = tmp_df.loc[
                    (
                        (tmp_df["date"] >= self.date_min)
                        & (tmp_df["date"] <= self.date_max)
                    )
                ]
                dfs.append(tmp_df)
            except FileNotFoundError:
                # Optionally, you can print a warning message here
                print(f"News file not found for {symbol}, skipping.")
                continue

        df = pd.concat(dfs, ignore_index=True)
        df["date"] = df["publishedDate"].apply(lambda r: r.date())
        del dfs
        self.news_df = df

    def format_news(self):
        news_context = [
            f"Symbol:{x[0]} - Title:{x[1]}\n\tText:{x[2]}\n"
            for x in self.news_df[["symbol", "title", "text"]].values
        ]
        self.news_context = pd.DataFrame(news_context, columns=["content"])

    @staticmethod
    def daily_summarization_pipeline(ds: dt.date, yesterday=True):
        # Check if ds is a string and convert it to datetime.date if necessary
        if isinstance(ds, str):
            try:
                ds = dt.datetime.strptime(ds, "%Y-%m-%d").date()
            except ValueError:
                raise ValueError("Invalid date string format. Please use 'YYYY-MM-DD'.")
        if yesterday:
            # summarize for yesterdays news
            # used for airflow date
            ds = ds - dt.timedelta(1)

        # instantiate
        pipeline = NewsSummarizer(date_min=ds, date_max=ds, watchlist=True, model=4)

        # File existence check
        output_fn = ms.news_output.format(
            SUMMARIZATION_DATE_DIR=pipeline.summarization_date_dir, DS=ds
        )
        if os.path.exists(output_fn):
            print(
                f"Summarization for {ds} already exists. Summarization process skipped."
            )
            return  # Skip the rest of the function

        client = OpenAI(api_key=os.environ["OPEN_AI_KEY"])

        # make file with raw news for upload
        pipeline.news_context.to_csv(ms.news_input, index=False)
        file = client.files.create(file=open(ms.news_input, "rb"), purpose="assistants")

        # make thread
        thread = client.beta.threads.create()

        # set prompt
        message = client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=pipeline.prompt,
            file_ids=[file.id],
        )

        # run marty
        run = client.beta.threads.runs.create(
            thread_id=thread.id, assistant_id=pipeline.marty_model
        )

        # wait for response to generate
        generating = True
        while generating:
            # Retrieve the current status of the run
            run = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)

            # Check if the status is 'completed' and break the loop if so
            if run.status == "completed":
                generating = False

            # Wait for a short period before making the next API call
            time.sleep(2)

        # get response
        response = client.beta.threads.messages.list(thread_id=thread.id)

        # Open the file for writing
        json_data = json.loads(response.json())["data"][0]["content"][0]["text"][
            "value"
        ]
        with open(output_fn, "w", encoding="utf-8") as md_file:
            # Write the content to the Markdown file
            md_file.write(json_data)

        # remove tmp tile
        os.remove(ms.news_input)
