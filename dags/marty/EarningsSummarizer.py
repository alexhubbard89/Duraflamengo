import pandas as pd
import fmp.settings as fmp_s
import derived_metrics.settings as der_s
import datetime as dt
import tda.settings as tda_s
from common import utils
import os

import openai

openai.api_key = os.environ["OPEN_AI_KEY"]
model_type = "gpt-3.5-turbo-16k-0613"


class EarningsSummarizer:
    def __init__(
        self, symbol, year, quarter, max_token_limit, use_free_summarizer=True
    ):
        self.symbol = symbol
        self.year = year
        self.quarter = quarter
        self.max_token_limit = max_token_limit
        self.use_free_summarizer = use_free_summarizer
        self.analysis_types = ["financial", "business", "guidance"]
        self.summary_data = pd.DataFrame()
        self.refined_summary = pd.DataFrame()
        # Load data
        self.load_transcript()
        self.make_empty_dir()
        # Load summarization prompts for Kathy
        self.PROMPT_DICT = {
            "financial": "Be brief and informative.\n\nRevenue: Highlight revenue trends discussed in the earnings call transcript.\n\nProfit: Summarize profit-related information from the earnings call transcript.\n\nGrowth Trends: Summarize growth trends discussed in the earnings call transcript.",
            "business": "Be brief and informative.\n\nMarket Trends: Highlight market trends discussed in the earnings call transcript.\n\nCompetition Analysis: Summarize competition analysis from the earnings call transcript.\n\nProduct Performance: Summarize product performance-related information from the earnings call transcript.\n\nCustomer Insights: Summarize customer insights discussed in the earnings call transcript.",
            "guidance": "Be brief and informative.\n\nFuture Projections: Summarize future projections discussed in the earnings call transcript.\n\nStrategic Insights: Highlight strategic insights from the earnings call transcript.\n\nSurprises and Notable Moments: Summarize surprises and notable moments discussed in the earnings call transcript.",
        }

    def make_empty_dir(self):
        """
        Make sure that path exists before making data.
        Make parent paths leading to the required child path.
        """
        dir_path = self.get_summary_dir()
        for i in [3, 2, 1]:
            utils.mk_dir("/".join(dir_path.split("/")[:-i]))
        utils.mk_dir(dir_path)

    def load_transcript(self):
        # Load transcript dataframe from data lake
        self.transcript_df = pd.read_parquet(
            f"{fmp_s.earning_call_transcript}/{self.symbol}.parquet"
        )
        self.transcript_df["report_year"] = self.transcript_df["date"].apply(
            lambda r: r.year
        )
        self.transcript_df["report_quarter"] = self.transcript_df["date"].apply(
            lambda r: (int(r.month / 4)) + 1
        )
        self.transcript_df = self.transcript_df.loc[
            (
                (self.transcript_df["report_year"] == self.year)
                & (self.transcript_df["report_quarter"] == self.quarter)
            )
        ].reset_index(drop=True)

    def get_summary_dir(self):
        # Generate the path for a summary dataset based on analysis type and lineage
        summary_path = f"{os.environ['DL_DIR']}/kathy/earnings-call-summary/{self.symbol}/{self.year}/{self.quarter}"
        return summary_path

    def get_summary_path(self, analysis_type, lineage):
        # Generate the path for a summary dataset based on analysis type and lineage
        summary_path = f"{os.environ['DL_DIR']}/kathy/earnings-call-summary/{self.symbol}/{self.year}/{self.quarter}/{analysis_type}_{lineage}.parquet"
        return summary_path

    def get_max_lineage(self):
        fn = self.get_summary_dir()
        fl = os.listdir(fn)
        fl = [x for x in fl if x[0] != "."]
        if len(fl) == 0:
            return -1
        return max([int(x.split(f"_")[1].split(".parquet")[0]) for x in fl])

    def check_summary_data_exists(self):
        # Check if summary datasets exist for the specified symbol, year, and quarter
        summary_exists = False
        max_lineage = self.get_max_lineage()
        for analysis_type in self.analysis_types:
            summary_path = self.get_summary_path(analysis_type, max_lineage)
            if os.path.isfile(summary_path):
                summary_exists = True
            else:
                summary_exists = False
                return False
        return summary_exists

    def summarize_manuel(self, prompt, text):
        """
        Placeholder summarizer function that prompts the user for input.

        Args:
        - summarize_prompt (str): The summarization prompt.

        Returns:
        - str: The user's input as the summary.
        """
        print("\n\nSummarization Prompt:\n\n")
        print(prompt)
        print(text)
        summary = input("Enter your summary: ")

        return summary

    def summarize_with_kathy(self, prompt, text):
        response = openai.ChatCompletion.create(
            model=model_type,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": text},
            ],
            max_tokens=self.max_token_limit,
        )
        return response["choices"][0]["message"]["content"]

    def add_prompt(self, analysis, prompt):
        """
        Add a prompt to the default set
        or edit a default prompt.
        """
        prompt = self.PROMPT_DICT[analysis]
        self.analysis_types.append(analysis)

    def summarize_text_chunks(self, analysis, analysis_lineage):
        """
        Summarizes lengthy text by segmenting it into chunks and generating summaries for each segment.

        This function employs a three-phase approach:

        Phase 1: Summarize the entire text using input chunks (raw earnings transcript text).
        - Each chunk receives a summary.

        Phase 2: Concatenate summaries for further summarization.
        - Ensure that concatenated text + prompt remains within the max token limit.
        - If exceeded, summarize the concatenated text, store the summary, start a new segment, and continue concatenating.
        - Repeat until the text is condensed below the token limit.

        Phase 3: After the text is below the token limit, perform a final summarization using a second prompt for refined output.

        Args:
        - analysis (str): The method of analsis for summarization.

        Returns:
        - str: The polished final summary of the input text.
        """
        # Initialize prompt
        prompt = self.PROMPT_DICT[analysis]

        ## turn transcript text into chunks with promp
        chunk_size = self.max_token_limit - (len(prompt) + 10)
        transcript_txt = self.transcript_df.loc[0, "content"]
        index_ = [x for x in range(0, len(transcript_txt), chunk_size)] + [
            len(transcript_txt)
        ]
        transcript_chunks = [
            transcript_txt[index_[i - 1] : index_[i]] for i in range(1, len(index_))
        ]

        # Initialize variables
        current_chunk = ""
        summary_df = pd.DataFrame()

        while len(transcript_chunks) != 1:
            print(
                f"Transcript Chunk size and analysis: {len(transcript_chunks)} {analysis}\n"
            )
            # Calculate the potential size of the concatenated chunk
            transcript_chunks_item = transcript_chunks.pop(0)
            potential_size = len(current_chunk) + len(
                " ".join([current_chunk, transcript_chunks_item])
            )

            ## time to summarize
            if potential_size > self.max_token_limit:
                ## Add prompt to start of chunk
                if self.use_free_summarizer:
                    summary_text = self.summarize_manuel(
                        prompt, current_chunk
                    )  ## replace with Kathy
                    analyst = "manuel"
                    df_index_ = len(summary_df)
                else:
                    summary_text = self.summarize_with_kathy(prompt, current_chunk)
                    analyst = "kathy"
                # Input data
                df_index_ = len(summary_df)
                summary_df.loc[df_index_, "analysis"] = analysis
                summary_df.loc[df_index_, "analyst"] = analyst
                summary_df.loc[df_index_, "prompt"] = prompt
                summary_df.loc[df_index_, "chunk"] = current_chunk
                summary_df.loc[df_index_, "chunk_lineage"] = df_index_
                summary_df.loc[df_index_, "analysis_lineage"] = analysis_lineage
                summary_df.loc[df_index_, "summary"] = summary_text
                ## Add to end end of chunks. This will shrink to 1
                transcript_chunks.append(summary_text)
                ## restart chunks
                current_chunk = ""

            elif potential_size <= self.max_token_limit:
                current_chunk = " ".join([current_chunk, transcript_chunks_item])

        fn = self.get_summary_path(analysis, analysis_lineage)
        summary_df.to_parquet(fn, index=False)

    def load_max_lineage_summary(self):
        # Load the summary with the maximum lineage for a specific analysis type
        max_lineage = self.get_max_lineage()
        dfs = []
        for analysis in self.analysis_types:
            summary_fn = self.get_summary_path(analysis, max_lineage)
            dfs.append(pd.read_parquet(summary_fn))
        self.summary_data = pd.concat(dfs, ignore_index=True)

    def load_refined_summary(self):
        self.refined_summary = self.summary_data.sort_values(
            "chunk_lineage", ascending=False
        ).drop_duplicates(subset=["analysis"])

    def complete_summarization(self):
        if self.check_summary_data_exists():
            # Prevent spending extra money atm
            return True  # Already have data
        next_lineage = self.get_max_lineage() + 1
        # Summarize using existing summaries for all analysis types
        for analysis in self.analysis_types:
            self.summarize_text_chunks(analysis, next_lineage)
        return True
