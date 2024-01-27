import datetime as dt
import os

## paths
DL_DIR = os.environ["DL_DIR"]

# news summarizer
news_input = DL_DIR + "/marty/news-summary/daily_news_tmp.csv"
news_output = DL_DIR + "/marty/news-summary/{SUMMARIZATION_DATE_DIR}/{DS}.md"
