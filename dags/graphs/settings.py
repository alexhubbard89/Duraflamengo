import datetime as dt
import os

## paths
DL_DIR = os.environ["DL_DIR"].replace("data", "assets")
full_candlestick = f"{DL_DIR}/candlestick-graphs"
price_consolidation_max = f"{DL_DIR}/price-consolidation-max"
price_consolidation_avg = f"{DL_DIR}/price-consolidation-avg"
