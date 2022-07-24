import pandas as pd
from fmp import stocks
import common.generic as generic
import fmp.settings as s
import common.utils as utils
import os
import tda.collect as tda

if __name__ == "__main__":
    ds = (
        pd.to_datetime(os.environ["collect_thirty_minute_price_ds"])
        .tz_convert("US/Eastern")
        .date()
    )
    collect_yesterday = False
    config_ = {}
    utils.format_buffer(
        ds, s.buffer_historical_thirty_minute_price, yesterday=collect_yesterday
    )
    generic.collect_generic_distributed(
        tda.get_option_collection_list,
        s.historical_thirty_minute_price,
        s.buffer_historical_thirty_minute_price,
        stocks.collect_thirty_minute_price,
        "collect-thirty-minute-price-daily",
        **config_
    )
