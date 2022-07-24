import pandas as pd
from fmp import stocks
import common.generic as generic
import fmp.settings as s
import common.utils as utils
import os

if __name__ == "__main__":
    ds = (
        pd.to_datetime(os.environ["collect_full_price_ds"])
        .tz_convert("US/Eastern")
        .date()
    )
    collect_yesterday = True
    config_ = {}
    utils.format_buffer(
        ds, s.buffer_historical_daily_price_full, yesterday=collect_yesterday
    )
    generic.collect_generic_distributed(
        utils.get_to_collect,
        s.historical_daily_price_full,
        s.buffer_historical_daily_price_full,
        stocks.collect_full_price,
        "collect-price-daily",
        **config_
    )
