from common import generic
import common.utils as utils
import datetime as dt
import fmp.settings as fmp_s

if __name__ == "__main__":
    ds = dt.date.today()

    ## 9 period
    utils.format_buffer(
        ds, fmp_s.buffer_technical_indicator_15_min_sma_9, yesterday=False
    )
    technical_indicator_15_min_sma_9 = {
        "get_distribution_list": utils.get_watchlist,
        "dl_loc": fmp_s.technical_indicator_15_min_sma_9,
        "buffer_loc": fmp_s.buffer_technical_indicator_15_min_sma_9,
        "distribute_through": generic.collect_generic_ticker,
        "spark_app": "technical_indicator_15_min_sma_9",
    }

    config_ = {
        "add_ticker": True,
        "url": fmp_s.SMA_9_FIFTEEN_MINUTE_TECHNICAL_INDICATOR,
        "buffer_dir": fmp_s.buffer_technical_indicator_15_min_sma_9,
        "dl_ticker_dir": fmp_s.technical_indicator_15_min_sma_9,
        "dtypes": fmp_s.technial_sma_types,
    }

    generic.collect_generic_distributed(**technical_indicator_15_min_sma_9, **config_)

    ## 50 period
    utils.format_buffer(
        ds, fmp_s.buffer_technical_indicator_15_min_sma_50, yesterday=False
    )
    technical_indicator_15_min_sma_50 = {
        "get_distribution_list": utils.get_watchlist,
        "dl_loc": fmp_s.technical_indicator_15_min_sma_50,
        "buffer_loc": fmp_s.buffer_technical_indicator_15_min_sma_50,
        "distribute_through": generic.collect_generic_ticker,
        "spark_app": "technical_indicator_15_min_sma_50",
    }

    config_ = {
        "add_ticker": True,
        "url": fmp_s.SMA_50_FIFTEEN_MINUTE_TECHNICAL_INDICATOR,
        "buffer_dir": fmp_s.buffer_technical_indicator_15_min_sma_50,
        "dl_ticker_dir": fmp_s.technical_indicator_15_min_sma_50,
        "dtypes": fmp_s.technial_sma_types,
    }

    generic.collect_generic_distributed(**technical_indicator_15_min_sma_50, **config_)
