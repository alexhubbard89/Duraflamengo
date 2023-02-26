from common import generic
import common.utils as utils
import datetime as dt
import fmp.settings as fmp_s

if __name__ == "__main__":
    ds = dt.date.today()
    utils.format_buffer(ds, fmp_s.buffer_technical_indicator_1_min_rsi, yesterday=False)

    technical_indicator_1_min_rsi = {
        "get_distribution_list": utils.get_watchlist,
        "dl_loc": fmp_s.technical_indicator_1_min_rsi,
        "buffer_loc": fmp_s.buffer_technical_indicator_1_min_rsi,
        "distribute_through": generic.collect_generic_ticker,
        "spark_app": "technical_indicator_1_min_rsi",
    }

    config_ = {
        "add_ticker": True,
        "url": fmp_s.RSI_ONE_MINUTE_TECHNICAL_INDICATOR,
        "buffer_dir": fmp_s.buffer_technical_indicator_1_min_rsi,
        "dl_ticker_dir": fmp_s.technical_indicator_1_min_rsi,
        "dtypes": fmp_s.technial_rsi_types,
    }

    generic.collect_generic_distributed(**technical_indicator_1_min_rsi, **config_)
