from common import generic
import common.utils as utils
import datetime as dt
import fmp.settings as fmp_s

if __name__ == "__main__":
    ds = dt.date.today()
    utils.format_buffer(ds, fmp_s.buffer_historical_chart_1_min, yesterday=False)

    historical_chart_1_min = {
        "get_distribution_list": utils.get_watchlist,
        "dl_loc": fmp_s.historical_chart_1_min,
        "buffer_loc": fmp_s.buffer_historical_chart_1_min,
        "distribute_through": generic.collect_generic_ticker,
        "spark_app": "historical_chart_1_min",
    }

    config_ = {
        "add_ticker": True,
        "url": fmp_s.ONE_MINUTE_INTRADAY_PRICE,
        "buffer_dir": fmp_s.buffer_historical_chart_1_min,
        "dl_ticker_dir": fmp_s.historical_chart_1_min,
        "dtypes": fmp_s.intraday_price_types,
    }

    generic.collect_generic_distributed(**historical_chart_1_min, **config_)
