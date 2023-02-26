from common import generic
import common.utils as utils
import datetime as dt
import fmp.settings as fmp_s

if __name__ == "__main__":
    ds = dt.date.today()
    utils.format_buffer(ds, fmp_s.buffer_earning_calendar, yesterday=False)

    earning_calendar = {
        "get_distribution_list": utils.get_watchlist,
        "dl_loc": fmp_s.earning_calendar,
        "buffer_loc": fmp_s.buffer_earning_calendar,
        "distribute_through": generic.collect_generic_ticker,
        "spark_app": "earning_calendar",
    }

    config_ = {
        "add_ticker": True,
        "url": fmp_s.EARNINGS_CALENDAR,
        "buffer_dir": fmp_s.buffer_earning_calendar,
        "dl_ticker_dir": fmp_s.earning_calendar,
        "dtypes": fmp_s.earning_calendar_types,
    }

    generic.collect_generic_distributed(**earning_calendar, **config_)
