from common import generic
import common.utils as utils
import datetime as dt
import fmp.settings as fmp_s

if __name__ == "__main__":
    ds = dt.date.today()
    utils.format_buffer(ds, fmp_s.buffer_shares_float, yesterday=False)

    shares_float = {
        "get_distribution_list": utils.get_watchlist,
        "dl_loc": fmp_s.shares_float,
        "buffer_loc": fmp_s.buffer_shares_float,
        "distribute_through": generic.collect_generic_ticker,
        "spark_app": "shares_float",
    }

    config_ = {
        "add_ticker": True,
        "url": fmp_s.SHARES_FLOAT,
        "buffer_dir": fmp_s.buffer_shares_float,
        "dl_ticker_dir": fmp_s.shares_float,
        "dtypes": fmp_s.shares_float_types,
    }

    generic.collect_generic_distributed(**shares_float, **config_)
