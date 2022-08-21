import datetime as dt
import os

## paths
DL_DIR = os.environ["DL_DIR"] + "/analysis"
calls_discovery_dir = DL_DIR + "/options_discovery/calls"
puts_discovery_dir = DL_DIR + "/options_discovery/puts"
credit_spreads_calls_dir = DL_DIR + "/credit_spreads/bear_call"
credit_spreads_puts_dir = DL_DIR + "/credit_spreads/bull_put"


## Data types
_types = {
    "symbol": str,
}
