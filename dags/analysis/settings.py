import datetime as dt
import os

## paths
DL_DIR = os.environ["DL_DIR"] + "/analysis"
calls_discovery_dir = DL_DIR + "/options_discovery/calls"
puts_discovery_dir = DL_DIR + "/options_discovery/puts"


## Data types
_types = {
    "symbol": str,
}
