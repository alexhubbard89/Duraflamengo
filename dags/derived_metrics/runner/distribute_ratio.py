from common import generic
from derived_metrics.fundamentals import make_ratios

if __name__ == "__main__":
    generic.distribute_function(make_ratios, "ratio")
