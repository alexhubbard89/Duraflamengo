## python scripts
import econ.marketwatch_financials as fins

if __name__ == "__main__":
    _ = fins.distributed_collection(.75, 'cash-flow', loop_collect=250)
