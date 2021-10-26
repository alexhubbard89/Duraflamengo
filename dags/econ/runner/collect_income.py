## python scripts
import econ.marketwatch_financials as fins

if __name__ == "__main__":
    _ = fins.distributed_collection(.75, 'income', loop_collect=250)