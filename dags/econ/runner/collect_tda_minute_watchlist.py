## python scripts
import econ.tda_collection as tda

if __name__ == "__main__":
    tda.minute_price_pipeline(collect_threshold=0.75, watchlist=True)
