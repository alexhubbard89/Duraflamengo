import datetime as dt
import os

## paths
DL_DIR = os.environ["DL_DIR"]
## price
price_dir = DL_DIR + "/fmp/historical-daily-price-full"
stock_avg_buffer = DL_DIR + "/buffer/der-m-avg-price"
avg_price = DL_DIR + "/derived-measurements/avg-price-vol"
# ratios
ratios = DL_DIR + "/derived-measurements/ratios"
industry_ratios = DL_DIR + "/derived-measurements/industry-ratios"
sector_ratios = DL_DIR + "/derived-measurements/sector-ratios"
industry_rating = DL_DIR + "/derived-measurements/industry-rating"
sector_rating = DL_DIR + "/derived-measurements/sector-rating"
## other
asset_metrics = DL_DIR + "/derived-measurements/asset-metrics"


## Data types
ratio_types = {
    "symbol": str,
    "date": dt.date,
    "adjClose": float,
    "netIncome": float,
    "revenue": float,
    "weightedAverageShsOut": float,
    "commonStock": float,
    "totalAssets": float,
    "totalLiabilities": float,
    "dividendsPaid": float,
    "eps": float,
    "pe": float,
    "ps": float,
    "pb": float,
}

industry_types = {
    "industry": str,
    "date": dt.date,
    "count": float,
    "mean": float,
    "std": float,
    "min": float,
    "25%": float,
    "50%": float,
    "75%": float,
    "max": float,
}

sector_types = {
    "sector": str,
    "date": dt.date,
    "count": float,
    "mean": float,
    "std": float,
    "min": float,
    "25%": float,
    "50%": float,
    "75%": float,
    "max": float,
}

industry_rating_types = {
    "industry": str,
    "date": dt.date,
    "count": float,
    "mean": float,
    "std": float,
    "min": float,
    "25%": float,
    "50%": float,
    "75%": float,
    "max": float,
}

sector_rating_types = {
    "sector": str,
    "date": dt.date,
    "count": float,
    "mean": float,
    "std": float,
    "min": float,
    "25%": float,
    "50%": float,
    "75%": float,
    "max": float,
}

asset_metrics_types = {
    "close": float,
    "date": dt.date,
    "open": float,
    "symbol": str,
    "volume": float,
    "close_slope": float,
    "close_avg": float,
    "volume_avg": float,
    "description": str,
    "discount": str,
    "long_term": str,
    "mid_term": str,
    "resistance": float,
    "short_term": str,
    "stop_loss": float,
    "support": float,
    "target_price": float,
    "avg_rating": float,
    "avg_pt": float,
    "n_rating": float,
    "max_date": dt.date,
    "srv_compare": float,
    "slrv_compare": float,
    "growth_rate": float,
}

## maps
rating_map = {
    "S+": 30,
    "S": 28,
    "S-": 26,
    "A+": 24,
    "A": 22,
    "A-": 20,
    "B+": 18,
    "B": 16,
    "B-": 14,
    "C+": 12,
    "C": 10,
    "C-": 8,
    "D+": 6,
    "D": 4,
    "D-": 2,
}
