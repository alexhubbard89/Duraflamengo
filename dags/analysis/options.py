import requests
import pandas as pd
import os
import datetime as dt
import analysis.settings as analysis_s


def options_discovery(ds: dt.date, distince_threshold: int = 0.25):
    """
    Discover assets that may be good CALL or PUT candidates.
    They will be near their support or resistance.
    """
    ## Use Marty to request data
    params = {
        "read_method": "parquet",
        "path": "asset_metrics",
        "subdir": f"{str(ds)}.parquet",
    }
    r = requests.post(os.environ["LOCAL_API"] + "read_data", json=params)
    asset_metrics_df = pd.DataFrame(r.json())
    if len(asset_metrics_df) == 0:
        return None

    ## I only care about things where I have support and resistance
    ## and when the price is between the two levels, with a cutoff.
    asset_metrics_df = (
        asset_metrics_df.loc[
            (
                (asset_metrics_df["srv_compare"].notnull())
                & (asset_metrics_df["srv_compare"] < 1.05)
                & (asset_metrics_df["srv_compare"] > -0.05)
            )
        ]
        .sort_values("srv_compare")[
            [
                "symbol",
                "close",
                "close_slope",
                "srv_compare",
                "growth_rate",
                "target_price",
                "support",
                "resistance",
                "avg_rating",
                "short_term",
                "long_term",
                "mid_term",
                "avg_pt",
                "n_rating",
                "max_date",
            ]
        ]
        .dropna()
    )
    put_df = asset_metrics_df.loc[asset_metrics_df["srv_compare"] <= distince_threshold]
    call_df = asset_metrics_df.loc[
        asset_metrics_df["srv_compare"] >= 1 - distince_threshold
    ]
    call_df.to_parquet(analysis_s.calls_discovery_dir + f"/{ds}.parquet", index=False)
    put_df.to_parquet(analysis_s.puts_discovery_dir + f"/{ds}.parquet", index=False)
