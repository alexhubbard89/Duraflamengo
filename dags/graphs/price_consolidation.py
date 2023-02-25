import pandas as pd
import datetime as dt
from common import utils
from graphs import settings as graph_s
import derived_metrics.settings as der_s
import matplotlib.pyplot as plt
import matplotlib as mpl


class Graph:
    @staticmethod
    def draw_candlestick(axis, data, color_up, color_down):

        # Check if stock closed higher or not
        if data["close"] > data["open"]:
            color = color_up
        else:
            color = color_down

        # Plot the candle wick
        axis.plot(
            [data["day_num"], data["day_num"]],
            [data["low"], data["high"]],
            linewidth=1.5,
            color="black",
            solid_capstyle="round",
            zorder=2,
        )

        # Draw the candle body
        rect = mpl.patches.Rectangle(
            (data["day_num"] - 0.25, data["open"]),
            0.5,
            (data["close"] - data["open"]),
            facecolor=color,
            edgecolor="black",
            linewidth=1.5,
            zorder=3,
        )

        # Add candle body to the axis
        axis.add_patch(rect)

        # Return modified axis
        return axis

    @staticmethod
    def draw_all_candlesticks(axis, data, color_up, color_down):
        for day in range(data.shape[0]):
            axis = Graph.draw_candlestick(axis, data.iloc[day], color_up, color_down)
        return axis

    @staticmethod
    def make(df, ticker, tick_n=1, method="max"):
        df["day_num"] = df.index
        color_map = {"red": "#FF3032", "green": "#00B061"}
        # Create figure and axes
        f, ax = plt.subplots(figsize=(24, 8))

        # Grid lines
        ax.grid(linestyle="-", linewidth=2, color="white", zorder=1)

        # Draw candlesticks
        ax = Graph.draw_all_candlesticks(
            ax, df, color_up=color_map["green"], color_down=color_map["red"]
        )

        # Set ticks to every nth day
        ax.set_xticks(list(df["day_num"])[::tick_n])
        ax.set_xticklabels(
            list(pd.to_datetime(df["date"]).dt.strftime("%m-%d"))[::tick_n],
            rotation=50,
        )

        # Add dollar signs
        formatter = mpl.ticker.FormatStrFormatter("$%.2f")
        ax.yaxis.set_major_formatter(formatter)

        # Append ticker symbol
        ax.text(
            0,
            1.05,
            f"{ticker} Price Consolidation Score",
            va="baseline",
            ha="left",
            size=30,
            transform=ax.transAxes,
        )

        ## Consolidation score
        ax2 = ax.twinx()
        cols = [str(x) for x in df["date"]]
        fn = f"{der_s.price_consolidation_heatmap}/{ticker}.parquet"
        consolidation_df = pd.read_parquet(fn).loc[:, cols]
        ## slice price
        mm_index = consolidation_df.loc[(consolidation_df > 0).sum(1) > 0].index
        plot_min, plot_max = min(mm_index) - 0.5, max(mm_index) + 0.5
        consolidation_df = consolidation_df.loc[plot_max:plot_min]

        ## plot {method} score
        x = df.index
        if method == "max":
            graph_fn = f"{graph_s.price_consolidation_max}/{ticker}.jpg"
            y = pd.Series(consolidation_df.max().values)
        elif method == "avg":
            graph_fn = f"{graph_s.price_consolidation_avg}/{ticker}.jpg"
            y = pd.Series(consolidation_df.mean().values)
        y2 = y.rolling(3).mean()
        ax2.plot(x, y, "black", label="Price Consolidation Score")
        ax2.plot(x, y2, "grey", ls="--", label="Price Consolidation Rolling Score")

        ax.set_ylabel("Daily Price")
        ax2.set_ylabel("Consolidation Score")

        # save plot
        plt.savefig(graph_fn, bbox_inches="tight")
        plt.close()


def graph_watchlist_consolidation(method="max"):
    """
    Make price consolidation with candlestick graphs for everything on my TDA watchlist.
    """
    watch_list = utils.get_watchlist()
    for ticker in watch_list:
        graph_df = pd.read_parquet(f"{der_s.candlestick_graph_prep}/{ticker}.parquet")
        graph_df = graph_df.loc[
            graph_df["date"] >= (dt.date.today() - dt.timedelta(90))
        ].reset_index(drop=True)
        Graph.make(df=graph_df, ticker=ticker, method=method)
