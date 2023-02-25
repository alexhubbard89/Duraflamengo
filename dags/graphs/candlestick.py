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
    def make(df, ticker, tick_n=1):
        df["day_num"] = df.index
        color_map = {"red": "#FF3032", "green": "#00B061"}
        # Create figure and axes
        f, (ax, ax1, ax2, ax3) = plt.subplots(
            4, 1, figsize=(30, 20), gridspec_kw={"height_ratios": [3, 1, 2, 1]}
        )

        # Grid lines
        ax.grid(linestyle="-", linewidth=2, color="white", zorder=1)

        # Draw candlesticks
        ax = Graph.draw_all_candlesticks(
            ax, df, color_up=color_map["green"], color_down=color_map["red"]
        )

        # Set ticks to every nth day
        for _ax in [ax, ax1]:
            _ax.set_xticks(list(df["day_num"])[::tick_n])
            _ax.set_xticklabels(
                list(pd.to_datetime(df["date"]).dt.strftime("%m-%d"))[::tick_n],
                rotation=50,
            )

        # Add dollar signs
        formatter = mpl.ticker.FormatStrFormatter("$%.2f")
        ax.yaxis.set_major_formatter(formatter)

        # Append ticker symbol
        ax.text(
            0, 1.05, ticker, va="baseline", ha="left", size=30, transform=ax.transAxes
        )

        # Set axis limits
        ax.set_xlim(-1, df["day_num"].iloc[-1] + 1)

        x = df["day_num"]
        for i in [5, 13, 50, 100, 200]:
            y = df[f"close_avg_{i}"]
            ax.plot(x, y, label=f"Rolling {i}")
        ax.legend(loc="upper left")

        ax1.bar(df["day_num"], df["volume"])
        ax1.grid(False)

        ax2.plot(df["day_num"], df["rsi"])
        ax2.set_ylim(0, 100)
        ax2.axhline(30, ls="--", color=color_map["red"])
        ax2.axhline(70, ls="--", color=color_map["red"])
        ax2.grid(False)

        ax3.plot(df["day_num"], df["macd"])
        ax3.grid(False)

        pattern_list = df["pattern_list"]
        annotate_y = df["high"] + 1
        for i in range(len(pattern_list)):
            if len(pattern_list[i]) == 0:
                continue
            txt = "\n".join([s for s in pattern_list[i]])
            ax.annotate(txt, (i - 0.5, annotate_y[i]), fontsize=14)

        # save plot
        plt.savefig(f"{graph_s.full_candlestick}/{ticker}.jpg", bbox_inches="tight")


def graph_watchlist_candlesticks():
    """
    Make enriched candlestick graphs for everything on my TDA watchlist.
    """
    watch_list = utils.get_watchlist()
    for ticker in watch_list:
        graph_df = pd.read_parquet(f"{der_s.candlestick_graph_prep}/{ticker}.parquet")
        graph_df = graph_df.loc[
            graph_df["date"] >= (dt.date.today() - dt.timedelta(90))
        ].reset_index(drop=True)
        Graph.make(df=graph_df, ticker=ticker)
