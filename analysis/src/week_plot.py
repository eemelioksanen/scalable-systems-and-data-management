from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import polars as pl


def plot(out: Path):
    plt.rcParams.update({"font.size": 10, "figure.facecolor": "white", "axes.spines.top": False,
                         "axes.spines.right": False, "svg.hashsalt": "trading-week-analysis"})

    def save(fig, name):
        fig.tight_layout()
        fig.savefig(out / f"{name}.png", dpi=160)
        fig.savefig(out / f"{name}.svg", metadata={"Date": None})
        plt.close(fig)

    rates = pl.read_csv(out / "daily_rates_summary.csv")
    days = sorted(rates["day"].unique().to_list())
    fig, axes = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
    for exchange, color in (("ALL", "#222222"), ("FR", "#2367a3"), ("NL", "#bd5c21"), ("ETR", "#39804a")):
        rows = rates.filter(pl.col("exchange") == exchange).sort("day")
        x = [days.index(day) for day in rows["day"]]
        axes[0].plot(x, rows["peak_events_per_second"], marker=".", color=color, label=exchange)
        axes[1].plot(x, rows["p99_events_per_second"], marker=".", color=color, label=exchange)
    axes[0].set(ylabel="Peak events/s", title="Daily rates by recorded system timestamp")
    axes[1].set(ylabel="p99 events/s", xlabel="Recorded day at fixed UTC+2")
    axes[1].set_xticks(range(len(days)), [day[5:] for day in days])
    axes[0].legend(frameon=False, ncol=4)
    save(fig, "daily_rates")

    mix = pl.read_csv(out / "daily_taxonomy.csv").group_by("day", "category").agg(pl.col("rows").sum())
    fig, ax = plt.subplots(figsize=(10, 4))
    bottom = np.zeros(len(days))
    colors = {"empty_payload": "#d9d9d9", "quote": "#2367a3", "price": "#39804a",
              "other": "#bd5c21", "price_zero": "#764f91"}
    categories = list(colors) + sorted(set(mix["category"].to_list()) - colors.keys())
    for category in categories:
        color = colors.get(category, "#a33f4a")
        values = []
        for day in days:
            rows = mix.filter(pl.col("day") == day)
            values.append(rows.filter(pl.col("category") == category)["rows"].sum() / max(rows["rows"].sum(), 1))
        ax.bar(range(len(days)), values, bottom=bottom, color=color, label=category)
        bottom += np.array(values)
    ax.set_xticks(range(len(days)), [day[5:] for day in days])
    ax.set(ylabel="Share of raw data rows", xlabel="File day", title="Daily event mix")
    ax.legend(frameon=False, ncol=3, loc="upper left", bbox_to_anchor=(0, -0.19))
    save(fig, "daily_event_mix")

    bounds = pl.read_csv(out / "watermark_bounds.csv")
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for count in sorted(bounds["partitions"].unique().to_list()):
        rows = bounds.filter(pl.col("partitions") == count).sort("bound_ms")
        axes[0].plot(rows["bound_ms"], rows["discarded_events"], marker=".", label=str(count))
        axes[1].plot(rows["bound_ms"], rows["changed_closes"], marker=".", label=str(count))
    for ax, label in zip(axes, ("Discarded price events", "Missing or changed closes")):
        ax.set_xscale("symlog", linthresh=1)
        ax.set_yscale("symlog", linthresh=1)
        ax.set(xlabel="Watermark bound, ms", ylabel=label)
    axes[0].set_title("Continuous full-week source-order model")
    axes[1].legend(title="Kafka partitions", frameon=False)
    save(fig, "watermark_effects")
