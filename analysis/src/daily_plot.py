import argparse
import datetime as dt
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import polars as pl

from daily_parse import PARTITION_COUNTS, offset_ms


COLORS = {"FR": "#2367a3", "NL": "#bd5c21", "ETR": "#39804a"}


def plot(out, zone_offset):
    plt.rcParams.update({"font.size": 10, "figure.facecolor": "white", "axes.spines.top": False,
                         "axes.spines.right": False, "svg.hashsalt": "trading-analysis"})
    zone = dt.timezone(dt.timedelta(milliseconds=offset_ms(zone_offset)))

    def save(fig, name):
        fig.tight_layout()
        fig.savefig(out / f"{name}.png", dpi=160)
        fig.savefig(out / f"{name}.svg", metadata={"Date": None})
        plt.close(fig)

    rates = pl.read_csv(out / "rates_minute.csv")
    fig, axes = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
    for exchange, color in COLORS.items():
        rows = rates.filter(pl.col("exchange") == exchange).sort("minute")
        if rows.is_empty():
            continue
        minutes = np.arange(rows["minute"].min(), rows["minute"].max()+60, 60)
        indices = (rows["minute"].to_numpy()-minutes[0])//60
        average, peak = np.zeros(len(minutes)), np.zeros(len(minutes))
        average[indices] = rows["events"].to_numpy()/60
        peak[indices] = rows["peak_events_second"].to_numpy()
        clock = [dt.datetime.fromtimestamp(int(second), zone) for second in minutes]
        axes[0].plot(clock, average, label=exchange, color=color, linewidth=1)
        axes[1].plot(clock, peak, label=exchange, color=color, linewidth=1)
    axes[0].set(title=f"Events by recorded system timestamp · {out.name}", ylabel="Events/s, minute average")
    axes[1].set(ylabel="Peak events/s within minute", xlabel=f"Recorded system time, UTC{zone_offset}")
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=zone))
    axes[0].legend(frameon=False, ncol=3)
    save(fig, "arrival_rates")

    symbols = pl.read_csv(out / "symbols.csv").group_by("symbol").agg(pl.col("raw_events","accepted_price_events").sum())
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for column, label, color in [("raw_events","All rows","#2367a3"),("accepted_price_events","Accepted price events","#bd5c21")]:
        values = np.sort(symbols[column].to_numpy())[::-1]
        axes[0].loglog(np.arange(1,len(values)+1), np.where(values>0,values,np.nan),label=label,color=color)
        axes[1].plot(np.arange(1,len(values)+1),np.cumsum(values)/max(values.sum(),1),label=label,color=color)
    axes[0].set(xlabel="Symbol rank",ylabel="Events",title="Rank frequency")
    axes[1].set(xlabel="Top k symbols",ylabel="Share of events",title="Concentration")
    axes[0].legend(frameon=False)
    save(fig,"symbol_skew")

    load = pl.read_csv(out / "partition_load.csv")
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for ax,column,label in zip(axes,["raw_events","accepted_price_events"],["All rows","Accepted price events"]):
        for n in PARTITION_COUNTS:
            values = load.filter(pl.col("partitions")==n).sort("partition")[column].to_numpy()
            ax.plot(np.arange(n),values/max(values.mean(),1),marker=".",label=str(n))
        ax.axhline(1,color="#999999",linewidth=0.7)
        ax.set(title=label,xlabel="Kafka partition",ylabel="Events / mean partition events")
    axes[1].legend(title="Partitions",frameon=False,ncol=2)
    save(fig,"partition_load")

    residue = pl.read_csv(out / "timestamp_residues.csv")
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for clock,color in [("system","#2367a3"),("trading","#bd5c21")]:
        rows=residue.filter(pl.col("clock")==clock)
        axes[0].plot(rows["residue_ms"],rows["events"],label=clock,color=color)
    axes[0].set(xlabel="Milliseconds within second",ylabel="Price events",yscale="symlog",title="Timestamp resolution")
    axes[0].legend(frameon=False)
    gaps=pl.read_csv(out / "timestamp_gap_histogram.csv")
    axes[1].plot(gaps["gap_bucket_ms"],gaps["events"],".",markersize=2,color="#2367a3")
    axes[1].set_xscale("symlog", linthresh=1500)
    axes[1].set(xlabel="System minus trading timestamp, ms",ylabel="Price events",yscale="symlog",title="Timestamp gaps, 10 ms bins")
    save(fig,"timestamp_quality")

    windows=pl.read_csv(out / "window_statistics.csv")
    fig,axes=plt.subplots(1,2,figsize=(10,4))
    session=windows.filter(pl.col("observed_exchange_windows")>0)
    axes[0].hist(session["empty_observed_exchange_windows"].to_numpy()/session["observed_exchange_windows"].to_numpy(),bins=30,color="#2367a3")
    axes[0].set(xlabel="Empty share within observed exchange span",ylabel="Symbols",title="Empty five-minute windows")
    hist=pl.read_csv(out / "window_event_histogram.csv")
    axes[1].loglog(hist["price_events"],hist["windows"],".",color="#bd5c21")
    axes[1].set(xlabel="Price events in nonempty window",ylabel="Windows",title="Window density")
    save(fig,"window_density")


if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("out",type=Path)
    parser.add_argument("--zone-offset",default="+02:00")
    args=parser.parse_args()
    plot(args.out,args.zone_offset)
