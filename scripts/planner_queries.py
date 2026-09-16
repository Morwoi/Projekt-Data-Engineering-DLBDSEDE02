"""
City-planner query example for the sensor pipeline.

Planners doing trend analysis shouldn't be handed 8,640 raw 10-second
samples per station per day. This script shows the intended query
pattern instead: a MongoDB aggregation that rolls raw readings up into
daily per-station statistics (mean/min/max per metric, reading count),
which is what a planning dashboard or trend model would actually consume.

Metric stats are computed from real (source="api") readings only.
See daily_trend()'s docstring for why blending in fallback-simulator
values would be a problem for a planner specifically.

Raw readings are only kept for RAW_RETENTION_DAYS (see consumer/consumer.py,
default 90 days, TTL-enforced). A dashboard needing history beyond that
window would persist this aggregation's output (see
materialize_daily_stats.py) before the raw documents expire.

Run from the host (requires `pip install pymongo`) while
`docker compose up` is running:

    python scripts/planner_queries.py --days 30
    python scripts/planner_queries.py --days 7 --station station-01
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018")
MONGO_DB = os.environ.get("MONGO_DB", "environment_monitoring")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "sensor_readings")

# Metrics a planner cares about as trends, each rolled up to daily
# mean/min/max. Precipitation is additionally summed, since "total rainfall
# that day" is the planning-relevant figure, not its average.
METRICS = ["temperature_c", "humidity_pct", "wind_speed_kmh", "pressure_hpa"]


def daily_trend(collection, days=30, station_id=None):
    """Aggregates raw readings into one document per (station, day) with
    mean/min/max per metric, total precipitation, and reading count.

    The mean/min/max/precipitation figures use source="api" readings
    only. Averaging in fallback-simulator readings would make a day's
    trend number less trustworthy without anything in the output showing
    that. citizen_status.py already refuses to do this for a single
    reading; this is the same idea applied to an aggregate, just harder
    to notice there if you don't guard against it. api_reading_count,
    simulated_count and api_coverage_pct expose how much of a day's
    number actually rests on real measurements, so that's something the
    caller can weigh instead of it being decided for them.
    """
    group_fields = {}
    for metric in METRICS:
        api_value = {"$cond": [{"$eq": ["$source", "api"]}, f"${metric}", None]}
        group_fields[f"{metric}_avg"] = {"$avg": api_value}
        group_fields[f"{metric}_min"] = {"$min": api_value}
        group_fields[f"{metric}_max"] = {"$max": api_value}

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    initial_match = {"timestamp": {"$gte": cutoff.isoformat()}}
    if station_id:
        initial_match["station_id"] = station_id

    pipeline = [
        # Pre-filter on the indexed string fields before parsing dates.
        {"$match": initial_match},
        # timestamp is stored as an ISO string; parse to a real date so
        # it can be truncated to a calendar day.
        {"$addFields": {"_ts": {"$dateFromString": {"dateString": "$timestamp"}}}},
        {"$group": {
            "_id": {
                "station_id": "$station_id",
                "day": {"$dateTrunc": {"date": "$_ts", "unit": "day"}},
            },
            **group_fields,
            "precipitation_mm_total": {
                "$sum": {"$cond": [{"$eq": ["$source", "api"]}, "$precipitation_mm", 0]}
            },
            "reading_count": {"$sum": 1},
            "api_reading_count": {
                "$sum": {"$cond": [{"$eq": ["$source", "api"]}, 1, 0]}
            },
            "simulated_count": {
                "$sum": {"$cond": [{"$eq": ["$source", "simulated"]}, 1, 0]}
            },
        }},
        {"$sort": {"_id.station_id": ASCENDING, "_id.day": ASCENDING}},
    ]
    rows = list(collection.aggregate(pipeline))
    for row in rows:
        row["api_coverage_pct"] = (
            round(100 * row["api_reading_count"] / row["reading_count"], 1)
            if row["reading_count"] else 0.0
        )
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--days", type=int, default=30, help="how many days back to aggregate (default: 30)")
    parser.add_argument("--station", default=None, help="restrict to one station_id (default: all stations)")
    args = parser.parse_args()

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except PyMongoError as exc:
        print(f"Cannot reach MongoDB at {MONGO_URI}: {exc}")
        print("Is `docker compose up` running?")
        sys.exit(1)

    collection = client[MONGO_DB][MONGO_COLLECTION]
    rows = daily_trend(collection, days=args.days, station_id=args.station)

    if not rows:
        print(f"No readings in the last {args.days} day(s)"
              + (f" for {args.station}" if args.station else "") + ".")
        return

    def fmt(value, width, decimals=1):
        if value is None:
            return "n/a".rjust(width)
        return f"{value:{width}.{decimals}f}"

    header = (f"{'Station':<14} {'Day':<12} {'Temp avg':>9} {'Temp min/max':>14} "
              f"{'Humidity avg':>13} {'Wind avg':>9} {'Precip total':>13} "
              f"{'#Readings':>10} {'API cov.':>9}")
    print(header)
    print("-" * len(header))
    for row in rows:
        station_id = row["_id"]["station_id"]
        day = row["_id"]["day"].strftime("%Y-%m-%d")
        print(
            f"{station_id:<14} {day:<12} "
            f"{fmt(row['temperature_c_avg'], 6)}C "
            f"{fmt(row['temperature_c_min'], 4)}/{fmt(row['temperature_c_max'], 4)}C "
            f"{fmt(row['humidity_pct_avg'], 10)}% "
            f"{fmt(row['wind_speed_kmh_avg'], 6)} "
            f"{row['precipitation_mm_total']:>12.1f}mm "
            f"{row['reading_count']:>10} "
            f"{row['api_coverage_pct']:>8.0f}%"
        )

    print()
    low_coverage_days = sum(1 for row in rows if row["api_coverage_pct"] < 50)
    print("Temp/Humidity/Wind/Precip figures above are computed from real "
          "(source=\"api\") readings only; fallback-simulator readings never "
          "enter these numbers. 'API cov.' is the share of that day's readings "
          "that were real. A day at 0% had no real readings at all, so its "
          "averages show as 'n/a' rather than a synthetic-only number.")
    if low_coverage_days:
        print(f"[!] {low_coverage_days} day(s) below 50% API coverage: treat "
              "those trend points as low-confidence even though a number is shown.")


if __name__ == "__main__":
    main()
