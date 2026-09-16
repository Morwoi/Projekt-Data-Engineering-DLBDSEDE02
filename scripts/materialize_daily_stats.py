"""
Materializes planner_queries.py's daily aggregation into a persistent
`daily_station_stats` collection, upserted per (station_id, day).

planner_queries.py recomputes the daily rollup on demand from raw
readings, but those expire after RAW_RETENTION_DAYS (see
consumer/consumer.py) via a TTL index. Anything a planner wants to trend
over longer than that window needs to be persisted before the underlying
raw documents expire.

In production this would run once per day via cron shortly after
midnight UTC. For this prototype, run it manually or on whatever schedule
you control:

    python scripts/materialize_daily_stats.py --days 2
"""

import argparse
import os
import sys
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

sys.path.insert(0, os.path.dirname(__file__))
from planner_queries import daily_trend, MONGO_DB, MONGO_COLLECTION  # noqa: E402  (reuse the same aggregation)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018")
DAILY_STATS_COLLECTION = os.environ.get("DAILY_STATS_COLLECTION", "daily_station_stats")


def materialize(source_collection, target_collection, days, station_id=None):
    """Recompute the daily aggregation over the requested window and
    upsert each (station, day) row. Idempotent, so a missed cron run can
    be caught up by widening --days on the next run."""
    target_collection.create_index(
        [("station_id", 1), ("day", 1)], unique=True
    )

    rows = daily_trend(source_collection, days=days, station_id=station_id)
    if not rows:
        return 0

    now = datetime.now(timezone.utc)
    ops = []
    for row in rows:
        key = {"station_id": row["_id"]["station_id"], "day": row["_id"]["day"]}
        doc = {**key, **{k: v for k, v in row.items() if k != "_id"}, "materialized_at": now}
        ops.append(UpdateOne(key, {"$set": doc}, upsert=True))

    result = target_collection.bulk_write(ops, ordered=False)
    return result.upserted_count + result.modified_count


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--days", type=int, default=2,
        help="how many trailing days to (re)materialize (default: 2, "
             "covers today-so-far and yesterday in case the previous run "
             "was missed)",
    )
    parser.add_argument("--station", default=None, help="restrict to one station_id (default: all stations)")
    args = parser.parse_args()

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except PyMongoError as exc:
        print(f"Cannot reach MongoDB at {MONGO_URI}: {exc}")
        print("Is `docker compose up` running?")
        sys.exit(1)

    db = client[MONGO_DB]
    written = materialize(
        db[MONGO_COLLECTION], db[DAILY_STATS_COLLECTION], args.days, args.station
    )
    print(f"Materialized {written} (station, day) row(s) into "
          f"{MONGO_DB}.{DAILY_STATS_COLLECTION}.")


if __name__ == "__main__":
    main()
