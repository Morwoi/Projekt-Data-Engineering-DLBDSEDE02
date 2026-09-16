"""
Pipeline health check for the sensor project.

Connects to MongoDB and reports, per station, the latest stored reading
and how long ago it arrived. A station is flagged STALE if its newest
reading is older than three times the producer's fetch interval.

Run from the host (requires `pip install pymongo`) while
`docker compose up` is running:

    python scripts/check_status.py
"""

import os
import sys
from datetime import datetime, timezone

from pymongo import MongoClient, DESCENDING
from pymongo.errors import PyMongoError

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018")
MONGO_DB = os.environ.get("MONGO_DB", "environment_monitoring")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "sensor_readings")
FETCH_INTERVAL_SECONDS = float(os.environ.get("FETCH_INTERVAL_SECONDS", "10"))
STALE_AFTER_SECONDS = FETCH_INTERVAL_SECONDS * 3


def format_age(seconds):
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    return f"{seconds / 3600:.1f}h"


def main():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except PyMongoError as exc:
        print(f"Cannot reach MongoDB at {MONGO_URI}: {exc}")
        print("Is `docker compose up` running?")
        sys.exit(1)

    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    dead_letters = db[f"{MONGO_COLLECTION}_dead_letters"]

    total = collection.count_documents({})
    dead_letter_count = dead_letters.count_documents({})

    print(f"Pipeline status: {MONGO_DB}.{MONGO_COLLECTION}")
    print("=" * 72)
    print(f"Total readings stored : {total}")
    print(f"Dead-letter messages  : {dead_letter_count}"
          + ("  [!] check sensor_readings_dead_letters" if dead_letter_count else ""))
    print()

    # Dead letters mean a reading was permanently dropped, so this should
    # affect the exit code the same way staleness does.
    dead_letters_present = dead_letter_count > 0

    if total == 0:
        print("No readings stored yet, check producer/consumer logs.")
        sys.exit(1)

    station_ids = collection.distinct("station_id")
    now = datetime.now(timezone.utc)
    any_stale = False

    header = f"{'Station':<20} {'Last reading (UTC)':<30} {'Age':>6} {'Source':<10} {'Temp':>7}  Status"
    print(header)
    print("-" * len(header))

    for station_id in sorted(station_ids):
        latest = collection.find_one(
            {"station_id": station_id}, sort=[("timestamp", DESCENDING)]
        )
        timestamp = datetime.fromisoformat(latest["timestamp"])
        age = (now - timestamp).total_seconds()
        stale = age > STALE_AFTER_SECONDS
        any_stale = any_stale or stale
        status = "STALE" if stale else "OK"

        print(
            f"{station_id:<20} {latest['timestamp']:<30} "
            f"{format_age(age):>6} {latest.get('source', '?'):<10} "
            f"{latest.get('temperature_c', '?'):>6} C  {status}"
        )

    print()
    if any_stale:
        print("[!] At least one station has not reported recently, "
              "check `docker logs sensor-producer` / `sensor-consumer`.")
    if dead_letters_present:
        print(f"[!] {dead_letter_count} message(s) exhausted retries and were "
              "parked in the dead-letter collection. Check "
              "`sensor_readings_dead_letters` and `docker logs sensor-consumer`.")
    if any_stale or dead_letters_present:
        sys.exit(1)
    else:
        print("All stations reporting within the expected interval, no dead letters.")


if __name__ == "__main__":
    main()
