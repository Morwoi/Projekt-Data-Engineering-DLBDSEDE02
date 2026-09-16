"""
Citizen-app reliability contract for the sensor pipeline.

consumer/consumer.py already guarantees delivery: no reading gets lost
between producer and MongoDB. That's a different guarantee from what a
citizen app actually needs, though. The newest stored reading can still
be an unreliable description of *current* conditions for two reasons:

1. Nothing recent has arrived (plain message age, the obvious case).
2. Something arrives on schedule, but it's fake. During an Open-Meteo
   outage, the producer's fallback simulator (producer/producer.py)
   keeps publishing fresh timestamps every cycle so the pipeline itself
   doesn't stall. Checking message age alone would call a broken sensor
   "fine" indefinitely, since the synthetic data's timestamp is current.

get_all_station_statuses() classifies each station into one of three
states, each with an advisory message the app is expected to show:

- "ok"          a real (source="api") reading, fresh enough to trust
- "stale"       a real reading exists but is older than STALE_AFTER_SECONDS
- "unavailable" nothing recent has arrived, or the station has been
                running on the fallback simulator longer than
                SIMULATED_GRACE_SECONDS with no real reading in between

The optional `risk_profile` argument ("standard", default, or
"vulnerable") controls how fast these fire. "A bit older, use with
caution" might be a fine label for the general public, but it's not
necessarily an acceptable basis for a health-sensitive person deciding
whether it's safe to go outside. "vulnerable" halves the thresholds and,
once a reading counts as stale, switches the advisory from a passive age
label to actively recommending against relying on it.

Run from the host (requires `pip install pymongo`) while
`docker compose up` is running:

    python scripts/citizen_status.py
    python scripts/citizen_status.py --risk-profile vulnerable
"""

import argparse
import os
import sys
from datetime import datetime, timezone

from pymongo import MongoClient, DESCENDING
from pymongo.errors import PyMongoError

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018")
MONGO_DB = os.environ.get("MONGO_DB", "environment_monitoring")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "sensor_readings")

FETCH_INTERVAL_SECONDS = float(os.environ.get("FETCH_INTERVAL_SECONDS", "10"))
# Thresholds scale with the fetch interval so the 10s demo and a real
# deployment (e.g. 5min polling) use the same logic.
STALE_AFTER_SECONDS = FETCH_INTERVAL_SECONDS * 3
UNAVAILABLE_AFTER_SECONDS = FETCH_INTERVAL_SECONDS * 9
# How long the fallback simulator can stand in before we stop trusting it.
SIMULATED_GRACE_SECONDS = FETCH_INTERVAL_SECONDS * 6

STATUS_OK = "ok"
STATUS_STALE = "stale"
STATUS_UNAVAILABLE = "unavailable"

# Scales how early the thresholds above fire. A health-sensitive citizen
# pays a higher price for acting on a reading that turns out to be stale
# or synthetic than the general public does, so "vulnerable" halves the
# time budget before "stale"/"unavailable" kicks in.
RISK_PROFILES = {
    "standard": 1.0,
    "vulnerable": 0.5,
}


def classify(latest, latest_real_age_seconds, now, risk_profile="standard"):
    """Classify a station's latest reading into ok/stale/unavailable. Age
    alone isn't enough - a station only counts as trustworthy if backed
    by a recent real (source="api") reading, not just the fallback
    simulator keeping the pipeline alive."""
    factor = RISK_PROFILES.get(risk_profile, 1.0)
    stale_after = STALE_AFTER_SECONDS * factor
    unavailable_after = UNAVAILABLE_AFTER_SECONDS * factor
    simulated_grace = SIMULATED_GRACE_SECONDS * factor

    if latest is None:
        return {
            "status": STATUS_UNAVAILABLE,
            "age_seconds": None,
            "reading": None,
            "advisory": "No data has ever been received for this station. "
                        "Do not rely on this app for current conditions here.",
        }

    timestamp = datetime.fromisoformat(latest["timestamp"])
    age_seconds = (now - timestamp).total_seconds()

    if age_seconds > unavailable_after:
        return {
            "status": STATUS_UNAVAILABLE,
            "age_seconds": age_seconds,
            "reading": None,
            "advisory": (
                "Sensor has not reported recently enough to trust. Do not "
                "display a current-conditions value; tell the citizen the "
                "sensor is offline and to consult another source before "
                "going outside."
            ),
        }

    if latest_real_age_seconds is None or latest_real_age_seconds > simulated_grace:
        return {
            "status": STATUS_UNAVAILABLE,
            "age_seconds": age_seconds,
            "reading": None,
            "advisory": (
                "This station's real sensor has not returned a genuine "
                "reading in over "
                f"{simulated_grace:.0f}s. Data is currently being "
                "synthesized by a fallback simulator to keep the pipeline "
                "running and does not reflect real conditions. Tell the "
                "citizen the sensor is broken; do not show a value."
            ),
        }

    if age_seconds <= stale_after:
        status, advisory = STATUS_OK, "Reading reflects current conditions."
    else:
        status = STATUS_STALE
        if risk_profile == "vulnerable":
            advisory = (
                f"Last reading is {age_seconds:.0f}s old. For a vulnerable "
                "profile, do not present this as current conditions; "
                "advise waiting for a fresh reading or checking another "
                "source before going outside."
            )
        else:
            advisory = (
                f"Last reading is {age_seconds:.0f}s old and may no longer reflect "
                "current conditions. Show it labeled with its age, not as \"now\"."
            )

    return {
        "status": status,
        "age_seconds": age_seconds,
        "reading": latest,
        "advisory": advisory,
    }


def get_all_station_statuses(collection, station_ids, risk_profile="standard"):
    now = datetime.now(timezone.utc)
    result = {}
    for station_id in station_ids:
        latest = collection.find_one(
            {"station_id": station_id}, sort=[("timestamp", DESCENDING)]
        )
        latest_real = collection.find_one(
            {"station_id": station_id, "source": "api"}, sort=[("timestamp", DESCENDING)]
        )
        latest_real_age_seconds = None
        if latest_real is not None:
            real_timestamp = datetime.fromisoformat(latest_real["timestamp"])
            latest_real_age_seconds = (now - real_timestamp).total_seconds()
        result[station_id] = classify(latest, latest_real_age_seconds, now, risk_profile)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--risk-profile", choices=sorted(RISK_PROFILES), default="standard",
        help="citizen risk profile to classify for (default: standard)",
    )
    args = parser.parse_args()

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except PyMongoError as exc:
        print(f"Cannot reach MongoDB at {MONGO_URI}: {exc}")
        print("Is `docker compose up` running?")
        sys.exit(1)

    collection = client[MONGO_DB][MONGO_COLLECTION]
    station_ids = sorted(collection.distinct("station_id"))
    if not station_ids:
        print("No readings stored yet, check producer/consumer logs.")
        sys.exit(1)

    statuses = get_all_station_statuses(collection, station_ids, args.risk_profile)

    print(f"Citizen-facing station status (risk_profile={args.risk_profile})")
    print("=" * 72)
    for station_id, info in statuses.items():
        print(f"\n{station_id}: {info['status'].upper()}")
        print(f"  {info['advisory']}")
        if info["reading"] is not None:
            print(
                f"  temperature={info['reading']['temperature_c']}C  "
                f"age={info['age_seconds']:.0f}s"
            )


if __name__ == "__main__":
    main()
