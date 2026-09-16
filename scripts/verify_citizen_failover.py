"""
Checks that the citizen-app reliability contract (citizen_status.py)
actually catches a real sensor outage, not just that the logic looks
correct on paper.

Forces the producer's Open-Meteo request timeout to ~0
(API_TIMEOUT_SECONDS=0.001, see docker-compose.yml), so every API call
fails immediately and the producer's existing fallback path takes over -
the same code path a real outage would trigger, without needing any
firewall/network manipulation. Then it polls the live contract and prints
a timestamped transcript of every status transition, and restores the
normal timeout afterwards to confirm the status returns to "ok".

Requires the stack to already be running (`docker compose up -d`) and the
`docker` CLI on PATH.

    python scripts/verify_citizen_failover.py [--station station-01]
"""

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

from pymongo import MongoClient
from pymongo.errors import PyMongoError

sys.path.insert(0, os.path.dirname(__file__))
from citizen_status import get_all_station_statuses, MONGO_DB, MONGO_COLLECTION  # noqa: E402

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018")
POLL_SECONDS = 5
# citizen_status.py's thresholds at the default 10s interval top out at
# 90s (UNAVAILABLE_AFTER), so watch comfortably past that.
OUTAGE_WATCH_SECONDS = 130
RECOVERY_WATCH_SECONDS = 60


def recreate_producer(api_timeout_seconds):
    env = {**os.environ, "API_TIMEOUT_SECONDS": str(api_timeout_seconds)}
    subprocess.run(
        ["docker", "compose", "up", "-d", "--force-recreate", "producer"],
        env=env, check=True,
    )


def watch(collection, station_id, duration_seconds, label):
    """Poll the citizen-status contract and print only status changes."""
    last_status = None
    deadline = time.time() + duration_seconds
    while time.time() < deadline:
        status = get_all_station_statuses(collection, [station_id])[station_id]
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        if status["status"] != last_status:
            print(f"[{now}] ({label}) {station_id}: -> {status['status'].upper()}  {status['advisory']}")
            last_status = status["status"]
        time.sleep(POLL_SECONDS)
    return last_status


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--station", default=None, help="station_id to watch (default: first one found)")
    args = parser.parse_args()

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except PyMongoError as exc:
        print(f"Cannot reach MongoDB at {MONGO_URI}: {exc}")
        print("Is `docker compose up -d` running?")
        sys.exit(1)

    collection = client[MONGO_DB][MONGO_COLLECTION]
    station_ids = sorted(collection.distinct("station_id"))
    if not station_ids:
        print("No readings stored yet, let the pipeline run for a bit first.")
        sys.exit(1)
    station_id = args.station or station_ids[0]

    print(f"Watching {station_id}.\n")
    baseline = get_all_station_statuses(collection, [station_id])[station_id]
    print(f"Baseline: {baseline['status'].upper()}  {baseline['advisory']}")
    if baseline["status"] != "ok":
        print("(Baseline is not OK. The real API may already be unreachable "
              "from this network, or the pipeline just started. Results below "
              "may not show a clean transition.)")

    print("\nForcing an Open-Meteo outage (API_TIMEOUT_SECONDS=0.001, producer recreated)...")
    recreate_producer(api_timeout_seconds=0.001)

    print(f"Watching for up to {OUTAGE_WATCH_SECONDS}s, expecting a transition "
          f"through STALE and/or masked-by-simulator to UNAVAILABLE:\n")
    final_outage_status = watch(collection, station_id, OUTAGE_WATCH_SECONDS, "outage")

    print("\nRestoring the real API (API_TIMEOUT_SECONDS back to default, producer recreated)...")
    recreate_producer(api_timeout_seconds=5)

    print(f"Watching for up to {RECOVERY_WATCH_SECONDS}s, expecting a return to OK:\n")
    final_recovery_status = watch(collection, station_id, RECOVERY_WATCH_SECONDS, "recovery")

    print("\nResult:")
    print(f"  status at end of outage watch   : {final_outage_status}")
    print(f"  status at end of recovery watch : {final_recovery_status}")
    if final_outage_status == "unavailable" and final_recovery_status == "ok":
        print("  PASS: the contract caught the outage and recovered.")
        sys.exit(0)
    else:
        print("  Did not observe the expected OK -> UNAVAILABLE -> OK cycle "
              "within the watch windows. Re-run, or widen "
              "OUTAGE_WATCH_SECONDS/RECOVERY_WATCH_SECONDS above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
