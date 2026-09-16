"""
Kafka producer for the municipal environmental sensor pipeline.

Polls the Open-Meteo weather API once per interval for five simulated
sensor stations and publishes each reading as JSON to Kafka. Falls back
to a local simulator if the API is unreachable, so the stream keeps
running through an outage. Each message carries a "source" field ("api"
or "simulated") so consumers can tell real readings from fallback ones.
"""

import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from confluent_kafka import KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer] %(levelname)s %(message)s",
)
log = logging.getLogger("producer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sensor-readings")
FETCH_INTERVAL_SECONDS = float(os.environ.get("FETCH_INTERVAL_SECONDS", "10"))
API_TIMEOUT_SECONDS = float(os.environ.get("API_TIMEOUT_SECONDS", "5"))
API_BASE_URL = os.environ.get(
    "OPEN_METEO_URL", "https://api.open-meteo.com/v1/forecast"
)
CURRENT_VARS = "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,surface_pressure"

# Five fixed points around Hamburg, standing in for real sensor stations.
# In production this would come from a station registry instead.
STATIONS = [
    {"station_id": "station-01", "name": "City Center", "latitude": 53.5511, "longitude": 9.9937},
    {"station_id": "station-02", "name": "Harbor District", "latitude": 53.5396, "longitude": 9.9686},
    {"station_id": "station-03", "name": "Industrial Park", "latitude": 53.5100, "longitude": 9.9400},
    {"station_id": "station-04", "name": "Residential North", "latitude": 53.6000, "longitude": 10.0300},
    {"station_id": "station-05", "name": "Green Belt", "latitude": 53.5700, "longitude": 10.0600},
]

# Seeds the fallback simulator with each station's last real reading.
_last_good = {}

_shutdown = False


def _handle_shutdown(signum, frame):
    global _shutdown
    log.info("Shutdown signal received, finishing current cycle...")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


def connect_producer(retries=30, delay=5):
    """Create the producer and retry until Kafka is reachable (it starts
    slower than this container on `docker compose up`)."""
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 5,
        "linger.ms": 200,
    })
    for attempt in range(1, retries + 1):
        try:
            producer.list_topics(timeout=5)
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except KafkaException:
            log.warning(
                "Kafka not reachable yet (attempt %s/%s), retrying in %ss...",
                attempt, retries, delay,
            )
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after repeated retries")


def _delivery_report(err, msg):
    if err is not None:
        log.warning("Message delivery failed for %s: %s", msg.key(), err)


def fetch_from_api(station):
    """Fetch current weather for a station from Open-Meteo. Raises on any
    network/HTTP/parsing problem so the caller can fall back."""
    params = {
        "latitude": station["latitude"],
        "longitude": station["longitude"],
        "current": CURRENT_VARS,
        "timezone": "UTC",
    }
    response = requests.get(API_BASE_URL, params=params, timeout=API_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    current = payload["current"]

    return {
        "temperature_c": current["temperature_2m"],
        "humidity_pct": current["relative_humidity_2m"],
        "wind_speed_kmh": current["wind_speed_10m"],
        "precipitation_mm": current["precipitation"],
        "pressure_hpa": current["surface_pressure"],
    }


def simulate_reading(station):
    """Small random walk around the last known-good reading, or a
    sensible default on the first call for a station."""
    seed = _last_good.get(station["station_id"], {
        "temperature_c": 18.0,
        "humidity_pct": 60.0,
        "wind_speed_kmh": 10.0,
        "precipitation_mm": 0.0,
        "pressure_hpa": 1013.0,
    })
    return {
        "temperature_c": round(seed["temperature_c"] + random.uniform(-0.5, 0.5), 1),
        "humidity_pct": max(0, min(100, round(seed["humidity_pct"] + random.uniform(-2, 2), 1))),
        "wind_speed_kmh": max(0, round(seed["wind_speed_kmh"] + random.uniform(-1.5, 1.5), 1)),
        "precipitation_mm": max(0, round(seed["precipitation_mm"] + random.uniform(-0.1, 0.2), 2)),
        "pressure_hpa": round(seed["pressure_hpa"] + random.uniform(-0.5, 0.5), 1),
    }


def build_reading(station):
    """Try the real API first, transparently fall back to the simulator."""
    try:
        metrics = fetch_from_api(station)
        source = "api"
        _last_good[station["station_id"]] = metrics
    except (requests.RequestException, KeyError, ValueError) as exc:
        log.warning(
            "Open-Meteo unreachable for %s (%s), using fallback simulator",
            station["station_id"], exc,
        )
        metrics = simulate_reading(station)
        source = "simulated"

    return {
        "station_id": station["station_id"],
        "station_name": station["name"],
        "latitude": station["latitude"],
        "longitude": station["longitude"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
        **metrics,
    }


def main():
    producer = connect_producer()
    log.info(
        "Starting sensor stream: %s stations, every %ss, topic '%s'",
        len(STATIONS), FETCH_INTERVAL_SECONDS, KAFKA_TOPIC,
    )

    while not _shutdown:
        cycle_start = time.time()
        for station in STATIONS:
            reading = build_reading(station)
            producer.produce(
                KAFKA_TOPIC,
                key=station["station_id"],
                value=json.dumps(reading),
                callback=_delivery_report,
            )
            producer.poll(0)
            log.info("Published %s reading for %s", reading["source"], station["station_id"])

        producer.flush()
        elapsed = time.time() - cycle_start
        time.sleep(max(0.0, FETCH_INTERVAL_SECONDS - elapsed))

    log.info("Flushing producer...")
    producer.flush()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Producer crashed")
        sys.exit(1)
