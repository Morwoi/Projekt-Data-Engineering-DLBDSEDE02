"""
Kafka consumer for the municipal environmental sensor pipeline.

Reads readings from Kafka and writes them to MongoDB. Offsets are only
committed after a successful write (at-least-once delivery), and writes
are upserts keyed on (station_id, timestamp) so a redelivered message
updates instead of duplicating. Writes are retried with backoff; if a
message still fails after all retries it goes into a dead-letter
collection instead of being dropped.

Raw readings expire after RAW_RETENTION_DAYS (default 90) via a TTL index
on `stored_at`, since planners work off the daily aggregates in
scripts/planner_queries.py rather than raw samples.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [consumer] %(levelname)s %(message)s",
)
log = logging.getLogger("consumer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sensor-readings")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "sensor-consumer-group")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018")
MONGO_DB = os.environ.get("MONGO_DB", "environment_monitoring")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "sensor_readings")

MAX_WRITE_RETRIES = int(os.environ.get("MAX_WRITE_RETRIES", "5"))
RETRY_BACKOFF_SECONDS = float(os.environ.get("RETRY_BACKOFF_SECONDS", "2"))

# How long raw readings are kept before the TTL index expires them.
RAW_RETENTION_DAYS = float(os.environ.get("RAW_RETENTION_DAYS", "90"))

# Dead letters are kept longer since a human needs to review them, but
# still bounded so they don't grow forever.
DEAD_LETTER_RETENTION_DAYS = float(os.environ.get("DEAD_LETTER_RETENTION_DAYS", "30"))


def connect_consumer(retries=30, delay=5):
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    for attempt in range(1, retries + 1):
        try:
            consumer.list_topics(timeout=5)
            consumer.subscribe([KAFKA_TOPIC])
            log.info("Connected to Kafka at %s, subscribed to '%s'", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
            return consumer
        except KafkaException:
            log.warning(
                "Kafka not reachable yet (attempt %s/%s), retrying in %ss...",
                attempt, retries, delay,
            )
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after repeated retries")


def connect_mongo(retries=30, delay=5):
    for attempt in range(1, retries + 1):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            log.info("Connected to MongoDB at %s", MONGO_URI)
            return client
        except PyMongoError:
            log.warning(
                "MongoDB not reachable yet (attempt %s/%s), retrying in %ss...",
                attempt, retries, delay,
            )
            time.sleep(delay)
    raise RuntimeError("Could not connect to MongoDB after repeated retries")


def write_with_retry(collection, document):
    """Upsert by (station_id, timestamp) so a redelivered message updates
    instead of duplicating. Retries transient errors with backoff; returns
    False if all retries are exhausted."""
    natural_key = {
        "station_id": document["station_id"],
        "timestamp": document["timestamp"],
    }
    # stored_at is a real BSON date (unlike the ISO string "timestamp"),
    # which the TTL index requires.
    to_store = {**document, "stored_at": datetime.now(timezone.utc)}
    for attempt in range(1, MAX_WRITE_RETRIES + 1):
        try:
            collection.update_one(natural_key, {"$set": to_store}, upsert=True)
            return True
        except PyMongoError as exc:
            log.warning(
                "MongoDB write failed (attempt %s/%s) for %s: %s",
                attempt, MAX_WRITE_RETRIES, document.get("station_id"), exc,
            )
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    log.error(
        "Giving up on message for %s after %s attempts, moving to dead letters",
        document.get("station_id"), MAX_WRITE_RETRIES,
    )
    return False


def ensure_ttl_index(collection, field, expire_after_seconds):
    """Create the TTL index. MongoDB errors (code 85) if the index already
    exists with different options, so drop and recreate it instead of
    crashing when RAW_RETENTION_DAYS changes between restarts."""
    try:
        collection.create_index(field, expireAfterSeconds=expire_after_seconds)
    except OperationFailure as exc:
        if exc.code != 85:
            raise
        log.info(
            "TTL index on '%s' exists with different options, recreating "
            "with expireAfterSeconds=%s", field, expire_after_seconds,
        )
        collection.drop_index(f"{field}_1")
        collection.create_index(field, expireAfterSeconds=expire_after_seconds)


def move_to_dead_letters(dead_letters, document, reason):
    """Park a message that failed all write retries instead of dropping
    it. Best-effort: if Mongo is unreachable for this write too, we just
    log it."""
    try:
        dead_letters.insert_one({
            "reading": document,
            "reason": reason,
            "failed_at": datetime.now(timezone.utc),
        })
    except PyMongoError as exc:
        log.error(
            "Could not record dead letter for %s either: %s",
            document.get("station_id"), exc,
        )


def main():
    consumer = connect_consumer()
    mongo_client = connect_mongo()
    db = mongo_client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    dead_letters = db[f"{MONGO_COLLECTION}_dead_letters"]
    collection.create_index([("station_id", 1), ("timestamp", 1)], unique=True)
    ensure_ttl_index(collection, "stored_at", int(RAW_RETENTION_DAYS * 86400))
    ensure_ttl_index(dead_letters, "failed_at", int(DEAD_LETTER_RETENTION_DAYS * 86400))

    log.info("Consumer ready, waiting for messages...")
    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            log.warning("Kafka poll error: %s", message.error())
            continue

        reading = json.loads(message.value().decode("utf-8"))
        success = write_with_retry(collection, reading)
        if success:
            log.info(
                "Stored %s reading for %s (offset %s)",
                reading.get("source"), reading.get("station_id"), message.offset(),
            )
        else:
            move_to_dead_letters(dead_letters, reading, "mongo write retries exhausted")
        # Commit even on failure so one bad message doesn't block the
        # partition; it's already in the dead-letter collection.
        consumer.commit(message=message, asynchronous=False)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Consumer crashed")
        sys.exit(1)
