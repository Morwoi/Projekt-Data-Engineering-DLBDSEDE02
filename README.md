# Municipal Environmental Sensor Streaming Pipeline

Course: DLBDSEDE02 - Project: Data Engineering | Task 2: Design and implement a stream processing pipeline

## System description

This project simulates a municipal environmental sensor network. Five
fictional sensor stations placed across the city continuously report
environmental measurements (temperature, humidity, wind speed,
precipitation, air pressure). A Kafka producer polls the public
[Open-Meteo](https://open-meteo.com) weather API once every 10 seconds per
station and publishes each reading as a JSON message to the Kafka topic
`sensor-readings`. A Kafka consumer reads the stream and persists every
reading into MongoDB, where it becomes available to downstream
applications such as planner dashboards or a citizen warning app.

If the public API is temporarily unreachable (network outage, timeout,
rate limiting), the producer switches to a local fallback simulator. It
generates a plausible reading from the last known value, tags it
`"source": "simulated"`, and keeps the stream running without
interruption. This satisfies the task requirement that the system does
not break when data is temporarily inaccessible, and it also means the
pipeline can be demonstrated end to end without an internet connection,
for example during grading.

On the storage side, the consumer commits Kafka offsets only after a
successful MongoDB write (at-least-once delivery) and retries transient
MongoDB errors with backoff before giving up on a single message. Writes
are upserts keyed on `(station_id, timestamp)`, so a redelivered message
(possible under at-least-once delivery) updates the existing document
instead of creating a duplicate. If all retries on a message are
exhausted, it is parked in a `sensor_readings_dead_letters` collection
instead of being dropped, so a short database restart never silently
loses data.

### Expected usages / end-user context

- City planners query MongoDB (directly or through a BI/dashboard tool) to
  analyze environmental trends over time and prioritize interventions.
- A citizen-facing warning app (not part of this prototype) would read the
  latest readings per station from MongoDB and push alerts when values
  exceed recommended thresholds.
- Future sensor types (CO2, noise, fine dust) can be added by extending
  the `metrics` fields in a reading without changing the pipeline
  architecture, since Kafka messages and MongoDB documents are both
  schema-flexible.

### Reliability requirements: planners versus the citizen-facing app

Kafka guarantees delivery between producer and MongoDB, but this does not
by itself determine what a planner or a citizen actually needs from the
data on a day-to-day basis. The two cases are handled differently:

- Planners do not need raw 10-second samples. `planner_queries.py` rolls
  readings up into daily per-station statistics instead, and
  `materialize_daily_stats.py` persists that rollup into
  `daily_station_stats` so the history survives past the 90-day TTL on raw
  readings (see `RAW_RETENTION_DAYS` under "Configuration" below). The
  mean/min/max/precipitation figures use only real (`source="api"`)
  readings; mixing in fallback-simulator values would make a day's figures
  less trustworthy without that being visible in the output. Each daily
  row also reports `api_coverage_pct`, the share of that day's readings
  that were real, so a planner can decide independently whether to
  discount a low-confidence day, instead of that decision being made
  implicitly inside the average.
- A citizen deciding whether to go outside needs to know that the current
  reading is trustworthy, not just that some reading was stored. During an
  API outage, the fallback simulator keeps publishing plausible synthetic
  data so the pipeline itself does not stall, which means message age
  alone would report a broken sensor as fine. `citizen_status.py`
  therefore tracks the age of the last real (`source="api"`) reading
  separately and classifies each station as `ok` / `stale` / `unavailable`
  based on that (see the module docstring for the full logic). It also
  takes a `--risk-profile` option (`standard`, default, or `vulnerable`).
  For most users a slightly older reading may still be acceptable, but for
  someone with a health condition it may not be, so the `vulnerable`
  profile halves the thresholds and instructs the application to warn
  against relying on a stale reading instead of only reporting its age.

`verify_citizen_failover.py` forces a real outage against the running
stack and checks that the status flips to `unavailable` and back to `ok`,
rather than relying on the logic being correct on paper. Try it yourself
against the running stack:

```bash
docker compose up -d
python scripts/verify_citizen_failover.py
```

## Architecture

```
[Open-Meteo API]        [fallback simulator]
        \                     /
         v                   v
        +---------------------+
        |   Kafka Producer     |
        +---------------------+
                   |
                   v
        +---------------------+
        | Kafka topic:          |
        | sensor-readings       |
        +---------------------+
                   |
                   v
        +---------------------+
        |   Kafka Consumer     |
        +---------------------+
                   |
                   v
        +---------------------+
        |   MongoDB            |
        | environment_monitoring.sensor_readings |
        +---------------------+
```

Kafka runs in single-node KRaft mode (no ZooKeeper dependency), which keeps
the local prototype simple while still using the same broker software and
wire protocol as a distributed, multi-broker production deployment, so
moving to a cloud-hosted, multi-node cluster later is a configuration
change, not a rewrite. The `sensor-readings` topic is created with 5
partitions (matching the number of stations) and messages are keyed by
`station_id`, so additional consumers in the same consumer group would
parallelize by station once more than one is deployed. Running only one
broker locally does mean there's no replication or fault tolerance for the
data topic itself; that's a limitation of the local prototype, not of the
architecture.

## How to run

Requirements: Docker Desktop (or Docker Engine + Compose plugin).

```bash
git clone https://github.com/Morwoi/Projekt-Data-Engineering.git
cd Projekt-Data-Engineering
docker compose up --build
```

This builds the producer and consumer images, starts Kafka and MongoDB,
waits for both to report healthy, and then starts streaming simulated
sensor readings into MongoDB automatically. No manual setup steps needed.

Kafka UI ([provectuslabs/kafka-ui](https://github.com/provectus/kafka-ui))
starts alongside the pipeline and is reachable at
<http://localhost:8081>. It shows the `sensor-readings` topic (partitions,
offsets, message contents) and its current throughput, without requiring
the Kafka CLI. Broker JMX metrics are enabled so the throughput figures
reflect actual traffic instead of showing zero. The overview also lists
`__consumer_offsets`, an internal Kafka topic with a fixed 50 partitions;
together with `sensor-readings` (5 partitions) this results in 2 topics
and 55 partitions being shown, which is expected and not a
misconfiguration.

To inspect the stored data:

```bash
docker exec -it mongo mongosh environment_monitoring --eval "db.sensor_readings.find().sort({timestamp:-1}).limit(5).pretty()"
```

To check whether any message failed all retries and was parked in the
dead-letter collection:

```bash
docker exec -it mongo mongosh environment_monitoring --eval "db.sensor_readings_dead_letters.find().pretty()"
```

To check the pipeline's health from the host machine (latest reading per
station, how stale each one is, and dead-letter count) instead of querying
Mongo by hand:

```bash
pip install pymongo
python scripts/check_status.py
```

To see the query strategy a city planner would use (daily
per-station trend aggregates computed from real readings only, with an
API-coverage figure for each day):

```bash
python scripts/planner_queries.py --days 30
```

To see the reliability contract a citizen-facing app backend would call
(ok / stale / unavailable per station, never presenting a stale reading
as current conditions), for the general public or for a risk-sensitive
profile with tighter thresholds:

```bash
python scripts/citizen_status.py
python scripts/citizen_status.py --risk-profile vulnerable
```

To persist the daily aggregation into a `daily_station_stats` collection
(needed once history is wanted beyond `RAW_RETENTION_DAYS`, since raw
readings expire on a TTL index, see `consumer/consumer.py`):

```bash
python scripts/materialize_daily_stats.py --days 2
```

To verify that the citizen-status contract catches a real Open-Meteo
outage, rather than being fooled by the fallback simulator's plausible
synthetic readings (requires `docker compose up -d` running and the
`docker` CLI on PATH; takes a few minutes, forces the producer's API
timeout to ~0 and back via `API_TIMEOUT_SECONDS`):

```bash
python scripts/verify_citizen_failover.py
```

### Ports

| Service | Container port | Host port (default) | Why not the tool's own default |
|---|---|---|---|
| MongoDB | 27017 | 27018 | Avoids clashing with a MongoDB service some machines already run locally on 27017; connecting to the wrong database there produces no error, just wrong or empty results. |
| Kafka UI | 8080 | 8081 | Port 8080 is a common default for other local development tools, so collisions are frequent. |

Container-to-container traffic (e.g. producer/consumer -> `mongo:27017`,
`kafka-ui` -> `kafka:9092`) is unaffected by this, since services address
each other by container name over the `sensor-net` network and do not go
through the published host ports.

Both host ports are overridable via environment variables
(`MONGO_HOST_PORT`, `KAFKA_UI_HOST_PORT`) instead of being hardcoded, so if
even 27018 or 8081 happen to be taken on a given machine, that's a one-line
`.env` entry or `export` before `docker compose up`, not a compose-file
edit:

```bash
MONGO_HOST_PORT=27019 KAFKA_UI_HOST_PORT=8082 docker compose up -d
```

To stop everything:

```bash
docker compose down
```

Add `-v` to also remove the persisted Kafka/MongoDB volumes.

## Repository layout

```
docker-compose.yml      Orchestrates Kafka, Kafka UI, MongoDB, producer, consumer
producer/                Kafka producer: fetches Open-Meteo data or falls back to simulator
consumer/                Kafka consumer: writes readings into MongoDB with retry
scripts/                 check_status.py: host-side pipeline health check (fails on staleness or dead letters)
                          planner_queries.py: daily trend aggregation (real readings only, with API coverage) for city planners
                          materialize_daily_stats.py: persists the daily aggregation past raw-data TTL expiry
                          citizen_status.py: ok/stale/unavailable reliability contract for a citizen app, with a --risk-profile option
                          verify_citizen_failover.py: end-to-end test that the contract catches a real outage
docs/                    Portfolio submission texts (Phase 1 concept, Phase 2 explanation)
```

## Configuration

All settings are environment variables with sensible defaults (see
`docker-compose.yml`), e.g. `FETCH_INTERVAL_SECONDS`, `KAFKA_TOPIC`,
`MONGO_DB`, `RAW_RETENTION_DAYS` (how long raw readings are kept before a
MongoDB TTL index expires them; default 90), and `MONGO_HOST_PORT` /
`KAFKA_UI_HOST_PORT` (see "Ports" above). No API key is required for
Open-Meteo.
