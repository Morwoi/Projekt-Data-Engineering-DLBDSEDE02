Wohlgenannt-Markus_IU14080395_DataEngineering_P1_S

# Stream Processing Pipeline for Municipal Environmental Sensors

### Konzeptionsphase

Markus Wohlgenannt
Juli 10, 2026

## 1. Data source

I'm using the free Open-Meteo weather API as a stand-in for real sensor
hardware, since the municipality's actual IoT sensors aren't available
yet. Five fixed points around the city act as stations, and every ten
seconds the system polls temperature, humidity, wind speed, precipitation
and pressure for each one, simulating a near-real-time sensor feed. Each
reading becomes one flat JSON object, similar to what a real IoT gateway
would send, so new sensor types (CO2, noise, fine dust) can be added
later as extra fields without touching the pipeline itself.

## 2. Goal, usages and success criteria

The pipeline continuously ingests environmental data so that (a) city
planners can analyze trends later through dashboards, and (b) a
citizen-facing app can warn residents when values cross recommended
thresholds. Two user stories: a planner wants a reliable daily trend per
district to prioritize interventions, built from complete data rather
than a flood of raw per-second readings; a citizen-app backend wants to
poll new values without missing any, even after a brief outage. The
system succeeds if no reading is ever silently lost and data stays
available with low latency; it fails if the stream stalls on a temporary
fault, or data disappears between ingestion and storage.

## 3. Technology choice: Kafka + MongoDB

I chose Kafka over Spark Streaming because this is a straightforward
ingest-and-store pipeline, not one that needs in-stream aggregation.
Kafka's durable log gives reliability (messages persist until
committed), scalability (the topic is partitioned per station, so more
consumers can join later without a redesign), and maintainability (one
topic, one JSON schema). Running a single broker locally means no fault
tolerance against broker loss, but moving to a multi-broker cloud
cluster later is a configuration change, not a rewrite. MongoDB stores
the readings because its schema-less documents match the JSON messages
directly and can absorb new sensor types without migrations. Both run as
single containers locally but scale out (Kafka partitions/brokers,
MongoDB sharding) without application code changes.

## 4. Implementation plan

1. Provision Kafka (KRaft, single node) and MongoDB via Docker Compose.
2. Python producer polls Open-Meteo per station and publishes JSON to
   the `sensor-readings` topic, with a fallback simulator as a safety
   net.
3. Python consumer reads the topic and writes each reading into
   MongoDB, committing offsets only after a successful write.
4. Containerize both services and wire them together in
   `docker-compose.yml` so the pipeline starts with one command.
5. Publish the code to a public GitHub repository with setup
   instructions.
