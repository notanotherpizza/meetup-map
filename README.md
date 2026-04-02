# MeetupMap

A distributed Meetup group and event scraper built on **Aiven Kafka** and **Aiven Postgres**.

A seed producer publishes one message per group to a Kafka topic. Workers consume those messages, scrape group metadata and events, and publish raw JSON to two further topics. A sink consumer reads the raw topics and upserts into Postgres.

```
Seed producer → [groups-to-scrape] → Workers → [groups-raw] → Sink → Postgres
                                              → [events-raw]  ↗
```

Workers are stateless — run as many as you want locally, in Docker, on GitHub Actions, or on Fly.io.

Total workers from last run: 3

---

## Quick start (local)

### 1. Aiven services

Create two Aiven services:
- **Kafka** — any plan, Kafka 3.x
- **Postgres** — any plan, PG 15+

From the Kafka service page → **Connection information**:
- Download `ca.pem`, `service.cert`, `service.key` into `./certs/`
- Copy the **Service URI** (bootstrap servers)

From the Postgres service page:
- Copy the **Service URI** (connection string)

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your Aiven URIs and cert paths
```

### 3. Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
playwright install chromium
```

### 4. Create Postgres schema

```bash
psql $POSTGRES_URI -f infra/postgres/schema.sql
```

### 5. Run the seed producer

```bash
python -m seed.producer
# or: meetupmap-seed
```

This will:
1. Create the three Kafka topics if they don't exist
2. Fetch all groups from meetup.com/pro/pydata (and any other networks in `PRO_NETWORKS`)
3. Publish one `GroupSeed` message per group to `groups-to-scrape`

---

## Running workers (once built)

```bash
# One worker:
python -m worker.scraper

# Multiple workers (they share the consumer group, work is distributed):
python -m worker.scraper &
python -m worker.scraper &
python -m worker.scraper &
```

## Docker

```bash
docker build -t meetupmap .

# Seed
docker run --env-file .env -v $(pwd)/certs:/app/certs meetupmap seed.producer

# Worker
docker run --env-file .env -v $(pwd)/certs:/app/certs meetupmap worker.scraper

# Sink
docker run --env-file .env -v $(pwd)/certs:/app/certs meetupmap sink.consumer
```

---

## GitHub Actions secrets

For the seed workflow (`.github/workflows/seed.yml`), add these secrets to your repo:

| Secret | Value |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | from Aiven Kafka → Connection info |
| `KAFKA_CA_PEM` | contents of `ca.pem` |
| `KAFKA_SERVICE_CERT` | contents of `service.cert` |
| `KAFKA_SERVICE_KEY` | contents of `service.key` |
| `POSTGRES_URI` | from Aiven Postgres → Connection info |

---

## Project layout

```
meetupmap/
├── seed/           # Seed producer
│   └── producer.py
├── worker/         # Scraping workers (TODO)
│   └── scraper.py
├── sink/           # Sink consumer (TODO)
│   └── consumer.py
├── shared/         # Config, Kafka helpers, Pydantic models
│   ├── settings.py
│   ├── kafka_client.py
│   └── models.py
├── infra/
│   ├── kafka/      # Topic config reference
│   └── postgres/
│       └── schema.sql
├── .env.example
├── Dockerfile
└── pyproject.toml
```

## Adding more networks

Set `PRO_NETWORKS` in `.env` to a space-separated list:

```
PRO_NETWORKS=pydata kaggle tensorflow
```

The seed producer will loop over all of them. Workers and the sink are network-agnostic.
