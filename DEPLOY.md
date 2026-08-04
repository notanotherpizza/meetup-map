# Deploying to Aiven

## Architecture

```
Aiven Postgres              GitHub Pages
  groups/events/venues  ──►  docs/index.html    (search)
        ▲                    docs/group_map.html (map)
        │                    docs/data/*.json    (lazy-loaded)
  batch worker (daily)

Aiven Apps (compose.aiven.yaml):
  batch     — daily scraper, upserts into Postgres (restart: no)
  download  — FastAPI/DuckDB API (port 8000), queries Postgres live
```

Groups, events, and venues live in a single Aiven Postgres service. The
scraper upserts one current-state row per group/event/venue (see
`infra/postgres/schema.sql`), so both the map renderer and the download API
just run plain `SELECT`s — no lakehouse catalog, no object storage, no
compaction step to maintain.

---

## 1. Aiven Postgres

Create an Aiven Postgres service (`startup-4` or smaller is plenty — this is
a few thousand rows, not big data). Copy the **Service URI** from the Aiven
console:
```
postgres://avnadmin:<password>@<host>:<port>/defaultdb?sslmode=require
```
This becomes `POSTGRES_URI` everywhere below. The schema is applied
automatically on first connect (`shared/db.py: ensure_schema`) — no manual
migration step.

---

## 2. Aiven App (compose.aiven.yaml)

Create an Aiven App and upload `compose.aiven.yaml`. Set this environment
variable in the Aiven Apps UI (used by both `batch` and `download` services):

| Variable | Value |
|---|---|
| `POSTGRES_URI` | Aiven Postgres service URI (from step 1) |

Deploy the app. `batch` runs once per invocation (restart: no); `download`
starts automatically and serves on port 8000.

---

## 3. GitHub Actions secrets

Add these secrets to the GitHub repo (Settings → Secrets → Actions):

| Secret | Value |
|---|---|
| `POSTGRES_URI` | Same Aiven Postgres service URI |
| `DOWNLOAD_API` | Public URL of the `download` Aiven App |

---

## 4. Batch worker

The `batch` service in compose.aiven.yaml runs with `restart: "no"`. Trigger it via:
- Aiven Apps scheduled invocation (daily cron)
- Or `scrape.yaml` GitHub Actions workflow (`workflow_dispatch` → runs via the deployed image)

First run creates the schema and populates the tables. Subsequent runs upsert
— re-running is always safe.

---

## 5. Download service

The `download` service starts automatically with the app and serves on port 8000.

After it's running, copy its public URL and set it in `map/index_template.html`:
```js
const DOWNLOAD_API = 'https://<your-download-app>.aiven.app';
```
Then re-render and push:
```bash
python -m map.render
git add docs/
git commit -m "chore: set download API URL"
git push
```

---

## 6. Map render (GitHub Actions — daily)

`render-map.yml` runs at 06:00 UTC (after the 02:00 UTC batch), reads from
Postgres, and writes `docs/` to the repo for GitHub Pages.

---

## Local development

```bash
# Copy and fill in credentials
cp .env.example .env

# Start a local Postgres
docker compose -f compose.local.yaml up postgres -d

# Run a test scrape (5 groups) — creates the schema on first connect
python -m batch_worker.run --scrape-only --limit 5

# Render the map
python -m map.render

# Start download API
uvicorn download_service.main:app --reload
```
