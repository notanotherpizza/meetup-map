# Deploying to Aiven

## Architecture

```
Cloudflare R2 (EU)          GitHub Pages
  └── Parquet/Iceberg   ──►  docs/index.html    (search)
        ▲                    docs/group_map.html (map)
        │                    docs/data/*.json    (lazy-loaded)
  batch worker (daily)
  Lakekeeper catalog ──── Aiven Postgres (metadata)

Aiven Apps (compose.aiven.yaml):
  lakekeeper-migrate  — one-off DB migration on deploy
  lakekeeper          — Iceberg REST catalog (port 8181)
  batch               — daily scraper (restart: no)
  download            — FastAPI/DuckDB API (port 8000)
```

---

## 1. Aiven Postgres (Lakekeeper metadata store)

Create a new Aiven Postgres service (any plan — Lakekeeper's metadata is tiny).

Copy the **Service URI** from the Aiven console — it looks like:
```
postgres://avnadmin:<password>@<host>:<port>/defaultdb?sslmode=require
```

This becomes `LAKEKEEPER_PG_URL` in your Aiven App environment.

---

## 2. Aiven App (compose.aiven.yaml)

Create a new Aiven App and upload `compose.aiven.yaml`. Set these environment variables in the Aiven Apps UI:

| Variable | Value |
|---|---|
| `LAKEKEEPER_PG_URL` | Aiven Postgres service URI (from step 1) |
| `LAKEKEEPER_ENCRYPTION_KEY` | Random 32-byte hex — `python -c "import secrets; print(secrets.token_hex(32))"` |
| `LAKEKEEPER_BASE_URI` | Public URL Aiven assigns the app (set after first deploy, or use a custom domain) |
| `LAKEKEEPER_CATALOG_URI` | `http://lakekeeper:8181/catalog` (internal — batch and download reach lakekeeper by service name) |
| `R2_ENDPOINT_URL` | `https://<account_id>.eu.r2.cloudflarestorage.com` |
| `R2_ACCESS_KEY_ID` | Cloudflare R2 API token key |
| `R2_SECRET_ACCESS_KEY` | Cloudflare R2 API token secret |

Deploy the app. `lakekeeper-migrate` runs first and exits; `lakekeeper` starts serving on port 8181.

### First-time Lakekeeper bootstrap

After the first deploy, bootstrap Lakekeeper and create the warehouse (one-time only):

```bash
# Replace with your Lakekeeper public URL
LAKEKEEPER_URL=https://<your-app>.aiven.app:8181

# Bootstrap the server
curl -X POST "$LAKEKEEPER_URL/management/v1/bootstrap" \
  -H "Content-Type: application/json" \
  -d '{"accept-terms-of-use": true}'

# Create the warehouse pointing at your R2 bucket
curl -X POST "$LAKEKEEPER_URL/management/v1/warehouse" \
  -H "Content-Type: application/json" \
  -d '{
    "warehouse-name": "meetup-map",
    "project-id": "00000000-0000-0000-0000-000000000000",
    "storage-profile": {
      "type": "s3",
      "bucket": "<your-r2-bucket-name>",
      "endpoint": "https://<account_id>.eu.r2.cloudflarestorage.com",
      "region": "auto",
      "path-style-access": true,
      "sts-enabled": false
    },
    "storage-credential": {
      "type": "s3",
      "credential-type": "access-key",
      "aws-access-key-id": "<R2_ACCESS_KEY_ID>",
      "aws-secret-access-key": "<R2_SECRET_ACCESS_KEY>"
    }
  }'
```

The warehouse is now named `meetup-map` and ready for the batch worker.

---

## 3. GitHub Actions secrets

Add these secrets to the GitHub repo (Settings → Secrets → Actions):

| Secret | Value |
|---|---|
| `R2_ENDPOINT_URL` | Same as above |
| `R2_ACCESS_KEY_ID` | Same as above |
| `R2_SECRET_ACCESS_KEY` | Same as above |
| `LAKEKEEPER_CATALOG_URI` | **Public** Lakekeeper URL: `https://<your-app>.aiven.app:8181/catalog` |

GitHub Actions (render-map.yml) hits Lakekeeper from outside the Aiven network, so use the public URL here.

---

## 4. Batch worker

The `batch` service in compose.aiven.yaml runs with `restart: "no"`. Trigger it via:
- Aiven Apps scheduled invocation (daily cron)
- Or `scrape.yaml` GitHub Actions workflow (`workflow_dispatch` → runs via the deployed image)

First run will discover groups and populate the Iceberg tables. Subsequent runs scrape all known groups.

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

`render-map.yml` runs at 06:00 UTC (after the 02:00 UTC batch), reads from Iceberg, and writes `docs/` to the repo for GitHub Pages.

---

## Local development

```bash
# Copy and fill in credentials
cp .env.example .env

# Start Lakekeeper + local Postgres
docker compose up lakekeeper catalog-db -d

# Bootstrap (first time only)
curl -X POST http://localhost:8181/management/v1/bootstrap \
  -H "Content-Type: application/json" \
  -d '{"accept-terms-of-use": true}'

# Create warehouse (first time only) — bucket must already exist in R2
curl -X POST http://localhost:8181/management/v1/warehouse \
  -H "Content-Type: application/json" \
  -d '{"warehouse-name":"meetup-map","project-id":"00000000-0000-0000-0000-000000000000","storage-profile":{"type":"s3","bucket":"<bucket>","endpoint":"<R2_ENDPOINT_URL>","region":"auto","path-style-access":true,"sts-enabled":false},"storage-credential":{"type":"s3","credential-type":"access-key","aws-access-key-id":"<key>","aws-secret-access-key":"<secret>"}}'

# Run a test scrape (5 groups)
python -m batch_worker.run --scrape-only --limit 5

# Render the map
python -m map.render

# Start download API
uvicorn download_service.main:app --reload
```
