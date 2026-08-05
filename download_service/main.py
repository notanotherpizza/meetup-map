"""
download-service/main.py
────────────────────────
FastAPI + DuckDB service for querying and downloading meetup-map data.

DuckDB attaches directly to Postgres (via the postgres scanner extension) and
queries it live — no copy step, no cache-refresh interval. groups/events/venues
are plain views over the attached tables since Postgres already stores one
current-state row per id (upserted by the scraper), so no dedup is needed.

Exposes REST endpoints for search, filtering, and bulk download.
Supports output formats: json (default), csv, parquet

Usage:
    uvicorn download_service.main:app --host 0.0.0.0 --port 8000

Env: DATABASE_URL (same as scraper/batch worker).
"""
import io
import json
import logging
import threading

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from shared.settings import Settings

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

settings = Settings()
app = FastAPI(
    title="Meetup Map Download Service",
    description="Query and download group, event, and venue data from the Meetup Map dataset.",
    version="0.1.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── DuckDB attached to Postgres ─────────────────────────────────────────────

_conn_lock = threading.Lock()
_conn: duckdb.DuckDBPyConnection | None = None


def _connect() -> duckdb.DuckDBPyConnection:
    log.info("Attaching to Postgres...")
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL postgres; LOAD postgres;")
    dsn = settings.database_url.replace("'", "''")
    conn.execute(f"ATTACH '{dsn}' AS pg (TYPE postgres, READ_ONLY)")
    conn.execute("CREATE VIEW groups AS SELECT * FROM pg.groups")
    conn.execute("CREATE VIEW events AS SELECT * FROM pg.events")
    conn.execute("CREATE VIEW venues AS SELECT * FROM pg.venues")
    log.info("Attached.")
    return conn


def get_conn() -> duckdb.DuckDBPyConnection | None:
    global _conn
    with _conn_lock:
        if _conn is None:
            try:
                _conn = _connect()
            except Exception as e:
                log.error("Failed to attach to Postgres: %s", e)
    return _conn


@app.on_event("startup")
def startup():
    try:
        get_conn()
    except Exception as e:
        log.error("Startup connection failed: %s", e)


# ── Helpers ────────────────────────────────────────────────────────────────

def _apply_date_filter(table: str, col: str, from_date: str | None, to_date: str | None) -> str:
    clauses = []
    if from_date:
        clauses.append(f"{col} >= TIMESTAMPTZ '{from_date}T00:00:00Z'")
    if to_date:
        clauses.append(f"{col} <= TIMESTAMPTZ '{to_date}T23:59:59Z'")
    if clauses:
        return f"WHERE {' AND '.join(clauses)}"
    return ""


def _respond(arrow_table: pa.Table | pa.RecordBatchReader, fmt: str, filename: str) -> Response:
    if isinstance(arrow_table, pa.RecordBatchReader):
        arrow_table = arrow_table.read_all()
    if fmt == "parquet":
        buf = io.BytesIO()
        pq.write_table(arrow_table, buf)
        buf.seek(0)
        return StreamingResponse(
            buf,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{filename}.parquet"'},
        )
    if fmt == "csv":
        # Use DuckDB to render CSV from Arrow
        tmp = duckdb.connect(":memory:")
        tmp.register("t", arrow_table)
        csv_bytes = tmp.execute("SELECT * FROM t").df().to_csv(index=False).encode()
        return Response(
            content=csv_bytes,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}.csv"'},
        )
    # JSON (default) — use to_json so NaN float values become null
    tmp = duckdb.connect(":memory:")
    tmp.register("t", arrow_table)
    df = tmp.execute("SELECT * FROM t").fetchdf()
    rows = json.loads(df.to_json(orient="records"))
    return {"data": rows, "count": len(rows)}


def _text_filter_clause(q: str, *cols: str) -> str:
    """Simple case-insensitive substring match across multiple columns."""
    term = q.replace("'", "''")
    parts = [f"LOWER({c}) LIKE '%{term.lower()}%'" for c in cols]
    return f"({' OR '.join(parts)})"


# ── Endpoints ──────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {
        "service": "meetup-map-download",
        "endpoints": ["/groups", "/events", "/venues", "/search", "/stats", "/refresh"],
        "formats": ["json", "csv", "parquet"],
    }


@app.get("/stats")
def stats():
    conn = get_conn()
    if conn is None:
        return {"status": "loading", "message": "Data not yet available — batch scrape in progress"}
    row = conn.execute("""
        SELECT
            (SELECT COUNT(*) FROM groups) AS total_groups,
            (SELECT COUNT(*) FROM events) AS total_events,
            (SELECT COUNT(*) FROM venues) AS total_venues,
            (SELECT COUNT(DISTINCT pro_network) FROM groups) AS total_networks,
            (SELECT COUNT(*) FROM groups WHERE events_scrape_ok) AS groups_with_events,
            (SELECT MAX(scraped_at) FROM groups) AS last_scraped_at
    """).fetchone()
    return {
        "total_groups":      row[0],
        "total_events":      row[1],
        "total_venues":      row[2],
        "total_networks":    row[3],
        "groups_with_events": row[4],
        "last_scraped_at":   row[5].isoformat() if row[5] else None,
    }


@app.get("/groups")
def get_groups(
    from_date: str | None = Query(None, description="ISO date, e.g. 2025-01-01"),
    to_date:   str | None = Query(None, description="ISO date, e.g. 2025-12-31"),
    network:   str | None = Query(None, description="Pro-network name"),
    country:   str | None = Query(None, description="ISO country code, e.g. GB"),
    platform:  str | None = Query(None, description="meetup, luma, …"),
    q:         str | None = Query(None, description="Full-text search term"),
    limit:     int        = Query(0, ge=0, description="Max rows (0 = all)"),
    fmt:       str        = Query("json", alias="format", description="json|csv|parquet"),
):
    conn = get_conn()
    if conn is None:
        raise HTTPException(503, "Data not yet available — batch scrape in progress")
    where = []
    date_clause = _apply_date_filter("groups", "scraped_at", from_date, to_date)
    if date_clause:
        where.append(date_clause.replace("WHERE ", ""))
    if network:
        where.append(f"pro_network = '{network.replace(chr(39), chr(39)*2)}'")
    if country:
        where.append(f"LOWER(country) = '{country.lower()}'")
    if platform:
        where.append(f"platform = '{platform.replace(chr(39), chr(39)*2)}'")
    if q:
        where.append(_text_filter_clause(q, "name", "city", "country", "pro_network"))

    sql = f"SELECT * FROM groups" + (f" WHERE {' AND '.join(where)}" if where else "")
    sql += " ORDER BY name"
    if limit:
        sql += f" LIMIT {limit}"

    arrow = conn.execute(sql).arrow()
    return _respond(arrow, fmt, "meetupmap_groups")


@app.get("/events")
def get_events(
    from_date:    str | None = Query(None, description="Filter by starts_at ≥ this date"),
    to_date:      str | None = Query(None, description="Filter by starts_at ≤ this date"),
    group:        str | None = Query(None, description="group_urlname to filter by"),
    status:       str | None = Query(None, description="upcoming or past"),
    network:      str | None = Query(None, description="Pro-network name (joins groups)"),
    is_online:    bool | None = Query(None, description="Filter online/in-person"),
    q:            str | None = Query(None, description="Full-text search on title"),
    limit:        int        = Query(0, ge=0),
    fmt:          str        = Query("json", alias="format"),
):
    conn = get_conn()
    if conn is None:
        raise HTTPException(503, "Data not yet available — batch scrape in progress")
    where = []
    date_clause = _apply_date_filter("events", "starts_at", from_date, to_date)
    if date_clause:
        where.append(date_clause.replace("WHERE ", ""))
    if group:
        where.append(f"e.group_urlname = '{group.replace(chr(39), chr(39)*2)}'")
    if status:
        where.append(f"e.status = '{status}'")
    if is_online is not None:
        where.append(f"e.is_online = {str(is_online).upper()}")
    if q:
        where.append(_text_filter_clause(q, "e.title"))
    if network:
        where.append(f"g.pro_network = '{network.replace(chr(39), chr(39)*2)}'")

    join = "LEFT JOIN groups g ON e.group_urlname = g.group_urlname" if network else ""
    alias_prefix = "e." if network else ""
    sql = f"""
        SELECT e.* FROM events e
        {join}
        {"WHERE " + " AND ".join(where) if where else ""}
        ORDER BY e.starts_at DESC
        {"LIMIT " + str(limit) if limit else ""}
    """
    arrow = conn.execute(sql).arrow()
    return _respond(arrow, fmt, "meetupmap_events")


@app.get("/venues")
def get_venues(
    from_date: str | None = Query(None),
    to_date:   str | None = Query(None),
    country:   str | None = Query(None),
    q:         str | None = Query(None, description="Search venue name, city, country"),
    limit:     int        = Query(0, ge=0),
    fmt:       str        = Query("json", alias="format"),
):
    conn = get_conn()
    if conn is None:
        raise HTTPException(503, "Data not yet available — batch scrape in progress")
    where = []
    date_clause = _apply_date_filter("venues", "scraped_at", from_date, to_date)
    if date_clause:
        where.append(date_clause.replace("WHERE ", ""))
    if country:
        where.append(f"LOWER(country) = '{country.lower()}'")
    if q:
        where.append(_text_filter_clause(q, "name", "city", "country"))

    sql = f"SELECT * FROM venues" + (f" WHERE {' AND '.join(where)}" if where else "")
    sql += " ORDER BY scraped_at DESC"
    if limit:
        sql += f" LIMIT {limit}"
    arrow = conn.execute(sql).arrow()
    return _respond(arrow, fmt, "meetupmap_venues")


@app.get("/search")
def search(
    q:     str  = Query(..., description="Search term"),
    type:  str  = Query("groups", description="groups | events | both"),
    limit: int  = Query(20, ge=1, le=500),
):
    """
    Fast text search across groups and/or events.
    Returns lightweight JSON for powering the search page.
    """
    conn = get_conn()
    if conn is None:
        raise HTTPException(503, "Data not yet available — batch scrape in progress")
    results = {"groups": [], "events": []}

    if type in ("groups", "both"):
        filt = _text_filter_clause(q, "name", "city", "country", "pro_network", "description")
        rows = conn.execute(f"""
            SELECT
                group_urlname  AS id,
                name,
                city,
                country,
                lat, lon,
                member_count   AS members,
                pro_network    AS network,
                platform,
                source_url     AS url,
                total_past_events,
                events_scrape_ok,
                scraped_at
            FROM groups
            WHERE {filt}
            ORDER BY member_count DESC NULLS LAST
            LIMIT {limit}
        """).fetchdf().to_dict(orient="records")
        results["groups"] = rows

    if type in ("events", "both"):
        filt = _text_filter_clause(q, "e.title")
        rows = conn.execute(f"""
            SELECT
                e.event_id      AS id,
                e.title,
                e.event_url,
                e.status,
                e.is_online,
                e.starts_at,
                e.rsvp_count,
                e.group_urlname AS group_id,
                g.name          AS group_name,
                g.city,
                g.country,
                g.lat,
                g.lon
            FROM events e
            LEFT JOIN groups g USING (group_urlname)
            WHERE {filt}
            ORDER BY e.starts_at DESC
            LIMIT {limit}
        """).fetchdf().to_dict(orient="records")
        results["events"] = rows

    return results


@app.post("/refresh")
def refresh():
    """No-op: data is queried live from Postgres, kept for API compatibility."""
    return {"status": "ok", "message": "data is queried live from Postgres — no refresh needed"}
