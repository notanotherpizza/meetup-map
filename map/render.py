"""
map/render.py
─────────────
Queries Postgres and renders a self-contained Leaflet map as docs/index.html,
ready for GitHub Pages.

Usage:
    python -m map.render
"""
import json
import logging
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from shared.settings import Settings

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

DOCS_DIR = Path("docs")


def fetch_groups(pg: psycopg.Connection) -> list[dict]:
    with pg.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                g.id,
                g.name,
                g.city,
                g.country,
                g.lat,
                g.lon,
                g.member_count,
                g.meetup_url,
                g.pro_network,
                COUNT(e.id)                                        AS total_events,
                COUNT(e.id) FILTER (WHERE e.status = 'upcoming')  AS upcoming_events,
                MAX(e.starts_at) FILTER (WHERE e.status = 'past') AS last_event_at
            FROM groups g
            LEFT JOIN events e ON e.group_id = g.id
            WHERE g.lat IS NOT NULL AND g.lon IS NOT NULL
            GROUP BY g.id
            ORDER BY g.name
        """)
        return cur.fetchall()


def groups_to_js(groups: list[dict]) -> str:
    """Serialise groups to a JS array literal embedded in the HTML."""
    features = []
    for g in groups:
        last_event = None
        if g["last_event_at"]:
            le = g["last_event_at"]
            last_event = le.isoformat() if hasattr(le, "isoformat") else str(le)

        # Days since last event — used for colour coding
        days_inactive = None
        if last_event:
            try:
                le_dt = datetime.fromisoformat(last_event)
                if le_dt.tzinfo is None:
                    le_dt = le_dt.replace(tzinfo=timezone.utc)
                days_inactive = (datetime.now(timezone.utc) - le_dt).days
            except Exception:
                pass

        features.append({
            "lat": g["lat"],
            "lon": g["lon"],
            "name": g["name"] or g["id"],
            "city": g["city"] or "",
            "country": (g["country"] or "").upper(),
            "members": g["member_count"] or 0,
            "total_events": int(g["total_events"] or 0),
            "upcoming": int(g["upcoming_events"] or 0),
            "days_inactive": days_inactive,
            "url": g["meetup_url"] or "",
            "network": g["pro_network"] or "",
        })

    return json.dumps(features, ensure_ascii=False)


def render(groups: list[dict], generated_at: str) -> str:
    groups_json = groups_to_js(groups)
    total = len(groups)
    total_members = sum(g["member_count"] or 0 for g in groups)
    total_events = sum(int(g["total_events"] or 0) for g in groups)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Meetup Map</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css"/>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css"/>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }}
#map {{ width: 100vw; height: 100vh; }}
#panel {{
  position: absolute; top: 12px; left: 50px; z-index: 1000;
  background: rgba(255,255,255,0.95);
  border-radius: 8px; padding: 10px 14px; font-size: 13px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.15);
  max-width: 240px;
}}
#panel h1 {{ font-size: 14px; font-weight: 600; margin-bottom: 6px; }}
#panel .stat {{ color: #555; margin-bottom: 2px; }}
#panel .stat span {{ font-weight: 600; color: #111; }}
#updated {{ font-size: 11px; color: #999; margin-top: 6px; }}
.leaflet-popup-content {{ font-size: 13px; line-height: 1.5; min-width: 180px; }}
.popup-name {{ font-weight: 600; font-size: 14px; margin-bottom: 4px; }}
.popup-url {{ color: #2563eb; text-decoration: none; font-size: 12px; }}
.popup-url:hover {{ text-decoration: underline; }}
.popup-meta {{ color: #666; font-size: 12px; margin-top: 2px; }}
.tag-upcoming {{ background: #22c55e; color: #fff; border-radius: 3px; padding: 1px 5px; font-size: 11px; }}
</style>
</head>
<body>
<div id="map"></div>
<div id="panel">
  <h1>Meetup Map</h1>
  <div class="stat">Groups: <span>{total}</span></div>
  <div class="stat">Members: <span>{total_members:,}</span></div>
  <div class="stat">Events scraped: <span>{total_events:,}</span></div>
  <div id="updated">Updated {generated_at}</div>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
<script>
const GROUPS = {groups_json};

const map = L.map('map').setView([20, 10], 2);

L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
  maxZoom: 19
}}).addTo(map);

const clusters = L.markerClusterGroup({{
  maxClusterRadius: 40,
  disableClusteringAtZoom: 8,
}});

function popupHtml(g) {{
  const upcoming = g.upcoming > 0
    ? `<span class="tag-upcoming">${{g.upcoming}} upcoming</span> `
    : '';
  const last = g.days_inactive !== null
    ? `Last event ${{g.days_inactive}}d ago`
    : 'No past events';
  const members = g.members > 0 ? `${{g.members.toLocaleString()}} members · ` : '';
  return `
    <div class="popup-name">${{g.name}}</div>
    <div class="popup-meta">${{g.city}}${{g.city && g.country ? ', ' : ''}}${{g.country}}</div>
    <div class="popup-meta">${{members}}${{g.total_events}} events</div>
    <div class="popup-meta" style="margin-top:4px">${{upcoming}}${{last}}</div>
    ${{g.url ? `<div style="margin-top:6px"><a class="popup-url" href="${{g.url}}" target="_blank">View on Meetup →</a></div>` : ''}}
  `;
}}

GROUPS.forEach(g => {{
  const color = '#ee9041';
  const size = Math.max(6, Math.min(14, 6 + Math.log1p(g.members) * 0.8));
  const marker = L.circleMarker([g.lat, g.lon], {{
    radius: size,
    fillColor: color,
    color: 'transparent',
    fillOpacity: 0.85,
    weight: 0,
  }});
  marker.bindPopup(popupHtml(g), {{ maxWidth: 260 }});
  clusters.addLayer(marker);
}});

map.addLayer(clusters);


</script>
</body>
</html>"""


def main() -> None:
    settings = Settings.from_env()
    DOCS_DIR.mkdir(exist_ok=True)

    log.info("Connecting to Postgres…")
    with psycopg.connect(settings.postgres_uri, row_factory=dict_row) as pg:
        groups = fetch_groups(pg)

    log.info("Fetched %d groups with coordinates", len(groups))

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html = render(groups, generated_at)

    out = DOCS_DIR / "index.html"
    out.write_text(html, encoding="utf-8")
    log.info("Written %s (%.1f KB)", out, len(html) / 1024)


if __name__ == "__main__":
    main()