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
import hashlib
import colorsys
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from shared.settings import Settings

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

DOCS_DIR = Path("docs")

def network_colour(network: str) -> str:
    """Deterministic colour from network name — same network always same colour."""
    h = int(hashlib.md5(network.encode()).hexdigest()[:8], 16)
    hue = (h % 3600) / 3600
    r, g, b = colorsys.hls_to_rgb(hue, 0.45, 0.65)
    return f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}"


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


def fetch_networks(pg: psycopg.Connection) -> list[dict]:
    """Returns networks sorted by group count descending, with assigned colours."""
    with pg.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT pro_network, count(*) as group_count
            FROM groups
            GROUP BY pro_network
            ORDER BY group_count DESC
        """)
        rows = cur.fetchall()

    return [
        {"name": row["pro_network"], "colour": network_colour(row["pro_network"]), "count": row["group_count"]}
        for row in rows
    ]


def groups_to_js(groups: list[dict], colour_map: dict[str, str]) -> str:
    features = []
    for g in groups:
        last_event = None
        if g["last_event_at"]:
            le = g["last_event_at"]
            last_event = le.isoformat() if hasattr(le, "isoformat") else str(le)

        days_inactive = None
        if last_event:
            try:
                le_dt = datetime.fromisoformat(last_event)
                if le_dt.tzinfo is None:
                    le_dt = le_dt.replace(tzinfo=timezone.utc)
                days_inactive = (datetime.now(timezone.utc) - le_dt).days
            except Exception:
                pass

        # Deterministic jitter for groups geocoded to city-level coordinates
        # Spreads co-located groups without random movement between renders
        jitter_seed = int(hashlib.md5(g["id"].encode()).hexdigest()[:8], 16)
        jitter_lat = ((jitter_seed & 0xffff) / 0xffff - 0.5) * 0.04
        jitter_lon = ((jitter_seed >> 16 & 0xffff) / 0xffff - 0.5) * 0.06
        lat = round((g["lat"] or 0) + jitter_lat, 6)
        lon = round((g["lon"] or 0) + jitter_lon, 6)

        features.append({
            "lat": lat,
            "lon": lon,
            "name": g["name"] or g["id"],
            "city": g["city"] or "",
            "country": (g["country"] or "").upper(),
            "members": g["member_count"] or 0,
            "total_events": int(g["total_events"] or 0),
            "upcoming": int(g["upcoming_events"] or 0),
            "days_inactive": days_inactive,
            "url": g["meetup_url"] or "",
            "network": g["pro_network"] or "",
            "color": colour_map.get(g["pro_network"] or "", "#8b5cf6"),
        })

    return json.dumps(features, ensure_ascii=False)


def render(groups: list[dict], networks: list[dict], generated_at: str) -> str:
    colour_map = {n["name"]: n["colour"] for n in networks}
    groups_json = groups_to_js(groups, colour_map)
    networks_json = json.dumps(networks)
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
#legend {{
  position: absolute; bottom: 24px; left: 12px; z-index: 1000;
  background: rgba(255,255,255,0.95);
  border-radius: 8px; padding: 8px 12px; font-size: 12px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.15);
  max-height: 40vh; display: flex; flex-direction: column;
  min-width: 180px;
}}
#legend-title {{
  font-weight: 600; font-size: 12px; margin-bottom: 6px;
  display: flex; justify-content: space-between; align-items: center;
}}
#legend-search {{
  width: 100%; border: 1px solid #ddd; border-radius: 4px;
  padding: 3px 6px; font-size: 11px; margin-bottom: 6px;
  outline: none;
}}
#legend-list {{
  overflow-y: auto; flex: 1; line-height: 1.9;
}}
.legend-item {{
  cursor: pointer; padding: 1px 2px; border-radius: 3px;
  display: flex; align-items: center; gap: 6px;
  transition: background 0.1s;
}}
.legend-item:hover {{ background: #f5f5f5; }}
.legend-item.dimmed {{ opacity: 0.35; }}
.legend-dot {{
  width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0;
}}
.legend-label {{ font-size: 11px; flex: 1; }}
.legend-count {{ font-size: 10px; color: #999; }}
#legend-clear {{
  font-size: 10px; color: #2563eb; cursor: pointer;
  margin-top: 4px; text-align: center; display: none;
}}
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
  <div class="stat">Groups: <span id="visible-count">{total}</span> <span style="color:#999;font-weight:400">of {total}</span></div>
  <div class="stat">Members: <span>{total_members:,}</span></div>
  <div class="stat">Events scraped: <span>{total_events:,}</span></div>
  <div id="updated">Updated {generated_at}</div>
</div>
<div id="legend">
  <div id="legend-title">
    Networks
    <span style="color:#999;font-weight:400;font-size:11px">{len(networks)}</span>
  </div>
  <input id="legend-search" type="text" placeholder="Filter networks…" />
  <div id="legend-list"></div>
  <div id="legend-clear" onclick="clearFilter()">Show all</div>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
<script>
const GROUPS = {groups_json};
const NETWORKS = {networks_json};

const map = L.map('map').setView([20, 10], 2);
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
  maxZoom: 19
}}).addTo(map);

const clusters = L.markerClusterGroup({{ maxClusterRadius: 40, disableClusteringAtZoom: 8 }});
const markers = [];
let activeNetworks = null; // null = all visible

function popupHtml(g) {{
  const upcoming = g.upcoming > 0 ? `<span class="tag-upcoming">${{g.upcoming}} upcoming</span> ` : '';
  const last = g.days_inactive !== null ? `Last event ${{g.days_inactive}}d ago` : 'No past events';
  const members = g.members > 0 ? `${{g.members.toLocaleString()}} members · ` : '';
  return `
    <div class="popup-name">${{g.name}}</div>
    <div class="popup-meta">${{g.city}}${{g.city && g.country ? ', ' : ''}}${{g.country}}</div>
    <div class="popup-meta" style="color:${{g.color}}">${{g.network}}</div>
    <div class="popup-meta">${{members}}${{g.total_events}} events</div>
    <div class="popup-meta" style="margin-top:4px">${{upcoming}}${{last}}</div>
    ${{g.url ? `<div style="margin-top:6px"><a class="popup-url" href="${{g.url}}" target="_blank">View on Meetup →</a></div>` : ''}}
  `;
}}

GROUPS.forEach(g => {{
  const size = Math.max(6, Math.min(14, 6 + Math.log1p(g.members) * 0.8));
  const marker = L.circleMarker([g.lat, g.lon], {{
    radius: size, fillColor: g.color, color: 'transparent',
    fillOpacity: 0.85, weight: 0,
  }});
  marker.bindPopup(popupHtml(g), {{ maxWidth: 260 }});
  marker._network = g.network;
  markers.push(marker);
  clusters.addLayer(marker);
}});
map.addLayer(clusters);

function applyFilter() {{
  clusters.clearLayers();
  let count = 0;
  markers.forEach(m => {{
    if (!activeNetworks || activeNetworks.has(m._network)) {{
      clusters.addLayer(m);
      count++;
    }}
  }});
  document.getElementById('visible-count').textContent = count;
  document.getElementById('legend-clear').style.display = activeNetworks ? 'block' : 'none';
  renderLegend(document.getElementById('legend-search').value);
}}

function toggleNetwork(name) {{
  if (!activeNetworks) {{
    activeNetworks = new Set([name]);
  }} else if (activeNetworks.has(name)) {{
    activeNetworks.delete(name);
    if (activeNetworks.size === 0) activeNetworks = null;
  }} else {{
    activeNetworks.add(name);
  }}
  applyFilter();
}}

function clearFilter() {{
  activeNetworks = null;
  applyFilter();
}}

function renderLegend(filter) {{
  const list = document.getElementById('legend-list');
  const term = (filter || '').toLowerCase();
  list.innerHTML = NETWORKS
    .filter(n => !term || n.name.toLowerCase().includes(term))
    .map(n => {{
      const active = !activeNetworks || activeNetworks.has(n.name);
      return `<div class="legend-item ${{active ? '' : 'dimmed'}}" onclick="toggleNetwork('${{n.name}}')">
        <div class="legend-dot" style="background:${{n.colour}}"></div>
        <span class="legend-label">${{n.name}}</span>
        <span class="legend-count">${{n.count}}</span>
      </div>`;
    }}).join('');
}}

document.getElementById('legend-search').addEventListener('input', e => {{
  renderLegend(e.target.value);
}});

renderLegend('');
</script>
</body>
</html>"""


def main() -> None:
    settings = Settings.from_env()
    DOCS_DIR.mkdir(exist_ok=True)

    log.info("Connecting to Postgres…")
    with psycopg.connect(settings.postgres_uri, row_factory=dict_row) as pg:
        groups = fetch_groups(pg)
        networks = fetch_networks(pg)

    log.info("Fetched %d groups across %d networks", len(groups), len(networks))

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html = render(groups, networks, generated_at)

    out = DOCS_DIR / "index.html"
    out.write_text(html, encoding="utf-8")
    log.info("Written %s (%.1f KB)", out, len(html) / 1024)


if __name__ == "__main__":
    main()