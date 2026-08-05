"""
map/render.py
─────────────
Reads from Postgres and renders:
  - docs/group_map.html  — self-contained Leaflet map
  - docs/index.html      — search page (injected from map/index_template.html)

Usage:
    python -m map.render
"""
import json
import logging
import hashlib
import os
import colorsys
import shutil
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
import pandas as pd
import psycopg

from shared.settings import Settings

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

DOCS_DIR = Path("docs")
PLACE_BOUNDS_CACHE = Path("map/place_bounds_cache.json")
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "meetupmap/0.1 (github.com/notanotherpizza/meetup-map)"}


def network_colour(network: str) -> str:
    h = int(hashlib.md5(network.encode()).hexdigest()[:8], 16)
    hue = (h % 3600) / 3600
    r, g, b = colorsys.hls_to_rgb(hue, 0.45, 0.65)
    return f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}"


# ── Postgres data fetching ──────────────────────────────────────────────────────

def _query_df(conn: psycopg.Connection, sql: str) -> pd.DataFrame:
    cur = conn.execute(sql)
    cols = [c.name for c in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def fetch_data(settings: Settings) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load groups, events, venues from Postgres — already one current row per id."""
    with psycopg.connect(settings.database_url) as conn:
        groups_df = _query_df(conn, "SELECT * FROM groups")
        events_df = _query_df(conn, "SELECT * FROM events")
        venues_df = _query_df(conn, "SELECT * FROM venues")

    log.info("Loaded: %d groups, %d events, %d venues", len(groups_df), len(events_df), len(venues_df))
    return groups_df, events_df, venues_df


def build_groups(groups_df: pd.DataFrame, events_df: pd.DataFrame, venues_df: pd.DataFrame) -> list[dict]:
    """Join groups with event/venue data to produce the render-ready group list."""
    if groups_df.empty:
        return []

    # Per-group event aggregates
    if not events_df.empty:
        ev = events_df.copy()
        ev["starts_at"] = pd.to_datetime(ev["starts_at"], utc=True, errors="coerce")

        agg = ev.groupby("group_urlname").agg(
            total_events_in_db=("event_id", "count"),
            upcoming_events=("status", lambda s: (s == "upcoming").sum()),
            last_event_at=("starts_at", lambda s: s[ev.loc[s.index, "status"] == "past"].max()),
        ).reset_index()

        # Last past event venue lat/lon
        past = ev[ev["status"] == "past"].dropna(subset=["venue_id"])
        if not past.empty and not venues_df.empty:
            past = past.sort_values("starts_at", ascending=False)
            last_venue_event = past.drop_duplicates(subset=["group_urlname"])
            merged = last_venue_event.merge(
                venues_df[["venue_id", "lat", "lon", "geocode_source"]],
                on="venue_id", how="left",
            )
            venue_loc = merged[["group_urlname", "lat", "lon", "geocode_source"]].rename(columns={
                "lat": "last_event_lat",
                "lon": "last_event_lon",
                "geocode_source": "last_event_geocode_source",
            })
            agg = agg.merge(venue_loc, on="group_urlname", how="left")
        else:
            agg["last_event_lat"] = None
            agg["last_event_lon"] = None
            agg["last_event_geocode_source"] = None

        groups_df = groups_df.merge(agg, on="group_urlname", how="left")
    else:
        groups_df["total_events_in_db"] = 0
        groups_df["upcoming_events"] = 0
        groups_df["last_event_at"] = None
        groups_df["last_event_lat"] = None
        groups_df["last_event_lon"] = None
        groups_df["last_event_geocode_source"] = None

    groups_df = groups_df[groups_df["lat"].notna() & groups_df["lon"].notna()]

    rows = []
    for _, g in groups_df.iterrows():
        rows.append({
            "id":                          g["group_urlname"],
            "name":                        g.get("name") or g["group_urlname"],
            "city":                        g.get("city") if pd.notna(g.get("city")) else "",
            "country":                     g.get("country") if pd.notna(g.get("country")) else "",
            "lat":                         g["lat"],
            "lon":                         g["lon"],
            "member_count":                int(g["member_count"] or 0),
            "source_url":                  g.get("source_url") or "",
            "platform":                    g.get("platform") or "meetup",
            "pro_network":                 g.get("pro_network") or "",
            "last_scraped_at":             g["scraped_at"],
            "events_scraped_at":           g["scraped_at"] if g.get("events_scrape_ok") else None,
            "total_past_events":           int(g["total_past_events"] or 0) if pd.notna(g.get("total_past_events")) else None,
            "total_events_in_db":          int(g.get("total_events_in_db") or 0),
            "upcoming_events":             int(g.get("upcoming_events") or 0),
            "last_event_at":               g.get("last_event_at"),
            "last_event_lat":              g.get("last_event_lat"),
            "last_event_lon":              g.get("last_event_lon"),
            "last_event_geocode_source":   g.get("last_event_geocode_source"),
        })
    return sorted(rows, key=lambda g: g["name"])


def build_networks(groups: list[dict]) -> list[dict]:
    counts: dict[str, int] = {}
    for g in groups:
        n = g["pro_network"] or ""
        counts[n] = counts.get(n, 0) + 1
    return sorted(
        [{"name": n, "colour": network_colour(n), "count": c} for n, c in counts.items()],
        key=lambda x: -x["count"],
    )


def build_upcoming_events(events_df: pd.DataFrame, groups_df: pd.DataFrame) -> list[dict]:
    if events_df.empty:
        return []
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=90)
    ev = events_df.copy()
    ev["starts_at"] = pd.to_datetime(ev["starts_at"], utc=True, errors="coerce")
    upcoming = ev[
        (ev["status"] == "upcoming") &
        ev["starts_at"].notna() &
        (ev["starts_at"] >= now) &
        (ev["starts_at"] <= cutoff)
    ].sort_values("starts_at")

    gmap = groups_df.set_index("group_urlname")[["name", "city", "country"]].to_dict("index") if not groups_df.empty else {}

    rows = []
    for _, e in upcoming.iterrows():
        g = gmap.get(e["group_urlname"], {})
        rows.append({
            "id":         e["event_id"],
            "title":      e.get("title") or "",
            "event_url":  e.get("event_url") or "",
            "status":     e.get("status") or "",
            "is_online":  bool(e.get("is_online")),
            "starts_at":  e["starts_at"].isoformat() if pd.notna(e["starts_at"]) else None,
            "rsvp_count": int(e["rsvp_count"] or 0) if pd.notna(e.get("rsvp_count")) else 0,
            "group_name": g.get("name") or e["group_urlname"],
            "group_id":   e["group_urlname"],
            "city":       g.get("city") if pd.notna(g.get("city")) else "",
            "country":    (g.get("country") if pd.notna(g.get("country")) else "").upper(),
        })
    return rows


# ── Place bounds (local JSON cache, Nominatim fallback) ───────────────────────

def nominatim_bbox(query: str) -> tuple | None:
    try:
        resp = httpx.get(
            NOMINATIM_URL,
            params={"q": query, "format": "json", "limit": 1},
            headers=NOMINATIM_HEADERS,
            timeout=10,
        )
        results = resp.json()
        time.sleep(1.1)
        if results and "boundingbox" in results[0]:
            bb = results[0]["boundingbox"]
            return (float(bb[0]), float(bb[1]), float(bb[2]), float(bb[3]))
    except Exception as e:
        log.warning("Nominatim bbox lookup failed for '%s': %s", query, e)
    return None


def fetch_place_bounds(groups: list[dict]) -> dict:
    from shared.geocoding import COUNTRY_CODE_TO_NAME

    cache: dict = {}
    if PLACE_BOUNDS_CACHE.exists():
        cache = json.loads(PLACE_BOUNDS_CACHE.read_text())

    places: set[str] = set()
    for g in groups:
        if g["city"]:
            places.add(g["city"].lower().strip())
        if g["country"]:
            code = g["country"].lower().strip()
            places.add(code)
            name = COUNTRY_CODE_TO_NAME.get(code, "").lower()
            if name:
                places.add(name)

    misses = [p for p in places if p not in cache]
    log.info("Place bounds: %d cached, %d Nominatim lookups needed", len(places) - len(misses), len(misses))

    for place in misses:
        bb = nominatim_bbox(place)
        if bb:
            cache[place] = list(bb)

    PLACE_BOUNDS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    PLACE_BOUNDS_CACHE.write_text(json.dumps(cache))
    return cache


# ── JS serialisation (unchanged from original) ─────────────────────────────────

def groups_to_js(groups: list[dict], colour_map: dict[str, str]) -> str:
    features = []
    for g in groups:
        last_event = None
        if g["last_event_at"] is not None and pd.notna(g["last_event_at"]):
            le = g["last_event_at"]
            last_event = le.isoformat() if hasattr(le, "isoformat") else str(le)

        days_inactive = None
        if last_event:
            try:
                le_dt = datetime.fromisoformat(str(last_event))
                if le_dt.tzinfo is None:
                    le_dt = le_dt.replace(tzinfo=timezone.utc)
                days_inactive = (datetime.now(timezone.utc) - le_dt).days
            except Exception:
                pass

        use_event_location = (
            g["last_event_lat"] is not None and pd.notna(g["last_event_lat"]) and
            g["last_event_lon"] is not None and pd.notna(g["last_event_lon"])
        )
        if use_event_location:
            lat = round(float(g["last_event_lat"]), 6)
            lon = round(float(g["last_event_lon"]), 6)
            geocode_source = g["last_event_geocode_source"] or "group"
        else:
            lat = round(float(g["lat"] or 0), 6)
            lon = round(float(g["lon"] or 0), 6)
            geocode_source = "group"

        total_events_in_db = int(g["total_events_in_db"] or 0)
        total_events = int(g["total_past_events"] or 0) if g["total_past_events"] else total_events_in_db

        if g["last_scraped_at"] is None:
            event_status = "unscraped"
        elif g["events_scraped_at"] is None:
            event_status = "events_failed"
        elif total_events == 0:
            event_status = "no_events"
        else:
            event_status = "ok"

        features.append({
            "id":             g["id"],
            "lat":            lat,
            "lon":            lon,
            "name":           g["name"] or g["id"],
            "city":           g["city"] or "",
            "country":        (g["country"] or "").upper(),
            "members":        g["member_count"] or 0,
            "total_events":   total_events,
            "upcoming":       int(g["upcoming_events"] or 0),
            "days_inactive":  days_inactive,
            "url":            g["source_url"] or "",
            "network":        g["pro_network"] or "",
            "platform":       g["platform"] or "meetup",
            "color":          colour_map.get(g["pro_network"] or "", "#8b5cf6"),
            "event_location": use_event_location,
            "geocode_source": geocode_source,
            "event_status":   event_status,
        })
    return json.dumps(features, ensure_ascii=False, default=str)


def events_to_js(events: list[dict]) -> str:
    return json.dumps(events, ensure_ascii=False, default=str)


# ── HTML rendering (unchanged) ─────────────────────────────────────────────────

def render(groups: list[dict], networks: list[dict], place_bounds: dict, generated_at: str) -> str:
    colour_map = {n["name"]: n["colour"] for n in networks}
    groups_json = groups_to_js(groups, colour_map)
    networks_json = json.dumps(networks)
    place_bounds_json = json.dumps(place_bounds)
    total = len(groups)
    total_members = sum(g["member_count"] or 0 for g in groups)
    total_events = sum(int(g["total_events_in_db"] or 0) for g in groups)

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
.legend-section-title {{
  font-weight: 600; font-size: 12px; margin-bottom: 6px;
  display: flex; justify-content: space-between; align-items: center;
  cursor: pointer; user-select: none;
}}
.legend-section-title:hover {{ background: rgba(0,0,0,0.05); border-radius: 4px; padding: 2px; }}
.legend-toggle {{
  font-size: 10px; transition: transform 0.2s;
}}
.legend-section-content {{
  margin-bottom: 8px;
}}
.legend-section-content.collapsed {{
  display: none;
}}
.legend-section-title.collapsed .legend-toggle {{
  transform: rotate(-90deg);
}}
#legend-search {{
  width: 100%; border: 1px solid #ddd; border-radius: 4px;
  padding: 3px 6px; font-size: 11px; margin-bottom: 6px;
  outline: none;
}}
#legend-list {{ overflow-y: auto; flex: 1; line-height: 1.9; }}
.legend-item {{
  cursor: pointer; padding: 1px 2px; border-radius: 3px;
  display: flex; align-items: center; gap: 6px;
  transition: background 0.1s;
}}
.legend-item:hover {{ background: #f5f5f5; }}
.legend-item.dimmed {{ opacity: 0.35; }}
.legend-dot {{ width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }}
.legend-label {{ font-size: 11px; flex: 1; }}
.legend-count {{ font-size: 10px; color: #999; }}
#legend-clear {{
  font-size: 10px; color: #2563eb; cursor: pointer;
  margin-top: 4px; text-align: center; display: none;
}}
#map-key {{
  position: absolute; bottom: 24px; right: 12px; z-index: 1000;
  background: rgba(255,255,255,0.95);
  border-radius: 8px; padding: 8px 12px; font-size: 11px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.15);
  line-height: 2;
}}
#map-key-title {{ font-weight: 600; font-size: 12px; margin-bottom: 4px; }}
.filter-check {{ display: flex; align-items: center; gap: 5px; cursor: pointer; font-size: 11px; color: #444; }}
.filter-check input {{ cursor: pointer; }}
.key-item {{ display: flex; align-items: center; gap: 6px; }}
.key-dot {{ width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }}
.leaflet-popup-content {{ font-size: 13px; line-height: 1.5; min-width: 180px; }}
.popup-name {{ font-weight: 600; font-size: 14px; margin-bottom: 4px; }}
.popup-url {{ color: #2563eb; text-decoration: none; font-size: 12px; }}
.popup-url:hover {{ text-decoration: underline; }}
.popup-meta {{ color: #666; font-size: 12px; margin-top: 2px; }}
.tag-upcoming {{ background: #22c55e; color: #fff; border-radius: 3px; padding: 1px 5px; font-size: 11px; }}
.tag-unscraped {{ background: #9ca3af; color: #fff; border-radius: 3px; padding: 1px 5px; font-size: 11px; }}
.tag-events-failed {{ background: #ef4444; color: #fff; border-radius: 3px; padding: 1px 5px; font-size: 11px; }}
.tag-no-events {{ background: #f59e0b; color: #fff; border-radius: 3px; padding: 1px 5px; font-size: 11px; }}
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
  <div id="networks-legend-title" class="legend-section-title" onclick="toggleLegendSection('networks')">
    Networks
    <span style="color:#999;font-weight:400;font-size:11px">{len(networks)}</span>
    <span class="legend-toggle">▼</span>
  </div>
  <div id="networks-legend-content" class="legend-section-content">
    <input id="networks-legend-search" type="text" placeholder="Filter networks..." />
    <div id="networks-legend-list"></div>
    <div id="networks-legend-clear" onclick="clearNetworkFilter()">Show all</div>
  </div>
  <div id="groups-legend-title" class="legend-section-title" onclick="toggleLegendSection('groups')">
    Groups
    <span style="color:#999;font-weight:400;font-size:11px">{len(groups)}</span>
    <span class="legend-toggle">▼</span>
  </div>
  <div id="groups-legend-content" class="legend-section-content">
    <input id="groups-legend-search" type="text" placeholder="Filter groups..." />
    <div id="groups-legend-list"></div>
    <div id="groups-legend-clear" onclick="clearGroupFilter()">Show all</div>
  </div>
</div>
<div id="map-key">
  <div id="map-key-title">Event data</div>
  <div class="key-item"><div class="key-dot" style="background:#22c55e"></div> Has events</div>
  <div class="key-item"><div class="key-dot" style="background:#f59e0b"></div> Scraped — no events found</div>
  <div class="key-item"><div class="key-dot" style="background:#ef4444"></div> Events fetch failed</div>
  <div class="key-item"><div class="key-dot" style="background:#9ca3af; opacity:0.5"></div> Not yet scraped</div>
  <div id="map-key-title" style="margin-top:8px">Location source</div>
  <div class="key-item">
    <label class="filter-check"><input type="checkbox" checked onchange="toggleLocationFilter('postcode', this.checked)"> Postcode</label>
  </div>
  <div class="key-item">
    <label class="filter-check"><input type="checkbox" checked onchange="toggleLocationFilter('address', this.checked)"> Address</label>
  </div>
  <div class="key-item">
    <label class="filter-check"><input type="checkbox" checked onchange="toggleLocationFilter('city', this.checked)"> City-level</label>
  </div>
  <div class="key-item">
    <label class="filter-check"><input type="checkbox" checked onchange="toggleLocationFilter('group', this.checked)"> Group geocode only</label>
  </div>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
<script>
const GROUPS = {groups_json};
const NETWORKS = {networks_json};
const PLACE_BOUNDS = {place_bounds_json};

const renderer = L.svg({{ padding: 0.5 }});
const map = L.map('map', {{ renderer }}).setView([20, 10], 2);
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
  maxZoom: 19
}}).addTo(map);

const clusters = L.markerClusterGroup({{
  maxClusterRadius: 20,
  spiderfyOnMaxZoom: true,
  spiderfyDistanceMultiplier: 2.5,
  zoomToBoundsOnClick: true,
}});

clusters.on('clusterclick', function(e) {{
  const childMarkers = e.layer.getAllChildMarkers();
  const latlngs = childMarkers.map(m => m.getLatLng());
  const allSame = latlngs.every(ll =>
    ll.lat === latlngs[0].lat && ll.lng === latlngs[0].lng
  );
  if (allSame) {{
    e.layer.spiderfy();
  }}
}});

const markers = [];
let activeNetworks = null;
let activeGroups = null;
let activeLocationSources = new Set(['postcode', 'address', 'city', 'miss', 'group']);

function markerStyle(g) {{
  let fillColor, fillOpacity, dashArray, weight;
  if (g.event_status === 'unscraped') {{
    fillColor = '#9ca3af'; fillOpacity = 0.4; dashArray = '3,3'; weight = 0.5;
  }} else if (g.event_status === 'events_failed') {{
    fillColor = '#ef4444'; fillOpacity = 0.7; dashArray = '3,3'; weight = 0.5;
  }} else if (g.event_status === 'no_events') {{
    fillColor = '#f59e0b'; fillOpacity = 0.75; dashArray = null; weight = 0.5;
  }} else {{
    fillColor = g.color;
    fillOpacity = 0.85;
    dashArray = null;
    weight = 0.5;
  }}
  const size = Math.max(5, Math.min(16, 5 + Math.log1p(g.members) * 0.9));
  return {{
    radius: size,
    fillColor,
    color: 'white',
    weight: weight || 0.8,
    fillOpacity,
    dashArray,
  }};
}}

function locationLabel(g) {{
  if (!g.event_location) return '<div class="popup-meta" style="color:#9ca3af;font-size:11px">📍 City-level location</div>';
  if (g.geocode_source === 'postcode') return '<div class="popup-meta" style="color:#6366f1;font-size:11px">📍 Postcode geocoded</div>';
  if (g.geocode_source === 'address')  return '<div class="popup-meta" style="color:#06b6d4;font-size:11px">📍 Address geocoded</div>';
  return '<div class="popup-meta" style="color:#9ca3af;font-size:11px">📍 City-level location</div>';
}}

function popupHtml(g) {{
  const upcoming = g.upcoming > 0 ? `<span class="tag-upcoming">${{g.upcoming}} upcoming</span> ` : '';
  const last = g.days_inactive !== null ? `Last event ${{g.days_inactive}}d ago` : 'No past events';
  const members = g.members > 0 ? `${{g.members.toLocaleString()}} members · ` : '';
  let statusTag = '';
  if (g.event_status === 'unscraped') {{
    statusTag = '<span class="tag-unscraped">Not yet scraped</span> ';
  }} else if (g.event_status === 'events_failed') {{
    statusTag = '<span class="tag-events-failed">Events fetch failed</span> ';
  }} else if (g.event_status === 'no_events') {{
    statusTag = '<span class="tag-no-events">No events found</span> ';
  }}
  return `
    <div class="popup-name">${{g.name}}</div>
    <div class="popup-meta">${{g.city}}${{g.city && g.country ? ', ' : ''}}${{g.country}}</div>
    <div class="popup-meta" style="color:${{g.color}}">${{g.network}}</div>
    <div class="popup-meta">${{members}}${{g.total_events}} events</div>
    <div class="popup-meta" style="margin-top:4px">${{statusTag}}${{upcoming}}${{g.event_status === 'ok' ? last : ''}}</div>
    ${{locationLabel(g)}}
    ${{g.url ? `<div style="margin-top:6px"><a class="popup-url" href="${{g.url}}" target="_blank">View on Meetup →</a></div>` : ''}}
  `;
}}

GROUPS.forEach(g => {{
  const style = markerStyle(g);
  const marker = L.circleMarker([g.lat, g.lon], Object.assign({{}}, style, {{ renderer }}));
  marker.bindPopup(popupHtml(g), {{ maxWidth: 260 }});
  marker._network = g.network;
  marker._geocodeSource = g.geocode_source;
  marker._group = g;
  markers.push(marker);
  clusters.addLayer(marker);
}});
map.addLayer(clusters);

function applyFilter() {{
  clusters.clearLayers();
  let count = 0;
  markers.forEach(m => {{
    const networkOk  = !activeNetworks || activeNetworks.has(m._network);
    const locationOk = activeLocationSources.has(m._geocodeSource);
    const groupOk    = !activeGroups || activeGroups.has(m._group.name);
    if (networkOk && locationOk && groupOk) {{
      clusters.addLayer(m);
      count++;
    }}
  }});
  document.getElementById('visible-count').textContent = count;
  document.getElementById('networks-legend-clear').style.display = activeNetworks ? 'block' : 'none';
  document.getElementById('groups-legend-clear').style.display = activeGroups ? 'block' : 'none';
  renderLegend(document.getElementById('networks-legend-search').value);
  renderGroupsLegend(document.getElementById('groups-legend-search').value);
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

function clearNetworkFilter() {{
  activeNetworks = null;
  applyFilter();
}}

function toggleGroup(name) {{
  if (!activeGroups) {{
    activeGroups = new Set([name]);
  }} else if (activeGroups.has(name)) {{
    activeGroups.delete(name);
    if (activeGroups.size === 0) activeGroups = null;
  }} else {{
    activeGroups.add(name);
  }}
  applyFilter();
}}

function clearGroupFilter() {{
  activeGroups = null;
  applyFilter();
}}

function toggleLocationFilter(source, enabled) {{
  if (source === 'city') {{
    if (enabled) {{ activeLocationSources.add('city'); activeLocationSources.add('miss'); activeLocationSources.add('group'); }}
    else         {{ activeLocationSources.delete('city'); activeLocationSources.delete('miss'); activeLocationSources.delete('group'); }}
  }} else {{
    if (enabled) activeLocationSources.add(source);
    else         activeLocationSources.delete(source);
  }}
  applyFilter();
}}

function toggleLegendSection(section) {{
  const title   = document.getElementById(`${{section}}-legend-title`);
  const content = document.getElementById(`${{section}}-legend-content`);
  const isCollapsed = content.classList.contains('collapsed');
  if (isCollapsed) {{
    content.classList.remove('collapsed');
    title.classList.remove('collapsed');
  }} else {{
    content.classList.add('collapsed');
    title.classList.add('collapsed');
  }}
}}

function renderLegend(filter) {{
  const list = document.getElementById('networks-legend-list');
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

function renderGroupsLegend(filter) {{
  const list = document.getElementById('groups-legend-list');
  const term = (filter || '').toLowerCase();
  list.innerHTML = GROUPS
    .filter(g => !term || g.name.toLowerCase().includes(term))
    .map(g => {{
      const active = !activeGroups || activeGroups.has(g.name);
      return `<div class="legend-item ${{active ? '' : 'dimmed'}}" onclick="toggleGroup('${{g.name.replace(/'/g, "\\'")}}')">
        <div class="legend-dot" style="background:${{g.color}}"></div>
        <span class="legend-label">${{g.name}}</span>
        <span class="legend-count">${{g.members}}</span>
      </div>`;
    }}).join('');
}}

document.getElementById('networks-legend-search').addEventListener('input', e => {{
  renderLegend(e.target.value);
}});

document.getElementById('groups-legend-search').addEventListener('input', e => {{
  renderGroupsLegend(e.target.value);
}});

renderLegend('');
renderGroupsLegend('');

(function() {{
  const params       = new URLSearchParams(window.location.search);
  const cityParam    = (params.get('city')    || '').toLowerCase().trim();
  const countryParam = (params.get('country') || '').toLowerCase().trim();
  const networkParam = (params.get('network') || '').toLowerCase().trim();

  if (!cityParam && !countryParam && !networkParam) return;

  if (networkParam) {{
    const matched = NETWORKS.find(n => n.name.toLowerCase() === networkParam);
    if (matched) {{
      activeNetworks = new Set([matched.name]);
      applyFilter();
    }}
  }}

  const bb = PLACE_BOUNDS[cityParam] || PLACE_BOUNDS[countryParam];
  if (bb) {{
    const bounds = L.latLngBounds([bb[0], bb[2]], [bb[1], bb[3]]);
    const pad = cityParam ? -0.3 : 0.05;
    map.fitBounds(bounds.pad(pad));
  }} else if (cityParam || countryParam) {{
    console.warn('No cached bbox for:', cityParam || countryParam);
  }}
}})();
</script>
</body>
</html>"""


def main() -> None:
    settings = Settings()
    DOCS_DIR.mkdir(exist_ok=True)

    log.info("Loading data from Postgres...")
    groups_df, events_df, venues_df = fetch_data(settings)

    log.info("Building groups...")
    groups = build_groups(groups_df, events_df, venues_df)
    log.info("Groups: %d", len(groups))

    networks = build_networks(groups)
    log.info("Networks: %d", len(networks))

    events = build_upcoming_events(events_df, groups_df)
    log.info("Upcoming events (90d): %d", len(events))

    log.info("Fetching place bounds...")
    place_bounds = fetch_place_bounds(groups)

    generated_at  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    colour_map    = {n["name"]: n["colour"] for n in networks}
    groups_json   = groups_to_js(groups, colour_map)
    events_json   = events_to_js(events)
    networks_json = json.dumps(networks)

    # ── Static data artifacts (lazy-loaded by map pages) ──────────────────
    data_dir = DOCS_DIR / "data"
    data_dir.mkdir(exist_ok=True)

    (data_dir / "groups.json").write_text(groups_json, encoding="utf-8")
    log.info("Written docs/data/groups.json (%.1f KB)", len(groups_json) / 1024)

    (data_dir / "events.json").write_text(events_json, encoding="utf-8")
    log.info("Written docs/data/events.json (%.1f KB)", len(events_json) / 1024)

    (data_dir / "networks.json").write_text(networks_json, encoding="utf-8")
    log.info("Written docs/data/networks.json")

    place_bounds_json = json.dumps(place_bounds)
    (data_dir / "place_bounds.json").write_text(place_bounds_json, encoding="utf-8")
    log.info("Written docs/data/place_bounds.json")

    # ── HTML pages ─────────────────────────────────────────────────────────
    html = render(groups, networks, place_bounds, generated_at)
    out  = DOCS_DIR / "group_map.html"
    out.write_text(html, encoding="utf-8")
    log.info("Written %s (%.1f KB)", out, len(html) / 1024)

    template_path = Path("map/index_template.html")
    if template_path.exists():
        # Index template now lazy-loads groups/events/networks from data/*.json
        # so we only inject __NETWORKS__ (small) for fast sidebar rendering
        download_api = os.environ.get("DOWNLOAD_API", "")
        filled = (template_path.read_text(encoding="utf-8")
            .replace("__NETWORKS__", networks_json)
            .replace("__DOWNLOAD_API__", download_api))
        index_out = DOCS_DIR / "index.html"
        index_out.write_text(filled, encoding="utf-8")
        log.info("Written %s (%.1f KB)", index_out, len(filled) / 1024)
    else:
        log.warning("map/index_template.html not found — skipping search page")

    for asset in ["hero.jpg", "favicon.ico", "logo.png"]:
        src = Path("map") / asset
        if src.exists():
            shutil.copy2(src, DOCS_DIR / asset)

    stats = {
        "groups":       len(groups),
        "events":       sum(int(g["total_events_in_db"] or 0) for g in groups),
        "platforms":    len(set(g["platform"] for g in groups if g.get("platform"))),
        "generated_at": generated_at,
    }
    (DOCS_DIR / "stats.json").write_text(json.dumps(stats))
    log.info("Written docs/stats.json")


if __name__ == "__main__":
    main()
