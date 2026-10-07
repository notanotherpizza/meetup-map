-- Read-only queries the triage agent runs through the Aiven MCP (aiven_pg_read)
-- against project `meetup-map`, service `catalog-db`. It never writes.

-- 1. Scrape progress and health, per platform
SELECT platform,
       count(*)                                                  AS groups,
       count(*) FILTER (WHERE scraped_at > now() - interval '1 hour')  AS scraped_last_hour,
       count(*) FILTER (WHERE NOT events_scrape_ok)              AS events_failed,
       count(*) FILTER (WHERE scraped_at < now() - interval '30 days') AS stale_30d,
       min(scraped_at) AS oldest_scrape, max(scraped_at) AS newest_scrape
FROM groups GROUP BY platform ORDER BY groups DESC;

-- 2. Groups whose events failed to scrape, largest first (dead/private candidates:
--    name is just the URL slug and member_count is NULL)
SELECT group_urlname, name, platform, member_count, total_past_events, events_scrape_ok, scraped_at
FROM groups
WHERE NOT events_scrape_ok OR total_past_events IS NULL
ORDER BY member_count DESC NULLS LAST LIMIT 25;

-- 3. Most valuable stale groups: big, active, tech-ish, not refreshed in 30+ days
SELECT group_urlname, name, member_count, total_past_events, scraped_at
FROM groups
WHERE events_scrape_ok AND scraped_at < now() - interval '30 days'
ORDER BY member_count DESC NULLS LAST LIMIT 50;
