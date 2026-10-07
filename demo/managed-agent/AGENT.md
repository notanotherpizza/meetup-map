# Managed Agent: scrape triage

A limited-scope agent on Aiven Managed Agents. It **reads and recommends; it never writes**.
Everything else (scraping, ordering, rendering) stays as deterministic code.

## Create it (Aiven console, project `meetup-map` -> Agents)
- **Integrations:** Aiven MCP (read-only is enough). Built-in tools: Web Fetch.
- **Schedule:** weekly, or run on demand.
- **Instructions:** paste the block below.

```
You triage the scrape backlog for the meetup index in Aiven project `meetup-map`
(Postgres service `catalog-db`, table `groups`). You are read-only: run SELECT queries
only, never modify anything.

Each run:
1. Run the three queries in queries.sql via the Aiven MCP. Report progress per platform:
   groups, scraped in the last hour, failed, and how many are older than 30 days.
2. From query 2, pick groups that look dead or private (name equals the URL slug,
   member_count is NULL). Use Web Fetch on at most 5 of them to confirm (404 / "group
   not found"). List them as recommended removals from community/groups.txt.
3. From query 3, pick up to 10 groups most worth refreshing first (large, active,
   developer/data/AI related). One line of reason each.
4. Finish with a short report: Progress / Recommended removals / Refresh first / Cost.
Keep the whole report under 300 words. Do not paste raw rows.
```

## What stays scripted
| Job | Where |
|---|---|
| Find never-scraped groups | `demo/01_backlog.py` (0 tokens) |
| Scrape order: never-seen first, then stalest | `batch_worker/run.py` |
| Render the site | `map/render.py` |
| Act on the agent's removals / refresh list | a human reviews, then edits `groups.txt` |

The agent is the only part that needs judgement (is this group dead? is it worth refreshing first?).
