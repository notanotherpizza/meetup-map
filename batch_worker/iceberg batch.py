import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    from dotenv import load_dotenv

    load_dotenv()
    return (os,)


@app.cell
def _(os):
    import requests

    mgmt_url = os.environ["LAKEKEEPER_CATALOG_URI"].replace("/catalog", "")

    _project_id = os.environ["LAKEKEEPER_PROJECT_ID"]
    print(f"Using project: {_project_id}")

    # Register warehouse (409 = already exists, safe to ignore)
    r = requests.post(
        f"{mgmt_url}/management/v1/warehouse",
        json={
            "warehouse-name": "meetup-map",
            "project-id": _project_id,
            "storage-profile": {
                "type": "s3",
                "bucket": "meetup-map",
                "endpoint": os.environ["R2_ENDPOINT_URL"],
                "region": "auto",
                "path-style-access": True,
                "flavor": "s3-compat",
                "sts-enabled": False,
                "remote-signing-enabled": False,
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": os.environ["R2_ACCESS_KEY_ID"],
                "aws-secret-access-key": os.environ["R2_SECRET_ACCESS_KEY"],
            },
        },
    )
    if r.status_code in (409, 400) and "Overlap" in r.text:
        print("Warehouse already registered")
    else:
        r.raise_for_status()
        print(f"Warehouse registered: {r.json()['warehouse-id']}")
    return (requests,)


@app.cell
def _():
    from shared.iceberg import make_catalog, get_tables
    from shared.settings import Settings

    _settings = Settings()
    catalog = make_catalog(_settings)
    print(f"Catalog OK — namespaces: {catalog.list_namespaces()}")
    return catalog, get_tables, make_catalog


@app.cell
def _(catalog, get_tables):
    groups_table, events_table, venues_table = get_tables(catalog)
    print(f"groups:  {groups_table.scan().to_arrow().num_rows} rows")
    print(f"events:  {events_table.scan().to_arrow().num_rows} rows")
    print(f"venues:  {venues_table.scan().to_arrow().num_rows} rows")
    return events_table, groups_table, venues_table


@app.cell
async def _():
    import httpx
    from worker.platforms.meetup import MeetupPlatform
    from shared.models import GroupSeed
    from datetime import datetime, timezone

    TEST_GROUP = "londonpython"

    seed = GroupSeed(
        group_urlname=TEST_GROUP,
        group_url=f"https://www.meetup.com/{TEST_GROUP}/",
        pro_network="test",
        seeded_at=datetime.now(timezone.utc),
    )
    async with httpx.AsyncClient(timeout=30) as client:
        result = await MeetupPlatform().scrape(
            seed=seed, browser=None, http_client=client,
            max_past_events=10, worker_id="notebook",
        )

    print(f"Group:   {result.group.name}")
    print(f"Events:  {len(result.past_events)} past, {len(result.upcoming_events)} upcoming")
    print(f"Venues:  {len(result.venues)}")
    print(f"Desc:    {(result.group.description or '')[:80] or 'none'}")
    return (result,)


@app.cell
def _(events_table, groups_table, result, venues_table):
    from shared.iceberg import write_result

    write_result(result, groups_table, events_table, venues_table)
    _all_events = result.past_events + result.upcoming_events
    print(f"Wrote 1 group + {len(_all_events)} events + {len(result.venues)} venues")
    return


@app.cell
def _(events_table, groups_table, venues_table):
    groups_arrow = groups_table.scan().to_arrow()
    events_arrow = events_table.scan().to_arrow()
    venues_arrow = venues_table.scan().to_arrow()

    print(f"groups:  {groups_arrow.num_rows} rows")
    print(f"events:  {events_arrow.num_rows} rows")
    print(f"venues:  {venues_arrow.num_rows} rows")
    return events_arrow, groups_arrow, venues_arrow


@app.cell
def _(groups_arrow):
    import pyarrow.compute as pc

    _cols = ["group_urlname", "name", "country", "member_count", "scraped_at"]
    print(groups_arrow.select(_cols).to_pydict())
    return (pc,)


@app.cell
def _(events_arrow):
    _cols = ["event_id", "title", "status", "starts_at", "rsvp_count", "is_online"]
    tbl = events_arrow.select(_cols).sort_by([("starts_at", "descending")])
    for row in tbl.to_pylist()[:10]:
        print(f"  [{row['status']}] {row['title'][:50]} — {row['starts_at']} ({row['rsvp_count']} rsvps)")
    return


@app.cell
def _(events_arrow, pc):
    upcoming = pc.sum(pc.equal(events_arrow["status"], "upcoming")).as_py()
    past = pc.sum(pc.equal(events_arrow["status"], "past")).as_py()
    online = pc.sum(events_arrow["is_online"]).as_py()
    print(f"upcoming: {upcoming}  past: {past}  online: {online}")
    return


@app.cell
def _(groups_table):
    # Time travel — list snapshots
    for snap in groups_table.history():
        print(f"  snapshot {snap.snapshot_id} — {snap.timestamp_ms}")
    return


if __name__ == "__main__":
    app.run()
