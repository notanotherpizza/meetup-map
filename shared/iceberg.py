"""
shared/iceberg.py
─────────────────
Shared Iceberg catalog + table helpers used by the scraper and batch runner.
"""
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    BooleanType,
    FloatType,
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

from shared.settings import Settings

GROUPS_SCHEMA = Schema(
    NestedField(1,  "group_urlname",      StringType(),      required=True),
    NestedField(2,  "name",               StringType(),      required=True),
    NestedField(3,  "pro_network",        StringType(),      required=True),
    NestedField(4,  "platform",           StringType(),      required=True),
    NestedField(5,  "city",               StringType(),      required=False),
    NestedField(6,  "country",            StringType(),      required=False),
    NestedField(7,  "lat",                FloatType(),       required=False),
    NestedField(8,  "lon",                FloatType(),       required=False),
    NestedField(9,  "member_count",       IntegerType(),     required=False),
    NestedField(10, "source_url",         StringType(),      required=True),
    NestedField(11, "scraped_at",         TimestamptzType(), required=True),
    NestedField(12, "scrape_method",      StringType(),      required=True),
    NestedField(13, "description",        StringType(),      required=False),
    NestedField(14, "total_past_events",  IntegerType(),     required=False),
    NestedField(15, "events_scrape_ok",   BooleanType(),     required=True),
    NestedField(16, "worker_id",          StringType(),      required=True),
    NestedField(17, "scrape_duration_ms", IntegerType(),     required=True),
)

EVENTS_SCHEMA = Schema(
    NestedField(1,  "event_id",      StringType(),      required=True),
    NestedField(2,  "group_urlname", StringType(),      required=True),
    NestedField(3,  "title",         StringType(),      required=True),
    NestedField(4,  "event_url",     StringType(),      required=True),
    NestedField(5,  "status",        StringType(),      required=True),
    NestedField(6,  "is_online",     BooleanType(),     required=True),
    NestedField(7,  "venue_id",      StringType(),      required=False),
    NestedField(8,  "starts_at",     TimestamptzType(), required=False),
    NestedField(9,  "ends_at",       TimestamptzType(), required=False),
    NestedField(10, "rsvp_count",    IntegerType(),     required=False),
    NestedField(11, "description",   StringType(),      required=False),
    NestedField(12, "scraped_at",    TimestamptzType(), required=True),
    NestedField(13, "scrape_method", StringType(),      required=True),
)

VENUES_SCHEMA = Schema(
    NestedField(1,  "venue_id",       StringType(),      required=True),
    NestedField(2,  "name",           StringType(),      required=False),
    NestedField(3,  "address",        StringType(),      required=False),
    NestedField(4,  "city",           StringType(),      required=False),
    NestedField(5,  "state",          StringType(),      required=False),
    NestedField(6,  "country",        StringType(),      required=False),
    NestedField(7,  "lat",            FloatType(),       required=False),
    NestedField(8,  "lon",            FloatType(),       required=False),
    NestedField(9,  "geocode_source", StringType(),      required=False),
    NestedField(10, "scraped_at",     TimestamptzType(), required=True),
)

# Partition all tables by day of scraped_at (field ID 11 for groups/events, 10 for venues)
_GROUPS_PARTITION = PartitionSpec(
    PartitionField(source_id=11, field_id=1000, transform=DayTransform(), name="scraped_at_day")
)
_EVENTS_PARTITION = PartitionSpec(
    PartitionField(source_id=12, field_id=1000, transform=DayTransform(), name="scraped_at_day")
)
_VENUES_PARTITION = PartitionSpec(
    PartitionField(source_id=10, field_id=1000, transform=DayTransform(), name="scraped_at_day")
)


def make_catalog(settings: Settings) -> RestCatalog:
    # R2 Data Catalog enforces that all table data lives under __r2_data_catalog/.
    # Direct S3 access to that prefix is blocked; the catalog vends signed credentials
    # via the REST token (remote signing). Do not pass raw R2 keys here.
    return load_catalog(
        "r2-catalog",
        **{
            "type": "rest",
            "uri": settings.catalog_uri,
            "token": settings.catalog_token,
            "warehouse": settings.catalog_warehouse,
            "s3.endpoint": settings.r2_endpoint_url,
            "s3.region": "auto",
            "s3.remote-signing-enabled": "true",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )


def get_tables(catalog: RestCatalog):
    """Return (groups_table, events_table, venues_table), creating if needed."""
    catalog.create_namespace_if_not_exists("meetupmap")
    groups = catalog.create_table_if_not_exists(
        "meetupmap.groups", schema=GROUPS_SCHEMA, partition_spec=_GROUPS_PARTITION
    )
    events = catalog.create_table_if_not_exists(
        "meetupmap.events", schema=EVENTS_SCHEMA, partition_spec=_EVENTS_PARTITION
    )
    venues = catalog.create_table_if_not_exists(
        "meetupmap.venues", schema=VENUES_SCHEMA, partition_spec=_VENUES_PARTITION
    )
    return groups, events, venues


def to_arrow(records, iceberg_table) -> pa.Table:
    rows = [r.model_dump() for r in records]
    return pa.Table.from_pylist(rows, schema=iceberg_table.schema().as_arrow())


def write_result(result, groups_table, events_table, venues_table) -> None:
    """Write a ScrapeResult to all three Iceberg tables."""
    groups_table.append(to_arrow([result.group], groups_table))

    all_events = result.past_events + result.upcoming_events
    if all_events:
        events_table.append(to_arrow(all_events, events_table))

    if result.venues:
        venues_table.append(to_arrow(result.venues, venues_table))
