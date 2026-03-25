resource "aiven_kafka" "meetupmap" {
  project      = var.aiven_project
  cloud_name   = var.cloud_name
  plan         = "free-1"
  service_name = "meetupmap-kafka"

  kafka_user_config {
    # Enable the Kafka REST proxy — useful for debugging messages without a client
    kafka_rest = true

    kafka {
      # Allow automatic topic creation by producers (seed producer uses AdminClient
      # to create topics explicitly, but this is a useful safety net)
      auto_create_topics_enable = true
    }
  }
}

# ── Topics ────────────────────────────────────────────────────────────────────
# 6 partitions = up to 6 parallel workers per topic.
# Free tier has limited storage so retention is set to 24h.

resource "aiven_kafka_topic" "groups_to_scrape" {
  project      = var.aiven_project
  service_name = aiven_kafka.meetupmap.service_name
  topic_name   = "groups-to-scrape"
  partitions   = 6
  replication  = 1

  config {
    retention_ms     = 86400000   # 24 hours
    cleanup_policy   = "delete"
  }
}

resource "aiven_kafka_topic" "groups_raw" {
  project      = var.aiven_project
  service_name = aiven_kafka.meetupmap.service_name
  topic_name   = "groups-raw"
  partitions   = 6
  replication  = 1

  config {
    retention_ms     = 86400000
    cleanup_policy   = "delete"
  }
}

resource "aiven_kafka_topic" "events_raw" {
  project      = var.aiven_project
  service_name = aiven_kafka.meetupmap.service_name
  topic_name   = "events-raw"
  partitions   = 6
  replication  = 1

  config {
    retention_ms     = 86400000
    cleanup_policy   = "delete"
  }
}
