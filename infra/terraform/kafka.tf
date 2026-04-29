resource "aiven_kafka" "meetupmap" {
  project                 = var.aiven_project
  cloud_name              = var.cloud_name
  plan                    = "startup-4"
  service_name            = "meetupmap-kafka"
  project_vpc_id          = aiven_project_vpc.meetupmap.id

  kafka_user_config {
    kafka_rest = true
    kafka {
      auto_create_topics_enable = true
    }
  }
}

resource "aiven_kafka_topic" "groups_to_scrape" {
  project      = var.aiven_project
  service_name = aiven_kafka.meetupmap.service_name
  topic_name   = "groups-to-scrape"
  partitions   = 2
  replication  = 2
  config {
    retention_ms   = 86400000
    cleanup_policy = "delete"
  }
}

resource "aiven_kafka_topic" "groups_raw" {
  project      = var.aiven_project
  service_name = aiven_kafka.meetupmap.service_name
  topic_name   = "groups-raw"
  partitions   = 2
  replication  = 2
  config {
    retention_ms   = 86400000
    cleanup_policy = "delete"
  }
}

resource "aiven_kafka_topic" "events_raw" {
  project      = var.aiven_project
  service_name = aiven_kafka.meetupmap.service_name
  topic_name   = "events-raw"
  partitions   = 2
  replication  = 2
  config {
    retention_ms   = 86400000
    cleanup_policy = "delete"
  }
}
