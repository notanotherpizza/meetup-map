resource "aiven_pg" "meetupmap" {
  project      = var.aiven_project
  cloud_name   = var.cloud_name
  plan         = "free-1"
  service_name = "meetupmap-pg"

  pg_user_config {
    pg_version = "16"
  }
}
