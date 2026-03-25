resource "aiven_pg" "meetupmap" {
  project      = var.aiven_project
  cloud_name   = "do-lon"
  plan         = "free-1-1gb"
  service_name = "meetupmap-pg"

  pg_user_config {
    pg_version = "16"
  }
}
