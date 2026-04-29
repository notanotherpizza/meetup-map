resource "aiven_pg" "meetupmap" {
  project                 = var.aiven_project
  cloud_name              = var.cloud_name
  plan                    = "startup-4"
  service_name            = "meetupmap-pg"
  project_vpc_id          = aiven_project_vpc.meetupmap.id

  pg_user_config {
    pg_version = "16"
  }
}
