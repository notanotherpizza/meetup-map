resource "aiven_project_vpc" "meetupmap" {
  project      = var.aiven_project
  cloud_name   = var.cloud_name
  network_cidr = var.vpc_cidr

  timeouts {
    create = "15m"
  }
}
