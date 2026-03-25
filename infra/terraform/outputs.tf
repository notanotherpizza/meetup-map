# After `terraform apply`, pipe certs and connection details into your project:
#
#   terraform output -raw kafka_service_uri  (→ KAFKA_BOOTSTRAP_SERVERS in .env)
#   terraform output -raw postgres_uri       (→ POSTGRES_URI in .env)
#   terraform output -raw kafka_ca_cert      > ../../certs/ca.pem
#   terraform output -raw kafka_access_cert  > ../../certs/service.cert
#   terraform output -raw kafka_access_key   > ../../certs/service.key

output "kafka_service_uri" {
  description = "Host:port for KAFKA_BOOTSTRAP_SERVERS"
  value       = "${aiven_kafka.meetupmap.service_host}:${aiven_kafka.meetupmap.service_port}"
}

output "kafka_ca_cert" {
  description = "CA certificate — save to certs/ca.pem"
  value       = aiven_kafka.meetupmap.service_ca_cert
  sensitive   = true
}

output "kafka_access_cert" {
  description = "Client certificate — save to certs/service.cert"
  value       = aiven_kafka.meetupmap.service_cert
  sensitive   = true
}

output "kafka_access_key" {
  description = "Client key — save to certs/service.key"
  value       = aiven_kafka.meetupmap.service_key
  sensitive   = true
}

output "postgres_uri" {
  description = "Full connection string for POSTGRES_URI"
  value       = aiven_pg.meetupmap.service_uri
  sensitive   = true
}
