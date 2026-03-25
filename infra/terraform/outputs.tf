# Connection details — run after `terraform apply`
#
#   terraform output -raw kafka_service_uri   → KAFKA_BOOTSTRAP_SERVERS in .env
#   terraform output -raw postgres_uri        → POSTGRES_URI in .env
#
# Download certs manually from the Aiven console:
#   Kafka service → Connection information → Download CA, Access cert, Access key
#   Save to ../../certs/ca.pem, service.cert, service.key

output "kafka_service_uri" {
  description = "Host:port for KAFKA_BOOTSTRAP_SERVERS"
  value       = "${aiven_kafka.meetupmap.service_host}:${aiven_kafka.meetupmap.service_port}"
}

output "postgres_uri" {
  description = "Full connection string for POSTGRES_URI"
  value       = aiven_pg.meetupmap.service_uri
  sensitive   = true
}
