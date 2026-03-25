#!/usr/bin/env bash
# infra/terraform/extract-config.sh
# Run from the infra/terraform directory after `terraform apply`.
# Writes certs to ../../certs/ and prints .env values to stdout.
#
# Usage:
#   cd infra/terraform
#   bash extract-config.sh

set -euo pipefail

REPO_ROOT="$(cd ../.. && pwd)"
CERTS_DIR="$REPO_ROOT/certs"
mkdir -p "$CERTS_DIR"

echo "Extracting certs to $CERTS_DIR ..."
terraform output -raw kafka_ca_cert     > "$CERTS_DIR/ca.pem"
terraform output -raw kafka_access_cert > "$CERTS_DIR/service.cert"
terraform output -raw kafka_access_key  > "$CERTS_DIR/service.key"
chmod 600 "$CERTS_DIR"/*

echo "Done. Add these to your .env:"
echo ""
echo "KAFKA_BOOTSTRAP_SERVERS=$(terraform output -raw kafka_service_uri)"
echo "POSTGRES_URI=$(terraform output -raw postgres_uri)"
echo "KAFKA_SSL_CA_FILE=./certs/ca.pem"
echo "KAFKA_SSL_CERT_FILE=./certs/service.cert"
echo "KAFKA_SSL_KEY_FILE=./certs/service.key"
