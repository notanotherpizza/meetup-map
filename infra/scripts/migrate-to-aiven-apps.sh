#!/usr/bin/env bash
#
# migrate-to-aiven-apps.sh
# ────────────────────────
# Complete migration script for meetup-map from Fly.io to Aiven Apps.
#
# Prerequisites:
#   - Aiven CLI installed: pip install aiven-client
#   - Logged in: avn user login
#   - Terraform installed
#   - pg_dump and psql available
#
# Usage:
#   cd /path/to/meetup-map
#   ./infra/scripts/migrate-to-aiven-apps.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TF_DIR="$REPO_ROOT/infra/terraform"
BACKUP_DIR="$REPO_ROOT/infra/backups"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARNING:${NC} $*"; }
error() { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $*" >&2; }

# ── Step 0: Pre-flight checks ────────────────────────────────────────────────

log "Running pre-flight checks..."

command -v avn >/dev/null 2>&1 || { error "Aiven CLI not found. Install with: pip install aiven-client"; exit 1; }
command -v terraform >/dev/null 2>&1 || { error "Terraform not found."; exit 1; }
command -v pg_dump >/dev/null 2>&1 || { error "pg_dump not found."; exit 1; }
command -v psql >/dev/null 2>&1 || { error "psql not found."; exit 1; }

# Check Aiven login
if ! avn user info >/dev/null 2>&1; then
    error "Not logged into Aiven CLI. Run: avn user login"
    exit 1
fi

log "All pre-flight checks passed."

# ── Step 1: Load current config ──────────────────────────────────────────────

log "Loading current configuration from .env..."

if [[ ! -f "$REPO_ROOT/.env" ]]; then
    error ".env file not found at $REPO_ROOT/.env"
    exit 1
fi

# shellcheck source=/dev/null
source "$REPO_ROOT/.env"

OLD_POSTGRES_URI="${POSTGRES_URI:-}"
OLD_KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-}"

if [[ -z "$OLD_POSTGRES_URI" ]]; then
    error "POSTGRES_URI not set in .env"
    exit 1
fi

log "Old Postgres: ${OLD_POSTGRES_URI%%@*}@..."
log "Old Kafka: $OLD_KAFKA_BOOTSTRAP"

# ── Step 2: Backup existing Postgres data ────────────────────────────────────

log "Creating backup directory..."
mkdir -p "$BACKUP_DIR"

BACKUP_FILE="$BACKUP_DIR/meetupmap-$(date +%Y%m%d-%H%M%S).sql"

log "Backing up existing Postgres data to $BACKUP_FILE..."
log "This may take a few minutes depending on data size..."

pg_dump "$OLD_POSTGRES_URI" \
    --no-owner \
    --no-acl \
    --clean \
    --if-exists \
    > "$BACKUP_FILE"

BACKUP_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
log "Backup complete: $BACKUP_FILE ($BACKUP_SIZE)"

# ── Step 3: Terraform - Create new infrastructure ────────────────────────────

log "Creating new Aiven infrastructure with Terraform..."

cd "$TF_DIR"

# Check for tfvars
if [[ ! -f "terraform.tfvars" ]]; then
    if [[ -f "terraform.tfvars.example" ]]; then
        warn "terraform.tfvars not found. Creating from example..."
        cp terraform.tfvars.example terraform.tfvars
        echo ""
        echo "Please edit $TF_DIR/terraform.tfvars with your Aiven API token and project name."
        echo "Then re-run this script."
        exit 1
    else
        error "No terraform.tfvars or terraform.tfvars.example found"
        exit 1
    fi
fi

log "Running terraform init..."
terraform init

log "Running terraform plan..."
terraform plan -out=tfplan

echo ""
echo "═══════════════════════════════════════════════════════════════════════════"
echo "Review the plan above. This will create:"
echo "  - VPC in aws-eu-west-1"
echo "  - Kafka (dev-1 plan) in VPC"
echo "  - Postgres (dev-1 plan) in VPC"
echo "═══════════════════════════════════════════════════════════════════════════"
echo ""
read -rp "Apply this plan? (yes/no): " CONFIRM

if [[ "$CONFIRM" != "yes" ]]; then
    log "Aborted by user."
    exit 0
fi

log "Applying Terraform..."
terraform apply tfplan

# Get outputs
NEW_KAFKA_URI=$(terraform output -raw kafka_service_uri)
NEW_POSTGRES_URI=$(terraform output -raw postgres_uri)
VPC_ID=$(terraform output -raw vpc_id)

log "New infrastructure created:"
log "  Kafka: $NEW_KAFKA_URI"
log "  Postgres: [hidden - contains password]"
log "  VPC ID: $VPC_ID"

# ── Step 4: Wait for services to be running ──────────────────────────────────

log "Waiting for services to be ready..."

AIVEN_PROJECT=$(grep -E '^aiven_project\s*=' terraform.tfvars | sed 's/.*=\s*"\(.*\)"/\1/' || echo "")
if [[ -z "$AIVEN_PROJECT" ]]; then
    AIVEN_PROJECT=$(terraform output -raw aiven_project 2>/dev/null || echo "")
fi

if [[ -z "$AIVEN_PROJECT" ]]; then
    warn "Could not determine Aiven project name. Please check service status manually."
else
    log "Checking service status in project: $AIVEN_PROJECT"

    for SERVICE in meetupmap-kafka meetupmap-pg; do
        log "Waiting for $SERVICE..."
        for i in {1..60}; do
            STATE=$(avn service get "$SERVICE" --project "$AIVEN_PROJECT" --json 2>/dev/null | grep -o '"state":"[^"]*"' | head -1 | cut -d'"' -f4 || echo "unknown")
            if [[ "$STATE" == "RUNNING" ]]; then
                log "$SERVICE is RUNNING"
                break
            fi
            echo -n "."
            sleep 10
        done
        echo ""
    done
fi

# ── Step 5: Initialize schema on new Postgres ────────────────────────────────

log "Initializing schema on new Postgres..."

psql "$NEW_POSTGRES_URI" < "$REPO_ROOT/infra/postgres/schema.sql"

log "Schema created."

# ── Step 6: Restore data to new Postgres ─────────────────────────────────────

log "Restoring data to new Postgres..."

# Use psql instead of pg_restore since we used pg_dump with plain format
psql "$NEW_POSTGRES_URI" < "$BACKUP_FILE"

log "Data restored."

# ── Step 7: Verify data ──────────────────────────────────────────────────────

log "Verifying data migration..."

echo ""
echo "Row counts on NEW database:"
psql "$NEW_POSTGRES_URI" -c "
SELECT 'groups' as table_name, COUNT(*) as rows FROM groups
UNION ALL
SELECT 'venues', COUNT(*) FROM venues
UNION ALL
SELECT 'events', COUNT(*) FROM events
UNION ALL
SELECT 'geocode_cache', COUNT(*) FROM geocode_cache
UNION ALL
SELECT 'scrape_runs', COUNT(*) FROM scrape_runs
UNION ALL
SELECT 'scrape_log', COUNT(*) FROM scrape_log
ORDER BY table_name;
"

echo ""
read -rp "Does the data look correct? (yes/no): " DATA_OK

if [[ "$DATA_OK" != "yes" ]]; then
    warn "Data verification failed. Please investigate manually."
    warn "Backup file is at: $BACKUP_FILE"
    exit 1
fi

# ── Step 8: Download Kafka certs ─────────────────────────────────────────────

log "Downloading Kafka certificates..."

CERTS_DIR="$REPO_ROOT/certs"
mkdir -p "$CERTS_DIR"

if [[ -n "$AIVEN_PROJECT" ]]; then
    avn service user-creds-download meetupmap-kafka \
        --project "$AIVEN_PROJECT" \
        --username avnadmin \
        -d "$CERTS_DIR"

    log "Certificates downloaded to $CERTS_DIR"
else
    warn "Could not download certs automatically. Download manually from Aiven Console:"
    warn "  Kafka service → Connection information → Download CA, Access cert, Access key"
    warn "  Save to: $CERTS_DIR/ca.pem, service.cert, service.key"
fi

# ── Step 9: Update .env file ─────────────────────────────────────────────────

log "Updating .env file..."

# Backup old .env
cp "$REPO_ROOT/.env" "$REPO_ROOT/.env.backup-$(date +%Y%m%d-%H%M%S)"

# Update values
sed -i.bak "s|^KAFKA_BOOTSTRAP_SERVERS=.*|KAFKA_BOOTSTRAP_SERVERS=$NEW_KAFKA_URI|" "$REPO_ROOT/.env"
sed -i.bak "s|^POSTGRES_URI=.*|POSTGRES_URI=$NEW_POSTGRES_URI|" "$REPO_ROOT/.env"
rm -f "$REPO_ROOT/.env.bak"

log ".env updated with new connection strings."

# ── Step 10: Summary ─────────────────────────────────────────────────────────

echo ""
echo "═══════════════════════════════════════════════════════════════════════════"
echo "  Migration complete!"
echo "═══════════════════════════════════════════════════════════════════════════"
echo ""
echo "New services:"
echo "  Kafka:    $NEW_KAFKA_URI"
echo "  Postgres: (see .env)"
echo "  VPC ID:   $VPC_ID"
echo ""
echo "Next steps:"
echo "  1. Test locally:  python -m worker.scraper"
echo "  2. Deploy to Aiven Apps via Console:"
echo "     - Go to console.aiven.io → your project → Applications → Deploy app"
echo "     - Connect GitHub repo: notanotherpizza/meetup-map"
echo "     - Select compose.aiven.yaml"
echo "     - Configure all services to use VPC: $VPC_ID"
echo "     - Deploy"
echo ""
echo "  3. After verifying Aiven Apps is working, decommission old services:"
echo "     - Fly.io: fly apps destroy meetupmap-worker"
echo "     - Old Kafka: (delete from dev-tier-testing project)"
echo "     - Old Postgres: (delete meetupmap-pg from hugh-one project)"
echo ""
echo "Backup file: $BACKUP_FILE"
echo "═══════════════════════════════════════════════════════════════════════════"
