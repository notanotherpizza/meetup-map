#!/bin/bash
set -e

case "${APP_MODE}" in
  batch)
    exec python -m batch-worker.run
    ;;
  batch-discover-only)
    exec python -m batch-worker.run --discover-only
    ;;
  batch-scrape-only)
    exec python -m batch-worker.run --scrape-only
    ;;
  scraper)
    exec python -m worker.scraper
    ;;
  *)
    echo "ERROR: APP_MODE must be one of: batch, batch-discover-only, batch-scrape-only, scraper"
    exit 1
    ;;
esac
