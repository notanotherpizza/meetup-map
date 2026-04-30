#!/bin/bash
set -e

case "${APP_MODE}" in
  discovery)
    exec python -m discovery.worker
    ;;
  worker)
    exec python -m worker.scraper
    ;;
  sink)
    exec python -m sink.consumer
    ;;
  *)
    echo "ERROR: APP_MODE must be set to 'discovery', 'worker', or 'sink'"
    exit 1
    ;;
esac
