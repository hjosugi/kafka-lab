#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
action="${1:-demo}"
case "$action" in
  test)
    PYTHONPATH=src python3 -m unittest discover -s tests -v
    ;;
  offline)
    PYTHONPATH=src python3 -m streaming_lab.cli offline
    ;;
  up)
    docker compose up -d kafka postgres risingwave
    ;;
  demo)
    docker compose up -d kafka postgres risingwave
    docker compose --profile tools run --rm --build lab demo
    ;;
  init|events|publish|consume|replay|verify|query)
    docker compose --profile tools run --rm lab "$action"
    ;;
  recovery)
    docker compose --profile tools run --rm --entrypoint python lab scripts/recovery.py
    ;;
  down)
    docker compose down
    ;;
  reset)
    if [[ "${2:-}" != "--delete-lab-data" ]]; then
      echo 'Reset deletes this lab project volumes. Run: bash scripts/lab.sh reset --delete-lab-data' >&2
      exit 2
    fi
    docker compose down -v
    ;;
  *)
    echo 'Usage: lab.sh {test|offline|up|demo|init|events|publish|consume|replay|verify|query|recovery|down|reset}' >&2
    exit 2
    ;;
esac
