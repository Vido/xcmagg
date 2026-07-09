#!/usr/bin/env bash

set -euo pipefail
set -o xtrace

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
SERVER=root@164.92.148.125
REMOTE_DIR=/var/www/xcmagg

rsync -avz --delete \
  --exclude '__pycache__' \
  --exclude '.venv' \
  --exclude '*.pyc' \
  --exclude '.env' \
  --exclude 'docker-compose.override.yaml' \
  "$REPO_ROOT/web" "$REPO_ROOT/infra" \
  "$SERVER:$REMOTE_DIR/"

ssh "$SERVER" "cd $REMOTE_DIR && docker compose -f infra/docker-compose.yaml up --build -d"

# Optional DB migration: ./infra/deploy.sh --migrate
if [[ "${1:-}" == "--migrate" ]]; then
  ssh "$SERVER" "cd $REMOTE_DIR && docker compose -f infra/docker-compose.yaml exec -T web uv run python manage.py migrate"
fi

# Optional seed categories: ./infra/deploy.sh --categories
if [[ "${1:-}" == "--categories" ]]; then
  ssh "$SERVER" "cd $REMOTE_DIR && docker compose -f infra/docker-compose.yaml exec -T web uv run python manage.py seed_categories"
fi
