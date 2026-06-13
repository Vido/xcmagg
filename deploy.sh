#!/usr/bin/env bash

set -euo pipefail
set -o xtrace

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVER=root@164.92.148.125
REMOTE_DIR=/var/www/xcmagg

rsync -avz --delete \
  --exclude '__pycache__' \
  --exclude '.venv' \
  --exclude '*.pyc' \
  "$SCRIPT_DIR/web" "$SCRIPT_DIR/infra" \
  "$SERVER:$REMOTE_DIR/"

ssh "$SERVER" "cd $REMOTE_DIR && docker compose -f infra/docker-compose.yaml up --build -d"
