#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
DATA_DIR="$REPO_ROOT/data"
PUBLIC_DIR="$REPO_ROOT/public"

source "$SCRIPT_DIR/.env"

deploy() {
    cp "$DATA_DIR/gold/data.jsonl" "$PUBLIC_DIR/data.jsonl"

    # sitemap.xml is now generated dynamically by Django at /sitemap.xml — no longer
    # a static file; the scraper publish must not touch it.

    scp -r "$PUBLIC_DIR"/* root@164.92.148.125:/var/www/xcmagg/public/
    scp "$DATA_DIR/events.duckdb" root@164.92.148.125:/var/www/xcmagg/data/events.duckdb
}

if [[ "$1" == "--deploy" ]]; then
    deploy
else
    cd "$SCRIPT_DIR" && uv run main.py
    deploy
fi


