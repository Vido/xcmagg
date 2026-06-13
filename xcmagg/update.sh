#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
DATA_DIR="$REPO_ROOT/data"
PUBLIC_DIR="$REPO_ROOT/public"

source "$SCRIPT_DIR/.env"

deploy() {
    cp "$DATA_DIR/gold/data.jsonl" "$PUBLIC_DIR/data.jsonl"

    TODAY=$(date -u +%Y-%m-%d)
    sed -i "s|<lastmod>.*</lastmod>|<lastmod>${TODAY}</lastmod>|g" "$PUBLIC_DIR/sitemap.xml"

    scp "$PUBLIC_DIR"/* root@164.92.148.125:/var/www/xcmagg/public/

    sed "s|fetch('data.jsonl')|fetch('https://racefeed.com.br/data.jsonl')|" "$PUBLIC_DIR/index.html" > /tmp/index_prod.html
    scp /tmp/index_prod.html root@164.92.148.125:/var/www/xcmagg/public/index.html
    rm /tmp/index_prod.html
}

if [[ "$1" == "--deploy" ]]; then
    deploy
else
    cd "$SCRIPT_DIR" && uv run main.py
    deploy
fi


