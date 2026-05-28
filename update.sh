#!/usr/bin/env bash

source .env

deploy() {
    cp data/gold/data.jsonl public/data.jsonl

    TODAY=$(date -u +%Y-%m-%d)
    sed -i "s|<lastmod>.*</lastmod>|<lastmod>${TODAY}</lastmod>|g" public/sitemap.xml

    scp public/* root@164.92.148.125:/var/www/xcmagg/

    sed "s|fetch('data.jsonl')|fetch('https://racefeed.com.br/data.jsonl')|" public/index.html > /tmp/index_prod.html
    scp /tmp/index_prod.html root@164.92.148.125:/var/www/xcmagg/index.html
    rm /tmp/index_prod.html
}

if [[ "$1" == "--deploy" ]]; then
    deploy
else
    uv run main.py
    deploy
fi


