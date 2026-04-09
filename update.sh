#!/usr/bin/env bash

source .env
uv run main.py
cp data/gold/data.jsonl public/data.jsonl

TODAY=$(date -u +%Y-%m-%d)
sed -i "s|<lastmod>.*</lastmod>|<lastmod>${TODAY}</lastmod>|g" public/sitemap.xml

scp public/* root@164.92.148.125:/var/www/xcmagg/

