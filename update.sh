#!/usr/bin/env bash

source .env
uv run main.py
cp data/gold/data.jsonl public/data.jsonl 
scp public/* root@lvido.tech:/var/www/xcmagg/

