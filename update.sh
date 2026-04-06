#!/usr/bin/env bash

source .env
uv run main.py
cp data/gold/data.jsonl public/data.jsonl 
scp public/* root@164.92.148.125:/var/www/xcmagg/

