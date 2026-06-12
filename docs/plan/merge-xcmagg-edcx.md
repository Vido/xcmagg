# Plan: Merge xcmagg + edcx into monorepo — racefeed.com.br

## Decisions

- **edcx Django** = main product, owns `/`, runs in Docker on server
- **xcmagg scraper** = data pipeline, runs on developer's local machine, never Dockerized
- **One git repo** (xcmagg), one deploy unit
- **DuckDB** = scraper's DB, local only, gitignored
- **SQLite3** = Django's DB, server + local, gitignored
- **Single `data/`** at repo root holds both DB files
- **Scraper runs weekly** on dev machine → commits updated `public/data.jsonl`
- **Static tool pages** (gearftp, fuelplan, gear-matrix) served by nginx directly — no Django involvement
- **User media/uploads** → Cloudflare R2, not local disk
- **No litestream yet** — revisit if SQLite backup becomes a concern

---

## Repo Structure

```
xcmagg/                        ← git repo
├── xcmagg/                    ← aggregator pipeline (uv, local only)
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   ├── main.py
│   ├── db.py
│   ├── agents.py
│   ├── aggregators.py
│   ├── cronos.py
│   ├── geo.py
│   ├── scripts/
│   └── pyproject.toml
├── web/                       ← edcx Django app
│   ├── config/
│   ├── catalog/
│   ├── profiles/
│   └── manage.py
├── public/                    ← static files (nginx serves directly)
│   ├── index.html
│   ├── gearftp.html
│   ├── fuelplan.html
│   ├── gear-matrix.html
│   ├── stem-comparison.html
│   ├── style.css
│   └── data.jsonl             ← scraper output, committed to git
├── data/                      ← gitignored, all DB files
│   ├── events.duckdb          ← scraper (local only)
│   └── db.sqlite3             ← Django (local + prod via bind mount)
├── infra/
│   ├── Dockerfile
│   ├── nginx.conf
│   └── docker-compose.yaml
└── deploy.sh
```

---

## Data Directory

Single `data/` at repo root. Gitignored.

**`.gitignore` addition:**
```
data/
```

**xcmagg `db.py`** — resolves from `xcmagg/`:
```python
BASE = Path(__file__).parent.parent / 'data'
self.CONN = duckdb.connect(str(BASE / 'events.duckdb'))
```

**Django settings** — `DATA_DIR` env var, defaults to repo root for local dev:
```python
# web/config/settings.py
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent  # = web/
DATA_DIR = Path(os.environ.get('DATA_DIR', BASE_DIR.parent / 'data'))

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': DATA_DIR / 'db.sqlite3',
    }
}
```

Local: `DATA_DIR` unset → `repo_root/data/db.sqlite3` ✓  
Docker: `DATA_DIR=/app/data` in compose env → `/app/data/db.sqlite3` ✓

---

## Nginx Config (lvido-proxy)

nginx lives in **lvido-proxy** container, not on the host directly. Configs in `lvido-proxy/user_conf.d/`. Proxy reaches backends via Docker networks using container names.

**Update `lvido-proxy/user_conf.d/racefeed_com_br.conf`:**

```nginx
server {
    listen 443 ssl;
    listen [::]:443 ssl;

    server_name racefeed.com.br;
    server_name www.racefeed.com.br;

    ssl_certificate         /etc/letsencrypt/live/racefeed_com_br/fullchain.pem;
    ssl_certificate_key     /etc/letsencrypt/live/racefeed_com_br/privkey.pem;
    ssl_trusted_certificate /etc/letsencrypt/live/racefeed_com_br/chain.pem;
    ssl_dhparam /etc/letsencrypt/dhparams/dhparam.pem;

    root /var/www/xcmagg;
    index index.html;

    access_log /var/log/nginx/racefeed_com_br/access.log;
    error_log  /var/log/nginx/racefeed_com_br/error.log error;

    gzip on;
    gzip_vary on;
    gzip_proxied any;
    gzip_comp_level 6;
    gzip_buffers 16 8k;
    gzip_http_version 1.1;
    gzip_min_length 256;
    gzip_types text/plain text/css application/json application/javascript application/x-javascript text/xml application/xml application/xml+rss text/javascript image/x-icon image/bmp image/svg+xml font/ttf font/opentype font/otf;

    # Static files — served directly from /var/www/xcmagg (proxy volume mount)
    location ~* \.(html|css|js|webp|png|svg|ico|jsonl|woff2?)$ {
        try_files $uri =404;
    }

    # Everything else → Django container via xcmagg_network
    location / {
        proxy_pass http://xcmagg-web-1:8000;
        include /etc/nginx/includes/proxy.conf;
    }
}
```

**Update `lvido-proxy/docker-compose.yml`** — add `xcmagg_network`:

```yaml
# add to networks section:
networks:
  ...
  xcmagg_network:
    external: true

# add to nginx-certbot service networks:
services:
  nginx-certbot:
    networks:
      - ...
      - xcmagg_network
```

**xcmagg `infra/docker-compose.yaml`** — use `xcmagg_network`:

```yaml
networks:
  xcmagg_network:
    name: xcmagg_network
```

**Deploy path for static files** — `public/` rsyncs to `/var/www/xcmagg/` on server (proxy already mounts this volume, no change needed).

---

## Docker (prod only)

Based on `edcx/docker-compose.yaml` + `edcx/infra/Dockerfile`, adapted for monorepo.

```yaml
# infra/docker-compose.yaml
version: "3.9"

services:
  web:
    build:
      context: ..
      dockerfile: infra/Dockerfile
    env_file:
      - .env
    environment:
      DATA_DIR: /app/data
    volumes:
      - /var/www/xcmagg/data:/app/data   # bind mount — data/ at repo root
    restart: unless-stopped
    networks:
      - xcmagg_network

  memcached:
    image: memcached:latest
    restart: unless-stopped
    networks:
      - xcmagg_network

networks:
  xcmagg_network:
    name: xcmagg_network
```

No port binding needed — proxy reaches container via Docker network (`xcmagg-web-1:8000`).

```dockerfile
# infra/Dockerfile
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN apt-get update && apt-get install -y \
    libsqlite3-0 \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m django

WORKDIR /app

COPY web/pyproject.toml web/uv.lock ./
RUN uv sync --frozen --no-dev

COPY web/ .

RUN uv run python manage.py collectstatic --noinput

USER django

CMD ["uv", "run", "uvicorn", "config.asgi:application", \
    "--host", "0.0.0.0", \
    "--port", "8000", \
    "--workers", "2"]
```

**Changes from edcx originals:**
- `pip` → `uv` throughout
- `COPY src/` → `COPY web/`
- `project.asgi` → `config.asgi` (bug fix)
- Named volume `sqlite_data` → bind mount `/var/www/xcmagg/data:/app/data`
- Port binding `127.0.0.1:8000:8000` added for nginx proxy
- `web/pyproject.toml` replaces `infra/requirements.txt` (migration needed)

---

## Local Dev (no Docker)

**xcmagg aggregator:**
```bash
cd xcmagg
uv run python main.py          # writes data/ and public/data.jsonl
```

**Django:**
```bash
cd web
uv sync
uv run python manage.py migrate
uv run python manage.py runserver
# http://localhost:8000
```

No env vars needed. Both find `data/` via relative paths.

---

## Scraper → Feed Workflow

1. Developer runs scraper locally (weekly, before weekend events)
2. Scraper writes `data/events.duckdb` + `public/data.jsonl`
3. `git add public/data.jsonl && git commit`
4. `deploy.sh` rsyncs repo to server
5. nginx serves updated `data.jsonl` immediately (static file)

---

## Deploy Steps (Ordered)

1. Restructure repo: move xcmagg pipeline files → `xcmagg/`, edcx files → `web/`
2. Convert `web/` from `infra/requirements.txt` → `web/pyproject.toml` + `uv sync`
3. Update `xcmagg/db.py` path (`parent.parent / 'data'`)
4. Update `xcmagg/gold.py` JSONL output path (`../public/data.jsonl`)
5. Add `DATA_DIR` to Django settings
6. Update `infra/Dockerfile` — uv, `COPY web/`, `config.asgi` fix
7. Update `infra/docker-compose.yaml` — bind mount, port binding, network rename
8. Write `infra/nginx.conf` with try_files + proxy
9. Update `deploy.sh` — single rsync for whole repo
10. Add `data/` to `.gitignore`
11. On server: create `xcmagg_network` (`docker network create xcmagg_network`)
12. On server: `docker compose up -d --build`
13. On server: `docker compose exec web uv run python manage.py migrate`
14. On server: `docker compose exec web uv run python manage.py collectstatic`
15. Update nginx on server, `nginx -s reload`

---

## SEO Safety

- All xcmagg URLs (`/`, `/gearftp.html`, `/data.jsonl`, etc.) served by nginx from `public/` — no change in behavior
- Django owns new routes only (accounts, catalog, community features)

---

## Future Considerations

- **DuckDB direct read from Django**: edcx view opens `events.duckdb` read-only for server-side filtering — activate when community features need event queries
- **Litestream → R2**: SQLite continuous replication if backup becomes a concern
- **Static tool pages → Django templates**: only if personalization/auth needed in tools
