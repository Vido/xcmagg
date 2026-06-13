# xcmagg

## Environment

Use `uv run python` for all Python commands in this project. Do not use `python` or `python3` directly.

```bash
uv run python script.py
uv run python -c "..."
```

## Django Settings

The `web/` Django app uses split settings under `web/config/`:

- `base_settings.py` — shared base (INSTALLED_APPS, `DATA_DIR`, etc.). Imported by the others via `from .base_settings import *`.
- `dev_settings.py` — **local default**. `DEBUG=True`, `ALLOWED_HOSTS=['*']`, LocMemCache, console email, placeholder `SECRET_KEY` + Turnstile test keys. Needs no real secrets to run.
- `prod_settings.py` — production. `DEBUG=False`, real secrets required (fail-fast — no defaults), PyMemcacheCache (`MEMCACHED_LOCATION` defaults to the compose `memcached:11211`), Postmark email.

Selection: `manage.py`, `config/asgi.py`, `config/wsgi.py` all `setdefault('DJANGO_SETTINGS_MODULE', 'config.dev_settings')` → **dev is the default locally**. Prod is opt-in via `DJANGO_SETTINGS_MODULE=config.prod_settings`, set in `infra/docker-compose.yaml`.

Env vars are read with python-decouple `config()` — see `web/.env.example` for the var list and defaults. Prod env file: `web/.env` (gitignored, **independent from the scraper's `xcmagg/.env`**), loaded into the web container via `env_file: ../web/.env` in compose. No `export ` prefixes — docker `env_file` does not parse them. `infra/deploy.sh` excludes `.env` from rsync; place it on the server manually once.

## Infrastructure

### Reverse Proxy

nginx runs inside **lvido-proxy** Docker container (image: `jonasal/nginx-certbot`), not bare on the host.

- Configs: `lvido-proxy/user_conf.d/*.conf` (deployed to `/etc/nginx/user_conf.d/` inside container)
- Shared proxy headers: `lvido-proxy/etc/nginx/includes/proxy.conf` — use `include /etc/nginx/includes/proxy.conf;`
- Backends reached by **Docker container name**, not `127.0.0.1:port` (e.g. `proxy_pass http://xcmagg-web-1:8000`)
- Proxy joins backend Docker networks to reach containers — each backend network must be added to both `lvido-proxy/docker-compose.yml` and the backend's `docker-compose.yaml`
- Static files served via volume mounts into proxy container (e.g. `/var/www/xcmagg` already mounted)

### Networks

Pattern: each app creates its own named Docker network; lvido-proxy joins it.

```yaml
# backend docker-compose.yaml
networks:
  racefeed_network:
    name: racefeed_network

# lvido-proxy/docker-compose.yml — add to nginx-certbot networks + networks section
networks:
  racefeed_network:
    external: true
```

Container name format: `<compose-project-dir>-<service>-1` (e.g. `xcmagg-web-1` for service `web` in dir `xcmagg/`).

### Data Storage

Django SQLite DB + media live on a **host bind mount**: `/var/www/xcmagg/data` on the server → `/data` inside the container. `DATA_DIR=/data` env var points Django there, so the actual files (`db.sqlite3`, `uploads/`) are plainly accessible on the host FS at `/var/www/xcmagg/data/`.

The container runs as non-root user `django` (uid 1000), so the host dir must be owned by uid 1000:
```bash
sudo mkdir -p /var/www/xcmagg/data
sudo chown -R 1000:1000 /var/www/xcmagg/data
```
