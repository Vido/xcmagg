# xcmagg

## Environment

Use `uv run python` for all Python commands in this project. Do not use `python` or `python3` directly.

```bash
uv run python script.py
uv run python -c "..."
```

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
  xcmagg_network:
    name: xcmagg_network

# lvido-proxy/docker-compose.yml — add to nginx-certbot networks + networks section
networks:
  xcmagg_network:
    external: true
```

Container name format: `<compose-project-dir>-<service>-1` (e.g. `xcmagg-web-1` for service `web` in dir `xcmagg/`).
