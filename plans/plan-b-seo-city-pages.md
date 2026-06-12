# Plan B: SEO City Landing Pages

## Context

RaceFeed serves a single static `index.html`. Goal: city-specific landing pages (e.g. `/cidade/sao-paulo-sp/`) with full meta tags (title, og:*, twitter:*, canonical) for social sharing and crawlers — without generating 100s of HTML files.

Population data from Plan A (`data/geo/population.json`) enables filtering to major cities only.

## Architecture

Flask micro-server handles only `/cidade/<slug>` routes. Reads `index.html` + `data.jsonl` at startup, patches meta tags per-request. Nginx serves everything else as static files, proxies only `/cidade/` to Flask. Gunicorn runs Flask in production.

Zero generated HTML files. One Python file + one nginx block + one systemd unit.

## Files

### 1. `city_server.py` (new)

```python
from flask import Flask, abort
import json, re, unicodedata
from pathlib import Path

app = Flask(__name__)
WEBROOT = Path('/var/www/xcmagg')

def slugify(text):
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    return re.sub(r'[^a-z0-9]+', '-', text.lower()).strip('-')

def load_cities():
    cities = {}
    for line in (WEBROOT / 'data.jsonl').read_text().splitlines():
        e = json.loads(line)
        if not e.get('latitude'): continue
        slug = slugify(e['city']) + '-' + e['uf'].lower()
        cities.setdefault(slug, e)
    return cities

TEMPLATE = (WEBROOT / 'index.html').read_text()
CITIES = load_cities()

@app.route('/cidade/<slug>/')
@app.route('/cidade/<slug>')
def city_page(slug):
    city = CITIES.get(slug)
    if not city:
        abort(404)
    return patch(TEMPLATE, city, slug)

def patch(html, c, slug):
    title = f"Ciclismo em {c['city']} - {c['uf']} | RaceFeed"
    desc  = f"Calendário de provas de ciclismo e MTB em {c['city']}, {c['uf']}."
    canon = f"https://racefeed.com.br/cidade/{slug}/"
    inject = f"<script>window.CITY_LAT={c['latitude']};window.CITY_LON={c['longitude']};</script>"
    html = re.sub(r'<title>.*?</title>', f'<title>{title}</title>', html)
    html = re.sub(r'(<meta name="description" content=")[^"]*(")', rf'\g<1>{desc}\2', html)
    html = re.sub(r'(<link rel="canonical" href=")[^"]*(")', rf'\g<1>{canon}\2', html)
    html = re.sub(r'(og:title" content=")[^"]*(")', rf'\g<1>{title}\2', html)
    html = re.sub(r'(og:url" content=")[^"]*(")', rf'\g<1>{canon}\2', html)
    html = re.sub(r'(twitter:title" content=")[^"]*(")', rf'\g<1>{title}\2', html)
    html = html.replace('</head>', inject + '\n</head>', 1)
    return html
```

### 2. `pyproject.toml` (modify)

Add:
```
"flask>=3.1.0",
"gunicorn>=23.0.0",
```

### 3. `public/index.html` JS (modify)

At top of `<script>` block:
```js
const CITY_LAT = window.CITY_LAT ?? null;
const CITY_LON = window.CITY_LON ?? null;
```

After `applyFilters()` inside fetch `.then`:
```js
if (CITY_LAT !== null) {
    userLat = CITY_LAT; userLon = CITY_LON;
    calcDistances();
    nearMeActive = true;
    document.getElementById('sortSelect').value = 'distance';
    applyFilters();
}
```

### 4. Systemd unit on server: `/etc/systemd/system/city-server.service`

```ini
[Unit]
Description=RaceFeed city pages
After=network.target

[Service]
WorkingDirectory=/var/www/xcmagg
ExecStart=/usr/local/bin/gunicorn -w 2 -b 127.0.0.1:8081 city_server:app
Restart=always

[Install]
WantedBy=multi-user.target
```

### 5. Nginx (add to existing server block)

```nginx
location /cidade/ {
    proxy_pass http://127.0.0.1:8081;
    proxy_set_header Host $host;
}
```

### 6. `update.sh` (modify)

```bash
scp city_server.py root@164.92.148.125:/var/www/xcmagg/
ssh root@164.92.148.125 "systemctl restart city-server"
```

### 7. Sitemap

Generate city URLs in `update.sh` or small script — query distinct slugs from `data.jsonl`, append to `sitemap.xml`.

## City selection

Only cities with actual upcoming events in `data.jsonl` get a slug. Flask returns 404 for unknown slugs. Population data (Plan A) available for future ranking/prioritization.

## Request flow

```
GET /cidade/sao-paulo-sp/
  → nginx proxy_pass → gunicorn → Flask
  → CITIES['sao-paulo-sp'] → patch(index.html) → response with city meta tags
  → browser loads page → JS reads window.CITY_LAT/LON → auto near-me filter
  → fetch('/data.jsonl') → haversine filter → shows SP events
```

## Verification

1. `uv run flask --app city_server run` locally → `curl localhost:5000/cidade/sao-paulo-sp/ | grep title`
2. Verify og:title in source
3. After deploy: `curl https://racefeed.com.br/cidade/sao-paulo-sp/ | grep title`
4. Facebook Sharing Debugger → verify og:title shows city name
