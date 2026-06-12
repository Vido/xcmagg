import json
from functools import lru_cache
from pathlib import Path

DB_PATH = Path(__file__).parent / 'data' / 'geo' / 'municipios_ibge.json'

@lru_cache(maxsize=1)
def _load_db() -> dict:
    entries = json.loads(DB_PATH.read_text())
    return {
        (e['nome'].strip().lower(), e['uf'].upper()): str(e['ddd'])
        for e in entries
    }

def lookup_ddd(city: str, uf: str) -> str | None:
    if not city or not uf:
        return None
    return _load_db().get((city.strip().lower(), uf.strip().upper()))
