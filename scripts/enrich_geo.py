import json
import requests
from pathlib import Path

OUT = Path(__file__).parent / 'data' / 'geo' / 'population.json'
URL = (
    'https://servicodados.ibge.gov.br/api/v3/agregados/6579'
    '/periodos/2024/variaveis/9324?localidades=N6[all]'
)

print("Fetching IBGE population estimates...")
data = requests.get(URL, timeout=120).json()

pop = {}
for series in data[0]['resultados'][0]['series']:
    code = int(series['localidade']['id'])
    year = max(series['serie'].keys())
    raw = series['serie'][year]
    digits = ''.join(c for c in raw if c.isdigit())
    if digits:
        pop[code] = int(digits)

records = [{"codigo_ibge": k, "populacao": v} for k, v in sorted(pop.items())]
print(f"Fetched {len(records)} cities")
OUT.write_text(json.dumps(records, ensure_ascii=False, indent=2))
print(f"Written to {OUT}")
