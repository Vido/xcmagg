import json
from datetime import date
from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel
from openai import OpenAI
from decouple import config


client = OpenAI(api_key=config('OPENAI_API_KEY'))

parse_location_tool = {
    "type": "function",
    "function": {
        "name": "parse_location",
        "parameters": {
            "type": "object",
            "properties": {
                "address": {"type": ["string", "null"]},
                "city": {"type": ["string", "null"]},
                "uf": {
                    "type": ["string", "null"],
                    "enum": [
                        "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
                        "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
                        "RS","RO","RR","SC","SP","SE","TO"
                    ],
                },
                "confidence": {
                    "type": "string",
                    "enum": ["low", "high"]
                },
            },
        },
    },
}


def normalize_location(location_raw: str, model: str = "gpt-4.1-mini"):
    response = client.chat.completions.create(
        model=model,
        max_tokens=40,
        messages=[
            {
                "role": "system",
                "content": (
                    "Extract address, city, uf.\n"
                    "Rules:\n"
                    "- Event name → all null\n"
                    "- Place/venue → fill address\n"
                    "- Always infer uf from city (Brazil)\n"
                    "- If unsure city/uf → confidence=low\n"
                    "- ALL CAPS → Title Case\n"
                    "- all lowercase → Title Case\n"
                    "- Unknown → null"
                ),
            },
            {"role": "user", "content": location_raw},
        ],
        tools=[parse_location_tool],
        tool_choice={"type": "function", "function": {"name": "parse_location"}},
    )

    message = response.choices[0].message
    if not getattr(message, "tool_calls"):
        return {'address': None, 'city': None, 'uf': None, 'confidence': 'low'}

    print('normalize_location:', model, response.usage)
    return json.loads(message.tool_calls[0].function.arguments)



SPORTS = {
    # Ciclismo
    'Pedal':                'Ciclismo',
    'Ciclismo':             'Ciclismo',
    'Ciclismo de Estrada':  'Ciclismo',
    # Mountain bike
    'Mountain bike':        'Mountain bike',
    'Mountain Bike':        'Mountain bike',
    'MTB':                  'Mountain bike',
    'XCM':                  'Mountain bike',
    'XCO':                  'Mountain bike',
    # Triathlon
    'Cross Triathlon':      'Cross Triathlon',
    'X-Triathlon':          'Cross Triathlon',
    'Triathlon':            'Triathlon',
    'Triatlhon':            'Triathlon',
    'Triatlo':              'Triathlon',
    'Duathlon':             'Triathlon',
    'Duatlhon':             'Triathlon',
    # Natação:
    'Aquathon':             'Triathlon',
    'Natação':              'Natação',
    # Trail running
    'Trail running':        'Trail running',
    'Trail Run':            'Trail running',
    'Corrida Trail':        'Trail running',
    'Corrida de Montanha':  'Trail running',
    # Corrida de Rua
    'Corrida de Rua':       'Corrida de Rua',
    'Corrida':              'Corrida de Rua',
    # Cross Duathlon
    'Cross Duathlon':       'Cross Duathlon',
    'X-Duathlon':           'Cross Duathlon',
}

_CANONICAL_SPORTS = Enum('Sport', {v: v for v in dict.fromkeys(SPORTS.values())})


class _SportClassification(BaseModel):
    sport: Optional[str] = None
    confidence: Literal['low', 'high'] = 'low'

def classify_sport(content: str, model: str = 'gpt-5.4-mini') -> _SportClassification:
    model = 'gpt-5.4-mini'
    resp = client.beta.chat.completions.parse(
        model=model,
        temperature=0,
        #max_tokens=20,
        messages=[
            {
                "role": "system",
                "content": (
                    "Classify sport.\n"
                    f"Classes: {', '.join(e.value for e in _CANONICAL_SPORTS)}\n"
                    "Unknown → null\n"
                    "Uncertain → confidence=low"
                ),
            },
            {"role": "user", "content": content},
        ],
        response_format=_SportClassification,
    )

    print('classify_sport:', model, resp.usage)
    return resp.choices[0].message.parsed


def search_classify_sport(title: str, url: str) -> _SportClassification:
    # Step 1: web search — title and url only
    response = client.chat.completions.create(
        model="gpt-4o-mini-search-preview",
        messages=[{"role": "user", "content": f"{title} {url}"}],
    )
    print(response.usage)
    context = response.choices[0].message.content or ""

    # Step 2: structured classification with search context piped in
    return classify_sport(f"{title} — {url}\n\nContexto: {context}", url="", model="gpt-4.1-mini")


def search_event_location(event_title: str) -> str:
    response = client.chat.completions.create(
        model="gpt-4o-mini-search-preview",
        messages=[{
            "role": "user",
            "content": (
                f"Em qual cidade e estado brasileiro ocorre o evento esportivo '{event_title}'? "
                "Informe cidade, UF e, se possível, o local exato. Responda de forma concisa."
            )
        }]
    )
    print('search_event_location', model,response.usage)
    return response.choices[0].message.content or ""


class DateRange(BaseModel):
    multi_day: Optional[bool] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None


def normalize_daterange(date_raw: str, model: str = "gpt-4.1-nano"):
    resp = client.beta.chat.completions.parse(
        model=model,
        temperature=0,
        max_tokens=30,
        messages=[
            {
                "role": "system",
                "content": (
                    "Extract date(s) → YYYY-MM-DD.\n"
                    "Rules:\n"
                    "- single day → multi_day=false\n"
                    "- range → set start_date + end_date\n"
                    "- missing/invalid → all null\n"
                    "- ignore time"
                ),
            },
            {"role": "user", "content": date_raw},
        ],
        response_format=DateRange,
    )
    print('normalize_daterange:', model, resp.usage)
    return resp.choices[0].message.parsed

if __name__ == '__main__':

    locations = [
        "PENTÁUREA CLUBE MONTES CLAROS",
        "Colégio Anglo Salto, Avenida Brasília, 936, Jardim D'Icaraí - Salto / SP",
        "Parque estadual Serra de Jaraguá - S/N - Parque estadual Serra de Jaraguá , Zona Rural, Jaraguá - GO",
        "CAMARI - PORK ANGUS",
        "CURVELO/MG",
        "Local: Mairiporã/SP",
        "A DEFINIR",
        "Poliesportivo - Vereador Adilson Martins",
        "Parque do Povo: Marginal Pinheiros (via expressa) , São Paulo, SP, Brasil",
        "Evento: DUATHLON SÃO JOAQUIM DA BARRA Local: Poliesportivo - Vereador Adilson Martins",
        "Evento: 2026 Itaú BBA IRONMAN 70.3 Rio de Janeiro Local:",
        "Evento: DESAFIO BRUTTUS Local: Parque dos Namorados",
    ]

    for location_raw in locations:
        parsed = normalize_location(location_raw)
        print(location_raw, parsed)

    """
    dates = [
        "01/11/2025 - 07:00",
        "09 de Novembro de 2025",
        "30 - Domingo",
        "Data: 16/11/2025 até 16/11/2025",
        "15 e 16/11/2025",
        "2026-05-01T08:30:00"
        "Evento: 30/11/2025 BBF Race MTB - Santo Antonio de Posse SP 30 - Domingo",
        "Evento: 08 e 09 Novembro RIDE BIKE 2025 <96> MARATONA XCM Monte Sião MG 17 - Sábado"
    ]

    for date_raw in dates:
        parsed = normalize_daterange(date_raw)
        print(parsed, date_raw)
    """
