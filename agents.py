import json
from datetime import date

from openai import OpenAI
from decouple import config


client = OpenAI(api_key=config('OPENAI_API_KEY'))

parse_location_tool = {
    "type": "function",
    "function": {
        "name": "parse_location",
        "description": "Extract address, city, and UF (brazilian state).",
        "parameters": {
            "type": "object",
            "properties": {
                "address": {"type": ["string", "null"]},
                "city": {"type": ["string", "null"]},
                "uf": {"type": ["string", "null"]},
            },
        },
    },
}

parse_daterange_tools = {
    "type": "function",
        "function": {
            "name": "parse_daterange",
            "description": (
                "Parse an unstructured date or datetime string. "
                "Extract normalized start. Identify if it's multi-day, if so extract also normalized end date. "
                "All dates should be ISO format (YYYY-MM-DD). The input usualy in the brazilian convention: "
                "Day First, Month Second."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "multi_day": {"type": ["boolean", "null"], "description": "True if the event spans multiple days"},
                    "start_date": {"type": ["string", "null"], "description": "Start date in ISO format (YYYY-MM-DD)"},
                    "end_date": {"type": ["string", "null"], "description": "End date in ISO format (YYYY-MM-DD)"},
            },
        },
    },
}

def normalize_location(location_raw: str):
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": "You are a location parser. Extract address, city, and UF (brazilian state) if possible."
                "The input might be a local businnes or a city facility (like a polisportive stadium), consider this as address and try to find the brazilian city"
                "If the input looks like an sport event - do not consider it as address"
                "If this input is not an address - just the fields empty fields. Normalize the case-fold to title-case in all fields."
                ,
            },
            {"role": "user", "content": location_raw},],
        tools=[parse_location_tool],
        tool_choice={"type": "function", "function": {"name": "parse_location"}},
    )

    tool_call = response.choices[0].message.tool_calls[0]
    return json.loads(tool_call.function.arguments)


def normalize_daterange(date_raw: str):
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a date parser. Extract the date from natural-language "
                    "or formatted date expressions. Always output ISO 8601 dates (YYYY-MM-DD). "
                    "If the text refers to only one day, set multi_day=false and end_date to null. "
                    "If the input has no valid date (or missing mouth or year), return null for all fields."
                    "Ignore time of the day or timezone information."
                ),
            },
            {"role": "user", "content": date_raw},
        ],
        tools=[parse_daterange_tools],
        tool_choice={"type": "function", "function": {"name": "parse_daterange"}},
    )

    tool_call = response.choices[0].message.tool_calls[0]
    args = json.loads(tool_call.function.arguments)

    to_date = lambda s: date.fromisoformat(s) if s else None
    return {
        'multi_day': args.get('multi_day'),
        'start_date': to_date(args.get('start_date')),
        'end_date': to_date(args.get('end_date')),
    }

if __name__ == '__main__':

    """
    locations = [
        "PENTÁUREA CLUBE MONTES CLAROS",
        "Colégio Anglo Salto, Avenida Brasília, 936, Jardim D'Icaraí - Salto / SP",
        "Parque estadual Serra de Jaraguá - S/N - Parque estadual Serra de Jaraguá , Zona Rural, Jaraguá - GO",
        "CAMARI - PORK ANGUS",
        "EXTREMA/MG",
        "Local: Mairiporã/SP",
        "A DEFINIR",
        "Poliesportivo - Vereador Adilson Martins",
        "Parque do Povo: Marginal Pinheiros (via expressa) , São Paulo, SP, Brasil"
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
