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
                "address": {
                    "type": ["string", "null"],
                    "description": "Street, facility, or local business address if applicable.",
                },
                "city": {
                    "type": ["string", "null"],
                    "description": "Brazilian city name, normalized to title case.",
                },
                "uf": {
                    "type": ["string", "null"],
                    "enum": [None, "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"],
                    "description": "Brazilian UF abbreviation or null.",
                },
                "confidence": {
                    "type": "string",
                    "enum": ["low", "medium", "high"],
                    "description": ("Confidence level of the location extraction (low, medium, or high). "
                                    "Use 'high' when all fields are confidently identified, "
                                    "'medium' when partial uncertainty exists, "
                                    "and 'low' when the result is mostly inferred or ambiguous."),
                },
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
                "content": (
                    "You are a location parser specialized in Brazilian geography. "
                    "Your task is to extract three fields from the input: address, city, and UF (Brazilian state abbreviation). "
                    "If the text refers to a local business, venue, public facility, or landmark (for example, a gym, school, or stadium), treat it as an address and identify the corresponding city. "
                    "If it looks like an event name (e.g., a race or competition), do not treat it as an address and leave all fields null. "
                    "When a city name is identified, always infer its corresponding UF (Brazilian state) using your knowledge of Brazil. "
                    "Do not leave UF empty if you know which state the city belongs to. If you are unsure, use the city size as proxy."
					"If you are not completely certain about the city–UF pair, set confidence to 'low' "
                    "Normalize all text to title case. Return null fields for anything that cannot be confidently determined."
                ),
            },
            {"role": "user", "content": location_raw},
        ],
        tools=[parse_location_tool],
        tool_choice={"type": "function", "function": {"name": "parse_location"}},
    )

    message = response.choices[0].message
    if not getattr(message, "tool_calls", None):
        return {"address": None, "city": None, "uf": None}

    tool_call = message.tool_calls[0]
    return json.loads(tool_call.function.arguments)


def normalize_daterange(date_raw: str):
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
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
