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

COPY web/pyproject.toml .
RUN uv sync --no-dev

COPY web/ .

USER django

CMD ["uv", "run", "uvicorn", "config.asgi:application", \
    "--host", "0.0.0.0", \
    "--port", "8000", \
    "--workers", "2"]
