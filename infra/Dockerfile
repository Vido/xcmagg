FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    libsqlite3-0 \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m django

WORKDIR /app

COPY web/pyproject.toml web/uv.lock ./
RUN uv sync --no-dev --frozen

COPY web/ .

USER django

CMD ["uv", "run", "uvicorn", "config.asgi:application", \
    "--host", "0.0.0.0", \
    "--port", "8000", \
    "--workers", "2", \
    "--proxy-headers", \
    "--forwarded-allow-ips", "172.16.0.0/12", \
    "--lifespan", "off"]
