FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    libsqlite3-0 \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m django

WORKDIR /app

COPY infra/requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy project
COPY src/ .

USER django

CMD ["uvicorn", "project.asgi:application", \
    # "--config", "uvicorn.conf.py", \
    "--host", "0.0.0.0", \
    "--port", "8000", \
    "--workers", "2"]
