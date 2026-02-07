FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock README.md ./
COPY plutus_bench ./plutus_bench

RUN pip install --no-cache-dir uv && \
    uv sync --frozen --no-dev

EXPOSE 8000

ENV MOCKFROST_SESSION_DATABASE_NAME=/data/SESSIONS.db

VOLUME ["/data"]

CMD ["uv", "run", "uvicorn", "plutus_bench.mockfrost.server:app", "--host", "0.0.0.0", "--port", "8000"]
