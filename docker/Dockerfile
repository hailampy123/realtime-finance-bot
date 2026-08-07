FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.4 /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY ingest/ ./ingest/
COPY config/ ./config/

ENV PATH="/app/.venv/bin:${PATH}"
CMD ["python", "-m", "ingest.cli"]
