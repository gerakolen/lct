FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY pyproject.toml ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev --no-editable

FROM python:3.11-slim-bookworm AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

RUN mkdir -p /app

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

COPY app/ ./app/
COPY pyproject.toml ./

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PORT=8998

EXPOSE 8998
