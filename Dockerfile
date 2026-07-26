FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ARG VCS_REF=unknown
LABEL org.opencontainers.image.source="https://github.com/wzsg/polymarket-raw-log-collector"
LABEL org.opencontainers.image.description="Concurrent Polygon raw log collector"
LABEL org.opencontainers.image.revision="${VCS_REF}"

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN uv sync --frozen --no-dev

ENTRYPOINT ["polymarket-raw-log-collector"]
CMD ["--help"]
