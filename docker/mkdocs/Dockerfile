FROM ghcr.io/astral-sh/uv:python3.12-alpine AS builder

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY doc/pyproject.toml ./doc/pyproject.toml

RUN uv sync \
    --frozen \
    --no-dev \
    --package odapi-doc

COPY doc/ ./doc/

RUN uv run \
    --frozen \
    --no-sync \
    --package odapi-doc \
    mkdocs build \
    --strict \
    --clean \
    --config-file doc/mkdocs.yml \
    --site-dir /app/site


FROM nginx:stable-alpine

COPY --from=builder /app/site/ /usr/share/nginx/html/

EXPOSE 80
