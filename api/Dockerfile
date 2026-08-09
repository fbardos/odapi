FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# System dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
        git \
        build-essential \
        gdal-bin \
        libgdal-dev \
        gnupg \
        libgpgme-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency definitions first so Docker can cache this layer
COPY pyproject.toml uv.lock ./

# Install dependencies, but not the project itself yet
RUN uv sync \
    --frozen \
    --no-dev \
    --no-install-project

# Copy application
COPY api/ .

# Install the project itself
RUN uv sync \
    --frozen \
    --no-dev

EXPOSE 80

CMD ["/app/.venv/bin/uvicorn", "odapi_api:app", "--host", "0.0.0.0", "--port", "80", "--workers", "4"]
