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
        swig \
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
    --no-install-project \
    --no-build-isolation-package gpg

# Copy application
COPY api/ .
COPY dag/ /tmp/odapi
COPY pyproject.toml /tmp/odapi

# Install the project itself
RUN uv pip install --no-deps /tmp/odapi

ENV VIRTUAL_ENV=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 80

CMD ["uvicorn", "odapi_api:app", "--host", "0.0.0.0", "--port", "80", "--workers", "4"]
