FROM ghcr.io/astral-sh/uv:python3.12-bookworm

ARG GIT_CLONE_ROOT=/tmp/odapi
ARG GIT_CLONE_USER=fbardos
ARG DBT__ABSOLUTE_PATH=/opt/dbt
ENV DBT__ABSOLUTE_PATH=${DBT__ABSOLUTE_PATH}
ARG DBT__PROFILES_PATH=/root/.dbt
ENV DBT__PROFILES_PATH=${DBT__PROFILES_PATH}
ENV DAGSTER_HOME=/opt/dagster/app

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app /root/.dbt
COPY ./docker/dagster/dagster.yaml /opt/dagster/app/
COPY ./docker/.deploy/.dbt/profiles.yml /root/.dbt/

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    wget \
    gdal-bin \
    libgdal-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Load files
COPY ./docker/.deploy/.env /opt/dagster/app/

ARG CACHEBUST=1

RUN --mount=type=secret,id=git_token,target=/run/secrets/git_token \
    mkdir /tmp/odapi \
    && git clone https://${GIT_CLONE_USER}:$(cat /run/secrets/git_token)@github.com/fbardos/odapi.git ${GIT_CLONE_ROOT} \
    && cp -R ${GIT_CLONE_ROOT}/dag/. ${DAGSTER_HOME} \
    && cp -R ${GIT_CLONE_ROOT}/models/. /opt/dbt \
    && cp -R ${GIT_CLONE_ROOT}/uv.lock ${DAGSTER_HOME} \
    && cp -R ${GIT_CLONE_ROOT}/pyproject.toml ${DAGSTER_HOME}

# I don't know why, but dagster-dbt needs a manifest.json file (will be overwritten),
# during prepare-and-package. An empty file is not enough, but only checks for the
# first level keys and will them fill during prepare-and-package.
COPY ./docker/.deploy/manifest.json /opt/dbt/target/

# Overwrite dagster.yaml again, will otherise not use psql/minio
COPY ./docker/dagster/dagster.yaml /opt/dagster/app/

COPY ./docker/.deploy/.env /opt/dagster/app

WORKDIR ${DAGSTER_HOME}
RUN uv venv .venv && \
    uv sync --frozen --all-groups  # read from uv.lock
RUN uv pip install --no-deps .
ENV PATH="${DAGSTER_HOME}/.venv/bin:${PATH}"
RUN dagster-dbt project prepare-and-package --file ${DAGSTER_HOME}/odapi/assets/dbt.py

# Load DBT deps
RUN dbt deps --project-dir /opt/dbt

EXPOSE 3000
ENTRYPOINT ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-f", "odapi/definitions.py"]
