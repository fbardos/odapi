FROM python:3.10-slim

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

# Install miniconda
ENV CONDA_DIR=/opt/conda
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-py310_24.7.1-0-Linux-x86_64.sh -O ~/miniconda.sh \
    && /bin/bash ~/miniconda.sh -b -p $CONDA_DIR \
    && rm ~/miniconda.sh
ENV PATH=$CONDA_DIR/bin:$PATH

# Must be placed after conda install, otherwise, will load wrong version
COPY ./environment.yml /tmp/environment.yml
RUN conda env create --solver libmamba -f /tmp/environment.yml && conda clean -ay

ARG CACHEBUST=1

RUN --mount=type=secret,id=git_token,target=/run/secrets/git_token \
    mkdir /tmp/odapi \
    && git clone https://${GIT_CLONE_USER}:$(cat /run/secrets/git_token)@github.com/fbardos/odapi.git ${GIT_CLONE_ROOT} \
    && cp -R ${GIT_CLONE_ROOT}/dag/. /opt/dagster/app \
    && cp -R ${GIT_CLONE_ROOT}/models/. /opt/dbt

# I don't know why, but dagster-dbt needs a manifest.json file (will be overwritten),
# during prepare-and-package. An empty file is not enough, but only checks for the
# first level keys and will them fill during prepare-and-package.
COPY ./docker/.deploy/manifest.json /opt/dbt/target/

# Overwrite dagster.yaml again, will otherise not use psql/minio
COPY ./docker/dagster/dagster.yaml /opt/dagster/app/

# Change shell
SHELL ["conda", "run", "-n", "odapi", "/bin/bash", "-c"]
RUN pip install --no-dependencies /opt/dagster/app

COPY ./docker/.deploy/.env /opt/dagster/app
ENV PROJ_LIB=/opt/conda/share/proj

# Load DBT deps
RUN dbt deps --project-dir /opt/dbt

WORKDIR ${DAGSTER_HOME}
RUN dagster-dbt project prepare-and-package --file ${DAGSTER_HOME}/odapi/assets/dbt.py

EXPOSE 3000
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "odapi", "dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]
