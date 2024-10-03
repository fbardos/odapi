ARG MAMBAFORGE_VERSION=latest
FROM condaforge/mambaforge:${MAMBAFORGE_VERSION}

ARG TIMEZONE=UTC
ARG PROFILE_TYPE
ARG PROFILE_HOST
ARG PROFILE_USER
ARG PROFILE_PASSWORD
ARG PROFILE_PORT
ARG PROFILE_DBNAME
ARG PROFILE_SCHEMA
ARG PROFILE_THREADS
ARG PYTHON_VERSION

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=${TIMEZONE}

RUN mkdir /usr/src/app \
    && mkdir /usr/src/app/profiles
WORKDIR /usr/src/app
COPY profiles-template.yml profiles/profiles.yml
COPY environment.yml .

# Replace values in environment.yml
RUN sed -i "s/PYTHON_VERSION/${PYTHON_VERSION}/g" environment.yml

RUN apt update &&  apt install -y tzdata \
  && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
  && apt clean \
  && rm -rf /var/lib/apt/lists/*

# Updates base environment with the provided environment.yml
RUN conda env update --name base -f environment.yml --solver=libmamba \
  && conda clean -ay

HEALTHCHECK CMD dbt --version || exit 1

# Replace profiles.yml values
RUN sed -i "s/TEMPLATE_TYPE/${PROFILE_TYPE}/g" profiles/profiles.yml \
    && sed -i "s/TEMPLATE_HOST/${PROFILE_HOST}/g" profiles/profiles.yml \
    && sed -i "s/TEMPLATE_USER/${PROFILE_USER}/g" profiles/profiles.yml \
    && sed -i "s/TEMPLATE_PASSWORD/${PROFILE_PASSWORD}/g" profiles/profiles.yml \
    && sed -i "s/TEMPLATE_PORT/${PROFILE_PORT}/g" profiles/profiles.yml \
    && sed -i "s/TEMPLATE_DBNAME/${PROFILE_DBNAME}/g" profiles/profiles.yml \
    && sed -i "s/TEMPLATE_SCHEMA/${PROFILE_SCHEMA}/g" profiles/profiles.yml \
    && sed -i "s/TEMPLATE_THREADS/${PROFILE_THREADS}/g" profiles/profiles.yml

# Copy DBT project files to workdir
COPY . .

# Install dbt dependencies
RUN dbt deps --profiles-dir profiles

# Build documentation
RUN dbt docs generate --profiles-dir profiles

EXPOSE 80
