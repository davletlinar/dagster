FROM python:3.12-slim

WORKDIR /tmp

RUN pip install \
    dagster \
    dagster-pipes \
    dagster-graphql \
    dagster-webserver \
    dagster-postgres \
    dagster-docker \
    pytest

COPY requirements.txt .

RUN pip install -r requirements.txt

ENV DAGSTER_HOME=/opt/dagster/app/
ENV DAGSTER_POSTGRES_HOST=postgres
ENV DAGSTER_POSTGRES_PORT=5432
ENV DAGSTER_POSTGRES_USER=${DAGSTER_POSTGRES_USER}
ENV DAGSTER_POSTGRES_PASSWORD=${DAGSTER_POSTGRES_PASSWORD}
ENV DAGSTER_POSTGRES_DB=${DAGSTER_POSTGRES_DB}
ENV ROLE_API_USERNAME=${ROLE_API_USERNAME}
ENV ROLE_API_PASSWORD=${ROLE_API_PASSWORD}
ENV ROLE_POSTGRES_DB=${ROLE_POSTGRES_DB}

RUN mkdir -p $DAGSTER_HOME
RUN mkdir -p $DAGSTER_HOME/repos

COPY dagster.yaml workspace.yaml $DAGSTER_HOME

WORKDIR $DAGSTER_HOME
