FROM python:3.9-slim

ENV DAGSTER_VERSION="0.15.6"
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV DBT_PROFILES_DIR=/opt/dagster/app
ENV DBT_PROJECT_DIR=/opt/dagster/app/dbt
ENV PYTHONPATH=/opt/dagster/app/project

RUN \
    pip install \
        dagster==${DAGSTER_VERSION} \
        dagster-postgres==${DAGSTER_VERSION} \
        dagster-celery[flower,redis,kubernetes]==${DAGSTER_VERSION} \
        dagster-dbt==${DAGSTER_VERSION} \
        dagster-gcp==${DAGSTER_VERSION} \
        dagster-k8s==${DAGSTER_VERSION} \
        dagster-celery-k8s==${DAGSTER_VERSION} \
        dbt-bigquery \
        tenacity \
# Cleanup
    &&  rm -rf /var \
    &&  rm -rf /root/.cache  \
    &&  rm -rf /usr/lib/python2.7 \
    &&  rm -rf /usr/lib/x86_64-linux-gnu/guile

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app
WORKDIR /opt/dagster/app

COPY dbt /opt/dagster/app/dbt
COPY dbt/prod_profiles.yml /opt/dagster/app/profiles.yml
COPY project /opt/dagster/app/project