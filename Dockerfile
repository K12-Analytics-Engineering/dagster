FROM python:3.9-slim

COPY --from=gcr.io/berglas/berglas:latest /bin/berglas /bin/berglas

ENV PYTHONUNBUFFERED=True
ENV PYTHONPATH=/opt/dagster/app/project
ENV PYTHONWARNINGS=ignore

ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV DAGSTER_GRPC_MAX_RX_BYTES=20000000

ENV DBT_PROFILES_DIR=/opt/dagster/app/dbt
ENV DBT_PROJECT_DIR=/opt/dagster/app/dbt

ENV PGPASSWORD=sm://<GOOGLE-PROJECT-ID>/dagster-db-password
ENV GCS_BUCKET_PROD=dagster-prod-<GOOGLE-PROJECT-ID>

ENV EDFI_BASE_URL=
ENV EDFI_API_KEY=
ENV EDFI_API_SECRET=


RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app

COPY prod_requirements.txt /opt/dagster/app/requirements.txt
WORKDIR /opt/dagster/app

RUN apt-get update && \
    apt-get install -y wget && \
    wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O cloud_sql_proxy  && \
    chmod +x cloud_sql_proxy
RUN mkdir /cloudsql
RUN chmod 777 /cloudsql

RUN pip install -r requirements.txt

COPY prod_workspace.yaml /opt/dagster/app/workspace.yaml
COPY project /opt/dagster/app/project
COPY dbt /opt/dagster/app/dbt
COPY dbt/profiles.yml /opt/dagster/app/
COPY prod_dagster.yaml /opt/dagster/dagster_home/dagster.yaml

COPY prod_run.sh /opt/dagster/app/
RUN ["chmod", "+x", "/opt/dagster/app/prod_run.sh"]

EXPOSE 3000

ENTRYPOINT exec /bin/berglas exec /opt/dagster/app/prod_run.sh