import json
import uuid
from typing import List, Dict

from dagster import get_dagster_logger
from dagster import resource
from google.cloud import bigquery


class BigQueryClient:
    """Class for loading data into BigQuery"""

    def __init__(self, dataset, staging_gcs_bucket):
        self.dataset = dataset
        self.staging_gcs_bucket = staging_gcs_bucket
        self.client = bigquery.Client()
        self._create_dataset()
        self.dataset_ref = bigquery.DatasetReference(self.client.project, self.dataset)
        self.log = get_dagster_logger()


    def _create_dataset(self):
        """
        Create BigQuery dataset if
        it does not exist.
        """
        self.client.create_dataset(
            bigquery.Dataset(f"{self.client.project}.{self.dataset}"), exists_ok=True
        )


    def create_table(self, table_name: str):
        """
        Create BigQuery external table using
        source URIs created from the passed in GCS folder.
        """
        schema = [
            bigquery.SchemaField("id", "STRING", "NULLABLE"),
            bigquery.SchemaField("data", "STRING", "NULLABLE"),
        ]
        table_ref = bigquery.Table(self.dataset_ref.table(table_name), schema=schema)

        external_config = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
        external_config.source_uris = list()
        for year in range(2018, 2031):
            # ie. gs://storage-bucket/edfi_api/2022/edfi_calendars/*.json
            external_config.source_uris.append(
                f"gs://{self.staging_gcs_bucket}/edfi_api/{year}/{table_name}/*.json"
            )

        table_ref.external_data_configuration = external_config
        table = self.client.create_table(table_ref, exists_ok=True)
        self.client.close()
        return f"Created table {table.project}.{table.dataset_id}.{table.table_id}"


    def append_data(self, table_name: str, schema: List, df) -> str:
        """
        Append data to BigQuery table using
        schema specified
        """
        table_ref = bigquery.Table(self.dataset_ref.table(table_name), schema=schema)
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND",
        )

        job = self.client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result()  # waits for the job to complete.
        self.client.close()

        return f"Created table {self.client.project}.{self.dataset}.{table_name}"


    def run_query(self, query: str):
        """
        Run SQL query and return the resulting QueryJob.
        """
        return self.client.query(
            query.format(project_id=self.client.project, dataset=self.dataset)
        )


@resource(
    config_schema={
        "dataset": str,
        "staging_gcs_bucket": str,
    },
    description="BigQuery client used to load data.",
)
def bq_client(context):
    """
    Initialize and return BigQueryClient()
    """
    return BigQueryClient(
        context.resource_config["dataset"],
        context.resource_config["staging_gcs_bucket"],
    )
