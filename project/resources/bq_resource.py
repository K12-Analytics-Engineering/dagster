import json
import uuid
from typing import List, Dict

from dagster import get_dagster_logger
from dagster import resource
from google.cloud import bigquery
import pandas as pd


class BigQueryClient:
    """Class for loading data into BigQuery"""

    def __init__(self, dataset):
        self.dataset = dataset
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


    def download_table(self, table_reference: str) -> pd.DataFrame:
        """
        Download table and return the resulting QueryJob.
        Returns empty dataframe if table not found or
        table has no rows.
        """
        try:
            query_job = self.client.query(f"SELECT * FROM {self.client.project}.{table_reference}")
            df = query_job.to_dataframe()
        except:
            self.log.warn("Failed to download table. Returning empty dataframe.")
            df = pd.DataFrame()

        self.log.info(f"Downloaded {len(df)} rows from table {self.client.project}.{table_reference}")

        return df


    def run_query(self, query: str):
        """
        Run SQL query and return the resulting QueryJob.
        """
        return self.client.query(
            query.format(project_id=self.client.project, dataset=self.dataset)
        )


@resource(
    config_schema={
        "dataset": str
    },
    description="BigQuery client used to load data.",
)
def bq_client(context):
    """
    Initialize and return BigQueryClient()
    """
    return BigQueryClient(
        context.resource_config["dataset"]
    )
