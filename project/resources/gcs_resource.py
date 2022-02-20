import json
import uuid

from dagster import get_dagster_logger
from dagster import resource
from google.cloud import storage


class GcsClient:
    """Class for loading data into GCS"""

    def __init__(self, staging_gcs_bucket):
        self.staging_gcs_bucket = staging_gcs_bucket
        self.log = get_dagster_logger()


    def delete_files(self, folder_name, school_year):
        """
        Delete all files in passed in bucket folder
        """
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.staging_gcs_bucket)
        gcs_path = f"edfi_api/{school_year}/{folder_name}/"
        blobs = list(bucket.list_blobs(prefix=gcs_path))
        for blob in blobs:
            blob.delete()

        self.log.info(f"Deleted {len(blobs)} files from {gcs_path}")


    def load_data(self, table_name, school_year, records) -> str:
        """
        Upload list of dictionaries to gcs
        as a JSON file.
        """
        gcs_path = f"edfi_api/{school_year}/{table_name}/"
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.staging_gcs_bucket)

        gcs_file = f"{gcs_path}{str(uuid.uuid4())}.json"
        output = ""
        for record in records:
            output = output + json.dumps(record) + "\r\n"

        bucket.blob(gcs_file).upload_from_string(
            output, content_type="application/json", num_retries=3
        )
        gcs_upload_path = f"gs://{self.staging_gcs_bucket}/{gcs_file}"
        self.log.debug(f"Uploaded JSON file to {gcs_upload_path}")

        return gcs_upload_path



@resource(
    config_schema={
        "staging_gcs_bucket": str,
    },
    description="BigQuery client used to load data.",
)
def gcs_client(context):
    """
    Initialize and return GcsClient()
    """
    return GcsClient(
        context.resource_config["staging_gcs_bucket"],
    )
