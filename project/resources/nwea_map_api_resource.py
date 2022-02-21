import io
import zipfile

import pandas as pd
import requests
from dagster import get_dagster_logger, resource



class NweaMapApiClient:
    """Class for interacting with the NWEA MAP API"""

    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.api_url = "https://api.mapnwea.org/services/reporting/dex"
        self.zip = self._get_zip_file()

    def _get_zip_file(self):
        """
        Retrieve ZIP file from NWEA MAP API
        """
        log = get_dagster_logger()
        session = requests.Session()
        session.auth = (self.username, self.password)
        response = session.request('GET', self.api_url)

        if response.ok is False:
            response.raise_for_status()

        log.info("Successfully retrieved ZIP file")
        return zipfile.ZipFile(io.BytesIO(response.content))


    def get_assessment_results(self):
        """
        Extract CSV from ZIP file,
        return as dataframe
        """
        return pd.read_csv(self.zip.open('AssessmentResults.csv'), dtype=str)


    def get_class_assignments(self):
        """
        Extract CSV from ZIP file,
        return as dataframe
        """
        return pd.read_csv(self.zip.open('ClassAssignments.csv'), dtype=str)


    def get_students_by_school(self):
        """
        Extract CSV from ZIP file,
        return as dataframe
        """
        return pd.read_csv(self.zip.open('StudentsBySchool.csv'), dtype=str)


@resource(
    config_schema={
        "username": str,
        "password": str
    },
    description="NWEA MAP API client",
)
def nwea_map_api_resource_client(context):
    return NweaMapApiClient(
        context.resource_config["username"],
        context.resource_config["password"]
    )
