from typing import List, Dict

import base64
import requests

from dagster import get_dagster_logger, resource
from tenacity import retry, stop_after_attempt, wait_exponential


class EdFiApiClient:
    """Class for interacting with an Ed-Fi API"""

    def __init__(self, base_url, api_key, api_secret, api_page_limit, api_mode, api_version):
        self.base_url = base_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_page_limit = api_page_limit
        self.api_mode = api_mode
        self.api_version = api_version
        self.log = get_dagster_logger()
        self.access_token = self.get_access_token()

    def get_access_token(self):
        """
        Retrieve access token from Ed-Fi API.
        """
        credentials_concatenated = ":".join((self.api_key, self.api_secret))
        credentials_encoded = base64.b64encode(credentials_concatenated.encode("utf-8"))
        access_url = f"{self.base_url}/oauth/token"
        access_headers = {"Authorization": b"Basic " + credentials_encoded}
        access_params = {"grant_type": "client_credentials"}

        response = requests.post(access_url, headers=access_headers, data=access_params)

        if response.ok:
            response_json = response.json()
            access_token = response_json["access_token"]
            self.log.debug(f"Retrieved access token {access_token}")
            return access_token
        else:
            raise Exception("Failed to retrieve access token")

    @retry(
        stop=stop_after_attempt(8), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _call_api(self, url):
        """
        Call GET on passed in URL and
        return response.
        """
        headers = {"Authorization": f"Bearer {self.access_token}"}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.warn(f"Failed to retrieve data: {err}")
            self.log.warn(response.reason)
            if response.status_code == 401 and response.reason == "Invalid token":
                self.log.info("Retrieving new access token")
                self.access_token = self.get_access_token()
            raise err

        return response.json()

    def get_available_change_versions(self, school_year) -> List[Dict]:
        """
        Call available change versions API
        and return response.
        """
        if self.api_mode == "YearSpecific":
            endpoint = f"{self.base_url}/changeQueries/v1/{school_year}/availableChangeVersions"
        else:
            endpoint = f"{self.base_url}/changeQueries/v1/availableChangeVersions"

        return self._call_api(endpoint)

    def get_data(
        self,
        api_endpoint: str,
        school_year: int,
        latest_processed_change_version: int,
        newest_change_version: int,
    ) -> List[Dict]:
        """
        Page through API endpoint using change version
        numbers and return response.
        """
        limit = 5000 if "/deletes" in api_endpoint else self.api_page_limit

        if self.api_mode == "YearSpecific":
            endpoint = (
                f"{self.base_url}/data/v3/{school_year}{api_endpoint}" f"?limit={limit}"
            )
        else:
            endpoint = f"{self.base_url}/data/v3{api_endpoint}" f"?limit={limit}"

        if (
            latest_processed_change_version is not None
            and newest_change_version is not None
        ):
            endpoint = (
                f"{endpoint}"
                f"&minChangeVersion={latest_processed_change_version + 1}"
                f"&maxChangeVersion={newest_change_version}"
            )

        offset = 0
        while True:
            endpoint_to_call = f"{endpoint}&offset={offset}"
            self.log.debug(endpoint_to_call)
            response = self._call_api(endpoint_to_call)

            # yield response allowing records
            # to be stored while continuing to pull
            # new records
            yield response

            if not response:
                # retrieved all data from api
                break
            else:
                # move onto next page
                offset = offset + limit


    def delete_data(self, id, school_year, api_endpoint) -> str:
        """
        """
        headers = { "Authorization": f"Bearer {self.access_token}" }
        if self.api_mode == "YearSpecific":
            endpoint = f"{self.base_url}/data/v3/{school_year}/{api_endpoint}/{id}"
        else:
            endpoint = f"{self.base_url}/data/v3/{api_endpoint}/{id}"

        self.log.debug(endpoint)

        try:
            response = requests.delete(
                endpoint,
                headers=headers
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if response.status_code == 404:
                self.log.info(f"Ed-Fi API ID {id} does not exist")
                return f"Ed-Fi API ID {id} does not exist"
            self.log.warn(response.text)
            self.log.warn(f"Failed to delete id: {err}")
            raise err

        return f"Ed-Fi API ID {id} successfully deleted"


    def post_data(
        self,
        records,
        school_year: int,
        api_endpoint: str) -> List:
        """
        Loop through payloads and POST
        to passed in Ed-Fi API endpoint.
        """
        headers = { "Authorization": f"Bearer {self.access_token}" }
        if self.api_mode == "YearSpecific":
            endpoint = f"{self.base_url}/data/v3/{school_year}/{api_endpoint}"
        else:
            endpoint = f"{self.base_url}/data/v3/{api_endpoint}"

        self.log.debug(endpoint)

        generated_ids = list()
        for record in records:
            response = requests.post(
                endpoint,
                headers=headers,
                json=record
            )
            response.raise_for_status()
            self.log.debug(f"Successfully posted {response.headers['location']}")
            generated_ids.append(response.headers['location'])

        self.log.debug(generated_ids)
        return generated_ids


@resource(
    config_schema={
        "base_url": str,
        "api_key": str,
        "api_secret": str,
        "api_page_limit": int,
        "api_mode": str,
        "api_version": str
    },
    description="Ed-Fi API client that retrieves data from various endpoints.",
)
def edfi_api_resource_client(context):
    return EdFiApiClient(
        context.resource_config["base_url"],
        context.resource_config["api_key"],
        context.resource_config["api_secret"],
        context.resource_config["api_page_limit"],
        context.resource_config["api_mode"],
        context.resource_config["api_version"]
    )
