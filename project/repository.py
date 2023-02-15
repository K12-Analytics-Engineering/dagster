import os

from assets.edfi_api import (
    change_query_versions,
    create_edfi_assets,
)
from dagster import (
    Definitions,
    fs_io_manager,
    multiprocess_executor,
    resource,
)
from dagster_gcp.gcs.io_manager import gcs_pickle_io_manager
from dagster_gcp.gcs.resources import gcs_resource
from resources.edfi_api_resource import edfi_api_resource_client
from resources.gcs_resource import gcs_client


SCHOOL_YEAR = 2023


@resource
def globals():
    return {
        "school_year": SCHOOL_YEAR,
    }


RESOURCES_LOCAL = {
    "gcs": gcs_resource,
    "io_manager": fs_io_manager,
    "data_lake": gcs_client.configured(
        {"staging_gcs_bucket": "dagster-dev-edfi-course"}
    ),
    "edfi_api_client": edfi_api_resource_client.configured(
        {
            "base_url": os.getenv("EDFI_BASE_URL"),
            "api_key": os.getenv("EDFI_API_KEY"),
            "api_secret": os.getenv("EDFI_API_SECRET"),
            "api_page_limit": 500,
            "api_mode": "Sandbox",  # DistrictSpecific, Sandbox, SharedInstance, YearSpecific
            "data_model": "3.3.1-b",
        }
    ),
    "globals": globals,
}


resource_defs_by_deployment_name = {
    "prod": None,
    "local": RESOURCES_LOCAL,
}


defs = Definitions(
    assets=([change_query_versions] + create_edfi_assets()),
    schedules=[],
    jobs=[],
    sensors=[],
    resources=resource_defs_by_deployment_name[
        os.environ.get("DAGSTER_DEPLOYMENT", "local")
    ],
    executor=multiprocess_executor.configured({"max_concurrent": 5}),
)
