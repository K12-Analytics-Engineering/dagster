import os

from dagster import (DefaultScheduleStatus, ScheduleDefinition,
                     define_asset_job, fs_io_manager, make_values_resource,
                     multiprocess_executor, repository, with_resources)
from dagster_dbt import load_assets_from_dbt_project
from dagster_gcp.gcs.resources import gcs_resource

from assets.edfi_api import change_query_versions, edfi_assets
from resources.dbt_resource import dbt_cli_resource
from resources.edfi_api_resource import edfi_api_resource_client
from resources.gcs_resource import gcs_client

dbt_assets = load_assets_from_dbt_project(
    project_dir=os.getenv("DBT_PROJECT_DIR"),
    profiles_dir=os.getenv("DBT_PROFILES_DIR"),
)

edfi_api_refresh_job = define_asset_job(
    name="edfi_api_job", tags={"dagster/max_retries": 3}
)

edfi_full_refresh_schedule = ScheduleDefinition(
    name="edfi_full_refresh",
    job=edfi_api_refresh_job,
    cron_schedule="0 6 * * 6",
    run_config={
        "ops": {
            "staging__change_query_versions": {"config": {"use_change_queries": False}}
        }
    },
    default_status=DefaultScheduleStatus.RUNNING,
)

edfi_delta_refresh_schedule = ScheduleDefinition(
    name="edfi_delta_refresh",
    job=edfi_api_refresh_job,
    cron_schedule="0 6 * * 7,1-5",
    run_config={
        "ops": {
            "staging__change_query_versions": {"config": {"use_change_queries": True}}
        }
    },
    default_status=DefaultScheduleStatus.RUNNING,
)


@repository(
    default_executor_def=multiprocess_executor.configured({"max_concurrent": 5})
)
def repository():
    edfi_assets_with_dev_resources = with_resources(
        definitions=[change_query_versions] + edfi_assets + dbt_assets,
        resource_defs={
            "gcs": gcs_resource,
            "io_manager": fs_io_manager,
            "data_lake": gcs_client.configured(
                {"staging_gcs_bucket": os.getenv("GCS_BUCKET_DEV")}
            ),
            "dbt": dbt_cli_resource.configured(
                {
                    "project_dir": os.getenv("DBT_PROJECT_DIR"),
                    "profiles_dir": os.getenv("DBT_PROFILES_DIR"),
                    "target": "dev",
                }
            ),
            "edfi_api_client": edfi_api_resource_client.configured(
                {
                    "base_url": os.getenv("EDFI_BASE_URL"),
                    "api_key": os.getenv("EDFI_API_KEY"),
                    "api_secret": os.getenv("EDFI_API_SECRET"),
                    "api_page_limit": 2500,
                    "api_mode": "YearSpecific",  # DistrictSpecific, SharedInstance, YearSpecific
                    "api_version": "5.3",
                }
            ),
            "globals": make_values_resource(school_year=int),
        },
        resource_config_by_key={"globals": {"config": {"school_year": 2022}}},
    )
    return [
        *edfi_assets_with_dev_resources,
        edfi_api_refresh_job,
        edfi_full_refresh_schedule,
        edfi_delta_refresh_schedule,
    ]
