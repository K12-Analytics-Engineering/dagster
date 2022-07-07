import json
import os
from datetime import datetime

import pandas as pd
from dagster import AssetKey, MetadataValue, Output, asset
from dagster_dbt import load_assets_from_dbt_project

from assets.edfi_api_endpoints import EDFI_API_ENDPOINTS


@asset(
    group_name="source",
    key_prefix=["staging"],
    required_resource_keys={"edfi_api_client", "globals"},
    config_schema={"use_change_queries": bool},
)
def change_query_versions(context):
    """
    Retrieve change query version from previous asset materialization
    and most recent version from Ed-Fi API.

    Use 0 if this is the first time the asset is being materialized.

    Use -1 and -1 if the run config is set to not use change queries.
    """
    if not context.op_config["use_change_queries"]:
        context.log.info("Will not use change queries")
        previous_change_version = -1
        newest_change_version = -1
    else:
        context.log.info("Using change queries")
        school_year = context.resources.globals["school_year"]
        previous_change_version = 0

        try:
            # get previous materialization events
            last_materialization = context.instance.get_latest_materialization_events(
                asset_keys=[AssetKey(("edfi_api", "change_query_versions"))]
            )
            # iterate through metadata entries looking for previous change version number
            for metadata_entry in last_materialization[
                AssetKey(("edfi_api", "change_query_versions"))
            ].dagster_event.event_specific_data.materialization.metadata_entries:
                if metadata_entry.label == "Newest change version":
                    # change version number found
                    previous_change_version = metadata_entry.entry_data.value
                    context.log.debug(previous_change_version)
        except:
            context.log.info("Failed to find previous asset materialization")

        response = context.resources.edfi_api_client.get_available_change_versions(
            school_year
        )
        context.log.debug(f"Retrieved response from change query api: {response}")

        newest_change_version = response["NewestChangeVersion"]

    return Output(
        {
            "previous_change_version": previous_change_version,
            "newest_change_version": newest_change_version,
        },
        metadata={
            "Previous change version": MetadataValue.int(previous_change_version),
            "Newest change version": MetadataValue.int(newest_change_version),
        },
    )


edfi_assets = list()
for edfi_endpoint in EDFI_API_ENDPOINTS:

    def make_func(edfi_endpoint):
        @asset(
            name=edfi_endpoint["asset"],
            group_name="source",
            key_prefix=["staging"],
            required_resource_keys={"data_lake", "edfi_api_client", "globals"},
        )
        def extract_and_load(context, change_query_versions):
            school_year = context.resources.globals["school_year"]
            # change versions to use for scoped api pull
            previous_change_version = change_query_versions["previous_change_version"]
            newest_change_version = change_query_versions["newest_change_version"]

            # dagster run datetime
            stats = context.instance.event_log_storage.get_stats_for_run(context.run_id)
            launch_datetime = datetime.utcfromtimestamp(stats.launch_time)

            changed_records = 0
            changed_records_gcs_paths = []
            deleted_records = 0
            deleted_records_gcs_paths = []
            for endpoint in edfi_endpoint["endpoints"]:

                if (
                    previous_change_version == -1
                    and newest_change_version == -1
                    and "/deletes" in endpoint
                ):
                    context.log.info(f"Skipping the endpoint {endpoint}")
                    continue

                file_number = 1
                # process yielded records from generator
                for yielded_response in context.resources.edfi_api_client.get_data(
                    endpoint,
                    school_year,
                    previous_change_version,
                    newest_change_version,
                ):

                    records_to_upload = [{}]
                    extract_type = "deletes" if "/deletes" in endpoint else "records"

                    # iterate through each payload
                    for response in yielded_response:
                        if "/deletes" in endpoint:
                            id = response["Id"].replace("-", "")
                            deleted_records += 1
                        else:
                            id = response["id"].replace("-", "")
                            changed_records += 1

                        records_to_upload.append(
                            {
                                "is_complete_extract": True
                                if previous_change_version == -1
                                else False,
                                "id": id,
                                "data": json.dumps(response),
                            }
                        )

                    # upload current set of records from generator
                    path = context.resources.data_lake.upload_json(
                        path=(
                            f"edfi_api/{edfi_endpoint['asset']}/api_version={context.resources.edfi_api_client.api_version}/"
                            f"school_year={school_year}/"
                            f"date_extracted={launch_datetime}/extract_type={extract_type}/"
                            f"{abs(hash(endpoint))}-{file_number:09}.json"
                        ),
                        records=records_to_upload,
                    )
                    if "/deletes" in endpoint:
                        deleted_records_gcs_paths.append(path)
                    else:
                        changed_records_gcs_paths.append(path)
                    file_number += 1
                    context.log.debug(path)

            return Output(
                "Task successful",
                metadata={
                    "Changed records": MetadataValue.int(changed_records),
                    "Deleted records": MetadataValue.int(deleted_records),
                    "Changed records GCS paths": MetadataValue.text(
                        ", ".join(changed_records_gcs_paths)
                    ),
                    "Deleted records GCS paths": MetadataValue.text(
                        ", ".join(deleted_records_gcs_paths)
                    ),
                },
            )

        return extract_and_load

    edfi_assets.append(make_func(edfi_endpoint))


dbt_assets = load_assets_from_dbt_project(
    project_dir=os.getenv("DBT_PROJECT_DIR"),
    profiles_dir=os.getenv("DBT_PROFILES_DIR"),
)
