import json

from datetime import datetime, timedelta

from typing import List, Dict, Optional, Union

from dagster import (
    AssetMaterialization,
    DynamicOut,
    DynamicOutput,
    ExpectationResult,
    op,
    Out,
    Output,
    RetryPolicy,
)
from dagster_dbt.cli.types import DbtCliOutput
from dagster_dbt.utils import generate_materializations

import pandas as pd
from google.cloud import bigquery
from google.api_core import exceptions


@op(
    description="Dynamically outputs Ed-Fi API enpoints for parallelization",
    out=DynamicOut(Dict),
)
def api_endpoint_generator(
    context, edfi_api_endpoints: List[Dict], use_change_queries: bool
) -> Dict:
    """
    Dynamically output each Ed-Fi API endpoint
    in the job's config. If job is configured to not
    use change queries, do not output the /deletes version
    of each endpoint.
    """
    for endpoint in edfi_api_endpoints:
        if "/deletes" in endpoint["endpoint"] and not use_change_queries:
            pass
        else:
            yield DynamicOutput(value=endpoint, mapping_key=endpoint["table_name"])


@op(
    description="Retrieves newest change version from Ed-Fi API",
    required_resource_keys={"data_lake", "edfi_api_client"},
    out=Out(Union[int, None]),
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    tags={"kind": "change_queries"},
)
def get_newest_api_change_versions(context, school_year: int, use_change_queries: bool):
    """
    If job is configured to use change queries, get
    the newest change version number from the target Ed-Fi API.
    Upload data to data lake.
    """
    if use_change_queries:
        stats = context.instance.event_log_storage.get_stats_for_run(context.run_id)
        launch_datetime = datetime.utcfromtimestamp(stats.launch_time)
        response = context.resources.edfi_api_client.get_available_change_versions(
            school_year
        )
        context.log.debug(response)

        path = context.resources.data_lake.upload_json(
            path=f"edfi_api/school_year_{school_year}/{launch_datetime}.json",
            records=[{
                "run_id": context.run_id,
                "oldest_change_version": response["OldestChangeVersion"],
                "newest_change_version": response["NewestChangeVersion"]
            }],
        )
        context.log.debug(path)

        return response["NewestChangeVersion"]
    else:
        context.log.info("Will not use change queries")
        return None


@op(
    description="Retrieves change version from last job run",
    required_resource_keys={"warehouse"},
    out=Out(Union[int, None]),
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    tags={"kind": "change_queries"},
)
def get_previous_change_version(context, school_year: int, use_change_queries: bool):
    """
    Run SQL query on table edfi_processed_change_versions
    to retrieve the change version number from the
    previous job run. If no data is returned, return -1.
    This will cause the extract step to pull all data from
    the target Ed-Fi API.
    """
    context.log.info(f"Use change queries is set to {use_change_queries}")
    if use_change_queries:
        query = f"""
            SELECT newest_change_version
            FROM `{{project_id}}.{{dataset}}.edfi_processed_change_versions`
            WHERE
                SchoolYear = {school_year}
                AND timestamp < TIMESTAMP '{datetime.now().isoformat()}'
            ORDER BY timestamp DESC
            LIMIT 1
        """
        try:
            for row in context.resources.warehouse.run_query(query):
                previous_change_version = row["newest_change_version"]
                context.log.debug(
                    f"Latest processed change version: {previous_change_version}"
                )
                return previous_change_version
        except exceptions.NotFound as err:
            context.log.debug(err)
            context.log.debug("Failed to query table. Table not found.")
            context.log.debug("Returning -1 as latest processed change version")
            return -1
    else:
        return None


@op(
    description="Retrieves data from the Ed-Fi API and uploads to data lake",
    required_resource_keys={"data_lake", "edfi_api_client"},
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    tags={"kind": "extract"},
)
def extract_and_upload_data(
    context,
    api_endpoint: Dict,
    school_year: int,
    previous_change_version: Optional[int] = None,
    newest_change_version: Optional[int] = None,
    use_change_queries: bool = False,
) -> str:
    """
    Retrieve data from API endpoint. For each record, add the run
    timestamp. Upload data as raw JSON to data lake.

    Args:
        api_endpoint (dict): Dict containing two keys. `endpoint` which
        is the Ed-Fi API endpoint path and `table_name` which is the warehouse
        table name.

        newest_change_version (bool): The latest change
        version number returned from target Ed-Fi API.
    """

    stats = context.instance.event_log_storage.get_stats_for_run(context.run_id)
    launch_datetime = datetime.utcfromtimestamp(stats.launch_time)

    file_number = 1
    # process yielded records from generator
    for yielded_response in context.resources.edfi_api_client.get_data(
        api_endpoint["endpoint"],
        school_year,
        previous_change_version,
        newest_change_version,
    ):

        records_to_upload = list()
        # iterate through each payload
        for response in yielded_response:
            records_to_upload.append(
                {
                    "is_complete_extract": not use_change_queries,
                    "data": json.dumps(response),
                }
            )

        # upload current set of records from generator
        path = context.resources.data_lake.upload_json(
            path=(
                f"edfi_api/{api_endpoint['table_name']}/school_year={school_year}/"
                f"date_extracted={launch_datetime}/{file_number:09}.json"
            ),
            records=records_to_upload,
        )
        file_number += 1
        context.log.debug(path)

    return "Task complete"


@op(
    description="Run all dbt models tagged with edfi and amt",
    required_resource_keys={"dbt"},
    tags={"kind": "transform"},
)
def run_edfi_models(context, retrieved_data) -> DbtCliOutput:
    """
    Run all dbt models tagged with edfi
    and amt. Yield asset materializations
    """
    context.resources.dbt.cli(command="deps")
    dbt_cli_edfi_output = context.resources.dbt.run(models=["tag:edfi"])
    for materialization in generate_materializations(
        dbt_cli_edfi_output, asset_key_prefix=["edfi"]
    ):

        yield materialization

    dbt_cli_amt_output = context.resources.dbt.run(models=["tag:marts"])
    for materialization in generate_materializations(
        dbt_cli_amt_output, asset_key_prefix=["marts"]
    ):

        yield materialization

    yield Output(dbt_cli_edfi_output)


@op(
    description="Test all dbt models tagged with edfi and amt",
    required_resource_keys={"dbt"},
    tags={"kind": "dbt"},
)
def test_edfi_models(context, start_after) -> DbtCliOutput:
    """
    Test all dbt models tagged with edfi and amt.
    """
    dbt_cli_edfi_output = context.resources.dbt.test(
        models=["tag:edfi", "tag:marts"], data=False, schema=False
    )
    return dbt_cli_edfi_output
