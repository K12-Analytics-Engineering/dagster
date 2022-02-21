import os

from dagster import fs_io_manager, graph, multiprocess_executor
from dagster_dbt import dbt_cli_resource, dbt_run_op
from dagster_gcp.gcs.resources import gcs_resource

from ops.nwea_map import (get_edfi_payloads,
                          get_extracts, load_extract, post_nwea_map_edfi_payloads,
                          post_nwea_map_edfi_descriptors)
from resources.bq_resource import bq_client
from resources.edfi_api_resource import edfi_api_resource_client
from resources.gcs_resource import gcs_client
from resources.nwea_map_api_resource import nwea_map_api_resource_client


@graph
def nwea_map():
    post_descriptors_result = post_nwea_map_edfi_descriptors()
    assessment_results, class_assignments, students_by_school = get_extracts()

    assessment_results_gcs_path = load_extract.alias("load_assessment_results")(assessment_results)
    class_assignments_gcs_path = load_extract.alias("load_class_assignments")(class_assignments)
    students_by_school_gcs_path = load_extract.alias("load_students_by_school")(students_by_school)

    dbt_run_result = dbt_run_op(start_after=[assessment_results_gcs_path, 
        class_assignments_gcs_path, students_by_school_gcs_path])

    edfi_assessments_json = get_edfi_payloads.alias(
        "get_edfi_assessment_payloads")(dbt_run_result)
    edfi_objective_assessments_json = get_edfi_payloads.alias(
        "get_edfi_objective_assessments_payloads")(dbt_run_result)
    edfi_student_assessments_json = get_edfi_payloads.alias(
        "get_edfi_student_assessments_payloads")(dbt_run_result)

    edfi_assessments_result = post_nwea_map_edfi_payloads.alias("post_edfi_assessment_payloads")(
        start_after=[post_descriptors_result, edfi_assessments_json],
        edfi_assessments_json=edfi_assessments_json)
    edfi_objective_assessments_result = post_nwea_map_edfi_payloads.alias("post_edfi_objective_assessments_payloads")(
        start_after=[post_descriptors_result, edfi_assessments_result],
        edfi_assessments_json=edfi_objective_assessments_json)
    post_nwea_map_edfi_payloads.alias("post_edfi_student_assessments_payloads")(
        start_after=[edfi_assessments_result, edfi_objective_assessments_result],
        edfi_assessments_json=edfi_student_assessments_json)


nwea_map_dev_job = nwea_map.to_job(
    executor_def=multiprocess_executor.configured({
        "max_concurrent": 8
    }),
    resource_defs={
        "gcs": gcs_resource,
        "data_lake": gcs_client.configured({
            "staging_gcs_bucket": os.getenv("GCS_BUCKET_DEV")
        }),
        "io_manager": fs_io_manager,
        "nwea_map_api_client": nwea_map_api_resource_client.configured({
            "username": os.getenv("NWEA_USERNAME"),
            "password": os.getenv("NWEA_PASSWORD"),
        }),
        "warehouse": bq_client.configured({
            "dataset": "dev_staging",
        }),
        "dbt": dbt_cli_resource.configured({
            "project_dir": os.getenv("DBT_PROJECT_DIR"),
            "profiles_dir": os.getenv("DBT_PROFILES_DIR"),
            "target": "dev",
            "models": [
                "+nwea_map_edfi_assessments",
                "+nwea_map_edfi_objective_assessments",
                "+nwea_map_edfi_student_assessments"
            ]
        }),
        "edfi_api_client": edfi_api_resource_client.configured({
            "base_url": os.getenv("EDFI_BASE_URL"),
            "api_key": os.getenv("EDFI_API_KEY"),
            "api_secret": os.getenv("EDFI_API_SECRET"),
            "api_page_limit": 5000,
            "api_mode": "YearSpecific" # DistrictSpecific, SharedInstance, YearSpecific
        }),
    },
    config={
        "ops": {
            "get_edfi_assessment_payloads": {
                "inputs": {
                    "table_reference": "dev_staging.nwea_map_edfi_assessments"
                }
            },
            "get_edfi_objective_assessments_payloads": {
                "inputs": {
                    "table_reference": "dev_staging.nwea_map_edfi_objective_assessments"
                }
            },
            "get_edfi_student_assessments_payloads": {
                "inputs": {
                    "table_reference": "dev_staging.nwea_map_edfi_student_assessments"
                }
            },
            "post_edfi_assessment_payloads": {
                "inputs": {
                    "school_year": "2022",
                    "api_endpoint": "ed-fi/assessments"
                }
            },
            "post_edfi_objective_assessments_payloads": {
                "inputs": {
                    "school_year": "2022",
                    "api_endpoint": "ed-fi/objectiveAssessments"
                }
            },
            "post_edfi_student_assessments_payloads": {
                "inputs": {
                    "school_year": "2022",
                    "api_endpoint": "ed-fi/studentAssessments"
                }
            },
            "post_nwea_map_edfi_descriptors": {
                "inputs": {
                    "school_year": "2022",
                    "load_descriptors": True
                }
            }
        }
    },
)
