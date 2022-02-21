import os

from dagster import fs_io_manager, graph, multiprocess_executor
from dagster_dbt import dbt_cli_resource, dbt_run_op
from dagster_gcp.gcs.resources import gcs_resource
from ops.google_forms import (delete_edfi_survey_response_questions,
                              delete_edfi_survey_responses,
                              download_bigquery_table, get_questions,
                              get_responses, load_data, post_google_forms_edfi_payloads,
                              post_google_forms_edfi_descriptors)
from resources.bq_resource import bq_client
from resources.edfi_api_resource import edfi_api_resource_client
from resources.gcs_resource import gcs_client
from resources.google_forms_api_resource import \
    google_forms_api_resource_client


@graph
def google_forms(form_ids, school_year):

    post_descriptors_result = post_google_forms_edfi_descriptors(school_year=school_year)

    # get data from google forms api and persist to data lake
    questions = get_questions(form_ids)
    questions_gcs_path = load_data.alias("load_questions")(questions)

    responses = get_responses(form_ids)
    responses_gcs_path = load_data.alias("load_responses")(responses)

    # model data for edfi api interactions
    dbt_run_result = dbt_run_op(start_after=[questions_gcs_path, responses_gcs_path])

    edfi_surveys = download_bigquery_table.alias("get_edfi_surveys_payloads")(dbt_run_result)
    edfi_survey_questions = download_bigquery_table.alias("get_edfi_survey_questions_payloads")(dbt_run_result)
    edfi_survey_responses = download_bigquery_table.alias("get_edfi_survey_responses_payloads")(dbt_run_result)
    edfi_survey_question_responses = download_bigquery_table.alias("get_edfi_survey_question_responses_payloads")(dbt_run_result)
    google_forms_deleted_responses = download_bigquery_table.alias("get_google_forms_deleted_responses")(dbt_run_result)

    # post data to edfi api
    edfi_surveys = post_google_forms_edfi_payloads.alias("post_edfi_surveys_payloads")(
        start_after=[post_descriptors_result],
        school_year=school_year,
        records=edfi_surveys)

    edfi_survey_questions_result = post_google_forms_edfi_payloads.alias("post_edfi_survey_questions_payloads")(
        start_after=[edfi_surveys],
        school_year=school_year,
        records=edfi_survey_questions)

    edfi_survey_responses_result = post_google_forms_edfi_payloads.alias("post_edfi_survey_responses_payloads")(
        start_after=[edfi_surveys],
        school_year=school_year,
        records=edfi_survey_responses)

    edfi_survey_question_responses_result = post_google_forms_edfi_payloads.alias("post_edfi_survey_question_responses_payloads")(
        start_after=[edfi_survey_responses_result],
        school_year=school_year,
        records=edfi_survey_question_responses)

    # delete data from edfi api
    removed_survey_response_questions = delete_edfi_survey_response_questions(
        school_year=school_year,
        records=google_forms_deleted_responses)

    removed_survey_responses = delete_edfi_survey_responses(
        start_after=[removed_survey_response_questions],
        school_year=school_year,
        records=google_forms_deleted_responses)


google_forms_dev_job = google_forms.to_job(
    executor_def=multiprocess_executor.configured({
        "max_concurrent": 8
    }),
    resource_defs={
        "gcs": gcs_resource,
        "data_lake": gcs_client.configured({
            "staging_gcs_bucket": os.getenv("GCS_BUCKET_DEV")
        }),
        "io_manager": fs_io_manager,
        "forms_client": google_forms_api_resource_client.configured({
            "forms_api_key": os.getenv('FORMS_API_KEY'),
            "forms_owner_email": "marcos@alcozer.dev"
        }),
        "warehouse": bq_client.configured({
            "dataset": "dev_staging",
        }),
        "dbt": dbt_cli_resource.configured({
            "project_dir": os.getenv("DBT_PROJECT_DIR"),
            "profiles_dir": os.getenv("DBT_PROFILES_DIR"),
            "target": "dev",
            "models": [
                "+google_forms_edfi_surveys",
                "+google_forms_edfi_survey_questions",
                "+google_forms_edfi_survey_responses",
                "+google_forms_edfi_survey_question_responses",
                "google_forms_deleted_responses"
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
        "inputs": {
            "form_ids": { "value": ["1f6_AQS3w-H4hER0pJrG-6rt6p1DmwXaVPLBYQ9dJ_og"] },
            "school_year": { "value": "2022" }
        },
        "ops": {
            "get_edfi_surveys_payloads": {
                "inputs": {
                    "table_reference": "dev_staging.google_forms_edfi_surveys"
                }
            },
            "get_edfi_survey_questions_payloads": {
                "inputs": {
                    "table_reference": "dev_staging.google_forms_edfi_survey_questions"
                }
            },
            "get_edfi_survey_responses_payloads": {
                "inputs": {
                    "table_reference": "dev_staging.google_forms_edfi_survey_responses"
                }
            },
            "get_edfi_survey_question_responses_payloads": {
                "inputs": {
                    "table_reference": "dev_staging.google_forms_edfi_survey_question_responses"
                }
            },
            "get_google_forms_deleted_responses": {
                "inputs": {
                    "table_reference": "dev_staging.google_forms_deleted_responses"
                }
            },
            "post_edfi_surveys_payloads": {
                "inputs": {
                    "api_endpoint": "ed-fi/surveys"
                }
            },
            "post_edfi_survey_questions_payloads": {
                "inputs": {
                    "api_endpoint": "ed-fi/surveyQuestions"
                }
            },
            "post_edfi_survey_responses_payloads": {
                "inputs": {
                    "api_endpoint": "ed-fi/surveyResponses"
                }
            },
            "post_edfi_survey_question_responses_payloads": {
                "inputs": {
                    "api_endpoint": "ed-fi/surveyQuestionResponses"
                }
            },
            "post_google_forms_edfi_descriptors": {
                "inputs": {
                    "load_descriptors": True
                }
            },
        }
    },
)
