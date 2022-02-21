import json
from typing import Dict, List

import pandas as pd
from dagster import (Out, Output, RetryPolicy, op)


@op(
    description="Delete Ed-Fi API survey responses",
    required_resource_keys={"edfi_api_client"},
    tags={"kind": "delete"},
)
def delete_edfi_survey_responses(context, start_after,
    school_year: str, records: pd.DataFrame) -> str:
    """
    """
    records.drop_duplicates('survey_responses_id', inplace=True)
    records.apply(
        lambda row: context.resources.edfi_api_client.delete_data(
            row["survey_responses_id"],
            school_year,
            "ed-fi/surveyResponses"
        ), axis=1)

    return "Records deleted"


@op(
    description="Delete Ed-Fi API survey response questions",
    required_resource_keys={"edfi_api_client"},
    tags={"kind": "delete"},
)
def delete_edfi_survey_response_questions(context, school_year: str, records: pd.DataFrame) -> str:
    """
    """
    records.apply(
        lambda row: context.resources.edfi_api_client.delete_data(
            row["survey_question_responses_id"],
            school_year,
            "ed-fi/surveyQuestionResponses"
        ), axis=1)

    return "Records deleted"


@op(
    description="Retrieve records from passed in BigQuery table name",
    required_resource_keys={"warehouse"},
    tags={"kind": "extract"},
)
def download_bigquery_table(context, dbt_run_result, table_reference: str) -> pd.DataFrame:
    """
    Extract BigQuery table and return the
    resulting JSON as a dict.
    """
    return context.resources.warehouse.download_table(table_reference)


@op(
    description="Retrieves questions for all Form ids passed in",
    required_resource_keys={"forms_client"},
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    tags={"kind": "extract"}
)
def get_questions(context, form_ids: List) -> List:
    """
    """
    records = list()
    for form_id in form_ids:
        records.append(context.resources.forms_client.get_forms_questions(context, form_id))

    yield Output(
        value=[{
            "folder_name": "questions",
            "value": records
        }],
        metadata={
            "record_count": len(records)
        }
    )


@op(
    description="Retrieves Form responses for all Form ids passed in",
    required_resource_keys={"forms_client"},
    out=Out(List[Dict]),
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    tags={"kind": "extract"}
)
def get_responses(context, form_ids: List) -> List:
    records = list()
    for form_id in form_ids:
        for responses in context.resources.forms_client.get_forms_responses(context, form_id):
            responses['formId'] = form_id
            records.append(responses)

    yield Output(
        value=[{
            "folder_name": "responses",
            "value": records
        }],
        metadata={
            "record_count": len(records)
        }
    )


@op(
    description="Persist extract to data lake",
    required_resource_keys={"data_lake"},
    tags={"kind": "load"},
)
def load_data(context, extract: List) -> str:
    """
    Upload extract to Google Cloud Storage.
    Return list of GCS file paths.
    """
    gcs_path = f"google_forms/{extract[0]['folder_name']}/"
    records = list()
    for set_of_records in extract:
        for record in set_of_records["value"]:
            records.append({
                "id": None,
                "data": json.dumps(record)
            })
    
    context.resources.data_lake.delete_files(
        gcs_path=gcs_path
    )

    return context.resources.data_lake.upload_json(
        gcs_path=gcs_path,
        records=records
    )


@op(
    description="POST JSON data to Ed-Fi API",
    required_resource_keys={"edfi_api_client"},
    tags={"kind": "load"},
)
def post_google_forms_edfi_payloads(context, start_after, school_year: str,
    records: pd.DataFrame, api_endpoint: str) -> List:
    """
    POST payloads to passed in Ed-Fi API endpoint,
    return generated Ed-Fi ids.
    """
    df_json = records.to_json(orient="records", date_format="iso")

    return context.resources.edfi_api_client.post_data(
        json.loads(df_json),
        school_year,
        api_endpoint
    )


@op(
    description="Create necessary descriptors in Google Forms namespace",
    required_resource_keys={"edfi_api_client"},
    tags={"kind": "load"},
)
def post_google_forms_edfi_descriptors(context, school_year: str, load_descriptors: bool) -> List:
    """
    POST payloads to passed in Ed-Fi API endpoint,
    return generated Ed-Fi ids.
    """
    descriptors = [
        {
            "endpoint": "ed-fi/questionFormDescriptors",
            "payload": [{
                    "codeValue": "Linear Scale",
                    "description": "Linear Scale",
                    "namespace": "uri://forms.google.com/QuestionFormDescriptor",
                    "shortDescription": "Linear Scale"
                }
            ]
        }
    ]

    generated_ids = list()
    if load_descriptors:
        for descriptor in descriptors:
            ids = context.resources.edfi_api_client.post_data(
                descriptor["payload"], school_year, descriptor["endpoint"])
            generated_ids.append(ids)

    return generated_ids
