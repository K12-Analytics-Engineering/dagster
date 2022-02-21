import json
import re
from typing import Dict, List

from dagster import (
    AssetMaterialization,
    Out,
    Output,
    op
)
from google.cloud import bigquery


@op(
    description="Gets extracts from NWEA MAP API",
    required_resource_keys={"nwea_map_api_client"},
    out={
        "assessment_results": Out(Dict),
        "class_assignments": Out(Dict),
        "students_by_school": Out(Dict)
    },
    tags={"kind": "extract"},
)
def get_extracts(context):
    """
    Retrieve ZIP file from NWEA MAP API.
    Yield 3 extracts as dict containing
    dataframe and filename metadata for
    GCS upload op.
    """
    assessment_results_df = context.resources.nwea_map_api_client.get_assessment_results()
    class_assignments_df = context.resources.nwea_map_api_client.get_class_assignments()
    students_by_school_df = context.resources.nwea_map_api_client.get_students_by_school()

    # ie. convert 'Fall 2021-2022' to 'fall_2021_2022'
    term_name = assessment_results_df["TermName"].iloc[0]
    term_name_snake_case = term_name.lower().replace(" ", "_").replace("-", "_")

    yield Output(
        value={
            "filename": f"{term_name_snake_case}_assessment_results.csv",
            "value": assessment_results_df
        },
        output_name="assessment_results",
        metadata={
            "record_count": len(assessment_results_df),
            "term_name": term_name
        }
    )

    yield Output(
        value={
            "filename": f"{term_name_snake_case}_class_assignments.csv",
            "value": class_assignments_df
        },
        output_name="class_assignments",
        metadata={
            "record_count": len(class_assignments_df),
            "term_name": term_name
        }
    )

    yield Output(
        value={
            "filename": f"{term_name_snake_case}_students_by_school.csv",
            "value": students_by_school_df
        },
        output_name="students_by_school",
        metadata={
            "record_count": len(students_by_school_df),
            "term_name": term_name
        }
    )


@op(
    description="Persist extract to data lake",
    required_resource_keys={"data_lake"},
    tags={"kind": "load"},
)
def load_extract(context, extract: Dict) -> str:
    """
    Upload extract to Google Cloud Storage.
    Return GCS file path of uploaded file.
    """
    return context.resources.data_lake.upload_df(
        folder_name="nwea_map",
        file_name=extract["filename"],
        df=extract["value"]
    )


@op(
    description="Retrieve data in Ed-Fi API spec",
    required_resource_keys={"warehouse"},
    tags={"kind": "extract"},
)
def get_edfi_payloads(context, dbt_run_result, table_reference: str) -> List:
    """
    Extract BigQUery table and return the 
    resulting JSON as a dict.
    """
    df = context.resources.warehouse.download_table(table_reference)
    df_json = df.to_json(orient="records", date_format="iso")
    df_dict = json.loads(df_json)
    return df_dict


@op(
    description="POST JSON data to Ed-Fi API",
    required_resource_keys={"edfi_api_client"},
    tags={"kind": "load"},
)
def post_nwea_map_edfi_payloads(context, start_after, edfi_assessments_json: List,
    school_year: str, api_endpoint: str) -> List:
    """
    POST payloads to passed in Ed-Fi API endpoint,
    return generated Ed-Fi ids.
    """
    return context.resources.edfi_api_client.post_data(
        edfi_assessments_json, school_year, api_endpoint)


@op(
    description="Create necessary descriptors in NWEA MAP namespace",
    required_resource_keys={"edfi_api_client"},
    tags={"kind": "load"},
)
def post_nwea_map_edfi_descriptors(context, school_year: str, load_descriptors: bool) -> List:
    """
    POST payloads to passed in Ed-Fi API endpoint,
    return generated Ed-Fi ids.
    """
    descriptors = [
        {
            "endpoint": "ed-fi/assessmentReportingMethodDescriptors",
            "payload": [{
                    "codeValue": "Growth Measure YN",
                    "description": "Growth Measure YN",
                    "namespace": "uri://nwea.org/AssessmentReportingMethodDescriptor",
                    "shortDescription": "Growth Measure YN"
                }, {
                    "codeValue": "Fall-To-Winter Met Projected Growth",
                    "description": "Fall-To-Winter Met Projected Growth",
                    "namespace": "uri://nwea.org/AssessmentReportingMethodDescriptor",
                    "shortDescription": "Fall-To-Winter Met Projected Growth"
                }, {
                    "codeValue": "Fall-To-Winter Projected Growth",
                    "description": "Fall-To-Winter Projected Growth",
                    "namespace": "uri://nwea.org/AssessmentReportingMethodDescriptor",
                    "shortDescription": "Fall-To-Winter Projected Growth"
                }, {
                    "codeValue": "Goal Name",
                    "description": "Goal Name",
                    "namespace": "uri://nwea.org/AssessmentReportingMethodDescriptor",
                    "shortDescription": "Goal Name"
                }, {
                    "codeValue": "Goal Range",
                    "description": "Goal Range",
                    "namespace": "uri://nwea.org/AssessmentReportingMethodDescriptor",
                    "shortDescription": "Goal Range"
                }, {
                    "codeValue": "Standard error measurement",
                    "description": "Standard error measurement",
                    "namespace": "uri://nwea.org/AssessmentReportingMethodDescriptor",
                    "shortDescription": "Standard error measurement"
                }
            ]
        }, {
            "endpoint": "ed-fi/performanceLevelDescriptors",
            "payload": [{
                    "codeValue": "Low",
                    "description": "Low",
                    "namespace": "uri://nwea.org/PerformanceLevelDescriptor",
                    "shortDescription": "Low"
                }, {
                    "codeValue": "LoAvg",
                    "description": "LoAvg",
                    "namespace": "uri://nwea.org/PerformanceLevelDescriptor",
                    "shortDescription": "LoAvg"
                }, {
                    "codeValue": "Avg",
                    "description": "Avg",
                    "namespace": "uri://nwea.org/PerformanceLevelDescriptor",
                    "shortDescription": "Avg"
                }, {
                    "codeValue": "HiAvg",
                    "description": "HiAvg",
                    "namespace": "uri://nwea.org/PerformanceLevelDescriptor",
                    "shortDescription": "HiAvg"
                }, {
                    "codeValue": "High",
                    "description": "High",
                    "namespace": "uri://nwea.org/PerformanceLevelDescriptor",
                    "shortDescription": "High"
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
