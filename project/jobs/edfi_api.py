import os

from dagster import (
    fs_io_manager,
    graph,
    multiprocess_executor,
    RunRequest,
    schedule,
    ScheduleEvaluationContext
)
from dagster_dbt import dbt_cli_resource
from dagster_gcp.gcs.io_manager import gcs_pickle_io_manager
from dagster_gcp.gcs.resources import gcs_resource

from ops.edfi import (
    api_endpoint_generator,
    get_current_max_change_version,
    get_previous_max_change_version,
    extract_and_upload_data,
    run_edfi_models,
    test_edfi_models
)
from resources.edfi_api_resource import edfi_api_resource_client
from resources.bq_resource import bq_client
from resources.gcs_resource import gcs_client


@graph(
    name="edfi_api_to_marts",
    description=(
        "Gets data from the Ed-Fi API and "
        "loads to Google Cloud Storage. Runs dbt "
        "to create various data marts "
        "in BigQuery."
    )
)
def edfi_api_to_marts(edfi_api_endpoints, school_year, use_change_queries):

    previous_max_change_version = get_previous_max_change_version(school_year)
    newest_max_change_version = get_current_max_change_version(
        start_after=previous_max_change_version,
        school_year=school_year,
        use_change_queries=use_change_queries)

    retrieved_data = api_endpoint_generator(
        edfi_api_endpoints=edfi_api_endpoints,
        use_change_queries=use_change_queries).map(
            lambda api_endpoint: extract_and_upload_data(
                api_endpoint=api_endpoint,
                school_year=school_year,
                previous_change_version=previous_max_change_version,
                newest_change_version=newest_max_change_version,
                use_change_queries=use_change_queries
            )
    ).collect()

    dbt_run_result = run_edfi_models(retrieved_data)
    test_edfi_models(start_after=dbt_run_result)


edfi_api_endpoints = [
    {"endpoint": "/ed-fi/assessments", "table_name": "base_edfi_assessments"},
    {"endpoint": "/ed-fi/assessments/deletes", "table_name": "base_edfi_assessments"},
    {"endpoint": "/ed-fi/localEducationAgencies", "table_name": "base_edfi_local_education_agencies"},
    {"endpoint": "/ed-fi/localEducationAgencies/deletes", "table_name": "base_edfi_local_education_agencies"},
    {"endpoint": "/ed-fi/calendars", "table_name": "base_edfi_calendars" },
    {"endpoint": "/ed-fi/calendars/deletes", "table_name": "base_edfi_calendars" },
    {"endpoint": "/ed-fi/calendarDates", "table_name": "base_edfi_calendar_dates" },
    {"endpoint": "/ed-fi/calendarDates/deletes", "table_name": "base_edfi_calendar_dates" },
    {"endpoint": "/ed-fi/courses", "table_name": "base_edfi_courses" },
    {"endpoint": "/ed-fi/courses/deletes", "table_name": "base_edfi_courses" },
    {"endpoint": "/ed-fi/courseOfferings", "table_name": "base_edfi_course_offerings" },
    {"endpoint": "/ed-fi/courseOfferings/deletes", "table_name": "base_edfi_course_offerings" },
    {"endpoint": "/ed-fi/disciplineActions", "table_name": "base_edfi_discipline_actions" },
    {"endpoint": "/ed-fi/disciplineActions/deletes", "table_name": "base_edfi_discipline_actions" },
    {"endpoint": "/ed-fi/disciplineIncidents", "table_name": "base_edfi_discipline_incidents" },
    {"endpoint": "/ed-fi/disciplineIncidents/deletes", "table_name": "base_edfi_discipline_incidents" },
    {"endpoint": "/ed-fi/grades", "table_name": "base_edfi_grades" },
    {"endpoint": "/ed-fi/grades/deletes", "table_name": "base_edfi_grades" },
    {"endpoint": "/ed-fi/gradingPeriods", "table_name": "base_edfi_grading_periods" },
    {"endpoint": "/ed-fi/gradingPeriods/deletes", "table_name": "base_edfi_grading_periods" },
    {"endpoint": "/ed-fi/gradingPeriodDescriptors", "table_name": "base_edfi_grading_period_descriptors" },
    {"endpoint": "/ed-fi/gradingPeriodDescriptors/deletes", "table_name": "base_edfi_grading_period_descriptors" },
    {"endpoint": "/ed-fi/objectiveAssessments", "table_name": "base_edfi_objective_assessments" },
    {"endpoint": "/ed-fi/objectiveAssessments/deletes", "table_name": "base_edfi_objective_assessments" },
    {"endpoint": "/ed-fi/parents", "table_name": "base_edfi_parents" },
    {"endpoint": "/ed-fi/parents/deletes", "table_name": "base_edfi_parents" },
    {"endpoint": "/ed-fi/programs", "table_name": "base_edfi_programs" },
    {"endpoint": "/ed-fi/programs/deletes", "table_name": "base_edfi_programs" },
    {"endpoint": "/ed-fi/schools", "table_name": "base_edfi_schools" },
    {"endpoint": "/ed-fi/schools/deletes", "table_name": "base_edfi_schools" },
    {"endpoint": "/ed-fi/schoolYearTypes", "table_name": "base_edfi_school_year_types" },
    {"endpoint": "/ed-fi/sections", "table_name": "base_edfi_sections" },
    {"endpoint": "/ed-fi/sections/deletes", "table_name": "base_edfi_sections" },
    {"endpoint": "/ed-fi/staffs", "table_name": "base_edfi_staffs" },
    {"endpoint": "/ed-fi/staffs/deletes", "table_name": "base_edfi_staffs" },
    {"endpoint": "/ed-fi/staffDisciplineIncidentAssociations", "table_name": "base_edfi_staff_discipline_incident_associations" },
    {"endpoint": "/ed-fi/staffDisciplineIncidentAssociations/deletes", "table_name": "base_edfi_staff_discipline_incident_associations" },
    {"endpoint": "/ed-fi/staffEducationOrganizationAssignmentAssociations", "table_name": "base_edfi_staff_education_organization_assignment_associations" },
    {"endpoint": "/ed-fi/staffEducationOrganizationAssignmentAssociations/deletes", "table_name": "base_edfi_staff_education_organization_assignment_associations" },
    {"endpoint": "/ed-fi/staffSchoolAssociations", "table_name": "base_edfi_staff_school_associations" },
    {"endpoint": "/ed-fi/staffSchoolAssociations/deletes", "table_name": "base_edfi_staff_school_associations" },
    {"endpoint": "/ed-fi/staffSectionAssociations", "table_name": "base_edfi_staff_section_associations" },
    {"endpoint": "/ed-fi/staffSectionAssociations/deletes", "table_name": "base_edfi_staff_section_associations" },
    {"endpoint": "/ed-fi/students", "table_name": "base_edfi_students" },
    {"endpoint": "/ed-fi/students/deletes", "table_name": "base_edfi_students" },
    {"endpoint": "/ed-fi/studentEducationOrganizationAssociations", "table_name": "base_edfi_student_education_organization_associations" },
    {"endpoint": "/ed-fi/studentEducationOrganizationAssociations/deletes", "table_name": "base_edfi_student_education_organization_associations" },
    {"endpoint": "/ed-fi/studentSchoolAssociations", "table_name": "base_edfi_student_school_associations" },
    {"endpoint": "/ed-fi/studentSchoolAssociations/deletes", "table_name": "base_edfi_student_school_associations" },
    {"endpoint": "/ed-fi/studentAssessments/deletes", "table_name": "base_edfi_student_assessments" },
    {"endpoint": "/ed-fi/studentAssessments", "table_name": "base_edfi_student_assessments" },
    {"endpoint": "/ed-fi/studentDisciplineIncidentAssociations", "table_name": "base_edfi_student_discipline_incident_associations" }, # deprecated
    {"endpoint": "/ed-fi/studentDisciplineIncidentAssociations/deletes", "table_name": "base_edfi_student_discipline_incident_associations" }, # deprecated
    # {"endpoint": "/ed-fi/studentDisciplineIncidentBehaviorAssociations", "table_name": "base_edfi_student_discipline_incident_behavior_associations" }, # implemented in v5.2
    # {"endpoint": "/ed-fi/studentDisciplineIncidentNonOffenderAssociations", "table_name": "base_edfi_student_discipline_incident_non_offender_associations" }, # implemented in v5.2
    {"endpoint": "/ed-fi/studentParentAssociations", "table_name": "base_edfi_student_parent_associations" },
    {"endpoint": "/ed-fi/studentParentAssociations/deletes", "table_name": "base_edfi_student_parent_associations" },
    {"endpoint": "/ed-fi/studentProgramAssociations", "table_name": "base_edfi_student_program_associations" },
    {"endpoint": "/ed-fi/studentProgramAssociations/deletes", "table_name": "base_edfi_student_program_associations" },
    {"endpoint": "/ed-fi/studentSchoolAttendanceEvents", "table_name": "base_edfi_student_school_attendance_events" },
    {"endpoint": "/ed-fi/studentSchoolAttendanceEvents/deletes", "table_name": "base_edfi_student_school_attendance_events" },
    {"endpoint": "/ed-fi/studentSectionAssociations", "table_name": "base_edfi_student_section_associations" },
    {"endpoint": "/ed-fi/studentSectionAssociations/deletes", "table_name": "base_edfi_student_section_associations" },
    {"endpoint": "/ed-fi/studentSectionAttendanceEvents", "table_name": "base_edfi_student_section_attendance_events" },
    {"endpoint": "/ed-fi/studentSectionAttendanceEvents/deletes", "table_name": "base_edfi_student_section_attendance_events" },
    {"endpoint": "/ed-fi/studentSpecialEducationProgramAssociations", "table_name": "base_edfi_student_special_education_program_associations" },
    {"endpoint": "/ed-fi/studentSpecialEducationProgramAssociations/deletes", "table_name": "base_edfi_student_special_education_program_associations" },
    # {"endpoint": "/ed-fi/surveys", "table_name": "base_edfi_surveys" },
    # {"endpoint": "/ed-fi/surveys/deletes", "table_name": "base_edfi_surveys" },
    # {"endpoint": "/ed-fi/surveyQuestions", "table_name": "base_edfi_survey_questions" },
    # {"endpoint": "/ed-fi/surveyQuestions/deletes", "table_name": "base_edfi_survey_questions" },
    # {"endpoint": "/ed-fi/surveyResponses", "table_name": "base_edfi_survey_responses" },
    # {"endpoint": "/ed-fi/surveyResponses/deletes", "table_name": "base_edfi_survey_responses" },
    # {"endpoint": "/ed-fi/surveyQuestionResponses", "table_name": "base_edfi_survey_question_responses" },
    # {"endpoint": "/ed-fi/surveyQuestionResponses/deletes", "table_name": "base_edfi_survey_question_responses" },
    {"endpoint": "/ed-fi/sessions", "table_name": "base_edfi_sessions" },
    {"endpoint": "/ed-fi/sessions/deletes", "table_name": "base_edfi_sessions" },
    {"endpoint": "/ed-fi/cohortTypeDescriptors", "table_name": "base_edfi_descriptors"},
    {"endpoint": "/ed-fi/cohortTypeDescriptors/deletes", "table_name": "base_edfi_descriptors"},
    {"endpoint": "/ed-fi/disabilityDescriptors", "table_name": "base_edfi_descriptors"},
    {"endpoint": "/ed-fi/disabilityDescriptors/deletes", "table_name": "base_edfi_descriptors"},
    {"endpoint": "/ed-fi/languageDescriptors", "table_name": "base_edfi_descriptors"},
    {"endpoint": "/ed-fi/languageDescriptors/deletes", "table_name": "base_edfi_descriptors"},
    {"endpoint": "/ed-fi/languageUseDescriptors", "table_name": "base_edfi_descriptors"},
    {"endpoint": "/ed-fi/languageUseDescriptors/deletes", "table_name": "base_edfi_descriptors"},
    {"endpoint": "/ed-fi/raceDescriptors", "table_name": "base_edfi_descriptors"},
    {"endpoint": "/ed-fi/raceDescriptors/deletes", "table_name": "base_edfi_descriptors"}
]

edfi_api_dev_job = edfi_api_to_marts.to_job(
    executor_def=multiprocess_executor.configured({
        "max_concurrent": 8
    }),
    resource_defs={
        "gcs": gcs_resource,
        "io_manager": fs_io_manager,
        "edfi_api_client": edfi_api_resource_client.configured({
            "base_url": os.getenv("EDFI_BASE_URL"),
            "api_key": os.getenv("EDFI_API_KEY"),
            "api_secret": os.getenv("EDFI_API_SECRET"),
            "api_page_limit": 5000,
            "api_mode": "YearSpecific", # DistrictSpecific, SharedInstance, YearSpecific
            "api_version": "5.3"
        }),
        "data_lake": gcs_client.configured({
            "staging_gcs_bucket": os.getenv("GCS_BUCKET_DEV")
        }),
        "warehouse": bq_client.configured({
            "dataset": "dev_staging"
        }),
        "dbt": dbt_cli_resource.configured({
            "project_dir": os.getenv("DBT_PROJECT_DIR"),
            "profiles_dir": os.getenv("DBT_PROFILES_DIR"),
            "target": "dev"
        })
    },
    config={
        "inputs": {
            "edfi_api_endpoints": { "value": edfi_api_endpoints },
            "school_year": { "value": 2022 },
            "use_change_queries": { "value": True }
        },
        "ops": {
            "get_previous_max_change_version": {
                "config": {
                    "table_reference": "dev_staging.edfi_processed_change_versions"
                }
            },
        }
    },
)

edfi_api_prod_job = edfi_api_to_marts.to_job(
    executor_def=multiprocess_executor.configured({
        "max_concurrent": 8
    }),
    resource_defs={
        "gcs": gcs_resource,
        "io_manager": gcs_pickle_io_manager.configured({
            "gcs_bucket": os.getenv("GCS_BUCKET_PROD"),
            "gcs_prefix": "dagster_io"
        }),
        "edfi_api_client": edfi_api_resource_client.configured({
            "base_url": os.getenv("EDFI_BASE_URL"),
            "api_key": os.getenv("EDFI_API_KEY"),
            "api_secret": os.getenv("EDFI_API_SECRET"),
            "api_page_limit": 5000,
            "api_mode": "YearSpecific" # DistrictSpecific, SharedInstance, YearSpecific
        }),
        "data_lake": gcs_client.configured({
            "staging_gcs_bucket": os.getenv("GCS_BUCKET_PROD")
        }),
        "warehouse": bq_client.configured({
            "dataset": "prod_staging"
        }),
        "dbt": dbt_cli_resource.configured({
            "project_dir": os.getenv("DBT_PROJECT_DIR"),
            "profiles_dir": os.getenv("DBT_PROFILES_DIR"),
            "target": "prod"
        })
    },
    config={
        "inputs": {
            "edfi_api_endpoints": { "value": edfi_api_endpoints },
            "school_year": { "value": 2022 },
            "use_change_queries": { "value": True }
        },
        "ops": {
            "get_previous_max_change_version": {
                "config": {
                    "table_reference": "prod_staging.edfi_processed_change_versions"
                }
            },
        }
    },
)

@schedule(job=edfi_api_prod_job, cron_schedule="0 6 * * 7,1-5")
def change_query_schedule(context: ScheduleEvaluationContext):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=None,
        run_config={
            "inputs": {
                "use_change_queries": {
                    "value": True
                }
            },
            "ops": {}
        },
        tags={"date": scheduled_date},
    )

@schedule(job=edfi_api_prod_job, cron_schedule="0 6 * * 6")
def full_run_schedule(context: ScheduleEvaluationContext):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=None,
        run_config={
            "inputs": {
                "use_change_queries": {
                    "value": False
                }
            },
            "ops": {}
        },
        tags={"date": scheduled_date},
    )
