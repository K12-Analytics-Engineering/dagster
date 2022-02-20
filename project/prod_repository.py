from dagster import repository, ScheduleDefinition

from jobs.edfi_api_to_amt import change_query_schedule, full_run_schedule


@repository
def prod_repo():
    return [
        change_query_schedule,
        full_run_schedule
    ]
