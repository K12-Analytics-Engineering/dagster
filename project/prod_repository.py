from dagster import repository, ScheduleDefinition

from jobs.edfi_api import change_query_schedule, full_run_schedule


@repository
def prod_repo():
    return [
        change_query_schedule,
        full_run_schedule
    ]
