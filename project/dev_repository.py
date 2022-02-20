from dagster import repository

from jobs.edfi_api_to_amt import edfi_api_dev_job


@repository
def dev_repo():
    return [ edfi_api_dev_job ]
