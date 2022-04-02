from dagster import repository

from jobs.edfi_api import edfi_api_prod_job
# from jobs.nwea_map import nwea_map_dev_job


@repository
def prod_repo():
    return [edfi_api_prod_job]
