from dagster import repository

from jobs.edfi_api import edfi_api_dev_job
from jobs.google_forms import google_forms_dev_job
from jobs.nwea_map import nwea_map_dev_job


@repository
def dev_repo():
    return [
        edfi_api_dev_job,
        google_forms_dev_job,
        nwea_map_dev_job]
