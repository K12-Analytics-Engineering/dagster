# Dagster
This repository contains various Dagster ELT jobs.

* [Ed-Fi API to BigQuery data marts](./docs/edfi_api.md)
* [NWEA MAP API to Ed-Fi API](./docs/nwea_map.md)
* Canvas LMS API to Ed-Fi LMS API

At the root of this repo is a `.env-sample` file. Copy the file to create a `.env` file. Complete the following missing values:

* `EDFI_BASE_URL`
* `EDFI_API_KEY`
* `EDFI_API_SECRET`

You will complete the other missing values in the steps below.

## Google Cloud Configuration
Create a Google Cloud Platform (GCP) project and set the `GCP_PROJECT` variable to the Google Cloud project ID.

### Service Account
Authentication with the GCP project happens through a service account. In GCP, head to _IAM & Admin --> Service Accounts_ to create your service account.

* Click **Create Service Account**
* Choose a name (ie. dagster) and click **Create**
* Grant the service account the following roles
    * BigQuery Job User
    * BigQuery User
    * BigQuery Data Editor
    * Storage Admin
* Click **Done** 
* Select the actions menu and click **Create key**. Create a JSON key, rename to _service.json_ and store in the root of the repository.


### Google Cloud Storage
Create a Google Cloud Storage bucket that will be used to house the JSON data retrieved from the target Ed-Fi API. In GCP, head to _Cloud Storage_ and click **Create Bucket**. Once created, set the `GCS_BUCKET_DEV` variable to the newly created bucket's name (ie. dagster-dev-123).


## Development Environment
After you clone this repo, poetry can be used to create a python virtual environment with all dependencies installed.

```bash

poetry env use 3.9;
poetry install;
env $(cat .env) poetry shell;
dagit -w workspace.yaml;

```
