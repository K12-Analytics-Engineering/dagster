# Dagster
This repository contains various Dagster ELT jobs.

* Ed-Fi API to BigQuery data marts
* NWEA MAP API to Ed-Fi API
* Canvas LMS API to Ed-Fi LMS API

This repository contains code that pulls data from a target Ed-Fi API and creates various data marts with dimension and fact tables organized around various business processes. If you are running the Ed-Fi API in YearSpecific mode or start with a new ODS at the start of the school year, this repository allows for extracting multiple school years of data and creates analytics tables that are multi-year.

This repository takes the viewpoint that each ODS should be limited to a single school year. It is recommended that LEAs run the Ed-Fi API mode in YearSpecific mode to have school year segmentation while having all years accessible via the Ed-Fi API.

It is also recommended that you utilize the Ed-Fi API's change query and deletes functionality. This will allow full pulls over the weekend, but only incremental pulls throughout the week.

![Ed-Fi API to AMT](/assets/edfi_api_elt.png)

[YouTube demo video](https://youtu.be/A1a7C9pDVL4)

More specifically, this repository is a [Dagster](https://dagster.io/) workspace that contains a job designed to:

1. Extract data from a set of Ed-Fi API endpoints
2. Store the raw data as JSON files in a data lake
3. Query the JSON files from a cloud data warehouse to produce:
    * Tables that represent Ed-Fi API endpoints
    * Dimension and fact tables using dimensional modeling

This repository currently supports Google Cloud Storage and BigQuery.

Dagster was chosen as the orchestration platform to run this job due to it being free and open-source, focus on workflows that are specifically data oriented, and due to its ability to abstract out intermediary IO and other resources. This means it would be possible to extend this job to support additional blob storage and cloud data warehouse services (ie. AWS S3 buckets and Snowflake).

This repository is designed to be opened on a machine with [Docker](https://www.docker.com/) installed. When opened in [Visual Studio Code](https://code.visualstudio.com/) with the [Remote Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension installed, Visual Studio Code can open the repository in its own container using all Python requirements specified in this repository's `requirements.txt` file.

At the root of this repo is a `.env-sample` file. Copy the file to create a `.env` file. Complete the following missing values:
* `EDFI_BASE_URL`
* `EDFI_API_KEY`
* `EDFI_API_SECRET`

If you are not using the survey endpoints of the Ed-Fi API, the District Hosted SIS Vendor claimset should be sufficient. If you are pulling from those endpoints, there is a JSON file in the `assets/` folder that can be used in the Ed-Fi Admin App to create a claimset for that extends District Hosted SIS Vendor to include the surveys domain.

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

## Dagster
Update `edfi_api_dev_job` in `edfi_api_to_amt.py` to set your Ed-Fi API paging limit, Ed-Fi API mode, and school year of data your ODS contains.

### Launching Dev Job
In Visual Studio Code, with the repo opened in a container, run the command below:

```bash

dagit -w workspace.yaml;

```

The command above launches dagit, Dagster's web UI. The menu top-left will allow you to access the Ed-Fi related job.

* Click **Launchpad**
* Click **Launch Run**


### Deploying to Production
This repository has been successfully deployed to production. Future documentation will be written if community interest is expressed.
