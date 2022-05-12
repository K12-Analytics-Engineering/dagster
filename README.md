# Dagster
This repository contains various Dagster ELT jobs.

* [Ed-Fi API to BigQuery data marts](./docs/edfi_api.md)
* [NWEA MAP API to Ed-Fi API](./docs/nwea_map.md)
* [Google Forms API to Ed-Fi API](./docs/google_forms.md)

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
This Dagster workspace has been tested on a Mac and on a PC running Ubuntu via WSL2. Setup guides have been created for [Mac](https://github.com/K12-Analytics-Engineering/bootcamp/blob/main/docs/mac_setup_guide.md) and [Windows](https://github.com/K12-Analytics-Engineering/bootcamp/blob/main/docs/pc_setup_guide.md). After you clone this repo, poetry can be used to create a python virtual environment with all dependencies installed.

```bash

poetry env use 3.9;
poetry install;
poetry shell;  #windows
env $(cat .env) poetry shell; # mac
dagit -w workspace.yaml;

```


## Production
The deployment strategy currently being explored for production runs is to use a Google Compute Engine VM for the Dagster daemon and dagit, and a Cloud SQL instance running PostgreSQL for the metadata storage. Specifics and pricing can be seen [here](https://github.com/K12-Analytics-Engineering/bootcamp/blob/main/docs/implementation_choices_and_cost.md).

Enable Artifact Registry and create a repository:
```sh
gcloud services enable artifactregistry.googleapis.com;

gcloud artifacts repositories create dagster \
    --repository-format=docker \
    --location=us-central1;
```

Add your dbt repo as a git submodule:
```sh
git submodule add https://github.com/K12-Analytics-Engineering/dbt.git dbt;
```

Create a Cloud SQL instance with a dagster database:
```sh
gcloud beta sql instances create \
    --zone us-central1-c \
    --database-version POSTGRES_13 \
    --tier db-f1-micro \
    --storage-auto-increase \
    --backup-start-time 08:00 dagster;

gcloud sql databases create 'dagster' --instance=dagster;
```

Build and push a Docker image to your Artifact Registry:
```sh
gcloud builds submit --config=cloudbuild.yaml;
```

Create a Compute Engine VM from the new image:
```sh
gcloud beta compute instances create-with-container dagster \
    --zone=us-central1-c \
    --machine-type=e2-medium \
    --provisioning-model=SPOT \
    --container-image=us-central1-docker.pkg.dev/${GCP_PROJECT}/dagster/dagster \
    --service-account=dagster@${GCP_PROJECT}.iam.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/trace.append,https://www.googleapis.com/auth/sqlservice.admin,https://www.googleapis.com/auth/devstorage.full_control \
    --container-restart-policy=always;
```

To view Dagit, the command below should be run from Cloud Shell. This will SSH into the VM and forward the port.
```sh
gcloud compute ssh --zone "us-central1-c" "dagster"  --project ${GOOGLE_CLOUD_PROJECT} -- -NL 8080:localhost:3000
```

Update GCE to use a new image
```sh
gcloud compute ssh dagster --command 'docker system prune -f -a';
gcloud compute instances update-container dagster --zone us-central1-c --container-image us-central1-docker.pkg.dev/${GOOGLE_CLOUD_PROJECT}/dagster/dagster --project ${GOOGLE_CLOUD_PROJECT};
```
