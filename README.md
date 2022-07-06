# Dagster
This repository contains various Dagster ELT jobs.

* [Ed-Fi API to BigQuery data marts](./docs/edfi_api.md)
* [NWEA MAP API to Ed-Fi API](./docs/nwea_map.md)
* [Google Forms API to Ed-Fi API](./docs/google_forms.md)


![Data stack](/assets/k12_data_stack.png)

The Dagster code that you likely came here for is the code that moves data from an Ed-Fi API into Google Cloud Storage and from there materializes the data into various data marts in Google BigQuery. This is a common ELT architecture where raw data is extracted from a source system and stored in a data lake (extract and load). From there transformation happens to turn that data into datasets built out for use in analytics.

> **Note**
> The Ed-Fi Data Standard is well defined and used across many organizations. This is done to provide vendors with a common API surface for which to build API clients against. Analytics is different. Analytics is more personal. This code is meant to provide you with a strong foundational layer for your analytics layer. You **should** modify the dbt data models to allow for your dims and facts to represent your context. This is not a plug and play piece of software. You are expected to learn python and SQL, and modify this code.

## Development Environment
Let's start by running Dagster locally on your machine. This is referred to as your `dev` environment. This is where you can make changes to the code and iterate quickly. Once you have a version of the code that's working well, that gets run in production and that's what your reports and dashboards are built from.

This Dagster code has been tested on a Mac and on a PC running Ubuntu via WSL2. Setup guides have been created for [Mac](https://github.com/K12-Analytics-Engineering/bootcamp/blob/main/docs/mac_setup_guide.md) and [Windows](https://github.com/K12-Analytics-Engineering/bootcamp/blob/main/docs/pc_setup_guide.md). These setup guides are meant to be opinionated to ensure that you're set up with a development workflow that doesn't hinder your ability to write code and iterate quickly.

The dagster and dbt repositories in this GitHub organization are meant to be used together. Click the *Use this template* button on both repositories to make a copy into your personal GitHub and clone both to your local machine.

```
├── code_folder
│   ├── dagster
|   |   |
|   |   └── ...
│   ├── dbt
|   |   |
|   |   └── ...
│   └── ...
```

At the root of both the dagster and dbt repos is a `.env-sample` file. Copy the file to create a `.env` file. In the sections below, you will be asked to complete specific values in the `.env` files.

Let's take a detour and configure your Google Cloud environment now.

### Google Cloud Configuration
Create a Google Cloud Platform (GCP) project and set the `GCP_PROJECT` variable to the Google Cloud project ID.

#### Service Account
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

After you've run through the setup guides linked above and cloned the repos, poetry can be used to create a python virtual environment with all dependencies installed.



* `EDFI_BASE_URL`
* `EDFI_API_KEY`
* `EDFI_API_SECRET`

You will complete the other missing values in the steps below.

```bash
# do this in both your dagster and dbt folders
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
