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

## Google Cloud Configuration
Create a Google Cloud Platform (GCP) project and set the following variables in your `.env` file:

* `GOOGLE_CLOUD_PROJECT` to the Google Cloud project ID
* `EDFI_BASE_URL` to your Ed-Fi API base URL
* `EDFI_API_KEY` to your Ed-Fi API key
* `EDFI_API_SECRET` to your Ed-Fi API secret
* `DBT_PROFILES_DIR` to the base path of your dbt folder
* `DBT_PROJECT_DIR` to the base path of your dbt folder
* `PYTHONPATH` to the project folder in your dagster folder

> **Note**
> If your machine is setup correctly, the `.env` should automatically be read into your environment when you `cd` into the dagster directory. `cd` into the dagster folder and run `echo $GOOGLE_CLOUD_PROJECT`. If your Google Cloud project ID does not print to the terminal, stop and troubleshoot.

### Enable APIs
Run the commands below to enable the APIs necessary for this repository.

```sh
gcloud config set project $GOOGLE_CLOUD_PROJECT;
gcloud config set compute/region us-central1;

gcloud services enable artifactregistry.googleapis.com;
gcloud services enable cloudbuild.googleapis.com;
gcloud services enable compute.googleapis.com;
gcloud services enable container.googleapis.com;
gcloud services enable servicenetworking.googleapis.com;
gcloud services enable sqladmin.googleapis.com;
gcloud services enable iamcredentials.googleapis.com;
```

### Service Account
Authentication with the GCP project happens through a service account. The commands below will create a service account and download a JSON key. This service account will also be used via Workload Identity when deployed in production on GKE.

```sh
gcloud config set project $GOOGLE_CLOUD_PROJECT;
gcloud iam service-accounts create dagster \
  --display-name="dagster";

export SA_EMAIL=`gcloud iam service-accounts list --format='value(email)' \
  --filter='displayName:dagster'`

gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member serviceAccount:$SA_EMAIL \
  --role roles/bigquery.jobUser;

gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member serviceAccount:$SA_EMAIL \
  --role roles/bigquery.user;

gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member serviceAccount:$SA_EMAIL \
  --role roles/bigquery.dataEditor;

gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member serviceAccount:$SA_EMAIL \
  --role roles/storage.admin;

gcloud iam service-accounts keys create service.json \
    --iam-account=$SA_EMAIL;
```

Store the downloaded JSON file in the root of the repository. Set the `GOOGLE_APPLICATION_CREDENTIALS` variable in your `.env` file to point to your service account JSON file.


### Google Cloud Storage
Google Cloud Storage will be used to house the JSON data retrieved from the target Ed-Fi API. Create two buckets:

```sh
gsutil mb "gs://dagster-dev-${GOOGLE_CLOUD_PROJECT}";
gsutil mb "gs://dagster-prod-${GOOGLE_CLOUD_PROJECT}";
```

Set the `GCS_BUCKET_DEV` and `GCS_BUCKET_PROD` variables to the newly created bucket names.

You should now have all variables completed in the `.env` files in both your dagster and dbt folders.

The setup guides recommend using poetry to manage your python virtual environments.
```bash
# do this in both your dagster and dbt folders
poetry env use 3.9;
poetry install;
```

> **Note**
> If your machine is setup correctly, when you `cd` into the dagster folder, your python virtual environment should automatically be loaded and your `.env` file should automatically be read into your environment. If that does not happen, stop and troubleshoot.

You should now be able to launch dagit via the command below.
```bash
dagit -w workspace.yaml;
```


