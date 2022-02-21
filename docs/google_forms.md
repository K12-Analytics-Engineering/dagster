# Google Forms API to Ed-Fi API

This Dagster job gets survey data from the Google Forms API and sends it to a target Ed-Fi API.

![Google Forms to Ed-Fi](/assets/google_forms_etl.png)

This job is designed to:

1. Extract Forms questions and responses from the Google Forms API
2. Load the raw JSON into Google Cloud Storage
3. Leverage dbt and BigQuery to transform the data to match the spec of the following Ed-Fi API endpoints:
    * `/ed-fi/surveys`
    * `/ed-fi/surveyQuestions`
    * `/ed-fi/surveyResponses`
    * `/ed-fi/surveyQuestionResponses`
4. Read the Ed-Fi API spec tables in BigQuery as JSON and POST to the Ed-Fi API

There is a JSON file in the assets/ folder that can be used in the Ed-Fi Admin App to create a claimset for the application created in Ed-Fi's Admin App.


### Google Forms configuration
Run the command below or head [here](https://console.cloud.google.com/apis/api/forms.googleapis.com/overview) to enable the Forms API in your Google Cloud project.

```bash
gcloud services enable forms.googleapis.com;
```

Create an API key under https://console.cloud.google.com/apis/credentials

Set `FORMS_API_KEY` in your `.env` file to the newly generated API key.

In the Google Workspace Admin console > Security > API Controls > Manage Domain Wide Delegation
* Click *Add new* and grant the service account ID access to the scopes below
    * https://www.googleapis.com/auth/forms.body.readonly
    * https://www.googleapis.com/auth/forms.responses.readonly


## Launching Dev Job
In Dagster, a graph determines the order and dependencies of the ops. Jobs are then built off the graph and it is those jobs that get scheduled. In `project/jobs/google_forms.py` you will find a `google_forms_dev_job` variable that defines the dev job that can be run on your local machine. Read through the config changing values as necessary. Two that will need to be changed are `forms_owner_email` and `form_ids`.


## Data Mapping

| Ed-Fi API endpoint       | Ed-Fi API field              | Google Forms API endpoint | Google Forms API field                                  |
| ------------------------ | ---------------------------- | --------------------------|-------------------------------------------------------- |
| `/surveys`               | `namespace`                  | N/A                       | *uri://forms.google.com*                                |
| `/surveys`               | `surveyIdentifier`           | `/forms/{formId}`         | `formId`                                                |
| `/surveys`               | `schoolYearTypeReference`    | N/A                       | *2022*                                                  |
| `/surveys`               | `surveyTitle`                | `/forms/{formId}`         | `info.documentTitle`                                    |
| `/surveyQuestions`               | `questionCode`                                    | `/forms/{formId}`         | `questionId`           |
| `/surveyQuestions`               | `surveyReference.namespace`                       | N/A         | *uri://forms.google.com*             |
| `/surveyQuestions`               | `surveyReference.surveyIdentifier`                | `/forms/{formId}`         | `formId`               |
| `/surveyQuestions`               | `surveyReference.surveyIdentifier`                | `/forms/{formId}`         | `formId`               |
| `/surveyResponses`               | ``                | ``         | ``                                    |
| `/surveyResponses`               | ``                | ``         | ``                                    |
| `/surveyResponses`               | ``                | ``         | ``                                    |
| `/surveyQuestionResponses`               | ``                | ``         | ``                                    |
| `/surveyQuestionResponses`               | ``                | ``         | ``                                    |
| `/surveyQuestionResponses`               | ``                | ``         | ``                                    |
