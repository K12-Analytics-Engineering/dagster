from typing import List, Dict

import google.auth
from dagster import resource
from googleapiclient import discovery


class GoogleFormsApiClient:
    """Class for interacting with Google Forms API"""

    SCOPES = [
        "https://www.googleapis.com/auth/forms.body.readonly",
        "https://www.googleapis.com/auth/forms.responses.readonly"
    ]

    def __init__(self, forms_api_key: str, forms_owner_email: str):
        self.forms_api_key=forms_api_key
        self.forms_owner_email=forms_owner_email
        self._build_credentials()


    def _build_credentials(self):
        credentials, _ = google.auth.default()
        creds = credentials.with_subject(self.forms_owner_email).with_scopes(self.SCOPES)
        self.service = discovery.build(
            serviceName='forms',
            version='v1beta',
            credentials=creds,
            discoveryServiceUrl=f'https://forms.googleapis.com/$discovery/rest?version=v1beta&key={self.forms_api_key}&labels=FORMS_BETA_TESTERS',
            num_retries=3
        )


    def get_forms_questions(self, context, form_id) -> dict:
        return self.service.forms().get(formId=form_id).execute()


    def get_forms_responses(self, context, form_id) -> List[Dict]:
        responses = list()
        request = self.service.forms().responses().list(formId=form_id, pageToken=None)

        while request is not None:
            response = request.execute()
            context.log.info(f"Retrieved  {len(response.get('responses'))} records")
            responses = responses + response.get('responses', [])
            request = self.service.forms().responses().list_next(request, response)

        return responses



@resource(
    config_schema={
        "forms_api_key": str,
        "forms_owner_email": str
    },
    description="A Google Forms client that retrieves data from their API.",
)
def google_forms_api_resource_client(context):
    return GoogleFormsApiClient(
        context.resource_config["forms_api_key"],
        context.resource_config["forms_owner_email"]
    )
