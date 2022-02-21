# Ed-Fi API to BigQuery data marts
This Dagster job gets data from a target Ed-Fi API and uploads it to Google Cloud Storage. In BigQuery via dbt, various data marts with dimension and fact tables organized around various business processes. If you are running the Ed-Fi API in YearSpecific mode or start with a new ODS at the start of the school year, this repository allows for extracting multiple school years of data and creates analytics tables that are multi-year.

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

If you are not using the survey endpoints of the Ed-Fi API, the District Hosted SIS Vendor claimset should be sufficient. If you are pulling from those endpoints, there is a JSON file in the `assets/` folder that can be used in the Ed-Fi Admin App to create a claimset for that extends District Hosted SIS Vendor to include the surveys domain.
