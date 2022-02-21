# NWEA MAP API to Ed-Fi API
This Dagster job gets assessment data from the NWEA MAP API and sends it to a target Ed-Fi API.

ELT, as opposed to ETL, moves the transformation step in a data workflow to the data warehouse. A focus is first put on extracting data and loading it in its raw state to a cloud data warehouse. After it has landed in the warehouse, SQL is run to transform that data to what is needed for downstream needs such as analytics.

This workflow is an implementation of the above, leveraging dbt to transform raw data from NWEA MAP to the data models necessary to load that data into Ed-Fi.

![NWEA MAP to Ed-Fi](/assets/nwea_map_elt.png)

This repository is a [Dagster](https://dagster.io/) workspace that contains a job designed to:

1. Extract the `AssessmentResults.csv` file from the NWEA MAP API
2. Load the CSV into Google Cloud Storage
3. Leverage dbt and BigQuery to transform the data to match the spec of the following Ed-Fi API endpoints:
    * `/ed-fi/assessments`
    * `/ed-fi/objectiveAssessments`
    * `/ed-fi/studentAssessments`
4. Read the Ed-Fi API spec tables in BigQuery as JSON and POST to the Ed-Fi API

Complete the following environment variables in your `.env` file:

* NWEA_USERNAME
* NWEA_PASSWORD


## Data Mapping

| Ed-Fi API Endpoint       | Ed-Fi API Field              | NWEA Assessment Results CSV field                                   |
| ------------------------ | ---------------------------- | ------------------------------------------------------------------- |
| `/assessments`     | `assessmentIdentifier`       | Concatenation of `TestType`, term part of `TermName`, and `course`. <br> ie. Survey With *Goals-Winter-Reading*
| `/assessments`     | `assessmentFamily`           | *NWEA MAP Growth*                                      |
| `/assessments`     | `assessmentTitle`            | `TestName` <br> ie. *Growth: Reading 6+ CCSS 2010 V4 (No TTS)*      |
| `/assessments`     | `namespace`                  | *uri://nwea.org*                                       |
| `/assessments`     | `adaptiveAssessment`         | *TRUE*                                                 |
| `/assessments`     | `period`                     | Sets assessmentPeriodDescriptor to 1 of 3 Ed-Fi core descriptors depending on test term. <br> *BOY*, *MOY*, or *EOY* <br> ie. If Fall term, then *uri://ed-fi.org/AssessmentPeriodDescriptor#BOY* |
| `/assessments`     | `academicSubjects`           | Sets academicSubjectDescriptor to Reading or Math depending on test taken |
| `/assessments`     | `performanceLevels`          |   |
| `/assessments`     | `scores`                     |   |
| `/objectiveAssessments`     | ``       |   |
| `/objectiveAssessments`     | ``       |   |
| `/objectiveAssessments`     | ``       |   |
| `/objectiveAssessments`     | ``       |   |
| `/objectiveAssessments`     | ``       |   |
| `/studentAssessments`     | `studentAssessmentIdentifier`       | `TestID` |
| `/studentAssessments`     | `assessmentReference`               |   |
| `/studentAssessments`     | `schoolYearTypeReference.SchoolYear`           | Uses school year from `TermName` field. <br> ie. If `TermName` is *Fall 2021-2022*, *2022* will be used |
| `/studentAssessments`     | `studentReference.studentUniqueId`           | `StudentID` |
| `/studentAssessments`     | `administrationDate`         | `TestStartDate` |
| `/studentAssessments`     | `performanceLevels[0].assessmentReportingMethodDescriptor`       | *uri://nwea.org/AssessmentReportingMethodDescriptor#Fall-To-Winter Met Projected Growth*   |
| `/studentAssessments`     | `performanceLevels[0].performanceLevelDescriptor`       | If `FallToWinterMetProjectedGrowth` is *Yes* or *Yes**:<br> *uri://ed-fi.org/PerformanceLevelDescriptor#Pass* <br><br> If `FallToWinterMetProjectedGrowth` is *No* or *No**:<br> *uri://ed-fi.org/PerformanceLevelDescriptor#Fail*  |
| `/studentAssessments`     | `scoreResults[0].assessmentReportingMethodDescriptor`       | *uri://ed-fi.org/AssessmentReportingMethodDescriptor#RIT scale score* |
| `/studentAssessments`     | `scoreResults[0].resultDatatypeTypeDescriptor`              | *uri://ed-fi.org/ResultDatatypeTypeDescriptor#Integer*                |
| `/studentAssessments`     | `scoreResults[0].result`                                    | `TestRITScore`                                                        |
| `/studentAssessments`     | `scoreResults[1].assessmentReportingMethodDescriptor`       | *uri://ed-fi.org/AssessmentReportingMethodDescriptor#Percentile*      |
| `/studentAssessments`     | `scoreResults[1].resultDatatypeTypeDescriptor`              | *uri://ed-fi.org/ResultDatatypeTypeDescriptor#Integer*                |
| `/studentAssessments`     | `scoreResults[1].result`                                    | `TestPercentile`                                                      |
| `/studentAssessments`     | `studentObjectiveAssessments[0].objectiveAssessmentReference.assessmentIdentifier` | ie. Survey With *Goals-Winter-Reading*  |
| `/studentAssessments`     | `studentObjectiveAssessments[0].objectiveAssessmentReference.identificationCode` | `Goal1Name`  |
| `/studentAssessments`     | `studentObjectiveAssessments[0].objectiveAssessmentReference.namespace` | *uri://nwea.org*  |
| `/studentAssessments`     | `studentObjectiveAssessments[0].performanceLevels[0].assessmentReportingMethodDescriptor` | *uri://ed-fi.org/AssessmentReportingMethodDescriptor#Proficiency level*            |
| `/studentAssessments`     | `studentObjectiveAssessments[0].performanceLevels[0].performanceLevelDescriptor`          | Concatenation of *uri://nwea.org/PerformanceLevelDescriptor#* and `Goal1Adjective` |
| `/studentAssessments`     | `studentObjectiveAssessments[0].scoreResults[0].assessmentReportingMethodDescriptor`      | *uri://ed-fi.org/AssessmentReportingMethodDescriptor#RIT scale score*              |
| `/studentAssessments`     | `studentObjectiveAssessments[0].scoreResults[0].resultDatatypeTypeDescriptor`             | *uri://ed-fi.org/ResultDatatypeTypeDescriptor#Integer*                             |
| `/studentAssessments`     | `studentObjectiveAssessments[0].scoreResults[0].result`                                   | `Goal1RitScore`                                                                    |
| `/studentAssessments`     | `studentObjectiveAssessments[0].objectiveAssessmentReference.assessmentIdentifier` | ie. Survey With *Goals-Winter-Reading*  |
| `/studentAssessments`     | `studentObjectiveAssessments[0].objectiveAssessmentReference.identificationCode` | `Goal2Name`  |
| `/studentAssessments`     | `studentObjectiveAssessments[0].objectiveAssessmentReference.namespace` | *uri://nwea.org*  |
| `/studentAssessments`     | `studentObjectiveAssessments[1].performanceLevels[0].assessmentReportingMethodDescriptor` | *uri://ed-fi.org/AssessmentReportingMethodDescriptor#Proficiency level*            |
| `/studentAssessments`     | `studentObjectiveAssessments[1].performanceLevels[0].performanceLevelDescriptor`          | Concatenation of *uri://nwea.org/PerformanceLevelDescriptor#* and `Goal1Adjective` |
| `/studentAssessments`     | `studentObjectiveAssessments[1].scoreResults[0].assessmentReportingMethodDescriptor`      | *uri://ed-fi.org/AssessmentReportingMethodDescriptor#RIT scale score*              |
| `/studentAssessments`     | `studentObjectiveAssessments[1].scoreResults[0].resultDatatypeTypeDescriptor`             | *uri://ed-fi.org/ResultDatatypeTypeDescriptor#Integer*                             |
| `/studentAssessments`     | `studentObjectiveAssessments[1].scoreResults[0].result`                                   | `Goal2RitScore`                                                                    |
| `/studentAssessments`     | `studentObjectiveAssessments[0].objectiveAssessmentReference.assessmentIdentifier` | ie. Survey With *Goals-Winter-Reading*  |
| `/studentAssessments`     | `studentObjectiveAssessments[0].objectiveAssessmentReference.identificationCode` | `Goal3Name`  |
| `/studentAssessments`     | `studentObjectiveAssessments[0].objectiveAssessmentReference.namespace` | *uri://nwea.org*  |
| `/studentAssessments`     | `studentObjectiveAssessments[2].performanceLevels[0].assessmentReportingMethodDescriptor` | *uri://ed-fi.org/AssessmentReportingMethodDescriptor#Proficiency level*            |
| `/studentAssessments`     | `studentObjectiveAssessments[2].performanceLevels[0].performanceLevelDescriptor`          | Concatenation of *uri://nwea.org/PerformanceLevelDescriptor#* and `Goal1Adjective` |
| `/studentAssessments`     | `studentObjectiveAssessments[2].scoreResults[0].assessmentReportingMethodDescriptor`      | *uri://ed-fi.org/AssessmentReportingMethodDescriptor#RIT scale score*              |
| `/studentAssessments`     | `studentObjectiveAssessments[2].scoreResults[0].resultDatatypeTypeDescriptor`             | *uri://ed-fi.org/ResultDatatypeTypeDescriptor#Integer*                             |
| `/studentAssessments`     | `studentObjectiveAssessments[2].scoreResults[0].result`                                   | `Goal2RitScore`                                                                    |
| `/studentAssessments`     | `studentObjectiveAssessments[0].objectiveAssessmentReference.assessmentIdentifier` | ie. Survey With *Goals-Winter-Reading*  |
| `/studentAssessments`     | `studentObjectiveAssessments[0].objectiveAssessmentReference.identificationCode` | `Goal4Name`  |
| `/studentAssessments`     | `studentObjectiveAssessments[0].objectiveAssessmentReference.namespace` | *uri://nwea.org*  |
| `/studentAssessments`     | `studentObjectiveAssessments[3].performanceLevels[0].assessmentReportingMethodDescriptor` | *uri://ed-fi.org/AssessmentReportingMethodDescriptor#Proficiency level*            |
| `/studentAssessments`     | `studentObjectiveAssessments[3].performanceLevels[0].performanceLevelDescriptor`          | Concatenation of *uri://nwea.org/PerformanceLevelDescriptor#* and `Goal1Adjective` |
| `/studentAssessments`     | `studentObjectiveAssessments[3].scoreResults[0].assessmentReportingMethodDescriptor`      | *uri://ed-fi.org/AssessmentReportingMethodDescriptor#RIT scale score*              |
| `/studentAssessments`     | `studentObjectiveAssessments[3].scoreResults[0].resultDatatypeTypeDescriptor`             | *uri://ed-fi.org/ResultDatatypeTypeDescriptor#Integer*                             |
| `/studentAssessments`     | `studentObjectiveAssessments[3].scoreResults[0].result`                                   | `Goal2RitScore`                                                                    |
