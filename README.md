# Climate Data Pipeline

[![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=fff)](#)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=fff)](#)
[![Postgres](https://img.shields.io/badge/Postgres-%23316192.svg?logo=postgresql&logoColor=white)](#)
[![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=fff)](#)
[![Google Cloud](https://img.shields.io/badge/Google%20Cloud-%234285F4.svg?logo=google-cloud&logoColor=white)](#)
[![Terraform](https://img.shields.io/badge/Terraform-844FBA?logo=terraform&logoColor=fff)](#)
[![Bash](https://img.shields.io/badge/Bash-4EAA25?logo=gnubash&logoColor=fff)](#)
[![Grafana](https://img.shields.io/badge/Grafana-F46800?logo=grafana&logoColor=white)](#)
[![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?logo=prometheus&logoColor=white)](#)

## Overview

Climate Data Pipeline is an end-to-end data engineering project for ingesting, transforming, validating, and serving daily climate and environmental observations for cities at scale.

The pipeline combines city metadata from [Simple Maps](https://simplemaps.com/data/world-cities) with climate, air-quality, river discharge, and land-surface observations from [Open Meteo](https://open-meteo.com/en/docs/climate-api) and [NASA Power](https://power.larc.nasa.gov/docs/services/api/temporal/daily/). It orchestrates processing with Apache Airflow and accounts for API quotas, different source publication schedules, historical backfills, data-quality failures, and late-arriving observations before loading analysis-ready data into PostgreSQL locally or BigQuery in Google Cloud. 

The project was designed to provide a reproducible way to consolidate climate and environmental observations from multiple public sources into datasets suitable for analysis, research, and modelling.

The dataset includes parameters related to the **[Essential Climate Variables (ECV)](https://gcos.wmo.int/site/global-climate-observing-system-gcos/essential-climate-variables)** defined by the Global Climate Observing System. 

The project supports both **local deployment** using **Docker** and **PostgreSQL** and cloud deployment on **Google Cloud Platform** using **Cloud Composer**, **Google Cloud Storage**, and **BigQuery.**


## Architecture

### Cloud Deployment

The cloud deployment uses Google Cloud Composer for Airflow orchestration, Google Cloud Storage for raw and transformed Parquet files, and BigQuery as the analytical warehouse.

Infrastructure is provisioned through Terraform. Cloud Logging provides centralized task and scheduler logs, while Cloud Monitoring receives both platform and custom pipeline metrics.

![Cloud Deployment Architecture](<images/Climate Data Pipeline - Cloud Deployment.jpg>)


### Data Pipeline Flow
![Data Pipeline Flow](<images/Climate Data Pipeline Flow.jpg>)



## Technology Stack

|Area|Technologies|
|-------|--------|
|Orchestration| Apache Airflow 3 (local), Google Cloud Managed Service for Apache Airflow|
|Programming Language| Python|
|Data Processing| Pandas|
|Data Quality| Great Expectations|
|Intermediate Format| Apache Parquet|
|Local Database|PostgreSQL|
|Cloud Warehouse| BigQuery|
|Cloud Object Storage|Google Cloud Storage|
|Infrastructure as Code|Terraform|
|Local Runtime| Docker & Docker Compose|
|Local Monitoring| Prometheus & Grafana|
|Cloud Monitoring| Google Cloud Monitoring & Cloud Logging|
|CI?CD| Google Cloud Build & GitHub| 
|Deployment Automation| Bash|
|External APIs| Open-Meteo, NASA POWER|

## Data Sources
### Open Meteo

Open-Meteo provides climate, air-quality, and river-discharge data used by the pipeline.

#### Climate
Parameters include:
- temperature
- cloud cover
- humidity
- precipitation
- wind speed 
- rain
- snowfall
- shortwave radiation

#### Air Quality
Parameters include: 
- particulate matter (PM2.5, PM10)
- carbon dioxide
- ozone 
- nitrogen dioxide
- sulphur dioxide


#### Flood 
Parameters include: 
- river discharge
 
#### Limitations:

##### API Rate Limits

Under the non-commercial API tier, the usage is limited according to Open-Meteo's weighted API-call calculation. Limits include:

- 600 calls per minute
- 5000 calls per hourl
- 10,000 calls per day

The calculation is described in [Open-Meteo's multi-location API article](https://openmeteo.substack.com/p/weather-data-for-multiple-locations)

The pipeline includes concurrency and quota-management mechanisms to reduce the likelihood of exceeding these limits.

##### Data Availability 
- Climate: 1 day (historical data available from 1950)
- Air Quality: 0 days as endpoint provides forecast (however only up to 3 months of past data is available)
- Flood: 0 days as it provides forecast up to 12 months (historical data available for 3 months)

### NASA Power:

NASA POWER provides additional meteorological and land-surface parameters.

Daily Parameters

Parameters used by the pipeline include:

- Surface pressure
- Sea-level pressure
- Land-surface temperature
- Root-zone soil wetness
- Total precipitable water
- Surface longwave downward irradiance
- Surface shortwave downward irradiance
- Surface shortwave upward irradiance
- Surface longwave upward irradiance
- Top-of-atmosphere shortwave downward irradiance
- All-sky surface albedo

Limitations
- Maximum of 20 parameters per request per point.
- Point requests accept one coordinate per request.
- Parameter availability depends on the underlying NASA dataset.
- Missing values may be represented using the -999 fill value.
- NASA POWER data also has parameter-dependent latency:
    - Meteorological data: approximately 3 days
    - Precipitation data: approximately 2 days
    - Radiation data: approximately 89 days

The difference in publication latency is accounted for when scheduling extraction of late-arriving data.

### Simple Maps

Simple Maps provides the city metadata used to determine extraction locations.

Fields used include:

- City name
- Latitude
- Longitude
- Country
- Population
- Capital status

Cities are ranked by population and the pipeline processes up to the configured maximum number of locations for the selected country.


**Licensing**: This project was developed using non-commercial access to its external data sources. Anyone intending to use the project commercially should independently review the current licensing and commercial-use requirements of each provider.

## Data Pipeline

## Airflow Architecture

Apache Airflow orchestrates extraction, transformation, validation, and loading.

### Daily DAG

The daily DAG:

- Begins scheduling when the project is deployed, with Airflow catchup disabled.
- Selects up to 100 of the most populated cities for the configured country.
- Splits cities into smaller extraction chunks.
- Extracts the latest available data from the configured APIs.
- Stores extracted data as raw Parquet files.
- Consolidates and transforms the extracted data.
- Converts source-specific missing-value indicators such as NASA POWER's -999 to null values.
- Stores transformed data as Parquet.
- Validates the transformed datasets using Great Expectations.
- Loads validated data into BigQuery for cloud deployments or PostgreSQL for local deployments.

Extraction dates are source-specific because Open-Meteo and NASA POWER datasets have different publication latencies.

Airflow catchup is disabled for the daily workflow. Historical processing is handled by a separate backfill workflow.

### Historical Backfill DAG

The historical DAG is responsible for retrieving historical data.

- The start date is user-configurable and defaults to 2001-01-01.
- The end date is calculated based on the most recent historical data expected to be available.
- The requested period is divided into yearly batches.
- Each batch passes independently through extraction, transformation, validation, and loading.
- Task Groups enable depth-first execution so one period can continue through the pipeline without waiting for every other historical period to complete.

### Dynamic Task Mapping

Dynamic Task Mapping is used to scale extraction according to the amount of data being requested.

City data is divided into chunks of 50 locations before extraction. During testing, requests containing 100 locations took more than three times as long to process as equivalent 50-location requests.

Airflow dynamically creates extraction tasks for these chunks rather than requiring a fixed number of tasks in the DAG definition.

The resulting files are consolidated during transformation to create the dataset required for validation and loading.

### Resource Pools

Airflow Pools limit concurrent access to constrained external and internal resources.

#### Database Upsert Pool

Limits concurrent database upsert operations to reduce  pressure on PostgreSQL or BigQuery.

#### Open-Meteo Extraction Pool

Limits simultaneous access to Open-Meteo and works with quota-aware deferral to reduce repeated requests after quota exhaustion.

#### NASA POWER Extraction Pool
Restricts concurrent NASA POWER extraction.

#### Task Prioritization
Current daily ingestion is assigned higher scheduling priority than historical backfills.

This allows large historical requests to run over an extended period without preventing current data from being processed.

### Deferrable Operators

Open-Meteo extraction uses a custom Airflow operator built around Airflow deferral.

When a rate-limit response is detected, the operator identifies the exhausted quota window and defers execution through the Airflow triggerer.

Current behaviour is:

|Limit|	Deferral
|------|-------
Minute limit|	1 minute
Hour limit|	1 hour
Daily limit| 	Until 00:30 the following day

Once the waiting period has elapsed, the task is rescheduled and continues extraction.

### Retries:

### Success/failure Notifications:

## Data Transformation

### Air Quality:

Hourly air-quality observations are aggregated into daily observations before loading.

| Variable | Aggregation |
|---|---|
| PM2.5 | Daily mean |
| PM10 | Daily mean |
| Ozone | Maximum 8-hour rolling mean |
| Carbon Monoxide | Maximum 8-hour rolling mean 
| Nitrogen Dioxide | Maximum 1-hour value |
| Sulfur Dioxide | Maximum 1-hour value | 
Carbon Dioxide| Daily mean

Aggregation choices are based on the characteristics of each pollutant and relevant environmental reporting conventions, including guidance published by the [U.S. EPA](https://www.epa.gov/sites/default/files/2015-10/documents/ace3_criteria_air_pollutants.pdf).

### Land Surface:
Transformation includes:

- Replacing NASA POWER -999 fill values with null
- Standardizing field names and data types.
- Consolidating extracted chunks into a single dataset for each processing period.

### Climate:
Transformation includes:

- Standardizing field names and data types.
- Consolidating city extraction chunks.
- Preparing the resulting dataset for validation and loading.

## Data Quality and Validation
Great Expectations provides a validation gate between transformation and loading.
- Schema checks:
    - compared extracted columns to columns created in the tables
    - severity: critical
- Uniqueness checks:
    - checks if city id and date in each transformed dataset is unique
    - severity: critical
- Range checks:
    - checks if values of specific columns are within expected ranges (eg. humidity values between 0 and 100, air quality parameters not less than zero, and radiation parameters between 0 and 1500)
    - severity: warning
- Failure rule:
    - only validations with a severity of critical fail the task
    - validations with severity of warning are logged

## Data Model
The primary analytical tables are:

- `cities`
- `daily_climate`
- `daily_air_quality`
- `daily_land_surface`

Environmental tables use (`date`, `city_id`) as their logical record key.

<img src="images/climate erd.png" width=600 height=600> 

### Idempotent Loading

Pipeline loads are designed to be idempotent at the (date, city_id) level.

Reprocessing an existing period updates the corresponding analytical record rather than intentionally creating duplicate observations.

PostgreSQL uses conflict-aware upsert behaviour, while BigQuery cloud loading uses equivalent merge semantics.

## Getting Started
The project can be run either:
- locally eit Docker or 
- on Google Cloud.

 Both deployment options use the same core Airflow pipelines but differ in their storage, database, monitoring, and infrastructure components.

 The instructions below are sufficient for the standard deployment path. More detailed configuration, initialization, CI/CD, and troubleshooting information is available in Deployment Documentation.

### Option 1: Local Deployment
#### Prerequisites
Install:

- Git
- Docker
- Docker Compose

Airflow, PostgreSQL, Prometheus, and Grafana run in containers and do not need to be installed directly on the host.

#### Setup
1. Clone the repository and navigate to the project directory:

    `git clone https://github.com/Outis09/climate-data-pipeline`

    `cd climate-data-pipeline`

2. Create the local environment file from the provided example
    `cp .env.example .env`

3. Open `.env` and provide the required configuration. Do not commit `.env` or credentials to git. 

4. Build and initialize the Docker environment:
    
    `docker compose build`

#### Run
Start the local environment:

`docker compose up -d`

After the containers have started, access the Airflow UI by opening `localhost:8080`. Confirm that the required DAGs have been discovered successfully.

#### Stop
Temporarily stop the environment:

`docker compose stop`

Restart it:

`docker compose start`

Remove the local environment:

`docker compose down`

### Option 2: GCP

#### Prerequisites
Before deploying to Google Cloud, ensure the following are available:

- A Google Cloud project with billing enabled
- Git
- Google Cloud CLI (gcloud)
- Terraform
- Bash-compatible shell
- A GitHub repository containing the project

The Google Cloud account used for deployment must have sufficient permissions to create and configure the required GCP resources.

#### Setup

1. Clone the repository:

    `git clone https://github.com/Outis09/climate-data-pipeline`

    `cd climate-data-pipeline`

2. Authenticate with Google Cloud:

   `gcloud auth login`
   
    `gcloud auth application-default login`

3. Set the target project:
    
    `gcloud config set project <PROJECT_ID>`

4. Switch to the terraform folder

    `cd terraform`

5. Create the local variables file from the example provided

    `cp .tfvars.example terraform.tfvars`

6. Configure the required values in the `terraform.tfvars` file. Do not commit credentials or sensitive Terraform variable files.


#### Deploy

Run the deployment script:

`./deploy.sh`

The deployment provisions/configures the required Google Cloud infrastructure and supporting pipeline resources according to the project configuration.

#### Choosing a Deployment Option



## Observability & Monitoring


The pipeline implements observability at both the orchestration and data pipeline levels. This makes it possible to monitor not only whether Airflow tasks are running successfully, but also whether the pipeline is processing the expected amount of climate data and progressing through long-running historical backfills.

Monitoring differs between the local and GCP deployments.

### Local Monitoring
The local deployment uses Airflow, StatsD, Prometheus, and Grafana.

Airflow emits operational metrics through StatsD, which are collected by the monitoring stack and visualized in Grafana.

### GCP Monitoring

The cloud deployment uses Google Cloud Monitoring and Cloud Logging for Cloud Composer and pipeline observability.

Cloud Composer task and scheduler logs are available through Cloud Logging, while custom pipeline metrics are written to Cloud Monitoring using custom.googleapis.com metric types.

### Airflow Monitoring
Operational monitoring includes metrics related to:

- Task successes and failures
- Task execution duration
- Task queue duration
- DAG run duration
- Running tasks
- Queued tasks
- Deferred tasks
- Pool utilization and available slots
- Scheduler and executor behaviour

These metrics are particularly useful for identifying resource contention, long-running tasks, excessive queue times, and the effect of API quota deferrals.

### Pipeline Metrics

In addition to Airflow's built-in metrics, the project emits custom metrics from the pipeline itself.

These provide visibility into the behaviour and progress of data processing rather than only the state of Airflow tasks.

Metrics include or are being developed for:

- Rows processed and loaded
- Historical years requested
- Historical years processed
- Historical years successfully loaded
- Backfill progress
- API rate-limit events
- Open-Meteo quota exhaustion
- Pipeline processing progress

Custom metrics are emitted from the pipeline components responsible for the corresponding operation rather than directly from DAG definitions.

This allows the monitoring logic to remain close to the operation being measured.

### Notifications

Airflow notifications are configured using SMTP.

Notifications focus on failures that require intervention rather than every temporary task failure. Tasks may retry automatically, so alerting on every failed attempt would create unnecessary notification noise.

The notification strategy therefore aims to distinguish between recoverable failures and failures that remain after the configured retry policy has been exhausted.

Only the historical backfill DAG sends a success notification because it only succeeds once, comapred to the daily DAG which may run every day.

## CI/CD
Development follows a feature-branch and pull-request workflow. 

Feature branch -> Pull request -> Cloud Build CI -> DAG / pytest validation -> Merge to main -> Deployment workflow -> Cloud Composer

Pull requests targeting main trigger validation before merge.

Tests are executed against an Airflow version compatible with the deployed Cloud Composer environment to reduce differences between local development and production.

Deployment automation synchronizes the required project assets with the cloud environment after approved changes are merged.

See Development and Operations for additional information.

## Testing

Automated tests can be executed locally and through CI.

The test suite includes or is intended to cover:

- DAG import/integrity validation
- DAG structure and dependency assertions
- custom operator behaviour
- pipeline utility functions

Locally, tests can be run using one of airflow's containers:

`docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests`


For the cloud deployment, testing is automated when CI/CD is configured.

## Infrastructure as Code
Terraform provisions the core Google Cloud infrastructure required by the project, including applicable:

- APIs
- Cloud Composer resources
- Google Cloud Storage
- BigQuery resources
- service accounts and IAM
- supporting cloud configuration

Bash scripts coordinate deployment steps that sit outside or around Terraform-managed infrastructure.

Infrastructure configuration is kept separate from application DAG code.

## Security

## Known Limitations
- Non-commercial API quotas constrain the speed of large historical backfills.
- Historical availability differs between datasets and variables.
- Some NASA POWER products have substantial publication latency.
- Large historical backfills may intentionally take several days because API quotas are respected.
- Air-quality historical coverage is not equivalent to long-term climate coverage.
- The project is currently designed around a configurable subset of the most populated cities rather than unrestricted global-scale ingestion.
- External API schema, availability, and licensing changes can affect extraction.
- Local Docker resources can constrain highly concurrent transformations or database loads.
- Great Expectations range checks identify potentially suspicious values but do not establish scientific validity.

## Acknowledgements
- [Open-Meteo API](https://open-meteo.com/en/docs/climate-api)
- [NASA POWER API](https://power.larc.nasa.gov/docs/services/api/)
- [Data Pipelines with Apache Airflow](https://www.astronomer.io/ebooks/data-pipelines-with-apache-airflow/)
- [Terraform with Shell Scripts](https://praneethreddybilakanti.medium.com/terraform-with-shell-scripts-e6007f975a90)
- [Prometheus StatsD Integration Example](https://github.com/slok/prometheus-statsd-integration-example/tree/master)
- [Apache Airflow: Or How to Stop Running Cron Jobs and Start Having More Professional Problems](https://www.linkedin.com/pulse/apache-airflow-how-stop-running-cron-jobs-start-more-abadia-lopez-loxae/)
- [GCP BigQuery UPSERT Using Python API Client](https://medium.com/@chandan3611/gcp-bigquery-upsert-using-python-api-client-edd8fa485677)
- [How to implement custom monitoring metrics using the Google Cloud Monitoring API](https://oneuptime.com/blog/post/2026-02-17-how-to-implement-custom-monitoring-metrics-using-the-google-cloud-monitoring-api/view)

- [How to Create Custom Metrics in Cloud Monitoring Using the API](https://oneuptime.com/blog/post/2026-02-17-how-to-create-custom-metrics-in-cloud-monitoring-using-the-api/view#writing-cumulative-metrics)
