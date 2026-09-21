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
|CI/CD| Google Cloud Build & GitHub| 
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



### NASA Power:

NASA POWER provides additional meteorological and land-surface parameters.


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
- Concurrent requests are limited.
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

> [!NOTE]
> This project was developed using non-commercial access to its external data sources. Anyone intending to use the project commercially should independently review the current licensing and commercial-use requirements of each provider.


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

### Late-Arriving Radiation Data DAG

This DAG runs monthly to extract late-arriving radiation data from the NASA POWER API.

- Runs monthly because NASA POWER currently releases late-data for a full month, with variable latency.
- Uses the same DAG structure as the Historical Backfill but the first request probes for dat availability and only proceeds if data is available.
- Extracted data is transformed, validated, and then upserted into the `daily_land_surface` table using `city_id` and `date` as unique identifiers.

### Dynamic Task Mapping

Dynamic Task Mapping is used to scale extraction according to the amount of data being requested. It is used in two ways:

#### City chunks
The pipeline currently processes data for up to 100 cities for a given country. However, not all countries have 100 cities in the data. So the city extraction uses dynamic extraction because the number of cities at runtime is unknown.

City data is divided into chunks of 50 locations before extraction. During testing, requests containing 100 locations took more than three times as long to process as equivalent 50-location requests. 

Airflow dynamically creates extraction tasks for these chunks rather than requiring a fixed number of tasks in the DAG definition. These tasks can then run concurrently and complete in less time than it takes to process 100 cities at once.

The resulting files are consolidated during transformation to create the dataset required for validation and loading.

#### Depth-first Execution Using Task Groups
Currently, users can set a historical start date or default to `2001-01-01`. Since the actual historical date at runtime is not known, dynamic task mapping divides the tasks into the number of years, so that each year processes separately.

### Task Groups
The `historical_backfill` DAG uses three task-groups (one for each data source). Task groups enable the DAG to execute depth-first. That means that if there are 20 dynamiccaly-mapped tasks, based on the years requested, each year is assigned to a group of extraction, transformation, validation, and upsert tasks. Therefore, each year's data can be fully processed even if other year's tasks are delayed or failing.

### Resource Pools

Airflow Pools limit concurrent access to constrained external and internal resources.

#### Database Upsert Pool

Limits concurrent database upsert operations to reduce  pressure on PostgreSQL or BigQuery.

#### Open-Meteo Extraction Pool

Limits simultaneous access to Open-Meteo and works with quota-aware deferral to reduce repeated requests after quota exhaustion. Includes deferred tasks so that multiple tasks do not hit the API after the quota has been hit, to avoid IP-banning.

#### NASA POWER Extraction Pool
Restricts concurrent NASA POWER extraction.

#### GX Validation Pool
Restricts conncurent access to GX expectation suites for data validation. Locally, multiple tasks can access the suites at the same time. However, in GCP, concurrent access leads to task failures, hence the pool.

#### Task Prioritization
Currently,  daily ingestion is assigned higher scheduling priority than historical backfills.

This allows large historical requests to run over an extended period without preventing current data from being processed.

### Deferrable Operators

Open-Meteo extraction uses a custom Airflow operator built around Airflow deferral and triggerers.

When a rate-limit response is detected, the operator identifies the exhausted quota window and defers execution through the Airflow triggerer.

Current behaviour is:

|Limit|	Deferral
|------|-------
Minute limit|	1 minute
Hour limit|	1 hour
Daily limit| 	Until 00:30 the following day

Once the waiting period has elapsed, the task is rescheduled and continues extraction.

### Retries:

The pipeline uses a general retry policy of 3, with a retry delay of 2 minutes, and exponential backoff activated. This ensures that transient errors do not fail the pipeline. 

### Success/failure Notifications:

The pipeline sends email notifications on DAG success and task failures. However, not all DAGs receive success notications

The daily DAG only sends task failure notifications because daily success emails will overwhelm users and is likely to make users overlook failure notifications.

The Historical DAG can only succeed once therefore it sends an email on success to indicate the completion of the DAG and complete availability of historical data. 

The late-arriving data DAG also sends success notifications to signify the arrival of radiation data for a period.

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

### Dead-letter Tables
When a batch of data fails schema checks, the entire batch is dropped. However, when a few rows fail validation, those rows are inserted into dead-letter tables where users can preview them and decide to delete, preserve, or add to the main tables.

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
- locally with Docker or 
- on Google Cloud.

 Both deployment options use the same core Airflow pipelines but differ in their storage, database, monitoring, and infrastructure components.

 The instructions below are sufficient for the standard deployment path. More detailed configuration, initialization, CI/CD, and troubleshooting information is available in Deployment Documentation.

### Option 1: Local Deployment
#### Prerequisites
Install:

- Git
- Python
- Docker
- Docker Compose

Airflow, PostgreSQL, Prometheus, and Grafana run in containers and do not need to be installed directly on the host.

#### Setup
1. Clone the repository and navigate to the project directory:

    `git clone https://github.com/Outis09/climate-data-pipeline`

    `cd climate-data-pipeline`

2. Create the local environment file:
    - Option 1: run a setup script that auto generates fernet keys and jwt secrets for airflow
    
    `python env-setup.py`

    - Option 2: copy the example file but you'll need to manually generate fernet keys and jwt secrets

    `cp .env.example .env`

3. Open `.env` and provide the required configuration. Do not commit `.env` or credentials to git. 

4. Start Docker by opening the Docker Desktop application.

5. Build and initialize the Docker environment:
    
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

#### Access the data
The data is accessible through pgAdmin 4.

1. Open `localhost:5050` in a browser.
2. Enter any requested credentials (can be found in .env file).
3. Drop down the `Climate_Server` server.
4. Table will be available in the `climate` schema.



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

#### Access the data
Data will be available in BigQuery.

#### Choosing a Deployment Option

##### Local 

###### Pros:
- Zero infrastructure cost
- Full control since data and infrastructure remains on user's machine
- No cloud account required

###### Cons:
- User manages the pipeline (responsible for starting and stopping services)
- Machine must stay running for scheduled pipeline runs
- Limited by local resources (CPU, memory, and disk)
- Less convenient for teams

##### GCP 

###### Pros:
- Runs continuously (does not depend on the user's machine being switched on)
- Less infrastructure management
- Scalable
- Centralized data access for teams
- CI/CD can be configured

###### Cons:
- Ongoing cost (even though this project can be run using the $300 free credit GCP gives to new accounts)
- More complex initial setup (requires a GCP project with billing enabled)
- The deployment relies heavily on GCP services.


## Observability & Monitoring


The pipeline implements observability at both the orchestration and data pipeline levels. This makes it possible to monitor not only whether Airflow tasks are running successfully, but also whether the pipeline is processing the expected amount of climate data and progressing through long-running historical backfills.

Monitoring differs between the local and GCP deployments.

### Local Monitoring
The local deployment uses Airflow, StatsD, Prometheus, and Grafana.

Airflow emits operational metrics through StatsD, which are collected by the monitoring stack and visualized in Grafana.

#### Access the monitoring dashboard on Grafana.
1. Open `localhost:3000` in a browser
2. Select dashboards in the left panel
3. Select the `Climate Data Pipeline Dashboard`
4. Monitor dashboard as DAGs run.

Below is a snpashot of the Grafana dashboard. 

![Sample Grafana Dashboard](<images/Sample Grafana Dashboard.png>)

Note: The metrics tracker on the dashboard may change overtime, therefore users might see a different dashboard than this one.

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

Security controls differ between the local Docker and GCP deployments.

### Docker Deployment

The Docker deployment is designed for use on a trusted local machine.

- **Secrets**: Credentials and configuration are stored in a local .env file, which is excluded from version control. .env.example contains only placeholders.
- **Generated secrets**: Airflow Fernet and JWT secrets are generated during setup rather than shared across installations.
- **Database access**: PostgreSQL is bound to 127.0.0.1, preventing access from external network interfaces while remaining accessible to local tools such as pgAdmin.
- **Internal networking**: Airflow, PostgreSQL, Prometheus, Grafana, StatsD, and other services communicate through the internal Docker network. Only services requiring user access expose host ports.
- **Credentials**: Database and Airflow credentials are supplied through environment variables/Airflow connections rather than hard-coded in DAGs or application code.
External APIs: Open-Meteo and NASA POWER are accessed over HTTPS and currently require no API credentials.
- **Monitoring**: Grafana, Prometheus, Airflow, and pgAdmin are intended for local access and should not be exposed publicly without additional authentication and network controls.

Users are responsible for securing their host machine, Docker installation, .env file, persisted volumes, and locally exposed interfaces.

### GCP Deployment

The GCP deployment uses managed Google Cloud security controls.

- **IAM**: Cloud Composer, BigQuery, Cloud Storage, Cloud Build, Secret Manager, and Cloud Monitoring access is controlled through IAM using least-privilege permissions.
- **Authentication**: Workloads use service accounts and Google Application Default Credentials instead of service-account keys stored in the repository.
- **Secret management**: Sensitive values, including SMTP and Airflow connection credentials, are stored in Secret Manager rather than DAGs or deployment scripts.
- **Data access**: BigQuery datasets and Cloud Storage resources are protected using IAM and are not intended for public access.
- **CI/CD**: Cloud Build uses its service identity to test and deploy changes, avoiding long-lived cloud credentials in the repository.
- **Infrastructure**: GCP resources are provisioned through Terraform, making infrastructure and security-related changes reproducible and reviewable. Terraform state should be treated as sensitive.
- **Logging and monitoring**: Logs and custom metrics contain operational information only; secrets and connection strings should never be emitted.

The pipeline currently processes public climate and environmental data rather than personally identifiable information. Additional access, encryption, retention, and governance controls should be considered if private or sensitive data sources are introduced.

> [!WARNING]
> Secrets, passwords, API credentials, Fernet keys, JWT secrets, and service-account keys must never be committed to the repository. 


## Known Limitations
- Non-commercial API quotas constrain the speed of large historical backfills.
- Historical availability differs between datasets and variables.
- Some NASA POWER products have substantial publication latency.
- Large historical backfills may intentionally take several days because API quotas are respected.
- Air-quality historical coverage is not equivalent to long-term climate coverage.
- The project is currently designed around a configurable subset of the most populated cities rather than unrestricted global-scale ingestion.
- The free version of the `SimpliMaps` cities data used may not contain all cities for a selected country. However, if auser desires, they can purchase the premium version and plug it into the project, instead of the free version. 
- External API schema, availability, and licensing changes can affect extraction.
- Local Docker resources can constrain highly concurrent transformations or database loads.
- Great Expectations range checks identify potentially suspicious values but do not establish scientific validity.

## Acknowledgements

These are API sources, books, and blogs with code samples that were used in this project.

- [Open-Meteo API](https://open-meteo.com/en/docs/climate-api)
- [NASA POWER API](https://power.larc.nasa.gov/docs/services/api/)
- [Data Pipelines with Apache Airflow](https://www.astronomer.io/ebooks/data-pipelines-with-apache-airflow/)
- [Terraform with Shell Scripts](https://praneethreddybilakanti.medium.com/terraform-with-shell-scripts-e6007f975a90)
- [Prometheus StatsD Integration Example](https://github.com/slok/prometheus-statsd-integration-example/tree/master)
- [Apache Airflow: Or How to Stop Running Cron Jobs and Start Having More Professional Problems](https://www.linkedin.com/pulse/apache-airflow-how-stop-running-cron-jobs-start-more-abadia-lopez-loxae/)
- [GCP BigQuery UPSERT Using Python API Client](https://medium.com/@chandan3611/gcp-bigquery-upsert-using-python-api-client-edd8fa485677)
- [How to implement custom monitoring metrics using the Google Cloud Monitoring API](https://oneuptime.com/blog/post/2026-02-17-how-to-implement-custom-monitoring-metrics-using-the-google-cloud-monitoring-api/view)

- [How to Create Custom Metrics in Cloud Monitoring Using the API](https://oneuptime.com/blog/post/2026-02-17-how-to-create-custom-metrics-in-cloud-monitoring-using-the-api/view#writing-cumulative-metrics)
