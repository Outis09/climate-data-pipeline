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

Climate Data Pipeline is a reproducible data pipeline that collects, processes, validates, and stores climate and environmental data for up to 100 of the most populated cities in a selected country.

The pipeline combines city metadata from [Simple Maps](https://simplemaps.com/data/world-cities) with climate, air-quality, river discharge, and land-surface observations from [Open Meteo](https://open-meteo.com/en/docs/climate-api) and [NASA Power](https://power.larc.nasa.gov/docs/services/api/temporal/daily/). 

The dataset includes parameters related to the **[Essential Climate Variables (ECV)](https://gcos.wmo.int/site/global-climate-observing-system-gcos/essential-climate-variables)** defined by the Global Climate Observing System. 

The project supports both **local deployment** using **Docker** and **PostgreSQL** and cloud deployment on **Google Cloud Platform** using **Cloud Composer**, **Google Cloud Storage**, and **BigQuery.**

## Objectives
- Build a reliable and reproducible climate data pipeline for climate analysis, research, and modelling. that data scientists, data analysts, AI engineers, climate researchers, and non-governmental organizations can use for climate analysis and models. Built with African countries in mind because access to this data would otherwise require bureaucratic hurdles of going through national agencies.

## Architecture


## Technology Stack

|Area|Technologies|
|-------|--------|
|Orchestration| Apache Airflow 3 (local), Google Cloud Managed Service for Apache Airflow|
|Language| Python|
|Data Processing| Pandas|
|Data Quality| Great Expectations|
|File Format| Apache Parquet|
|Local Database|PostgreSQL|
|Cloud Warehouse| BigQuery|
|Object Storage|Google Cloud Storage|
|Infrastructure |Terraform|
|Local Development| Docker, Docker Compose|
|Monitoring| Prometheus & Grafana (local)|
|Deployment Automation| Bash|
|External APIs| Open-Meteo, NASA POWER|

## Data Sources
### Open Meteo

Open-Meteo provides climate, air-quality, and river-discharge data used by the pipeline.

#### Climate
Parameters include:
- Temperature, cloud cover, humidity, precipitation, wind speed, rain, snowfall, shortwave radiation
#### Air Quality
Parameters include: 
- Particulate Matter, Carbon Dioxide, Ozone, Nitrogen Dioxide, Sulphur Dioxide
#### Flood 
Parameters include: River Discharge
 
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

Simple Maps provides the city metadata required to determine extraction coordinates.

Fields used include:

- City name
- Latitude
- Longitude
- Country
- Population
- Capital status

The free Basic World Cities database contains approximately 50,000 cities.

Note: This project uses non-commercial access to the external data APIs. Users intending to use the pipeline commercially should review the licensing and commercial-use requirements of each data provider.
        
**NB:** This project used the non-commercial usage licence of these APIs, therefore for commercial use, users are advised to purchase the commercial licences before proceeding. 

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

### Pools

Airflow Pools limit concurrent access to constrained external and internal resources.

#### Database Upsert Pool

Limits the number of concurrent database upsert operations to reduce unnecessary pressure on PostgreSQL or BigQuery loading operations.

#### Open-Meteo Extraction Pool

Limits the number of extraction tasks that can access Open-Meteo concurrently.

Deferred tasks remain included in the pool. Therefore, when the available API quota has been exhausted, additional extraction tasks do not immediately continue sending requests to the same resource.

#### NASA POWER Extraction Pool
Limits the number of extraction tasks that can access NASA POWER concurrently.

### Deferrable Operators

A custom quota-aware deferrable operator handles Open-Meteo API rate-limit responses.

When a quota is exhausted, the task is deferred to the Airflow triggerer instead of occupying a worker while waiting.

Current deferral behaviour is:

|API Limit|	Deferral
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
| PM2.5 | 24-hour average |
| PM10 | 24-hour average |
| Ozone | Max of 8-hour rolling average |
| Carbon Monoxide | Max of 8-hour rolling average 
| Nitrogen Dioxide | Daily aggregation |
| Sulfur Dioxide | Daily aggregation | 
Carbon Dioxide| Daily average

Aggregation choices are based on the characteristics of each pollutant and relevant environmental reporting conventions, including guidance published by the [U.S. EPA](https://www.epa.gov/sites/default/files/2015-10/documents/ace3_criteria_air_pollutants.pdf).

### Land Surface:
Transformation includes:

- Replacing NASA POWER -999 fill values with null values.
- Standardizing field names and data types.
- Consolidating extracted chunks into a single dataset for each processing period.

### Climate:
Transformation includes:

- Standardizing field names and data types.
- Consolidating city extraction chunks.
- Preparing the resulting dataset for validation and loading.

## Data Quality and Validation
- Framework: Great Expectations
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

![alt text](<climate erd.png>)


## Deployment

#### GCP Deployment

#### Local Deployment


## Observability & Monitoring


## Testing

## Security

## Acknowledgements
- [Open-Meteo API](https://open-meteo.com/en/docs/climate-api)
- [NASA POWER API](https://power.larc.nasa.gov/docs/services/api/)
- [Data Pipelines with Apache Airflow](https://www.astronomer.io/ebooks/data-pipelines-with-apache-airflow/)
- [Terraform with Shell Scripts](https://praneethreddybilakanti.medium.com/terraform-with-shell-scripts-e6007f975a90)
- [Prometheus StatsD Integration Example](https://github.com/slok/prometheus-statsd-integration-example/tree/master)
- [Apache Airflow: Or How to Stop Running Cron Jobs and Start Having More Professional Problems](https://www.linkedin.com/pulse/apache-airflow-how-stop-running-cron-jobs-start-more-abadia-lopez-loxae/)
- [GCP BigQuery UPSERT Using Python API Client](https://medium.com/@chandan3611/gcp-bigquery-upsert-using-python-api-client-edd8fa485677)