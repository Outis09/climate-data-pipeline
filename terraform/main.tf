provider "google-beta" {
  project = var.project_id
  region  = var.region
}

locals {
  required_apis = toset([
    "composer.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "iamcredentials.googleapis.com",
  ])
}

resource "google_project_service" "required_apis" {
  provider = google-beta
  for_each = local.required_apis

  project = var.project_id
  service = each.value

  disable_on_destroy = false
}


resource "google_service_account" "climate_pipeline_service_account" {
  provider = google-beta
  account_id   = var.service_account_name
  display_name = "Climate Pipeline Service Account"
}

resource "google_project_iam_member" "climate_pipeline_service_account" {
  provider = google-beta
  project  = var.project_id
  member   = format("serviceAccount:%s", google_service_account.climate_pipeline_service_account.email)
  role     = "roles/composer.worker"
}


resource "google_bigquery_dataset" "climate_data" {
  provider = google-beta
  dataset_id = var.bigquery_dataset_name
  project    = var.project_id
  location   = var.bq_location
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "cities_table" {
  provider = google-beta
  dataset_id = google_bigquery_dataset.climate_data.dataset_id
  table_id   = "cities"
  project    = var.project_id

  schema = file("${path.module}/bigquery_schemas/cities.json")

  deletion_protection = false
}

resource "google_bigquery_table" "climate_data_table" {
  provider = google-beta
  dataset_id = google_bigquery_dataset.climate_data.dataset_id
  table_id   = "daily_climate"
  project    = var.project_id

  schema = file("${path.module}/bigquery_schemas/daily_climate.json")

  time_partitioning {
    type = "DAY"
    field = "date"
  }

  clustering = ["city_id"]

  deletion_protection = false
  
}

resource "google_bigquery_table" "air_quality_data_table" {
  provider = google-beta
  dataset_id = google_bigquery_dataset.climate_data.dataset_id
  table_id   = "daily_air_quality"
  project    = var.project_id

  schema = file("${path.module}/bigquery_schemas/daily_air_quality.json")

  time_partitioning {
    type = "DAY"
    field = "date"
  }

  clustering = ["city_id"]

  deletion_protection = false
}

resource "google_bigquery_table" "land_surface_data_table" {
  provider = google-beta
  dataset_id = google_bigquery_dataset.climate_data.dataset_id
  table_id   = "daily_land_surface"
  project    = var.project_id

  schema = file("${path.module}/bigquery_schemas/daily_land_surface.json")

    time_partitioning {
        type = "DAY"
        field = "date"
    }

    clustering = ["city_id"]

    deletion_protection = false
}



resource "google_project_iam_member" "climate_pipeline_account_bq_job_user" {
  provider = google-beta
  project  = var.project_id
  member   = format("serviceAccount:%s", google_service_account.climate_pipeline_service_account.email)
  role     = "roles/bigquery.jobUser"
}

resource "google_bigquery_dataset_access" "climate_data_access" {
  provider = google-beta
  dataset_id = google_bigquery_dataset.climate_data.dataset_id
  role = "roles/bigquery.dataEditor"
  user_by_email = google_service_account.climate_pipeline_service_account.email
}

resource "google_storage_bucket" "climate_data_bucket" {
  provider = google-beta
  name     = var.bucket_name
  location = var.region

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  force_destroy = true
}

resource "google_storage_bucket_iam_member" "climate_data_bucket_access" {
  provider = google-beta
  bucket = google_storage_bucket.climate_data_bucket.name
  role   = "roles/storage.objectAdmin"
  member = format("serviceAccount:%s", google_service_account.climate_pipeline_service_account.email)
}

resource "google_composer_environment" "climate_data_environment" {
  provider = google-beta
  name = var.composer_environment_name
  project = var.project_id
  region  = var.region

  config {

    software_config {
      image_version = "composer-3-airflow-3.2.2-build.2"

      pypi_packages = {
      requests-cache = ">=1.3.3"
      retry-requests = ">=2.0.0"
      openmeteo-requests = ">=1.7.5"
      great-expectations = ">=1.19.1"

    }

    env_variables = {
      BUCKET_NAME = var.bucket_name
      BQ_DATASET_NAME = var.bigquery_dataset_name
    }
    }

    node_config {
      service_account = google_service_account.climate_pipeline_service_account.email
    }



  }

  depends_on = [ 
    google_project_iam_member.climate_pipeline_service_account,
    google_project_service.required_apis,
    # google_project_service.composer_api,
    # google_project_service.bigquery_api,
    # google_project_service.storage_api,
    google_bigquery_dataset_access.climate_data_access,
    google_storage_bucket_iam_member.climate_data_bucket_access]
}