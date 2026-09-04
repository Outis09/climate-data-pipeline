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
    "cloudbuild.googleapis.com",
    "secretmanager.googleapis.com",
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

resource "google_project_iam_member" "composer_bigquery_read_session" {
  project = var.project_id
  role    = "roles/bigquery.readSessionUser"

  member = "serviceAccount:${google_service_account.climate_pipeline_service_account.email}"
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
      cloudpathlib = ">=0.24.0"

    }

    env_variables = {
      BUCKET_NAME = var.bucket_name
      BQ_DATASET_NAME = var.bigquery_dataset_name
      STORAGE_BACKEND = "gcs"
      CLIMATE_COUNTRY = var.climate_country
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

// Create a secret containing the personal access token and grant permissions to the Service Agent
resource "google_secret_manager_secret" "github_token_secret" {
    project = var.project_id
    secret_id = var.secret_id

    replication {
        auto {}
    }
}

resource "google_secret_manager_secret_version" "github_token_secret_version" {
    secret = google_secret_manager_secret.github_token_secret.id
    secret_data = var.github_pat
}

data "google_iam_policy" "serviceagent_secretAccessor" {
    binding {
        role = "roles/secretmanager.secretAccessor"
        members = ["serviceAccount:service-${var.project_number}@gcp-sa-cloudbuild.iam.gserviceaccount.com"]
    }
}

resource "google_secret_manager_secret_iam_policy" "policy" {
  project = google_secret_manager_secret.github_token_secret.project
  secret_id = google_secret_manager_secret.github_token_secret.secret_id
  policy_data = data.google_iam_policy.serviceagent_secretAccessor.policy_data
}

// Create the GitHub connection
resource "google_cloudbuildv2_connection" "github_connection" {
    project = var.project_id
    location = var.region
    name = var.connection_name

    github_config {
        app_installation_id = var.installation_id
        authorizer_credential {
            oauth_token_secret_version = google_secret_manager_secret_version.github_token_secret_version.id
        }
    }
    depends_on = [google_secret_manager_secret_iam_policy.policy]
}

    resource "google_cloudbuildv2_repository" "my_repository" {
      project = var.project_id
      location = var.region
      name = var.repository_name
      parent_connection = google_cloudbuildv2_connection.github_connection.name
      remote_uri = var.remote_uri
  }

// service account for cloud build triggers
resource "google_service_account" "cloud_build_service_account" {
  provider = google-beta
  account_id   = "cloud-build-service-account"
  display_name = "Cloud Build Service Account"
}

resource "google_project_iam_member" "cloudbuild_sa_builder" {
  project  = var.project_id
  member   = format("serviceAccount:%s", google_service_account.cloud_build_service_account.email)
  role     = "roles/cloudbuild.builds.builder"
}

resource "google_project_iam_member" "cloudbuild_sa_log_accessor" {
  project  = var.project_id
  member   = format("serviceAccount:%s", google_service_account.cloud_build_service_account.email)
  role     = "roles/logging.logWriter"
}