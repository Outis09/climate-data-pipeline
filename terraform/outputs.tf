output "project_id" {
  value = var.project_id
}

output "region" {
  value = var.region
}

output "climate_bucket_name" {
  value = var.bucket_name
}

output "bigquery_dataset_name" {
  value = var.bigquery_dataset_name
}

output "dag_gcs_prefix" {
  value = google_composer_environment.climate_data_environment.config[0].dag_gcs_prefix
}

output "composer_data_gcs_prefix" {
  value = replace(google_composer_environment.climate_data_environment.config[0].dag_gcs_prefix, "dags", "data")
}

output "custom_gcs_prefix" {
    value = google_storage_bucket.climate_data_bucket.url
}

output "cities_table_id" {
    value = "${var.project_id}:${google_bigquery_dataset.climate_data.dataset_id}.${google_bigquery_table.cities_table.table_id}"
}

output "composer_environment_name" {
    value = google_composer_environment.climate_data_environment.name
}

output "historical_data_start_date" {
    value = var.historical_data_start_date
}

output "github_connection_name" {
    value = var.connection_name
}

output "cloud_build_presubmit_service_account" {
    value = google_service_account.cloud_build_service_account.email
}

output "github_repository_name" {
    value = var.repository_name
}