variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
}

variable "bq_location" {
  description = "The GCP region for BigQuery"
  type        = string
  default     = "US"
}

variable "service_account_name" {
  description = "The name of the service account to be created"
  type        = string
  default     = "climate-pipeline-service-account"
}

variable "composer_environment_name" {
  description = "The name of the Cloud Composer environment"
  type        = string
  default     = "climate-data-composer-env"
}

variable "bigquery_dataset_name" {
  description = "The name of the BigQuery dataset"
  type        = string
  default     = "climate_data"
}

variable "bucket_name" {
  description = "The name of the storage bucket"
  type        = string
}