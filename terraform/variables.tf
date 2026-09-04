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

variable "climate_country" {
  description = "The country for which climate data is being collected"
  type        = string
  default     = "Ghana"
}

variable "historical_data_start_date" {
  description = "The start date for historical climate data collection (YYYY-MM-DD)"
  type        = string
  default     = "2001-01-01"
}

variable "secret_id" {
  description = "The ID of the secret in Secret Manager"
  type        = string
}

variable "github_pat" {
  description = "The GitHub Personal Access Token for accessing private repositories"
  type        = string
  sensitive   = true
}

variable "connection_name" {
  description = "The name of the Cloud Build connection to GitHub"
  type        = string
}

variable "installation_id" {
  description = "The GitHub App installation ID"
  type        = string
}

variable "project_number" {
  description = "The GCP project number"
  type        = string
}

variable "repository_name" {
  description = "The name of the GitHub repository"
  type        = string
  
}

variable "remote_uri" {
  description = "The remote URI of the GitHub repository"
  type        = string
 }