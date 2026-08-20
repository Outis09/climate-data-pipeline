#!/bin/bash

set -eo pipefail

LOG_FILE="terraform_$(date +%F_%T).log"
echo "Deploying the Terraform configuration..."

echo "Initializing Terraform..." | tee -a "$LOG_FILE"
if ! terraform init 2>&1 | tee -a "$LOG_FILE"; then
    echo "Error during Terraform initialization. Check the log at $LOG_FILE and try again." | tee -a "$LOG_FILE"
    exit 1
fi

echo "Planning Terraform deployment..." | tee -a "$LOG_FILE"
if ! terraform plan -out=planfile -var-file=terraform.tfvars 2>&1 | tee -a "$LOG_FILE"; then
    echo "Error during Terraform plan. Check the log at $LOG_FILE and try again." | tee -a "$LOG_FILE"
    exit 1
fi

echo "Applying Terraform deployment..." | tee -a "$LOG_FILE"
if ! terraform apply -auto-approve planfile 2>&1 | tee -a "$LOG_FILE"; then
    echo "Error during Terraform apply. Check the log at $LOG_FILE and try again." | tee -a "$LOG_FILE"
    exit 1
fi

echo "Terraform deployment completed successfully." | tee -a "$LOG_FILE"

echo "Uploading dags..." | tee -a "$LOG_FILE"
DAG_GCS_PREFIX=$(terraform output -raw dag_gcs_prefix)
if gcloud storage rsync ./dags/ "$DAG_GCS_PREFIX" --quiet 2>&1 | tee -a "$LOG_FILE"; then
    echo "DAGs uploaded successfully to $DAG_GCS_PREFIX." | tee -a "$LOG_FILE"
fi