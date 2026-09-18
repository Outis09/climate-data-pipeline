#!/bin/bash

set -eo pipefail

LOG_FILE="logs/terraform_$(date +%F_%T).log"
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

echo "Configuring SMTP settings for receiving emails..." | tee -a "$LOG_FILE"
# read user email password
USER_EMAIL=$(terraform output -raw user_email)
SMTP_EMAIL="${USER_EMAIL//@/%40}"
read -sp "Enter the app password for $USER_EMAIL: " USER_PASSWORD
SMTP_DEFAULT="smtp://${SMTP_EMAIL}:${USER_PASSWORD}@smtp.gmail.com:587?disable_ssl=true&from_email=${SMTP_EMAIL}"
if ! gcloud secrets describe airflow-connections-smtp_default --quiet 2>/dev/null; then
    echo "Creating secret for SMTP settings..." | tee -a "$LOG_FILE"
else
    echo "Secret for SMTP settings already exists. Updating it..." | tee -a "$LOG_FILE"
    gcloud secrets delete airflow-connections-smtp_default --quiet 2>&1 | tee -a "$LOG_FILE"
fi
echo -n "$SMTP_DEFAULT" | gcloud secrets create airflow-connections-smtp_default --data-file=- --replication-policy=automatic --quiet

unset USER_PASSWORD
unset SMTP_DEFAULT

echo "Uploading dags..." | tee -a "$LOG_FILE"
DAG_GCS_PREFIX=$(terraform output -raw dag_gcs_prefix)
if gcloud storage rsync -r --exclude=".*__pycache__.*" ../dags/ "$DAG_GCS_PREFIX" --quiet 2>&1 | tee -a "$LOG_FILE"; then
    echo "DAGs uploaded successfully to $DAG_GCS_PREFIX." | tee -a "$LOG_FILE"
fi

echo "Configuring pools for Composer Environment..." | tee -a "$LOG_FILE"
COMPOSER_ENVIRONMENT_NAME=$(terraform output -raw composer_environment_name)
LOCATION=$(terraform output -raw region)
PROJECT_ID=$(terraform output -raw project_id)


if ! gcloud composer environments run "$COMPOSER_ENVIRONMENT_NAME" --project="$PROJECT_ID" --location="$LOCATION" pools set -- --include-deferred nasa_power_extraction_pool 4 "Pool for NASA Power Extraction Tasks"  2>&1 | tee -a "$LOG_FILE"; then
    echo "Error configuring NASA Power Extraction Pool for Composer Environment. Check the log at $LOG_FILE and try again." | tee -a "$LOG_FILE"
    exit 1
fi

if ! gcloud composer environments run "$COMPOSER_ENVIRONMENT_NAME" --project="$PROJECT_ID" --location="$LOCATION" pools set -- --include-deferred open_meteo_extraction_pool 4 "Pool for Open Meteo Extraction Tasks"  2>&1 | tee -a "$LOG_FILE"; then
    echo "Error configuring Open Meteo Extraction Pool for Composer Environment. Check the log at $LOG_FILE and try again." | tee -a "$LOG_FILE"
    exit 1
fi

if ! gcloud composer environments run "$COMPOSER_ENVIRONMENT_NAME" --project="$PROJECT_ID" --location="$LOCATION" pools set -- --include-deferred db_upsert_pool 4 "Pool for DB Upsert Tasks"  2>&1 | tee -a "$LOG_FILE"; then
    echo "Error configuring DB Upsert Pool for Composer Environment. Check the log at $LOG_FILE and try again." | tee -a "$LOG_FILE"
    exit 1
fi

echo "Preparing cities data for upload..." | tee -a "$LOG_FILE"
python3 ./clean_cities.py || python ./clean_cities.py

echo "Uploading cities data to GCS bucket..." | tee -a "$LOG_FILE"
CITIES_GCS_PREFIX=$(terraform output -raw custom_gcs_prefix)
if ! gcloud storage cp \
    ../worldcities_clean.csv \
    "$CITIES_GCS_PREFIX/worldcities_clean.csv" \
    2>&1 | tee -a "$LOG_FILE"; then

    echo "Error uploading cities data to GCS." | tee -a "$LOG_FILE"
    exit 1
fi

echo "Cities data uploaded successfully." | tee -a "$LOG_FILE"

echo "Uploading cities data to BigQuery..." | tee -a "$LOG_FILE"

if ! bq --project_id="$PROJECT_ID" load --source_format=CSV  --skip_leading_rows=1 --replace "$(terraform output -raw cities_table_id)" "$CITIES_GCS_PREFIX/worldcities_clean.csv" 2>&1 | tee -a "$LOG_FILE"; then
    echo "Error uploading cities data to BigQuery." | tee -a "$LOG_FILE"
    exit 1
fi
echo "Cities data uploaded successfully to BigQuery." | tee -a "$LOG_FILE"

echo "Setting up Great Expectations..." | tee -a "$LOG_FILE"

# copy placeholder file into transformed file location for great expectations to run against
if ! gcloud storage cp ./placeholder.parquet "$CITIES_GCS_PREFIX/transformed/daily_climate/placeholder.parquet" --quiet 2>&1 | tee -a "$LOG_FILE"; then
    echo "Error uploading placeholder.parquet to GCS. Check the log at $LOG_FILE and try again." | tee -a "$LOG_FILE"
    exit 1
fi

if ! gcloud storage cp ./placeholder.parquet "$CITIES_GCS_PREFIX/transformed/daily_air_quality/placeholder.parquet" --quiet 2>&1 | tee -a "$LOG_FILE"; then
    echo "Error uploading placeholder.parquet to GCS. Check the log at $LOG_FILE and try again." | tee -a "$LOG_FILE"
    exit 1
fi

if ! gcloud storage cp ./placeholder.parquet "$CITIES_GCS_PREFIX/transformed/daily_land_surface/placeholder.parquet" --quiet 2>&1 | tee -a "$LOG_FILE"; then
    echo "Error uploading placeholder.parquet to GCS. Check the log at $LOG_FILE and try again." | tee -a "$LOG_FILE"
    exit 1
fi

# copy setup fle into composer env bucket
COMPOSER_DATA_GCS_PREFIX=$(terraform output -raw composer_data_gcs_prefix)
if ! gcloud storage cp ../include/init-scripts/setup_gx.py "$COMPOSER_DATA_GCS_PREFIX/setup_gx.py" --quiet 2>&1 | tee -a "$LOG_FILE"; then
    echo "Error uploading include/init-scripts/setup_gx.py to GCS. Check the log at $LOG_FILE and try again." | tee -a "$LOG_FILE"
    exit 1
fi

# unpausing the setup_gx dag to run
if ! gcloud composer environments run "$COMPOSER_ENVIRONMENT_NAME" --project="$PROJECT_ID" --location="$LOCATION" dags unpause -- setup_great_expectations 2>&1 | tee -a "$LOG_FILE"; then
    echo "Error unpausing setup_great_expectations DAG. Check the log at $LOG_FILE and try again." | tee -a "$LOG_FILE"
    exit 1
fi
# trigger setup_gx dag to run
if ! gcloud composer environments run "$COMPOSER_ENVIRONMENT_NAME" --project="$PROJECT_ID" --location="$LOCATION" dags trigger -- setup_great_expectations 2>&1 | tee -a "$LOG_FILE"; then
    echo "Error setting up Great Expectations. Check the log at $LOG_FILE and try again." | tee -a "$LOG_FILE"
    exit 1
fi

# get date value from terraform output
HISTORICAL_DATA_START_DATE=$(terraform output -raw historical_data_start_date)

# validate that the date is in the correct format (YYYY-MM-DD)
if ! [[ "$HISTORICAL_DATA_START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] && date -d "$HISTORICAL_DATA_START_DATE" >/dev/null 2>&1; then
    echo "Error: historical_data_start_date is not in the correct format (YYYY-MM-DD). Please check your Terraform configuration." | tee -a "$LOG_FILE"
    exit 1
fi

# unpausing the historical_backfill dag to run
if ! gcloud composer environments run "$COMPOSER_ENVIRONMENT_NAME" --project="$PROJECT_ID" --location="$LOCATION" dags unpause -- historical_backfill 2>&1 | tee -a "$LOG_FILE"; then
    echo "Error unpausing historical_backfill DAG. Check the log at $LOG_FILE and try again." | tee -a "$LOG_FILE"
    exit 1
fi

# trigger the historical data extraction dag to run
if ! gcloud composer environments run "$COMPOSER_ENVIRONMENT_NAME" --project="$PROJECT_ID" --location="$LOCATION" dags trigger -- historical_backfill 2>&1 | tee -a "$LOG_FILE"; then
    echo "Error triggering historical_backfill DAG. Check the log at $LOG_FILE and try again." | tee -a "$LOG_FILE"
    exit 1
fi


GITHUB_CONNECTION_NAME=$(terraform output -raw github_connection_name)
SERVICE_ACCOUNT_EMAIL=$(terraform output -raw cloud_build_presubmit_service_account)
SERVICE_ACCOUNT="projects/${PROJECT_ID}/serviceAccounts/${SERVICE_ACCOUNT_EMAIL}"

# check if the trigger already exists
if gcloud builds triggers list --region="$LOCATION" --filter="name=presubmit-checks" | grep -q "presubmit-checks"; then
    echo "Cloud Build trigger for presubmit checks already exists. Skipping creation." | tee -a "$LOG_FILE"
else
    echo "Creating Cloud Build trigger for presubmit checks..." | tee -a "$LOG_FILE"
# create cloud build trigger for presubmit checks
    if ! gcloud builds triggers create github \
        --project="$PROJECT_ID" \
        --name="presubmit-checks" \
        --repository=projects/"$PROJECT_ID"/locations/"$LOCATION"/connections/"$GITHUB_CONNECTION_NAME"/repositories/climate-data-pipeline \
        --pull-request-pattern="^main$" \
        --build-config="test-dags.cloudbuild.yaml" \
        --region="$LOCATION"  \
        --service-account="$SERVICE_ACCOUNT" \
        --comment-control="COMMENTS_DISABLED"  2>&1 | tee -a "$LOG_FILE"; then
        echo "Error creating Cloud Build trigger for presubmit checks. Check the log at $LOG_FILE and try again." | tee -a "$LOG_FILE"
        exit 1
    fi
fi


ADD_DAGS_TO_COMPOSER_SERVICE_ACCOUNT_EMAIL=$(terraform output -raw cloud_build_add_dags_to_composer_sa)
ADD_DAGS_TO_COMPOSER_SERVICE_ACCOUNT="projects/${PROJECT_ID}/serviceAccounts/${ADD_DAGS_TO_COMPOSER_SERVICE_ACCOUNT_EMAIL}"
DAGS_BUCKET_NAME=$(echo "$DAG_GCS_PREFIX" | sed -E 's|^gs://([^/]+)/.*|\1|')
_DAGS_BUCKET=$(echo "$DAGS_BUCKET_NAME" | sed -E 's|/dags/?$||')

DAGS_BUCKET_NAME=$(echo "$DAGS_BUCKET" | sed -E 's|^gs://([^/]+)/.*|\1|')

# check if the trigger already exists
if gcloud builds triggers list --region="$LOCATION" --filter="name=add-dags-to-composer" | grep -q "add-dags-to-composer"; then
    echo "Cloud Build trigger for add_dags_to_composer already exists. Skipping creation." | tee -a "$LOG_FILE"
else
    echo "Creating Cloud Build trigger for add_dags_to_composer..." | tee -a "$LOG_FILE"
    # create cloud build trigger for add_dags_to_composer trigger
    if ! gcloud builds triggers create github \
        --project="$PROJECT_ID" \
        --name="add-dags-to-composer" \
        --repository=projects/"$PROJECT_ID"/locations/"$LOCATION"/connections/"$GITHUB_CONNECTION_NAME"/repositories/climate-data-pipeline \
        --branch-pattern="^main$" \
        --build-config="add-dags-to-composer.cloudbuild.yaml" \
        --region="$LOCATION"  \
        --service-account="$ADD_DAGS_TO_COMPOSER_SERVICE_ACCOUNT" \
        --substitutions=_DAGS_DIRECTORY="dags/",_DAGS_BUCKET=${_DAGS_BUCKET}  2>&1 | tee -a "$LOG_FILE"; then
        echo "Error creating Cloud Build trigger for add_dags_to_composer. Check the log at $LOG_FILE and try again." | tee -a "$LOG_FILE"
        exit 1
    fi
fi