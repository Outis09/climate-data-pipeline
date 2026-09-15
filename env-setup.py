from pathlib import Path
import secrets
from cryptography.fernet import Fernet

env_file = Path(".env")

if env_file.exists():
    raise SystemExit(".env file already exists. To overwrite, open file and edit directly.")

fernet_key = Fernet.generate_key().decode()
jwt_secret = secrets.token_urlsafe(48)

content = f"""
# -------------------------Generated secrets-------------------------
FERNET_KEY={fernet_key}
AIRFLOW__API_AUTH__JWT_SECRET={jwt_secret}
AIRFLOW__API_AUTH__JWT_ISSUER=climate-data-pipeline

# -------------------------Airflow configuration-------------------------
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
AIRFLOW_PG_USER=airflow
AIRFLOW_PG_PASSWORD=airflow
AIRFLOW_PG_DB=airflow

# -------------------------Weather database configuration-------------------------
WEATHER_DB_USER=weather
WEATHER_DB_PASSWORD=chnage_me
WEATHER_DB_NAME=weather_data

# ------------------------PGAdmin configuration------------------------------
PGADMIN_DEFAULT_EMAIL=admin@example.com
PGADMIN_DEFAULT_PASSWORD=admin

# -------------------------SMTP configuration-------------------------
# format: smtp://[url-encoded email]:[email app password]smtp.gmail.com:587?from_email=[url-encoded email]
SMTP_DEFAULT=smtp://some_email%40gmail.com:my_app_password@smtp.gmail.com:587?from_email=some_email%40gmail.com

# -------------------------Climate pipeline configuration-------------------------
CLIMATE_COUNTRY=Ghana
START_DATE=2001-01-01


# -------------------------Grafana configuration-------------------------
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=admin
"""

env_file.write_text(content)

print("Created .env with generated application secrets")