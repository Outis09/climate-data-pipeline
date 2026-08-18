FROM apache/airflow:3.3.0
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt 
# \
#     --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"