import os
import time
import google.auth
from airflow.sdk.observability import stats
from google.cloud import monitoring_v3

def emit_gauge(metric_name: str, value, labels=None):
    deployment_option = os.getenv('STORAGE_BACKEND')
    if deployment_option == 'local':
        metric_name = metric_name.replace('/', '.')
        stats.gauge(stat=metric_name, value=value, tags=labels)
    elif deployment_option == 'gcs':
        client = monitoring_v3.MetricServiceClient()
        credentials, project_id = google.auth.default()
        project_name = f"projects/{project_id}"
        
        series = monitoring_v3.TimeSeries()
        series.metric.type = f"custom.googleapis.com/{metric_name}"

        if labels:
            for key, val in labels.items():
                series.metric.labels[key] = val

        series.resource.type = "global"
        series.resource.labels['project_id'] = project_id

        point = monitoring_v3.Point()
        point.value.double_value = value
        point.interval.end_time.seconds = int(time.time())

        series.points = [point]

        client.create_time_series(name=project_name, time_series=[series])
