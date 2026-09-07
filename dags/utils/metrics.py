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

def emit_cumulative(metric_name, value, labels=None, dag_id=None, start_time=None, run_id=None, task_id=None, map_index=None, try_number=None):
    deployment_option = os.getenv('STORAGE_BACKEND')
    if deployment_option == 'local':
        metric_name = metric_name.replace('/', '.')
        stats.incr(stat=metric_name, count=value, tags=labels)
    elif deployment_option == 'gcs':
        client = monitoring_v3.MetricServiceClient()
        credentials, project_id = google.auth.default()
        project_name = f"projects/{project_id}"

        series = monitoring_v3.TimeSeries()

        series.metric.type = f"custom.googleapis.com/{metric_name}"

        series.metric.labels['limit_type'] = labels.get('limit_type')

        series.resource.type = "generic_task"

        series.resource.labels['project_id'] = project_id
        series.resource.labels['namespace'] = "airflow"

        series.resource.labels['job'] = f"{dag_id}:{run_id}"

        series.resource.labels['task_id'] = f"{task_id}:{map_index}:{try_number}"

        now = time.time()
        # start_seconds = int(start_time.timestamp())
        # start_nanos = int((start_time.timestamp() - start_seconds) * 1e9)

        end_seconds = int(now)
        end_nanos = int((now - end_seconds)  * 1e9)

        point = monitoring_v3.Point()
        point.value.int64_value = int(value)

        point.interval.start_time.seconds = int(start_time.timestamp())
        # point.interval.start_time.nanos = start_nanos

        point.interval.end_time.seconds = int(time.time())
        # point.interval.end_time.nanos = end_nanos

        series.points = [point]

        client.create_time_series(name=project_name, time_series=[series])
