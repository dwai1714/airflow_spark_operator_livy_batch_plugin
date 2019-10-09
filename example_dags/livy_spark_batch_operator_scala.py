from __future__ import print_function
from airflow.operators import LivySparkBatchOperator
from airflow.models import DAG
from datetime import datetime, timedelta
import os

"""
Pre-run Steps:

1. Open the Airflow WebServer
2. Navigate to Admin -> Connections
3. Add a new connection
    1. Set the Conn Id as "livy_http_conn"
    2. Set the Conn Type as "http"
    3. Set the host
    4. Set the port (default for livy is 8998)
    5. Save
"""

DAG_ID = "spark_livy_batch_operator_scala"

HTTP_CONN_ID = "livy_http_conn"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    }

dag = DAG(DAG_ID, default_args=default_args, schedule_interval=None, start_date=(datetime.now() - timedelta(minutes=1)))

import json
config_parameters = {
  "proxyUser": "spark",
  "args": ["dummy1", "dummy2"],
  "jars": ["random-0.0.1.jar", "impatient-1.0.jar"],
  "driverMemory": "12g",
  "conf": {"spark.yarn.maxAppAttempts": "1", "spark.network.timeout": "1500s"}
}
APPLICATION_JAR = "dummy.jar"
java_class = "com.dwaip.dummy"
SESSION_CONFIG = json.loads(config_parameters)
application_file = "local:" + APPLICATION_JAR

dummy = LivySparkBatchOperator(
    task_id=java_class.rsplit(".", 1)[1],
    application_file=application_file,
    class_name=java_class,
    http_conn_id="livy_http_conn",
    session_config=SESSION_CONFIG,
    poll_interval=3,
    provide_context=True,
    dag=dag)
