# airflow-spark-operator-livy-batch-plugin

## Influence
Infuenced by https://github.com/rssanders3/airflow-spark-operator-plugin and using batch instead of sessions

## Description

A plugin to Apache Airflow (Documentation: https://pythonhosted.org/airflow/, Source Code: https://github.com/apache/incubator-airflow) to allow you to run Apache Spark Commands as an Operator from Workflows


## TODO List

* Test extensively  
* Create more examples

## How do Deploy
 
1. Copy the spark_livy_operator_plugin.py file into the Airflow Plugins directory

    * The Airflow Plugins Directory is defined in the airflow.cfg file as the variable "plugins_folder"
    
    * The Airflow Plugins Directory is, by default, ${AIRFLOW_HOME}/plugins
    
    * You may have to create the Airflow Plugins Directory folder as it is not created by default
  
2. Restart the Airflow Services

3. Create or Deploy DAGs which utilize the Operator

4. Your done!

## Livy Spark Batch Operator

### Operator Definition

class **airflow.operators.LivySparkBatchOperator**(application_file, class_name, session_config,, http_conn_id=None, poll_interval=10, *args, **kwargs)

Bases: **airflow.models.BaseOperator**

Operator to facilitate interacting with the Livy Server which executes Apache Spark code via a REST API.

Parameters:

* **application_file** (string) - Jar to submit to the Livy Server (templated)
* **class_name** (string) - Class containing the main method.
* **session_config** (string) - all additional configuration to be passed as json.
* **http_conn_id** (string) - The http connection to run the operator against.
* **poll_interval** (integer) - The polling interval to use when checking if the code in spark_script has finished executing. In seconds. (default: 30 seconds)

### Session_config
Example of session_config to be sent  
{
  "proxyUser": "spark",  
  "args": ["dummy1", "dummy2"],  
  "jars": ["random-0.0.1.jar", "impatient-1.0.jar"],  
  "driverMemory": "12g",  
  "conf": {"spark.yarn.maxAppAttempts": "1", "spark.network.timeout": "1500s"}  
} 
For all the parameters that can be passed please visit  
https://livy.incubator.apache.org/docs/latest/rest-api.html
POST /batches section


### Prerequisites

1. The Livy Server needs to be setup on the desired server.
    
    * Livy Source Code: https://github.com/cloudera/livy

2. Add an entry to the Connections list that points to the Livy Server
 
    1. Open the Airflow WebServer
    2. Navigate to Admin -> Connections
    3. Create a new connection
        
      * Set the Conn Id as some unique value to identify it (example: livy_http_conn) and use this value as the http_conn_id
      * Set the Conn Type as "http"
      * Set the host
      * Set the port (default for livy is 8998)


## Steps done by the Operator

1. Accept all the required inputs
2. Establish an HTTP Connection with the Livy Server via the information provided in the http_conn_id
3. Submit the Batch
4. Poll to see if the Batch has completed running
5. Print the logs and the output of the Batch Logs
6. Close the Livy Spark Session


### How to use the Operator

There is a scala examples on how to use the operator under example_dags.

Import the LivySparkOperator using the following line:

    ```
    from airflow.operators import LivySparkBatchOperator

    ```
