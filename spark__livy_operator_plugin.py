# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils import apply_defaults
import logging
import textwrap
import time
import json

class LivySparkBatchOperator(BaseOperator):
    """
   Operator to facilitate interacting with the Livy Server which executes Apache Spark code via a REST API.

   :param application_file: the application file typically a jar file (templated)
   :type application_file: string
   :param session_config: Type of session to setup with Livy. This will determine which type of code will be accepted. Possible values include "spark" (executes Scala code), "pyspark" (executes Python code) or "sparkr" (executes R code).
   :type session_config: string
   :param http_conn_id: The http connection to run the operator against
   :type http_conn_id: string
   :param poll_interval: The polling interval to use when checking if the code in spark_script has finished executing. In seconds. (default: 30 seconds)
   :type poll_interval: integer
   """

    template_fields = ['http_conn_id']  # todo : make sure this works
    ui_color = '#34a8dd'  # Clouderas Main Color: Blue

    acceptable_response_codes = [200, 201]
    statement_non_terminated_status_list = ['running', 'starting']

    @apply_defaults
    def __init__(
            self,
            application_file,
            class_name,
            session_config,  # configuration as json (driverMemory etc)
            http_conn_id='http_default',
            poll_interval=10,
            *args, **kwargs):
        super(LivySparkBatchOperator, self).__init__(*args, **kwargs)

        self.application_file = application_file
        self.class_name = class_name
        self.session_config = session_config
        self.http_conn_id = http_conn_id
        self.poll_interval = poll_interval

    def execute(self, context):
        logging.info("Executing LivySparkOperator.execute(context)")
        logging.info("Validating arguments...")
        self._validate_arguments()
        logging.info("Finished validating arguments")

        logging.info("Creating a Livy Batch...")
        batch_id = self._submit_batch()
        logging.info("Finished creating a Livy batch. (batch_id: " + str(batch_id) + ")")

        batch_state = self._get_batch_state(batch_id=batch_id)

        poll_for_completion = (batch_state in self.statement_non_terminated_status_list)

        if poll_for_completion:
            logging.info("Spark job did not complete immediately. Starting to Poll for completion...")

        while self._get_batch_state(batch_id=batch_id) in self.statement_non_terminated_status_list:  # todo: test execution_timeout
            logging.info("Sleeping for " + str(self.poll_interval) + " seconds...")
            time.sleep(self.poll_interval)
            logging.info("Finished sleeping. Checking if Spark job has completed...")
            logging.info("Batch Logs:\n" + str(self._get_batch_logs(batch_id=batch_id)))
            if (self._get_batch_state(batch_id)) == "success":
                batch_state == "success"
            elif (self._get_batch_state(batch_id)) == "dead":
                print("am I dead")
                logging.info("Batch Logs:\n" + str(self._get_batch_logs(batch_id=batch_id)))
                logging.error("Batch failed. (batch_id: " + str(batch_id) + "\n" )
                response = self._close_session(batch_id=batch_id)
                logging.error("Closed session. (response: " + str(response) + ")")
                raise Exception("Batch failed. (batch_id: " + str(batch_id)) + "\n"
            logging.info("Finished checking if Spark job has completed. (batch_state: " + self._get_batch_state(batch_id=batch_id) + ")")
        if poll_for_completion:
            logging.info("Finished Polling for completion.")

        logging.info("Batch Logs:\n" + str(self._get_batch_logs(batch_id=batch_id)))
        logging.info("Closing session...")
        response = self._close_session(batch_id=batch_id)
        logging.info("Finished closing batch. (response: " + str(response) + ")")
        logging.info("Finished executing LivySparkOperator.execute(context)")


    def _validate_arguments(self):
        if self.session_config is None or self.session_config == "":
            raise Exception(
                "session_config argument is invalid. It is empty or None. (value: '" + str(self.session_config) + "')")

    def _get_batch_logs(self, batch_id):
        method = "GET"
        endpoint = "batches/" + str(batch_id) + "/log"
        response = self._http_rest_call(method=method, endpoint=endpoint)
        return response.json()

    def _get_batch_state(self, batch_id):
        method = "GET"
        endpoint = "batches/" + str(batch_id) + "/state"
        response = self._http_rest_call(method=method, endpoint=endpoint)
        return response.json()["state"]

    def _submit_batch(self):
        method = "POST"
        endpoint = "batches"
        application_jar_file = {"file": self.application_file}
        application_class_name = {"className": self.class_name}
        raw_data = self.session_config
        raw_data.update(application_jar_file)
        raw_data.update(application_class_name)
        print(raw_data)
        string_data = json.dumps(raw_data)
        data = json.loads(string_data)
        logging.info("Executing Spark batch: \n" + str(self.class_name))

        response = self._http_rest_call(method=method, endpoint=endpoint, data=data)

        if response.status_code in self.acceptable_response_codes:
            response_json = response.json()
            return response_json["id"]
        else:
            raise Exception("Call to create a new batch didn't return " + str(self.acceptable_response_codes) + ". Returned '" + str(response.status_code) + "'.")

    def _close_session(self, batch_id):
        method = "DELETE"
        endpoint = "batches/" + str(batch_id)
        return self._http_rest_call(method=method, endpoint=endpoint)

    def _http_rest_call(self, method, endpoint, data=None, headers=None, extra_options=None):
        if not extra_options:
            extra_options = {}
        logging.debug("Performing HTTP REST call... (method: " + str(method) + ", endpoint: " + str(endpoint) + ", data: " + str(data) + ", headers: " + str(headers) + ")")
        self.http = HttpHook("GET", http_conn_id=self.http_conn_id)
        self.http.method = method
        response = self.http.run(endpoint, json.dumps(data), headers, extra_options=extra_options)

        logging.debug("status_code: " + str(response.status_code))
        logging.debug("response_as_json: " + str(response.json()))

        return response


# Defining the plugin class
class SparkOperatorPlugin(AirflowPlugin):
    name = "spark_livy_operator_plugin"
    operators = [LivySparkBatchOperator]
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = []
    menu_links = []
