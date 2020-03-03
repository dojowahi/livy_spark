import requests
import logging
from random import randint
import json
import time


class SparkLivySql:

    __livy_url = None
    __headers = None
    __livy_session = None

    def __init__(self, livy_url, ):
        self.__livy_url = livy_url
        self.__headers = {'Content-Type': 'application/json'}


    def get_livy_session(self, spark_conf, session_prefix):
        """
        Create livy session for submitting interactive Spark SQL commands
        :param spark_conf: spark config for the queries that need to be executed
        :param session_prefix: a name for the session
        :return: livy session id
        """

        value = randint(1, 1000)

        directive = '/sessions'
        session_name = session_prefix+"_"+str(value)
        data = {'kind': 'sql', 'name': session_name, 'conf': spark_conf}

        session_response = requests.post(self.__livy_url + directive, headers=self.__headers, data=json.dumps(data))
        print(session_response.json())

        if session_response.status_code == requests.codes.created:
            session_id = session_response.json()['id']
            current_state = session_response.json()['state']
            print(session_id)
        else:
            raise Exception("Unable to create session")
            exit(1)

        while current_state != 'idle':
            time.sleep(20)
            get_session_status = requests.get(self.__livy_url + f'/sessions/{session_id}')
            current_state = get_session_status.json()['state']
            print("Session {1} State is {0}".format(current_state, session_id))

        get_session_status = requests.get(self.__livy_url + f'/sessions/{session_id}')
        app_id = get_session_status.json()['appId']

        print("Now you are ready to submit SQL statements to session {0} and application Id {1}".format(session_id, app_id))
        return session_id

    # Pass session_id created by function above, and query that needs to be exc
    def post_spark_sql(self, session_id, query):
        """
        Execute spark sql
        :param session_id: value returned by method get_livy_session
        :param query: spark-sql that needs to be executed
        :return: stmt id under which the spark sql is being executed
        """
        data = {'code': query}
        directive = f'/sessions/{session_id}/statements'

        post_sql = requests.post(self.__livy_url + directive, headers=self.__headers, data=json.dumps(data))
        print(post_sql.json())

        if post_sql.status_code == requests.codes.created:
            stmt_id = post_sql.json()['id']
            # status = self.monitor_sql(session_id, stmt_id)
            return stmt_id
        else:
            logging.error("SQL could not be submitted. Messed up")
            raise Exception("Unable to submit query - {}".format(query))
            return -999

    def monitor_sql(self, session_id, stmt_id, sleeper=30):
        """
        Monitor progress of the Spark SQL
        :param session_id: livy session under which sql is being executed
        :param stmt_id: id of the spark sql that needs to be monitored
        :param sleeper: number of seconds after which progress has to be checked
        :return: status of sql
        """
        sql_monitor = 0
        while True:
            sql_response = requests.get(self.__livy_url + f'/sessions/{session_id}/statements/{stmt_id}')
            data = sql_response.json()
            resp_stmt_id = data['id']
            state = data['state']
            progress = data['progress']
            print("Session {0} Stmt {1} State {2} Progress {3}".format(session_id, resp_stmt_id, state, progress))
            if sql_response.status_code == requests.codes.ok:
                state = sql_response.json()['state']
                if state in ('waiting', 'running'):
                    time.sleep(sleeper)
                elif state in ('cancelling', 'cancelled', 'error'):
                    print("job is failing")
                else:
                    break
            else:
                print("SQL Response Failed. Messed up")
                sql_monitor = 1

        if sql_monitor == 1:
            return False
        else:
            print(sql_response.json()['output'])
            return True

    def cleanup_session(self, session_id):
        """
        Delete livy session
        :param session_id: livy session that needs to be deleted
        :return: json response
        """
        try:
            print("Delete session {0}".format(session_id))
            delete_resp = requests.delete(self.__livy_url + f'/sessions/{session_id}')
        except Exception as ex:
            logging.error("Session Not found")

    def post_spark_job_submit(self, **kwargs):
        body = {}
        try:
            for key, value in kwargs.items():
                body[key] = value
            print(body)
            param = requests.post(self.__livy_url + '/batches', data=json.dumps(body), headers=self.__headers)
            return param.json()
        except Exception as ex:
            print("Livy failed to submit spark-job")

    def get_spark_job_status(self, batch_id):
        try:
            param = requests.get(self.__livy_url + '/batches/' + str(batch_id))
            response = param.json()
            print(response)
            batch_id = response["id"]
            state = response["state"]
            app_id = response["appId"]
            driver_log_url = response.get("appInfo").get("driverLogUrl")

            spark_ui_url = response.get("appInfo").get("sparkUiUrl")
            return {'LIVY_BATCH_ID': batch_id, 'SPARK_APP_ID': app_id, 'STATE': state, 'SPARK_UI_URL': spark_ui_url,
                    'SPARK_LOG_URL': driver_log_url}
        except Exception as ex:
            raise Exception("Failed to get_spark_job_status | host=%s | batch_id: %s", self.__livy_url, batch_id)

    def monitor_tier_process(self, batch_id):
        """Monitors ETL process
        :param batch_id: Livy batch id of the execution process
        :param livy_url: URL at which Livy is running
        :return: True if ETL process finishes successfully
        """
        # Execute while loop for monitoring till ETL fails or completes successfully
        while True:
            try:
                # Check status after every minute
                time.sleep(600)
                response = self.get_spark_job_status(batch_id)
                state = response['STATE']
                spark_app_id = response['SPARK_APP_ID']
                spark_ui_url = response['SPARK_UI_URL']
                batch_id = response['LIVY_BATCH_ID']
                if response['SPARK_LOG_URL'] is not None:
                    spark_log_url = response['SPARK_LOG_URL']

                # Take appropriate action based on state of ETL job
                if state in ['starting', 'running', 'busy', 'not_started']:
                    # Continue monitoring
                    print("Batch Id {} is {} at app {}".format(batch_id,state,spark_app_id))
                    continue
                elif state in ['killed', 'dead', 'error', 'shutting_down']:
                    print('ETL has been failed with state {0}'.format(state))

                elif state == 'success':
                    print('ETL has been completed successfully')
                else:
                    print('ETL has been failed with state {0}'.format(state))

                return True

            except Exception as ex:
                print('Failed to get_spark_job_status | batch_id: {0}'.format(batch_id))

    def list_statements(self, session_id):
        try:
            print("Listing statement ids {0}".format(session_id))
            list_resp = requests.get(self.__livy_url + f'/sessions/{session_id}/statements')
            print(list_resp)
        except Exception as ex:
            logging.error("Session Not found")