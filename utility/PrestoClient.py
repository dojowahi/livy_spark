from pyhive import presto
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import logging
import inspect


class PrestoClient:
    __host = None
    __user = None
    __password = None
    __db = None
    __port = None
    __auth = None
    __con = None
    __cur = None

    def __init__(self, user, pwd):
       #self.__host = '10.220.33.77'
       self.__host = 'enterprisehadoopuser.bb.com'
       self.__user = user
       self.__password = pwd  # prompted below for password
       self.__port = 8081
       self.__auth = 'CUSTOM'

    def connect(self):
        try:
            requests.packages.urllib3.disable_warnings()
            req_kw = {'auth': HTTPBasicAuth(self.__user, self.__password),'verify':False}
            self.__con = presto.connect(host=self.__host,port=int(self.__port), protocol='https',catalog='hive',username=self.__user,
                                        requests_kwargs=req_kw)
            self.__cur = self.__con.cursor()
            print('Successfully connected to presto')
        except Exception as e:
            logging.error("Failed to Connect Hive | caller_function: %s | host: %s", inspect.stack()[1][3], self.__host)

    def disconnect(self):
        try:
            self.__cur.close()
            self.__con.close()
        except Exception as e:
            logging.error('failed closing hive connection', e)

    def fetch(self, query):
        try:
            return pd.read_sql(sql=query, con=self.__con)
        except Exception as ex:
            logging.error('Hive exception: %s | caller_function: %s | query: %s',
                          inspect.stack()[1][3], query, exc_info=1)

    def execute_query(self, query):
            self.__cur.execute(query) #orig
            result=self.__cur.next()
