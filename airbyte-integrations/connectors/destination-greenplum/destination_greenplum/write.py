import psycopg2
import logging
from typing import Any, Iterable, Mapping

logging = logging.getLogger("airbyte")

class GreenplumWriter:
    def __init__(self, configs: Mapping[str, Any]):
        self.host = configs.get("host")
        self.port = configs.get("port")
        self.username = configs.get("username")
        self.password = configs.get("password")
        self.database = configs.get("database")
        self.schema = configs.get("schema")
        
    def greenplum__writer(self, query, values=None) -> None:
        logging.info(msg=f"Connecting to Greenplum {self.host}:{self.port}")
        connector = psycopg2.connect(host=self.host, port=self.port, user=self.username, password=self.password, database=self.database)
        cursor = connector.cursor()

        cursor.execute(query=query, vars=values)
        connector.commit()
        logging.info(msg=f'Sql Executed {query}', exc_info=True)
        cursor.close()
        connector.close()
        logging.info(msg=f"Objects written to Greenplum {self.host}:{self.port}")
        
    def _greenplum_connection(self) -> None:
        connector = psycopg2.connect(host=self.host, port=self.port, user=self.username, password=self.password, database=self.database)
        return connector

    def greenplum__writer_insert(self, query, values) -> None:
        logging.info(msg=f"Connecting to Greenplum {self.host}:{self.port}")
        connector = self._greenplum_connection()
        cursor = connector.cursor()
        cursor.executemany(query=query, vars_list=values)
        connector.commit()
        logging.info(msg=f'Sql Executed {query}', exc_info=True)
        cursor.close()
        logging.info(msg=f"Objects written to Greenplum {self.host}:{self.port}")

    def greenplum__writer_get(self, query) -> None:
        try:
            logging.info(msg=f"Connecting to Greenplum {self.host}:{self.port}")
            connector = self._greenplum_connection()
            cursor = connector.cursor()
            cursor.execute(query=query)
            records = cursor.fetchall()
            logging.info(msg=f'Sql Executed {query}', exc_info=True)
            cursor.close()
            logging.info(msg=f"Objects written to Greenplum {self.host}:{self.port}")
        except Exception as e:
            logging.info(msg=f"An exception occurred: {repr(e)}")
        return records
    
    def greenplum__connection_close(self) -> None:
        connector = self._greenplum_connection()
        connector.close()

