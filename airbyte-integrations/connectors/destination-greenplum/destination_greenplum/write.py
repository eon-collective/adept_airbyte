import psycopg2
from psycopg2 import extras
import logging
from typing import Any, Iterable, Mapping


from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    Status,
    Type,
)

logging = logging.getLogger("airbyte")

class GreenplumWriter:
    """
    The GreenplumWriter class is used to write data and manupulate objects on a Greenplum database.

    Args:
        configs (Mapping[str, Any]): A mapping of configuration properties to values.

    Attributes:
        host (str): The hostname or IP address of the Greenplum database.
        port (int): The port number of the Greenplum database.
        username (str): The username used to connect to the Greenplum database.
        password (str): The password used to connect to the Greenplum database.
        database (str): The name of the Greenplum database.
        schema (str): The name of the schema to which data will be written.

    """

    def __init__(self, configs: Mapping[str, Any]):
        self.host = configs.get("host")
        self.port = configs.get("port")
        self.username = configs.get("username")
        self.password = configs.get("password")
        self.database = configs.get("database")
        self.schema = configs.get("schema")

    def _greenplum_connection(self) -> psycopg2.connect:
        """
        Creates a psycopg2 connection to the Greenplum database.
        
        Returns:
        psycopg2.connection: A psycopg2 connection to the Greenplum database.
        """

        connector = psycopg2.connect(host=self.host, port=self.port, user=self.username, password=self.password, database=self.database)
        return connector
    
    def greenplum__writer(self, query, values=None) -> None:
        """
        Creates and manipulates objects (Tables, Databases, ...) on a Greenplum database.
        Important: This method can be used to write data when the values parameter is provided.

        Args:
            query (str): The SQL query to be executed.
            values (Any, optional): The values to be used in the query. Defaults to None.

        """
        logging.info(msg=f"Connecting to Greenplum {self.host}:{self.port}")
        connector = self._greenplum_connection()
        cursor = connector.cursor()

        cursor.execute(query=query, vars=values)
        connector.commit()
        logging.info(msg=f'Sql Executed {query}', exc_info=True)
        cursor.close()
        logging.info(msg=f"Objects written to Greenplum {self.host}:{self.port}")
        
    def greenplum__writer_insert(self, query, values) -> None:
        """
        Writes data to a Greenplum database using an insert query.

        Args:
            query (str): The SQL insert query to be executed.
            values (Iterable[Mapping[str, Any]]): A list of dictionaries containing the data to be inserted. Each dictionary must contain the column names as keys and the values as values.

        """
        logging.info(msg=f"Connecting to Greenplum {self.host}:{self.port}")
        connector = self._greenplum_connection()
        cursor = connector.cursor()
        extras.execute_batch(cur=cursor, sql=query, argslist=values, page_size=1000)
        # cursor.executemany(query=query, vars_list=values)
        # print(query)
        # (query=query, vars_list=values)
        connector.commit()
        logging.info(msg=f'Sql Executed {query}', exc_info=True)
        cursor.close()
        logging.info(msg=f"Objects written to Greenplum {self.host}:{self.port}")  

    def greenplum__connection_close(self) -> None:
        """
        Closes the psycopg2 connection to the Greenplum database.

        Returns:
            None
        """

        connector = self._greenplum_connection()
        connector.close()


class NormalizationWriter(GreenplumWriter):
    """
    The NormalizationWriter class is used to write data and manupulate objects on a Greenplum database.

    Args:
        configs (Mapping[str, Any]): A mapping of configuration properties to values."""
    
    def __init__(self, configs: Mapping[str, Any]):
        super().__init__(configs=configs)
        self.configured_catalog = ConfiguredAirbyteCatalog
        self.input_messages: Iterable[AirbyteMessage]

    def _datatypes_mapping(self):
        return {
            "string": "text",
            "boolean": "boolean",
            "bit": "boolean",
            "date": "date",
            "decimal": "NUMERIC",
            "double": "DOUBLE PRECISION",
            "float": "REAL",
            "int": "INTEGER",
            "json": "JSON",
            "jsonb": "JSONB",
            "text": "TEXT",
            "time": "TIME WITHOUT TIME ZONE",
            "timestamp": "TIMESTAMP WITHOUT TIME ZONE",
            "timestamptz": "TIMESTAMP WITH TIME ZONE",
            "uuid": "UUID",
        }


    
