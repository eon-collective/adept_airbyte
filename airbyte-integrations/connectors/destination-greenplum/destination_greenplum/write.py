import psycopg2
import logging
from typing import Any, Iterable, Mapping

class GreenplumWriter:
    def __init__(self, configs: Mapping[str, Any]):
        host = configs.get("host")
        port = configs.get("port")
        username = configs.get("username")
        password = configs.get("password")
        database = configs.get("database")

    def 
        

