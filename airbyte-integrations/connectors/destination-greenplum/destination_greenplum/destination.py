#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import datetime
import json
import uuid
from typing import Any, Iterable, Mapping
import psycopg2
import logging
from collections import defaultdict

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status
from destination_greenplum.write import GreenplumWriter

from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    Status,
    Type,
)

loger = logging.getLogger("airbyte")
class DestinationGreenplum(Destination):
    def write(
        self, 
        config: Mapping[str, Any], 
        configured_catalog: ConfiguredAirbyteCatalog, 
        input_messages: Iterable[AirbyteMessage]) -> Iterable[AirbyteMessage]:

        """
        
        Reads the input stream of messages, config, and catalog to write data to the destination.

        This method returns an iterable (typically a generator of AirbyteMessages via yield) containing state messages received
        in the input message stream. Outputting a state message means that every AirbyteRecordMessage which came before it has been
        successfully persisted to the destination. This is used to ensure fault tolerance in the case that a sync fails before fully completing,
        then the source is given the last state message output from this method as the starting point of the next sync.

        :param config: dict of JSON configuration matching the configuration declared in spec.json
        :param configured_catalog: The Configured Catalog describing the schema of the data being received and how it should be persisted in the
                                    destination
        :param input_messages: The stream of input messages received from the source
        :return: Iterable of AirbyteStateMessages wrapped in AirbyteMessage structs
        """
        streams =  {configured_stream.stream.name: configured_stream for configured_stream in configured_catalog.streams}
        loger.info(msg=f"Starting write to Greenplum {len(streams)} streams")

        schema_name = config.get("schema")
        greenplumwriter = GreenplumWriter(config)

        for configured_stream in configured_catalog.streams:
            name = configured_stream.stream.name
            table_name = f"_airbyte_raw_{name}"
            if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
                # delete the tables
                loger.info(msg=f"Dropping tables for overwrite: {table_name}")
                query = f"DROP TABLE IF EXISTS {table_name}"
                greenplumwriter.greenplum__writer(query)
                loger.info(msg=f"Table dropped: {table_name}")
                
            
            # create the table if needed
            query = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ( _airbyte_ab_id TEXT PRIMARY KEY, _airbyte_emitted_at timestamp, _airbyte_data JSON);"""
            loger.info(msg=f'{query}')
            greenplumwriter.greenplum__writer(query)
            loger.info(msg=f"Table created: {schema_name}.{table_name}")

        buffer = defaultdict(list)

        for message in input_messages:
            
            if message.type == Type.STATE:
                
                for stream_name in buffer.keys():

                    loger.info(f"---mesage: {message}")

                    query = f"""
                    INSERT INTO {schema_name}._airbyte_raw_{stream_name}
                    VALUES (%s, %s, %s)
                    """
                    loger.info(f"Inserting {len(buffer[stream_name])} rows into {table_name}")
                    greenplumwriter.greenplum__writer_insert(query=query, values=buffer[stream_name])
                    loger.info(msg=f"rows inserted: {len(buffer[stream_name])}")
                    greenplumwriter.greenplum__writer_insert(query=query, values=buffer[stream_name])
                    loger.info(f'Sql Executed {self.sql}', exc_info=True)
                    buffer = defaultdict(list)
                    yield message
            elif message.type == Type.RECORD:
                
                data = message.record.data
                stream = message.record.stream
                loger.info(msg=f"{stream} Details {message.type}")
                if stream not in streams:
                    loger.debug(f"Stream {stream} was not present in configured streams, skipping")
                    continue

                # add to buffer
                buffer[stream].append(
                    (
                        str(uuid.uuid4()), 
                        datetime.datetime.now().isoformat(),
                        json.dumps(data)
                    )
                )
                
            else:
                loger.info(f"Message type {message.type} not supported, skipping")

        # flush any remaining messages
        for stream_name in buffer.keys():
            table_name = f"_airbyte_raw_{stream_name}"
            query = f"""
            INSERT INTO {schema_name}.{table_name}
            VALUES (%s,%s,%s)
            """
            loger.info(msg=f"Inserting {len(buffer[stream_name])} rows into {table_name}")
            greenplumwriter.greenplum__writer_insert(query=query, values=buffer[stream])
            loger.info(msg=f"rows inserted: {len(buffer[stream_name])}")
            buffer = defaultdict(list)
            yield message


    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the destination with the needed permissions
            e.g: if a provided API token or password can be used to connect and write to the destination.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this destination, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            host = config.get("host")
            port = config.get("port")
            username = config.get("username")
            password = config.get("password")
            database = config.get("database")
            connector = psycopg2.connect(host=host, port=port, user=username, password=password, database=database)
            cursor = connector.cursor()
            cursor.execute("select 1;")
            cursor.fetchall()
            return AirbyteConnectionStatus(status=Status.SUCCEEDED, message="Succesfully connected to Greenplum Instance!!!!")
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
