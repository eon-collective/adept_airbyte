#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


# from logging import Logger
from typing import Any, Iterable, List, Mapping, Optional, cast
import urllib
from datetime import datetime
import requests
import pandas as pd
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    Status,
    Type,
)
from destination_veeva_vault.client import VeevaVaultClient
from destination_veeva_vault.config import VeevaVaultConfig
from destination_veeva_vault.writer import VeevaVaultWriter



class DestinationVeevaVault(Destination):
    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
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
        config = cast(VeevaVaultConfig, config)
        writer = VeevaVaultWriter(VeevaVaultClient(config, self.table_metadata(configured_catalog.streams)))

        # Setup: Clear tables if in overwrite mode; add indexes if in append_dedup mode.
        streams_to_delete = []
        indexes_to_add = {}
        metadata = {}
        for configured_stream in configured_catalog.streams:
            metadata["fields"] = configured_stream.stream.json_schema
            metadata["name"] = configured_stream.stream.name
            metadata["sync_mode"] = configured_stream.stream.supported_sync_modes
            metadata["default_cursor_field"]=configured_stream.stream.default_cursor_field
            metadata["source_defined_primary_key"]=configured_stream.stream.source_defined_primary_key
            metadata["namespace"]=configured_stream.stream.namespace

            if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
                streams_to_delete.append(configured_stream.stream.name)
            elif configured_stream.destination_sync_mode == DestinationSyncMode.append_dedup and configured_stream.primary_key:
                indexes_to_add[configured_stream.stream.name] = configured_stream.primary_key
        # if len(streams_to_delete) != 0:
        #     writer.delete_tables(streams_to_delete)
        # if len(indexes_to_add) != 0:
        #     writer.add_indexes(indexes_to_add)

        # Process records
        for message in input_messages:
            if message.type == Type.STATE:
                # Emitting a state message indicates that all records which came before it have been written to the destination. So we flush
                # the queue to ensure writes happen, then output the state message to indicate it's safe to checkpoint state
                writer.flush()
                yield message
            elif message.type == Type.RECORD and message.record is not None:
                start_time = datetime.now()
                start_time = start_time.strftime("%Y-%m-%d %H:%M:%S.%f")

                table_name = self.table_name_for_stream(
                    message.record.namespace,
                    message.record.stream,
                )
                msg = {
                    "tableName": table_name,
                    "data": message.record.data,
                }
                writer.queue_write_operation(msg)

                end_time = datetime.now()
                end_time = end_time.strftime("%Y-%m-%d %H:%M:%S.%f")
                metadata["start_time"] = start_time
                metadata["end_time"] = end_time

                VeevaVaultClient(
                    config, 
                    self.table_metadata(configured_catalog.streams)
                ).write_metadata(metadata)

            else:
                # ignore other message types for now
                continue

        # Make sure to flush any records still in the queue
        writer.flush()

    def table_name_for_stream(self, namespace: Optional[str], stream_name: str) -> str:
        if namespace is not None:
            return f"{namespace}_{stream_name}"
        return stream_name

    def table_metadata(
        self,
        streams: List[ConfiguredAirbyteStream],
    ) -> Mapping[str, Any]:
        table_metadata = {}
        for s in streams:
            # Only send a primary key for dedup sync
            if s.destination_sync_mode != DestinationSyncMode.append_dedup:
                s.primary_key = None
            stream = {
                "primaryKey": s.primary_key,
                "jsonSchema": s.stream.json_schema,
            }
            name = self.table_name_for_stream(
                s.stream.namespace,
                s.stream.name,
            )
            table_metadata[name] = stream
        return table_metadata

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """
        Tests the connection and the API key for the Veeva Vault API Service.
        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        veevaDNS = config.get("vaultDNS")
        api_version = config.get("api_version")
        username = config.get("username")
        password = config.get("password")
        try:
            print("Running check for Veeva Vault Connection. With the following configuration: "+ str(config))
            base_url = f"https://{veevaDNS}.veevavault.com/api/{api_version}"
            final_url = f"{base_url}/auth"

            payload=f'username={urllib.parse.quote(username)}&password={urllib.parse.quote(password)}'
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json'
            }

            response = requests.request("POST", final_url, headers=headers, data=payload)

            status = response.status_code
            logger.info(f"Response code from Veeva Vault API Instance while checking for connection: {status}. DNS: {final_url}")
            logger.debug(response.text)
            if status == 200:
                if "sessionId" not in response.text:
                    logger.error(f"'sessionId' not found in response from {final_url}. Failing source veeva vault connection check")
                    return AirbyteConnectionStatus(status=Status.FAILED, message=response.text)
                return AirbyteConnectionStatus(status=Status.SUCCEEDED)
            return AirbyteConnectionStatus(status=Status.FAILED, message=response.text)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED,
                message=f"An exception occurred: {e}. \nStacktrace: \n{e.format_exc()}",
            )