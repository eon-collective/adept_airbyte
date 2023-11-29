#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, Mapping
import requests
import urllib
import time
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, DestinationSyncMode, Type
from destination_veeva_vault.client import VeevaVaultClient
from destination_veeva_vault.writer import VeevaVaultWriter

# def get_client(config: Mapping[str, Any]) -> Client:
#     api_key = config.get("api_key")
#     host = config.get("host")
#     port = config.get("port") or "8108"
#     protocol = config.get("protocol") or "https"

#     client = Client({"api_key": api_key, "nodes": [{"host": host, "port": port, "protocol": protocol}], "connection_timeout_seconds": 3600})

#     return client

class DestinationVeevaVault(Destination):
    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:

        """
        TODO
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

        writer = VeevaVaultWriter(VeevaVaultClient(**config))

        for configured_stream in configured_catalog.streams:
            if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
                writer.delete_stream_entries(configured_stream.stream.name)

        for message in input_messages:
            if message.type == Type.STATE:
                # Emitting a state message indicates that all records which came before it have been written to the destination. So we flush
                # the queue to ensure writes happen, then output the state message to indicate it's safe to checkpoint state
                writer.flush()
                yield message
            elif message.type == Type.RECORD:
                record = message.record
                writer.queue_write_operation(
                    record.stream, record.data, time.time_ns() / 1_000_000
                )  # convert from nanoseconds to milliseconds
            else:
                # ignore other message types for now
                continue

        # Make sure to flush any records still in the queue
        writer.flush()

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