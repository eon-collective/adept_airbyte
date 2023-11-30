#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping
import urllib
import requests
import logging
from destination_veeva_vault.config import VeevaVaultConfig
import json
import os
import pandas as pd

logger = logging.getLogger("airbyte")

class VeevaVaultClient:
    def __init__(self, config: VeevaVaultConfig, table_metadata: Mapping[str, Any]):
        self.vaultDNS = config["vaultDNS"]
        self.username = config["username"]
        self.password = config["password"]
        self.api_version = config["api_version"]
        self.table_metadata = table_metadata

    def batch_write(self, records: List[Mapping[str, Any]]) -> requests.Response:
        """
        See VeevaVault docs: https://docs.VeevaVault.dev/http-api/#post-apistreaming_importimport_airbyte_records
        """
        filename = ""
        for message in records:
            filename = f"{message['tableName']}.csv"

        data_records = [record['data'] for record in records]
        df = pd.DataFrame(data_records)
        df.to_csv(filename, index=False)
        file_path = os.path.abspath(filename)

        # request_body = {"tables": self.table_metadata, "messages": records}
        request_body={
        'name__v': filename,
        'type__v': 'Unclassified',
        'lifecycle__v': 'Inbox',
        }
        files=[
            ('file',(f'{filename}',open(
                f'{file_path}',
                'rb'),
                'text/csv'))
        ]
        # application/vnd.openxmlformats-officedocument.wordprocessingml.document            
        logger.info(f"formatting message to destination: {request_body}")
        return self._request("POST", endpoint="objects/documents", data=request_body, files=files, file_path=file_path)

    def delete(self, keys: List[str]) -> requests.Response:
        """
        See VeevaVault docs: https://docs.VeevaVault.dev/http-api/#put-apistreaming_importclear_tables
        """
        request_body = {"tableNames": keys}
        return self._request("PUT", endpoint="clear_tables", json=request_body)

    def add_primary_key_indexes(self, indexes: Mapping[str, List[List[str]]]) -> requests.Response:
        """
        See VeevaVault docs: https://docs.VeevaVault.dev/http-api/#put-apistreaming_importadd_primary_key_indexes
        """
        return self._request("PUT", "add_primary_key_indexes", json={"indexes": indexes})

    def primary_key_indexes_ready(self, tables: List[str]) -> requests.Response:
        """
        See VeevaVault docs: https://docs.VeevaVault.dev/http-api/#get-apistreaming_importprimary_key_indexes_ready
        """
        return self._request("GET", "primary_key_indexes_ready", json={"tables": tables})

    def _get_auth_headers(self) -> Mapping[str, str]:
        veevaDNS = self.vaultDNS
        api_version = self.api_version
        username = self.username
        password = self.password
        sessionId = ""
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
                else:
                    sessionId = response.json()['sessionId']
                    return {"Authorization": f"{sessionId}"}
        except Exception as e:
            logger.error(f"An exception occurred: {e}. \nStacktrace: \n{e.format_exc()}")

    def _request(
        self,
        http_method: str,
        endpoint: str,
        data: Mapping[str, Any],
        files,
        file_path
    ) -> requests.Response:
        url = f"https://{self.vaultDNS}.veevavault.com/api/{self.api_version}/{endpoint}"
        # /api/{version}/objects/documents/batch
        headers = {
            "Accept": "application/json",
            **self._get_auth_headers(),
        }

        logger.info(f"Data: {data}")

        response = requests.request(
            method=http_method, 
            url=url, 
            headers=headers, 
            data=data,
            files=files
        )
        logger.info(f"Response: {response.json()}")
        if response.status_code != 200:
            raise Exception(f"Request to {url} failed with: {response.status_code}: {response.json()}")
        # else:
        #     if os.path.exists(file_path):
        #         os.remove(file_path)
        #         logger.info(f'The file {file_path} has been successfully deleted.')
        #     else:
        #         logger.info(f'The file {file_path} does not exist.')
        return response
