#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping
import urllib
import requests
import logging
from destination_veeva_vault.config import VeevaVaultConfig
import json

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
        # request_body = {"tables": self.table_metadata, "messages": records}
        request_body={'name__v': 'Airbyter.csv',
        'type__v': 'Unclassified',
        'lifecycle__v': 'Inbox'}
        files=[
            ('file',('ELTModel.csv',open('/Users/james.kimani/Downloads/ELTModel.csv','rb'),'text/csv'))
        ]                                           
        logger.info(f"formatting message to destination: {request_body}")
        return self._request("POST", endpoint="objects/documents/batch", json=request_body, files=files)

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
        json: Mapping[str, Any],
    ) -> requests.Response:
        url = f"https://{self.vaultDNS}.veevavault.com/api/{self.api_version}/{endpoint}"
        # /api/{version}/objects/documents/batch
        headers = {
            "Accept": "application/json",
            "Content-Type": "text/csv",
            **self._get_auth_headers(),
        }

        response = requests.request(method=http_method, url=url, headers=headers, data=json, files=[])

        if response.status_code != 200:
            raise Exception(f"Request to {url} failed with: {response.status_code}: {response.json()}")
        return response
