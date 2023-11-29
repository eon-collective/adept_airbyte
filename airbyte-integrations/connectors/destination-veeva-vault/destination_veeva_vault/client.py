#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping

import requests
from destination_veeva_vault.config import VeevaVaultConfig


class VeevaVaultClient:
    def __init__(self, config: VeevaVaultConfig, table_metadata: Mapping[str, Any]):
        self.deployment_url = config["deployment_url"]
        self.access_key = config["access_key"]
        self.table_metadata = table_metadata

    def batch_write(self, records: List[Mapping[str, Any]]) -> requests.Response:
        """
        See VeevaVault docs: https://docs.VeevaVault.dev/http-api/#post-apistreaming_importimport_airbyte_records
        """
        request_body = {"tables": self.table_metadata, "messages": records}
        return self._request("POST", endpoint="import_airbyte_records", json=request_body)

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
        return {"Authorization": f"VeevaVault {self.access_key}"}

    def _request(
        self,
        http_method: str,
        endpoint: str,
        json: Mapping[str, Any],
    ) -> requests.Response:
        url = f"{self.deployment_url}/api/streaming_import/{endpoint}"
        headers = {
            "Accept": "application/json",
            "VeevaVault-Client": "streaming-import-0.1.0",
            **self._get_auth_headers(),
        }

        response = requests.request(method=http_method, url=url, headers=headers, json=json)

        if response.status_code != 200:
            raise Exception(f"Request to {url} failed with: {response.status_code}: {response.json()}")
        return response
