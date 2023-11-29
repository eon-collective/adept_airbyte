#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import TypedDict

VeevaVaultConfig = TypedDict(
    "VeevaVaultConfig",
    {
        "vaultDNS": str,
        "username": str,
        "password": str,
        "api_version": str
    },
)
