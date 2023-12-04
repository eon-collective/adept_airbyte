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
        "api_version": str,
        "target_format": {
            "format_type": str,
            "delimiter": str,
            "escape_character": str,
            "text_enclosure_char": str
        },
        "type__v": str,
        "lifecycle__v": str,
        "publish_metadata_file": bool,
        "publish_to_adept": {
            "action": str,
            "adept_instance_url": str,
            "adept_project_id": str,
            "adept_project_stage_name": str
        }
    },
    
)

