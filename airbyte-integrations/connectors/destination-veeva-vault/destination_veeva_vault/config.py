#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import TypedDict

class TargetFormat(TypedDict):
    format_type: str
    delimiter: str
    escape_character: str
    text_enclosure_char: str

class PublishToAdept(TypedDict):
    push: bool
    adept_instance_url: str
    adept_project_id: str
    adept_project_stage_name: str

VeevaVaultConfig = TypedDict(
    "VeevaVaultConfig",
    {
        "vaultDNS": str,
        "username": str,
        "password": str,
        "api_version": str,
        "target_format": TargetFormat,
        "type__v": str,
        "lifecycle__v": str,
        "publish_metadata_file": bool,
        "publish_to_adept": PublishToAdept
    },
)

