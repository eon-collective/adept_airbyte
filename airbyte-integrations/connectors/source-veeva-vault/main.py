#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_veeva_vault import SourceVeevaVault

if __name__ == "__main__":
    source = SourceVeevaVault()
    launch(source, sys.argv[1:])
