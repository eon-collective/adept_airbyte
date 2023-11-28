#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from destination_veeva_vault import DestinationVeevaVault

if __name__ == "__main__":
    DestinationVeevaVault().run(sys.argv[1:])
