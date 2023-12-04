#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from destination_adept import DestinationAdept

if __name__ == "__main__":
    DestinationAdept().run(sys.argv[1:])
