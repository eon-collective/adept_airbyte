#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from destination_greenplum import DestinationGreenplum

if __name__ == "__main__":
    DestinationGreenplum().run(sys.argv[1:])
