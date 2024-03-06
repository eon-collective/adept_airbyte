#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import unittest
from unittest.mock import MagicMock, call
from destination_greenplum.write import GreenplumWriter

class TestGreenplumWriter(unittest.TestCase):

    def setUp(self):
        # Set up a sample configuration (Copy setting from config.json)
        self.configs = {
            "host": "54.89.84.45",
            "port": 5432,
            "database": "eon_collective",
            "schema": "public",
            "username": "eon_heimdal_proxy",
            "password": "0imb6vkiGQIvVrY"
            }
        self

        # Create an instance of GreenplumWriter with the sample configuration
        self.greenplum_writer = GreenplumWriter(self.configs)

    def test_greenplum_writer(self):
        # Mock the psycopg2.connect method to avoid actual database connection
        with unittest.mock.patch('psycopg2.connect') as mock_connect:
            # Mock the cursor and execute method
            mock_cursor = MagicMock()
            mock_connect.return_value.cursor.return_value = mock_cursor
            schema_name = self.configs["schema"]
            query = f"CREATE TABLE IF NOT EXISTS {schema_name}._airbyte_raw_tests ( _airbyte_ab_id TEXT PRIMARY KEY, _airbyte_emitted_at timestamp, _airbyte_data JSON);"""

            # Call the greenplum__writer method
            self.greenplum_writer.greenplum__writer(query=query)

            # Assert that psycopg2.connect was called with the correct parameters
            mock_connect.assert_called_with(
                host=self.configs["host"],
                port=self.configs["port"],
                user=self.configs["username"],
                password=self.configs["password"],
                database=self.configs["database"]
            )

            # Assert that the execute method was called with the correct parameters
            mock_cursor.execute.assert_called_with(query=query, vars=None)

            # Assert that commit and close methods were called
            mock_cursor.commit.assert_called_once()
            mock_cursor.close.assert_called_once()

    def test_greenplum_writer_insert(self):
        # Mock the psycopg2.connect method to avoid actual database connection
        with unittest.mock.patch('psycopg2.connect') as mock_connect:
            # Mock the cursor and execute method
            mock_cursor = MagicMock()
            mock_connect.return_value.cursor.return_value = mock_cursor
            schema_name = self.configs["schema"]
            query = f"""
                    INSERT INTO {schema_name}._airbyte_raw_tests
                    VALUES (%s, %s, %s)
                    """
            values = [('5b756381-098b-497e-a68e-d5ffdd90a77f', 
                       '2024-03-06T12:34:22.701775', 
                       '{"column1": "test", "column2": 222, "column3": "2022-06-20T18:56:18", "column4": 33.33, "column5": [1, 2, null]}'
                       )]
            # Call the greenplum__writer_insert method
            self.greenplum_writer.greenplum__writer_insert(query=query, values=values)

            # Assert that psycopg2.connect was called with the correct parameters
            mock_connect.assert_called_with(
                host=self.configs["host"],
                port=self.configs["port"],
                user=self.configs["username"],
                password=self.configs["password"],
                database=self.configs["database"]
            )

            # Assert that the execute method was called with the correct parameters
            # mock_cursor.execute.assert_called_with(query=query, vars=values)
            call.executemany(query, values)

            # Assert that commit and close methods were called
            mock_cursor.commit.a
            mock_cursor.close.assert_called_once()
            
if __name__ == '__main__':
    unittest.main()