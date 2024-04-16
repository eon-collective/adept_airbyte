import json
import jsonlines
from faker import Faker
from datetime import datetime

class DataGenerator:
    """
    A class to generate sample data records.
    """

    def __init__(self):
        self.fake = Faker()

    def generate_records(self, num_records=100):
        """
        Generate sample records conforming to a specific schema.

        Args:
            num_records (int): The number of records to generate. Default is 100.

        Returns:
            list: A list of generated records.
        """
        records = []
        for _ in range(num_records):
            record = {
                "order_id": self.fake.random_number(digits=5),
                "customer_id": self.fake.random_number(digits=5),
                "amount": round(self.fake.random_number(digits=4) + self.fake.random_number(digits=2) / 100, 2),
                "order_date": self.fake.date_time_this_year().isoformat()
            }
            records.append(record)
        return records


def generate_messages(records):
    """
    Generate messages for each record.

    Args:
        records (list): A list of records.

    Returns:
        list: A list of messages.
    """
    messages = []
    for record in records:
        message = {
            "type": "RECORD",
            "record": {
                "stream": "sales_data",
                "emitted_at": datetime.timestamp(datetime.now()),
                "data": record,
            }
        }
        messages.append(message)
    return messages

def saving_messages_to_file(messages, file_path):
    """
    Save messages to a file.

    Args:
        messages (list): A list of messages.
        file_path (str): The path to the file.
    """
    with open(file_path, "w") as f:
        for message in messages:
            json.dump(message, f)
            f.write("\n")


if __name__ == "__main__":
    data_generator = DataGenerator()
    records = data_generator.generate_records()
    messages = generate_messages(records)
    saving_messages_to_file(messages, "/Users/danielmalungu/Documents/adept_airbyte/airbyte-integrations/connectors/destination-greenplum/integration_tests/messages.jsonl")
