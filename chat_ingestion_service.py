import json
import logging
import uuid
from collections import defaultdict
from confluent_kafka import Consumer, KafkaException

import auth.secrets as secrets
import chat_database_connection
from datetime_helpers import get_month


class ChatIngestor:
    def __init__(self):
        self.database = chat_database_connection.DatabaseConnection("chat_data")

        # Kafka consumer configuration
        self.consumer = Consumer(
            {
                "bootstrap.servers": secrets.get_kafka_broker_url(),
                "group.id": "chat_ingestor_group",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        self.chat_topic = "twitch-chat-messages"
        self.consumer.subscribe([self.chat_topic])

        # Dictionary to hold messages until we have enough to write to the database
        self.message_batches = defaultdict(list)
        self.batch_size = 1000
        self.current_batch_size = 0

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        self.consumer.close()
        self.database.close()

    def start_consuming_chats(self):
        logging.info("Start consuming chats from Kafka topic")
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        continue
                    else:
                        logging.error(msg.error())
                        break

                self.handle_chat_message(msg)
        except KeyboardInterrupt:
            logging.info("Shutting down chat consumer.")
        finally:
            self.consumer.close()

    def handle_chat_message(self, msg):
        message_fields = json.loads(msg.value().decode())
        message_fields["message_id"] = uuid.UUID(message_fields["message_id"])

        def get_partition_key(fields):
            return f"{fields['broadcaster_id']} {get_month(fields['timestamp'])}"

        def get_primary_key(fields):
            return " ".join(
                [
                    get_partition_key(fields),
                    str(fields["timestamp"]),
                    str(fields["message_id"]),
                ]
            )

        logging.info(
            f"Saving message, {get_primary_key(message_fields)}, to in-memory store"
        )

        # Convert the values in the message_fields dictionary to a tuple and then append it the list
        # holding all the messages in the same partition
        self.current_batch_size += 1
        self.message_batches[get_partition_key(message_fields)].append(
            tuple(message_fields[key] for key in list(message_fields))
        )

        if self.current_batch_size < self.batch_size:
            self.consumer.commit(message=msg)
            return

        # Insert data into database and commit offset
        for partition_key, message_list in self.message_batches.items():
            success = self.database.insert_chats([message for message in message_list])

            if success:
                logging.info(
                    f"Inserted {len(message_list)} messages into {partition_key} successfully"
                )
            else:
                logging.error(
                    f"There was an error inserting {len(message_list)} messages into {partition_key}"
                )

        self.message_batches = defaultdict(list)
        self.current_batch_size = 0
        logging.info(f"Finished inserting message batch")
        self.consumer.commit(message=msg)


def main():
    logging.basicConfig(
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    session = ChatIngestor()
    session.start_consuming_chats()


if __name__ == "__main__":
    main()
