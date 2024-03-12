import json
import logging
import uuid
from collections import defaultdict

import auth.secrets as secrets
import chat_database_connection
import pika
from datetime_helpers import get_month


class ChatIngestor:
    def __init__(self):
        self.database = chat_database_connection.DatabaseConnection("chat_data")

        self.message_queue_connection = pika.BlockingConnection(
            pika.URLParameters(secrets.get_cloudamqp_url())
        )
        self.channel = self.message_queue_connection.channel()

        # All chat messages are published to the chat exchange
        self.chat_exchange = "chat_fanout"
        self.channel.exchange_declare(self.chat_exchange, exchange_type="fanout")

        self.chat_queue = "chat_ingestion_queue"
        self.channel.queue_declare(queue=self.chat_queue, durable=True)

        self.channel.queue_bind(exchange=self.chat_exchange, queue=self.chat_queue)

        # Dictionary to hold messages until we have enough to write to the database
        # The key is the database partition key and value is a list of messages
        self.message_batches = defaultdict(list)
        self.batch_size = 1000
        self.current_batch_size = 0

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        self.message_queue_connection.close()
        self.database.close()

    def start_consuming_chats(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=self.chat_queue, on_message_callback=self.handle_chat_message
        )
        logging.info("Start consuming chats from queue")
        self.channel.start_consuming()

    def handle_chat_message(self, ch, method, properties, body):
        message_fields = json.loads(body.decode())
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
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

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
        ch.basic_ack(delivery_tag=method.delivery_tag)


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
