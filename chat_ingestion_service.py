import json
import uuid
from typing import Any

import auth.secrets as secrets
import chat_database_connection
import pika

from pika.channel import Channel
from pika.spec import Basic.Deliver


class ChatIngester:
    def __init__(self) -> None:
        self.database: chat_database_connection.DatabaseConnection = chat_database_connection.DatabaseConnection("chat_data")

        self.connection: pika.BlockingConnection = pika.BlockingConnection(
            pika.URLParameters(secrets.get_cloudamqp_url())
        )
        self.channel: Channel = self.connection.channel()
        self.channel.queue_declare(queue="chat_processing_queue", durable=True)

    def __del__(self) -> None:
        self.connection.close()

    def start_consuming_chats(self) -> None:
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue="chat_processing_queue", on_message_callback=self.handle_chat_message
        )
        print(f"Start consuming chats from queue")
        self.channel.start_consuming()

    def handle_chat_message(self, channel: Channel, method: Basic.Deliver, properties: Any, body: bytes) -> None:
        message_fields = json.loads(body.decode())

        print(
            f"Inserting message {message_fields['message_id']} posted in chat room {message_fields['broadcaster_id']} at {message_fields['timestamp']}"
        )

        # TODO: Consider batching these inserts by broadcaster_id after coordinating all messages
        # from a given broadcaster to go to a given worker.

        self.database.insert_chats(
            broadcaster_id=message_fields["broadcaster_id"],
            timestamp=message_fields["timestamp"],
            message_id=uuid.UUID(message_fields["message_id"]),
            message=message_fields["message"],
        )

        channel.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    session: ChatIngester = ChatIngester()
    session.start_consuming_chats()


if __name__ == "__main__":
    main()
