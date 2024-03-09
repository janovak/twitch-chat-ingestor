import json
import logging
import uuid

import auth.secrets as secrets
import chat_database_connection
import pika


class ChatIngester:
    def __init__(self):
        self.database = chat_database_connection.DatabaseConnection("chat_data")

        self.message_queue_connection = pika.BlockingConnection(
            pika.URLParameters(secrets.get_cloudamqp_url())
        )
        self.channel = self.message_queue_connection.channel()
        self.channel.queue_declare(queue="chat_processing_queue", durable=True)

    def __del__(self):
        self.shutdown

    def shutdown(self):
        self.message_queue_connection.close()
        self.database.close()

    def start_consuming_chats(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue="chat_processing_queue", on_message_callback=self.handle_chat_message
        )
        logging.info("Start consuming chats from queue")
        self.channel.start_consuming()

    def handle_chat_message(self, ch, method, properties, body):
        message_fields = json.loads(body.decode())

        logging.info(
            f"Inserting message {message_fields['message_id']} posted in chat room {message_fields['broadcaster_id']} at {message_fields['timestamp']}"
        )

        success = self.database.insert_chats(
            broadcaster_id=message_fields["broadcaster_id"],
            timestamp=message_fields["timestamp"],
            message_id=uuid.UUID(message_fields["message_id"]),
            message=message_fields["message"],
        )

        if success:
            logging.info(
                f"Message, {message_fields['message_id']}, inserted successfully"
            )
        else:
            logging.error(
                f"There was an error inserting message, {message_fields['message_id']}, into the database"
            )

        ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    logging.basicConfig(
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    session = ChatIngester()
    session.start_consuming_chats()


if __name__ == "__main__":
    main()
