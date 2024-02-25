import json
import time
import uuid

import auth.secrets as secrets
import pika
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import tuple_factory


class DatabaseConnection:
    def __init__(self, keyspace):
        self.session = self.get_session(keyspace)
        self.connection = pika.BlockingConnection(
            pika.URLParameters(secrets.get_cloudamqp_url())
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue="chat_processing_queue", durable=True)

    def __del__(self):
        self.session.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, *members):
        self.session.shutdown()

    def get_session(self, keyspace):
        auth_provider = PlainTextAuthProvider(
            secrets.get_chat_db_client_id(), secrets.get_chat_db_secret()
        )
        cluster = Cluster(
            cloud=secrets.get_astra_db_cloud_config(), auth_provider=auth_provider
        )
        session = cluster.connect(keyspace)
        return session

    def start_consuming_chats(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue="chat_processing_queue", on_message_callback=self.handle_chat_message
        )
        print(f"Start consuming chats from queue")
        self.channel.start_consuming()

    def get_month(self, timestamp):
        return time.strftime("%Y%m", time.gmtime(timestamp))

    def handle_chat_message(self, ch, method, properties, body):
        message_fields = json.loads(body.decode())

        print(
            f"Inserting message {message_fields['message_id']} posted in chat room {message_fields['broadcaster_id']} at {message_fields['timestamp']}"
        )

        statement = self.session.prepare(
            """
            INSERT INTO twitch_chat_by_broadcaster_and_timestamp (broadcaster_id, year_month, timestamp, message_id, message)
            VALUES (?, ?, ?, ?, ?)
            """
        )

        self.session.execute(
            statement,
            (
                message_fields["broadcaster_id"],
                int(self.get_month(message_fields["timestamp"])),
                message_fields["timestamp"],
                uuid.UUID(message_fields["message_id"]),
                message_fields["message"],
            ),
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def get_chats(self, broadcaster_id, start, end, after_timestamp, limit):
        self.session.row_factory = tuple_factory

        start_epoch = start.timestamp()
        year_month = int(self.get_month(start_epoch))

        start_epoch_milliseconds = int(start_epoch * 1000)
        end_epoch_milliseconds = int(end.timestamp() * 1000)

        start_epoch = max(start_epoch_milliseconds, after_timestamp)

        statement = self.session.prepare(
            """
            SELECT broadcaster_id, year_month, timestamp, message_id, message FROM twitch_chat_by_broadcaster_and_timestamp
            WHERE broadcaster_id=? AND year_month=? AND timestamp>=? AND timestamp<=?
            LIMIT ?
            """,
        )
        # TODO: need to handle timestamps that span multiple months
        rows = self.session.execute(
            statement,
            (
                broadcaster_id,
                year_month,
                start_epoch_milliseconds,
                end_epoch_milliseconds,
                limit + 1,
            ),
        )
        return list(rows)


def main():
    session = DatabaseConnection("chat_data")
    session.start_consuming_chats()


if __name__ == "__main__":
    main()
