import json

import auth.secrets as secrets
import pika
import streamer_database_connection


class StreamerIngester:
    def __init__(self):
        self.database = streamer_database_connection.DatabaseConnection()

        self.connection = pika.BlockingConnection(
            pika.URLParameters(secrets.get_cloudamqp_url())
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue="live_broadcasters_queue", durable=True)

    def __del__(self):
        self.connection.close()

    def start_consuming_streamers(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue="live_broadcasters_queue",
            on_message_callback=self.handle_live_streamers,
        )
        print(f"Start consuming streamers from queue")
        self.channel.start_consuming()

    def handle_live_streamers(self, ch, method, properties, body):
        streamer_ids = json.loads(body.decode())

        self.database.insert_streamers(streamer_ids)

        ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    session = StreamerIngester()
    session.start_consuming_streamers()


if __name__ == "__main__":
    main()
