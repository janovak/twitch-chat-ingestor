import json
import logging

import auth.secrets as secrets
import pika
import pybloomfilter
import streamer_database_connection


class StreamerIngester:
    def __init__(self):
        self.database = streamer_database_connection.DatabaseConnection()

        self.bloom_filter = pybloomfilter.BloomFilter(10000000, 0.001)

        self.connection = pika.BlockingConnection(
            pika.URLParameters(secrets.get_cloudamqp_url())
        )
        self.channel = self.connection.channel()

        # The broadcaster exchange is updated with all live streamers periodically. We bind our own
        # queue to the exchange to listen to all those messages.
        self.broadcaster_exchange = "broadcaster_fanout"
        self.channel.exchange_declare(self.broadcaster_exchange, exchange_type="fanout")

        self.broadcaster_queue = "ingest_broadcasters"
        self.channel.queue_declare(queue=self.broadcaster_queue, durable=True)

        self.channel.queue_bind(
            exchange=self.broadcaster_exchange, queue=self.broadcaster_queue
        )

    def __del__(self):
        self.connection.close()

    def initialize_bloom_filter(self):
        streamers = self.database.get_streamers()
        logging.info(f"Initializing the bloom filter with {len(streamers)} streamers")
        self.bloom_filter.update(streamers)

    def start_consuming_streamers(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=self.broadcaster_queue,
            on_message_callback=self.handle_live_streamers,
        )
        logging.info("Start consuming streamers from queue")
        self.channel.start_consuming()

    def handle_live_streamers(self, ch, method, properties, body):
        streamers = json.loads(body.decode())

        logging.info(f"Received {len(streamers)} live streamers")

        # Use the bloom filter to determine which streamers haven't been seen before
        new_streamers = []
        for user_id, _ in streamers:
            if user_id not in self.bloom_filter:
                new_streamers.append((user_id,))
                self.bloom_filter.add(user_id)

        logging.info(f"Inserting {len(new_streamers)} new live streamers")

        self.database.insert_streamers(new_streamers)

        ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    logging.basicConfig(
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    session = StreamerIngester()
    session.initialize_bloom_filter()
    session.start_consuming_streamers()


if __name__ == "__main__":
    main()
