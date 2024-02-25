import asyncio
import concurrent.futures
import json

import auth.secrets as secrets
import pika
import psycopg2
from bloom_filter2 import BloomFilter


class DatabaseConnection:
    def __init__(self):
        self.session = psycopg2.connect(secrets.get_neon_url())
        self.bloom_filter = BloomFilter(max_elements=10000000, error_rate=0.001)

        self.connection = pika.BlockingConnection(
            pika.URLParameters(secrets.get_cloudamqp_url())
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue="live_broadcasters_queue", durable=True)

    def __del__(self):
        self.session.close()

    def insert_streamers(self, ch, method, properties, body):
        streamer_ids = json.loads(body.decode())
        print(f"Received {len(streamer_ids)} streamer Ids")

        # TODO: right now we aren't handling the false positives returned by the bloom filter. i.e.
        # an item isn't in the database, but the bloom filter says it is, so we don't add it to the database
        # change this to get potential false positives and then check those against a cache.
        # the set of new ids is then the set not in the bloom filter + set not in cache
        new_ids = [(id,) for id in streamer_ids if id not in self.bloom_filter]
        print(f"Inserting {len(new_ids)} new streamer Ids")
        with self.session.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO Streamer (streamer_id) VALUES (%s) ON CONFLICT DO NOTHING",
                new_ids,
            )
            self.session.commit()

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consuming_live_broadcasters(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue="live_broadcasters_queue", on_message_callback=self.insert_streamers
        )
        self.channel.start_consuming()
        print(f"Start consuming live broadcasters from queue")

    def fetch_streamers(self):
        with self.session.cursor() as cursor:
            cursor.execute("SELECT * FROM Streamer")
            return cursor.fetchall()

    def initialize_bloom_filter(self):
        all_known_streamers = self.fetch_streamers()
        for id in all_known_streamers:
            self.bloom_filter.add(id[0])
        print(f"Loading {len(all_known_streamers)} streamer Ids into the bloom filter")


async def main():
    session = DatabaseConnection()
    session.initialize_bloom_filter()
    session.start_consuming_live_broadcasters()

    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, input, "Press enter to exit\n")


asyncio.get_event_loop().run_until_complete(main())
