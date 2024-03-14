import asyncio
import concurrent.futures
import json
import logging
from datetime import datetime
import auth.secrets as secrets
import chat_database_connection
import pika
import twitch_proxy


class ClipCreator:
    def __init__(self):
        self.twitch_session = twitch_proxy.TwitchAPIConnection()

        self.database = chat_database_connection.DatabaseConnection("chat_data")

        self.message_queue_connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=secrets.get_cloudamqp_url())
        )
        self.channel = self.message_queue_connection.channel()

        self.anomaly_exchange = "anomaly_fanout"
        self.channel.exchange_declare(self.anomaly_exchange, exchange_type="fanout")

        self.anomaly_queue = "anomaly_queue"
        self.channel.queue_declare(queue=self.anomaly_queue, durable=True)

        self.channel.queue_bind(
            exchange=self.anomaly_exchange, queue=self.anomaly_queue
        )

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        self.message_queue_connection.close()
        self.database.close()

    async def authenticate(self):
        await self.twitch_session.authenticate()

    def start_consuming_chats(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=self.anomaly_queue, on_message_callback=self.handle_chat_message
        )
        logging.info("Start consuming anomalies from queue")
        self.channel.start_consuming()

    def handle_chat_message(self, ch, method, properties, body):
        asyncio.run(self.handle_chat_message_async(ch, method, properties, body))

    async def handle_chat_message_async(self, ch, method, properties, body):
        message_fields = json.loads(body.decode())

        broadcaster_id = message_fields["broadcaster_id"]
        timestamp = message_fields["timestamp"]

        logging.info(f"Received anomaly at {timestamp} for {broadcaster_id}")

        # Clips can only go back 30 seconds from the time of the call, so we've missed
        # our opportunity to capture the moment if 30 seconds has gone by
        if datetime.now().timestamp() - timestamp > 30:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Clip is not guaranteed to be created. We're not going to bother with confirming
        # that the clip has been crated at this point in time.
        clip_id = await self.twitch_session.create_clip(broadcaster_id)

        self.database.insert_clip(clip_id, timestamp)

        ch.basic_ack(delivery_tag=method.delivery_tag)


async def main():
    logging.basicConfig(
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    session = ClipCreator()
    await session.authenticate()

    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, session.start_consuming_chats)


asyncio.get_event_loop().run_until_complete(main())
