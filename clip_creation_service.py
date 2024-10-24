import asyncio
import json
import logging
from datetime import datetime
from confluent_kafka import Consumer, KafkaError

import auth.secrets as secrets
import chat_database_connection
import twitch_proxy


class ClipCreator:
    def __init__(self):
        self.twitch_session = twitch_proxy.TwitchAPIConnection()
        self.database = chat_database_connection.DatabaseConnection("chat_data")
        self.consumer = None

    async def authenticate(self):
        await self.twitch_session.authenticate()

    async def start_consuming_chats(self):
        # Kafka consumer configuration
        conf = {
            "bootstrap.servers": secrets.get_kafka_broker_url(),
            "group.id": "anomaly_group",  # Consumer group ID
            "auto.offset.reset": "earliest",  # Start reading at the earliest message
        }

        # Create the Kafka consumer
        self.consumer = Consumer(conf)
        self.consumer.subscribe(["anomalies"])  # Subscribe to the topic

        try:
            while True:
                msg = self.consumer.poll(1.0)  # Wait for a message
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        continue
                    else:
                        logging.error(f"Error while consuming message: {msg.error()}")
                        continue

                body = json.loads(msg.value().decode())
                await self.handle_chat_message_async(body)
        finally:
            self.consumer.close()  # Ensure the consumer is closed on exit

    async def handle_chat_message_async(self, message_fields):
        broadcaster_id = str(message_fields["broadcaster_id"])
        timestamp = message_fields["timestamp"]

        logging.info(f"Received anomaly at {timestamp} for {broadcaster_id}")

        # Clips only go back 5 seconds from the time of the call, so we've missed
        # our opportunity to capture the moment if 5 seconds have gone by
        """
        if datetime.now().timestamp() - timestamp > 5:
            logging.warning(
                f"Anomaly at {timestamp} on {broadcaster_id}'s stream wasn't processed quickly enough"
            )
            return
        """
        logging.info(f"Seconds since anomaly: {datetime.now().timestamp() - timestamp}")

        # Schedule clip retrieval and insertion as a background task
        asyncio.create_task(self.retrieve_and_insert_clip(broadcaster_id, timestamp))

    async def retrieve_and_insert_clip(self, broadcaster_id, timestamp):
        await asyncio.sleep(5)

        try:
            clip_id = await self.twitch_session.create_clip(broadcaster_id)

            # Wait 15 seconds before creating the clip so the highlight is in the middle of the clip.
            await asyncio.sleep(15)

            id, url, thumbnail = await self.twitch_session.get_clip(clip_id)
            self.database.insert_clip(timestamp, id, url, thumbnail)
        except Exception as e:
            logging.error(f"Exception: {e}")

    async def shutdown(self):
        # Close the database connection
        self.database.close()
        # Ensure the consumer is closed
        if self.consumer:
            self.consumer.close()


async def main():
    logging.basicConfig(
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    session = ClipCreator()
    await session.authenticate()
    await session.start_consuming_chats()

    # Shutdown gracefully
    await session.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
