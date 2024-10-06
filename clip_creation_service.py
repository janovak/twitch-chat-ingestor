import asyncio
import json
import logging
from datetime import datetime
from aio_pika import connect_robust, ExchangeType

import auth.secrets as secrets
import chat_database_connection
import twitch_proxy


class ClipCreator:
    def __init__(self):
        self.twitch_session = twitch_proxy.TwitchAPIConnection()

        self.database = chat_database_connection.DatabaseConnection("chat_data")

    async def authenticate(self):
        await self.twitch_session.authenticate()

    async def start_consuming_chats(self):
        connection = await connect_robust(
            "amqp://guest:guest@localhost:5672/", heartbeat=60
        )
        channel = await connection.channel()

        await channel.set_qos(prefetch_count=1)
        exchange = await channel.declare_exchange("anomaly_fanout", ExchangeType.FANOUT)
        queue = await channel.declare_queue("anomaly_queue", durable=True)
        await queue.bind(exchange)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    body = json.loads(message.body.decode())
                    await self.handle_chat_message_async(body)

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
        self.database.close()


async def main():
    logging.basicConfig(
        filename="/var/log/clip_creation_service.py",
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
