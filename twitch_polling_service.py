import asyncio
import concurrent.futures
import json
import logging
import sys

import auth.secrets as secrets
import pika
import twitch_proxy
from apscheduler.schedulers.asyncio import AsyncIOScheduler


class TwitchAPIPoller:
    def __init__(self):
        self.twitch_session = twitch_proxy.TwitchAPIConnection()

        self.message_queue_connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=secrets.get_cloudamqp_url())
        )
        self.channel = self.message_queue_connection.channel()
        self.channel.confirm_delivery()

        # Create a fanout exchange to publish the broadcaster Ids to so that any service that needs
        # this information can bind a queue to this exchange
        self.broadcaster_exchange = "broadcaster_fanout"
        self.channel.exchange_declare(self.broadcaster_exchange, exchange_type="fanout")

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        self.message_queue_connection.close()
        self.twitch_session.close()

    async def authenticate(self):
        await self.twitch_session.authenticate()

    def start_polling_online_streamers(self):
        # Twitch caches are 1 to 3 minutes stale, so it doesn't make sense to poll any more frequently than that
        scheduler = AsyncIOScheduler()
        scheduler.add_job(self.get_top_streamers, "interval", minutes=1, args=(10,))
        scheduler.start()

    async def get_all_streamers(self):
        logging.info("Retrieving all currently live streamers")
        await self.get_online_streamers(sys.maxint)

    async def get_top_streamers(self, n):
        logging.info(f"Retrieving top {n} currently live streamers")
        await self.get_online_streamers(n)

    async def get_online_streamers(self, batch_size):
        batch_size = min(batch_size, 100)
        streamers = await self.twitch_session.get_online_streamers(batch_size)

        # Publishes a JSON list of streamer Ids to the chat queue. The list has a maximum of batch_size Ids
        async def publish_batch(ids):
            message = json.dumps(ids)
            self.channel.basic_publish(
                exchange=self.broadcaster_exchange,
                routing_key="",
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )

        counter = 0
        streamer_list = []
        tasks = []
        # Groups streamers into lists of 100 and then asyncronously publishes the list to the chat queue
        async for s in streamers:
            streamer_list.append((int(s.user_id), s.user_login))
            counter += 1
            if len(streamer_list) == batch_size:
                tasks.append(asyncio.create_task(publish_batch(streamer_list.copy())))
                streamer_list.clear()
            if counter == batch_size:
                break

        # Publish any remaining streamers if the total count is not a multiple of batch_size
        if streamer_list:
            counter += len(streamer_list)
            tasks.append(asyncio.create_task(publish_batch(streamer_list)))

        await asyncio.gather(*tasks)

        logging.info(f"Published {counter} Ids in batches of {batch_size}")


async def main():
    logging.basicConfig(
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    session = TwitchAPIPoller()
    await session.authenticate()
    session.start_polling_online_streamers()

    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, input, "Press enter to exit\n")


asyncio.get_event_loop().run_until_complete(main())
