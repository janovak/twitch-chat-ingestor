import asyncio
import concurrent.futures
import json
import logging
import sys

import auth.secrets as secrets
import pika
import twitch_proxy
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from twitchAPI.type import TwitchAPIException


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

        self.streamer_allows_clipping = {}

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
        scheduler.add_job(self.get_top_streamers, "interval", minutes=2, args=(100,))
        scheduler.start()

    async def get_all_streamers(self):
        logging.info("Retrieving all currently live streamers")
        await self.get_online_streamers(sys.maxint)

    async def get_top_streamers(self, n):
        logging.info(f"Retrieving top {n} currently live streamers")
        await self.get_online_streamers(n)

    async def get_online_streamers(self, batch_size):
        streamers = await self.twitch_session.get_online_streamers(batch_size)

        counter = 0
        # Keep track of which streamers have clipping disabled.
        async for streamer in streamers:
            user_id = streamer.user_id
            user_login = streamer.user_login

            # Check if we have already determined the clipping status for this streamer
            allows_clipping = self.streamer_allows_clipping.get(user_id)

            if allows_clipping is None:
                try:
                    await self.twitch_session.create_clip(user_id)
                    self.streamer_allows_clipping[user_id] = True
                except TwitchAPIException:
                    self.streamer_allows_clipping[user_id] = False
                    logging.info(
                        f"Skipping {user_login} because they have clipping disabled."
                    )
                    continue
            elif not allows_clipping:
                logging.debug(
                    f"Skipping {user_login} because we know they have clipping disabled."
                )
                continue

            message = json.dumps((int(streamer.user_id), streamer.user_login, counter))
            self.channel.basic_publish(
                exchange=self.broadcaster_exchange,
                routing_key="",
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )

            counter += 1
            if counter == batch_size:
                return


async def main():
    logging.basicConfig(
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    session = TwitchAPIPoller()
    await session.authenticate()
    session.start_polling_online_streamers()

    await asyncio.sleep(float("inf"))


asyncio.get_event_loop().run_until_complete(main())
