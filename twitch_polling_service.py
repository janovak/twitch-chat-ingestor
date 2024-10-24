import asyncio
import json
import logging
import sys

import auth.secrets as secrets
import twitch_proxy
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from confluent_kafka import Producer
from twitchAPI.type import TwitchAPIException


class TwitchAPIPoller:
    def __init__(self):
        self.twitch_session = twitch_proxy.TwitchAPIConnection()

        # Kafka configuration
        kafka_conf = {
            "bootstrap.servers": secrets.get_kafka_broker_url(),
            "client.id": "twitch-api-poller",
        }
        self.producer = Producer(kafka_conf)

        self.broadcaster_topic = "live_broadcasters"

        self.streamer_allows_clipping = {}

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        self.producer.flush()
        self.twitch_session.close()

    async def authenticate(self):
        await self.twitch_session.authenticate()

    def start_polling_online_streamers(self):
        # Twitch caches are 1 to 3 minutes stale, so it doesn't make sense to poll any more frequently than that
        scheduler = AsyncIOScheduler()
        scheduler.add_job(self.get_top_streamers, "interval", minutes=2, args=(5,))
        scheduler.start()

    async def get_all_streamers(self):
        logging.info("Retrieving all currently live streamers")
        await self.get_online_streamers(sys.maxsize)

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

            # Kafka message as JSON
            message = json.dumps((int(streamer.user_id), streamer.user_login, counter))

            # Publish to Kafka topic
            try:
                self.producer.produce(
                    self.broadcaster_topic, key=str(user_id), value=message
                )
                self.producer.poll(0)  # Trigger delivery report callbacks if any
            except Exception as e:
                logging.error(f"Failed to send message to Kafka: {e}")

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
