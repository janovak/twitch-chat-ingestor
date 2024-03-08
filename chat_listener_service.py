import asyncio
import concurrent.futures
import json
import logging

import auth.secrets as secrets
import pika
import redis
import twitch


class ChatRoomJoiner:
    def __init__(self):
        self.twitch_session = twitch.TwitchAPIConnection()

        # We keep an in-memory cache in addition to the redis cache in case the process needs to be restarted.
        # Without the in-memory cache we would never rejoin the chat rooms after restarting. This still isn't
        # ideal since we need to wait for another message listing all the online streamers, but it's good
        # enough for now
        self.online_streamers = set()

        # Keeps track of online streamers
        self.redis_cache = redis.Redis(
            host=secrets.get_redis_host_url(),
            port=secrets.get_redis_host_port(),
            password=secrets.get_redis_host_password(),
            db=0,
        )

        # Register callback to fire when a key expiers in the cache. Note that this gets called
        # at redis's convenience and not necessarily immediately when the key expires
        pubsub = self.redis_cache.pubsub()
        pubsub.psubscribe(
            **{"__keyevent@0__:expired": self.streamer_went_offline_callback}
        )
        pubsub.run_in_thread(sleep_time=1)

        self.message_queue_connection = pika.BlockingConnection(
            pika.URLParameters(secrets.get_cloudamqp_url())
        )
        self.channel = self.message_queue_connection.channel()

        # The broadcaster exchange is updated with all live streamers periodically. We bind our own
        # queue to the exchange to listen to all those messages.
        self.broadcaster_exchange = "broadcaster_fanout"
        self.channel.exchange_declare(self.broadcaster_exchange, exchange_type="fanout")

        self.broadcaster_queue = "join_broadcaster_chat_queue"
        self.channel.queue_declare(queue=self.broadcaster_queue, durable=True)

        self.channel.queue_bind(
            exchange=self.broadcaster_exchange, queue=self.broadcaster_queue
        )

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        self.message_queue_connection.close()
        self.redis_cache.close()
        self.twitch_session.close()

    async def initialize_twitch(self):
        await self.twitch_session.authenticate()
        await self.twitch_session.initialize_chat()

    def streamer_went_offline_callback(self, message):
        streamer = message["data"].decode()

        logging.info(f"{streamer} went offline")

        # Delete the streamer from the in-memory set
        self.online_streamers.remove(streamer)
        asyncio.run(self.twitch_session.leave_chat_room(streamer))

    def start_consuming_streamers(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=self.broadcaster_queue,
            on_message_callback=self.handle_live_streamers,
        )
        logging.info("Start consuming streamers from queue")
        self.channel.start_consuming()

    def handle_live_streamers(self, ch, method, properties, body):
        asyncio.run(self.handle_live_streamers_async(ch, method, properties, body))

    async def handle_live_streamers_async(self, ch, method, properties, body):
        streamers = json.loads(body.decode())

        logging.info(f"Received {len(streamers)} live streamers")

        if not streamers:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        tasks = []
        # Add streamers that just went live to the redis and in-memory caches as well as join their
        # chat room. Refresh all live streamers TTL. Streams that go offline won't be refreshed in the cashe,
        # and after a few minutes they will expire and be removed from the cache.
        for _, user_login in streamers:
            if user_login not in self.online_streamers:
                logging.info(f"{user_login} just came online")

                tasks.append(
                    asyncio.create_task(self.twitch_session.join_chat_room(user_login))
                )

                self.redis_cache.set(user_login, "")
                self.online_streamers.add(user_login)

            self.redis_cache.expire(user_login, 15)

        ch.basic_ack(delivery_tag=method.delivery_tag)

        await asyncio.gather(*tasks)


async def main():
    logging.basicConfig(
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    joiner = ChatRoomJoiner()
    await joiner.initialize_twitch()

    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, joiner.start_consuming_streamers)


asyncio.get_event_loop().run_until_complete(main())
