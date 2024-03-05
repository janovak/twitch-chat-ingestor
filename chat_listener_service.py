import asyncio
import concurrent.futures
import json

import auth.secrets as secrets
import pika
import redis
import twitch


class ChatRoomJoiner:
    def __init__(self):
        self.twitch = twitch.TwitchAPIConnection()

        self.cache = redis.Redis(
            host=secrets.get_redis_host_url(),
            port=secrets.get_redis_host_port(),
            password=secrets.get_redis_host_password(),
            db=0,
        )

        self.connection = pika.BlockingConnection(
            pika.URLParameters(secrets.get_cloudamqp_url())
        )
        self.channel = self.connection.channel()

        self.broadcaster_exchange = "broadcaster_fanout"
        self.channel.exchange_declare(self.broadcaster_exchange, exchange_type="fanout")

        self.broadcaster_queue = "join_broadcaster_chat_queue"
        self.channel.queue_declare(queue=self.broadcaster_queue, durable=True)

        self.channel.queue_bind(
            exchange=self.broadcaster_exchange, queue=self.broadcaster_queue
        )

    def __del__(self):
        self.cache.close()
        self.connection.close()

    async def initialize_twitch(self):
        auth = asyncio.create_task(self.twitch.authenticate())
        chat = asyncio.create_task(self.twitch.initialize_chat())
        await asyncio.gather(auth, chat)

    def start_consuming_streamers(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=self.broadcaster_queue,
            on_message_callback=self.handle_live_streamers,
        )
        print(f"Start consuming streamers from queue")
        self.channel.start_consuming()

    def handle_live_streamers(self, ch, method, properties, body):
        asyncio.run(self.handle_live_streamers_async(ch, method, properties, body))

    async def handle_live_streamers_async(self, ch, method, properties, body):
        streamers = json.loads(body.decode())

        print(f"Received {len(streamers)} live streamers")

        if not streamers:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # TODO: if this process dies and gets restarted before the keys in the cache expire,
        # it won't join the chat rooms it previously joined because they are still in the cache.
        # Possible solutions are to check the cache when we start up or to keep a separate
        # in-memory cache so it will be empty on start up.
        tasks = []
        for user_id, user_login in streamers:
            if not self.cache.exists(user_id):
                tasks.append(
                    asyncio.create_task(self.twitch.join_chat_room(user_login))
                )
                self.cache.set(user_id, "")
            self.cache.expire(user_id, 300)

        ch.basic_ack(delivery_tag=method.delivery_tag)

        await asyncio.gather(*tasks)


async def main():
    joiner = ChatRoomJoiner()
    await joiner.initialize_twitch()

    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, joiner.start_consuming_streamers)


asyncio.get_event_loop().run_until_complete(main())
