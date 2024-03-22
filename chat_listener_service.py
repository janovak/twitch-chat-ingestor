import asyncio
import concurrent.futures
import json
import logging
from datetime import datetime

import auth.secrets as secrets
import pika
import redis
import twitch_proxy

import gen.grpc.rate_limiter.rate_limiter_pb2 as rate_limiter_pb2
import gen.grpc.rate_limiter.rate_limiter_pb2_grpc as rate_limiter_pb2_grpc
import grpc


# gRPC client to interact with rate limiter server
rate_limiter_channel = grpc.insecure_channel(f"localhost:50051")
rate_limiter_client = rate_limiter_pb2_grpc.RateLimiterStub(rate_limiter_channel)


class ChatRoomJoiner:
    def __init__(self):
        self.twitch_session = twitch_proxy.TwitchAPIConnection()

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
            pika.ConnectionParameters(host=secrets.get_cloudamqp_url())
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
        if streamer in self.online_streamers:
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

    async def check_rate_limiter_with_retry(self, timeout):
        while True:
            if timeout == 0:
                logging.warning(f"Rate limiter check timing out.")
                return False

            try:
                # Requests are shared between all user ids, so just use 0 for the id
                response = rate_limiter_client.ConsumeToken(
                    rate_limiter_pb2.ConsumeTokenRequest(
                        timestamp=int(datetime.now().timestamp()),
                        id=0,
                    )
                )
                if response.success:
                    return True  # Success

            except grpc.RpcError as rpc_error:
                status_code = rpc_error.code()
                details = rpc_error.details()
                logging.error(f"gRPC error: {status_code} {details}")

            timeout -= 1
            await asyncio.sleep(1)  # Wait for 1 second before retrying

    def handle_live_streamers(self, ch, method, properties, body):
        asyncio.run(self.handle_live_streamers_async(ch, method, properties, body))

    async def handle_live_streamers_async(self, ch, method, properties, body):
        _, user_login = json.loads(body.decode())

        logging.info(f"{user_login} is currently live")

        # Add streamer to the redis cache, in-memory cache, and join their chat room if they just went live.
        if user_login not in self.online_streamers:
            logging.error(f"{user_login} just came online")

            limit_exceeded = not await self.check_rate_limiter_with_retry(300)
            if not limit_exceeded:
                self.redis_cache.set(user_login, "")
                self.online_streamers.add(user_login)
                await self.twitch_session.join_chat_room(user_login)

        # Refresh the streamer's TTL. Streams that go offline won't be refreshed in the cashe,
        # and after a few minutes they will expire and be removed from the cache.
        self.redis_cache.expire(user_login, 300)

        ch.basic_ack(delivery_tag=method.delivery_tag)


async def main():
    logging.basicConfig(
        filemode="w",
        level=logging.WARNING,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    joiner = ChatRoomJoiner()
    await joiner.initialize_twitch()

    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, joiner.start_consuming_streamers)


asyncio.get_event_loop().run_until_complete(main())
