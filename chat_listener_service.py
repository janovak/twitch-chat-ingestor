import asyncio
import json
import logging
from datetime import datetime

import auth.secrets as secrets
import aio_pika
import redis.asyncio as redis
import twitch_proxy

import gen.grpc.rate_limiter.rate_limiter_pb2 as rate_limiter_pb2
import gen.grpc.rate_limiter.rate_limiter_pb2_grpc as rate_limiter_pb2_grpc
import grpc

rate_limiter_channel = grpc.insecure_channel("localhost:50051")
rate_limiter_client = rate_limiter_pb2_grpc.RateLimiterStub(rate_limiter_channel)


class ChatRoomJoiner:
    def __init__(self):
        self.twitch_session = twitch_proxy.TwitchAPIConnection()

        # We keep an in-memory cache in addition to the redis cache in case the process needs to be restarted.
        # Without the in-memory cache we would never rejoin the chat rooms after restarting. This still isn't
        # ideal since we need to wait for another message listing all the online streamers, but it's good
        # enough for now
        self.online_streamers = set()

        self.redis_cache = redis.from_url(
            "redis://:"
            + secrets.get_redis_host_password()
            + "@"
            + secrets.get_redis_host_url()
            + ":"
            + str(secrets.get_redis_host_port())
            + "/0"
        )

        self.connection = None
        self.channel = None

    async def initialize_twitch(self):
        await self.twitch_session.authenticate()
        await self.twitch_session.initialize_chat()

    async def handle_expiring_keys(self):
        pubsub = self.redis_cache.pubsub()
        await pubsub.psubscribe("__keyevent@0__:expired")
        events = pubsub.listen()

        # Skip the first message since it's not an actual streamer
        async for message in events:
            break

        async for message in events:
            # Should await these tasks, but not critical. Awaiting will just help with a graceful shutdown
            asyncio.create_task(self.streamer_went_offline_callback(message))

    async def streamer_went_offline_callback(self, message):
        streamer = message["data"].decode()

        logging.info(f"{streamer} went offline")

        if streamer in self.online_streamers:
            self.online_streamers.remove(streamer)

        # TODO await this task during shutdown
        # real fix it to patch pytwitchapi by adding timeout to leave_room
        asyncio.create_task(self.twitch_session.leave_chat_room(streamer))

    async def start_consuming_streamers(self):
        connection = await aio_pika.connect_robust(
            "amqp://guest:guest@localhost:5672/", heartbeat=60
        )
        channel = await connection.channel()

        # Declare the exchange and queue
        broadcaster_exchange = await channel.declare_exchange(
            "broadcaster_fanout", aio_pika.ExchangeType.FANOUT
        )
        broadcaster_queue = await channel.declare_queue(
            "join_broadcaster_chat_queue", durable=True
        )

        await broadcaster_queue.bind(broadcaster_exchange)

        await channel.set_qos(prefetch_count=1)

        async with broadcaster_queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    await self.handle_live_streamers(message)

    async def check_rate_limiter_with_retry(self, timeout):
        while True:
            if timeout == 0:
                logging.warning(f"Rate limiter check timing out.")
                return False
            elif timeout < 0:
                logging.warning(f"oops")
                return False

            try:
                response = rate_limiter_client.ConsumeToken(
                    rate_limiter_pb2.ConsumeTokenRequest(
                        timestamp=int(datetime.now().timestamp()),
                        id=0,
                    )
                )
                if response.success:
                    return True

            except grpc.RpcError as rpc_error:
                status_code = rpc_error.code()
                details = rpc_error.details()
                logging.error(f"gRPC error: {status_code} {details}")

            timeout -= 1
            await asyncio.sleep(1)

    async def handle_live_streamers(self, message: aio_pika.IncomingMessage):
        _, user_login, rank = json.loads(message.body.decode())

        logging.info(f"{user_login} is currently live")

        if user_login not in self.online_streamers and rank < 20:
            logging.error(f"{user_login} just came online")

            limit_exceeded = not await self.check_rate_limiter_with_retry(35)
            if not limit_exceeded:
                self.online_streamers.add(user_login)
                await self.redis_cache.set(user_login, "")
                await self.twitch_session.join_chat_room(user_login)

        await self.redis_cache.expire(user_login, 300)


async def main():
    logging.basicConfig(
        filemode="w",
        level=logging.CRITICAL,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    joiner = ChatRoomJoiner()
    await joiner.initialize_twitch()
    await joiner.redis_cache.flushall()
    asyncio.create_task(joiner.handle_expiring_keys())

    await joiner.start_consuming_streamers()


if __name__ == "__main__":
    asyncio.run(main())
