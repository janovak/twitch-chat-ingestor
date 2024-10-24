import asyncio
import json
import logging
from datetime import datetime
from prometheus_client import start_http_server
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

import auth.secrets as secrets
import redis.asyncio as redis
import twitch_proxy

import gen.grpc.rate_limiter.rate_limiter_pb2 as rate_limiter_pb2
import gen.grpc.rate_limiter.rate_limiter_pb2_grpc as rate_limiter_pb2_grpc
import grpc

rate_limiter_channel = grpc.insecure_channel("localhost:50051")
rate_limiter_client = rate_limiter_pb2_grpc.RateLimiterStub(rate_limiter_channel)


class KafkaClient:
    def __init__(self):
        self.producer = Producer(
            {
                "bootstrap.servers": secrets.get_kafka_broker_url(),
                "client.id": "chat-listener",
            }
        )
        self.consumer = Consumer(
            {
                "bootstrap.servers": secrets.get_kafka_broker_url(),
                "group.id": "chat-listener-group",
                "auto.offset.reset": "earliest",
            }
        )

    def produce(self, topic, message):
        self.producer.produce(topic, message)
        self.producer.flush()

    def consume(self, topic):
        self.consumer.subscribe([topic])
        return self.consumer


class ChatRoomJoiner:
    def __init__(self):
        self.twitch_session = twitch_proxy.TwitchAPIConnection()
        self.kafka_client = KafkaClient()

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
            asyncio.create_task(self.streamer_went_offline_callback(message))

    async def streamer_went_offline_callback(self, message):
        streamer = message["data"].decode()
        logging.info(f"{streamer} went offline")
        if streamer in self.online_streamers:
            self.online_streamers.remove(streamer)
        await self.twitch_session.leave_chat_room(streamer)

    async def start_consuming_streamers(self):
        consumer = self.kafka_client.consume("live_broadcasters")

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Kafka error: {msg.error()}")
                    raise KafkaException(msg.error())

            await self.handle_live_streamers(msg.value())

    async def check_rate_limiter_with_retry(self, timeout):
        while True:
            if timeout <= 0:
                logging.warning(f"Rate limiter check timing out.")
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
                logging.error(f"gRPC error: {rpc_error.code()} {rpc_error.details()}")
            timeout -= 1
            await asyncio.sleep(1)

    async def handle_live_streamers(self, message):
        _, user_login, rank = json.loads(message.decode())

        logging.info(f"{user_login} is currently live")

        if user_login not in self.online_streamers and rank < 2:
            logging.info(f"{user_login} just came online")

            # Bypass rate limiter while figuring out 'StatusCode.UNIMPLEMENTED Method not found!' issue
            # limit_exceeded = not await self.check_rate_limiter_with_retry(35)
            limit_exceeded = False

            if not limit_exceeded:
                self.online_streamers.add(user_login)
                await self.redis_cache.set(user_login, "")
                await self.twitch_session.join_chat_room(user_login)

        await self.redis_cache.expire(user_login, 300)


async def main():
    logging.basicConfig(
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Start Prometheus server
    start_http_server(9100)

    joiner = ChatRoomJoiner()
    await joiner.initialize_twitch()
    await joiner.redis_cache.flushall()

    asyncio.create_task(joiner.handle_expiring_keys())
    await joiner.start_consuming_streamers()


if __name__ == "__main__":
    asyncio.run(main())
