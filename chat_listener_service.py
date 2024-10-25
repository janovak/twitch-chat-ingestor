import asyncio
import json
import logging
import json
import utilities
import uuid
from datetime import datetime
from prometheus_client import start_http_server, Counter
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from twitchAPI.chat import ChatMessage

import auth.secrets as secrets
import redis.asyncio as redis
import twitch_proxy

import gen.grpc.rate_limiter.rate_limiter_pb2 as rate_limiter_pb2
import gen.grpc.rate_limiter.rate_limiter_pb2_grpc as rate_limiter_pb2_grpc
import grpc

rate_limiter_channel = grpc.insecure_channel("localhost:50051")
rate_limiter_client = rate_limiter_pb2_grpc.RateLimiterStub(rate_limiter_channel)


def is_valid_message(msg: ChatMessage):
    if msg is None:
        logging.warning("msg is None")
        return False
    elif msg.id is None or not utilities.is_guid(msg.id):
        logging.warning(f"msg.id is {msg.id}")
        return False
    elif msg.sent_timestamp is None or msg.sent_timestamp <= 0:
        logging.warning(f"msg.sent_timestamp is {msg.sent_timestamp}")
        return False
    elif msg.room is None:
        logging.warning("msg.room is None")
        return False
    elif msg.room.room_id is None or int(msg.room.room_id) <= 0:
        logging.warning(f"msg.room.room_id is {msg.room.room_id}")
        return False
    elif msg.user is None:
        logging.warning("msg.user is None")
        return False
    return True


def serialize_message(msg: ChatMessage):
    room = {
        "name": msg.room.name,
        "is_emote_only": msg.room.is_emote_only,
        "is_subs_only": msg.room.is_subs_only,
        "is_followers_only": msg.room.is_followers_only,
        "is_unique_only": msg.room.is_unique_only,
        "follower_only_delay": msg.room.follower_only_delay,
        "room_id": msg.room.room_id,
        "slow": msg.room.slow,
    }

    user = {
        "name": msg.user.name,
        "badge_info": msg.user.badge_info,
        "badges": msg.user.badges,
        "color": msg.user.color,
        "display_name": msg.user.display_name,
        "mod": msg.user.mod,
        "subscriber": msg.user.subscriber,
        "turbo": msg.user.turbo,
        "id": msg.user.id,
        "user_type": msg.user.user_type,
        "vip": msg.user.vip,
    }

    message = {
        "text": msg.text,
        "is_me": msg.is_me,
        "bits": msg.bits,
        "sent_timestamp": msg.sent_timestamp,
        "reply_parent_msg_id": msg.reply_parent_msg_id,
        "reply_parent_user_id": msg.reply_parent_user_id,
        "reply_parent_user_login": msg.reply_parent_user_login,
        "reply_parent_display_name": msg.reply_parent_display_name,
        "reply_parent_msg_body": msg.reply_parent_msg_body,
        "reply_thread_parent_msg_id": msg.reply_thread_parent_msg_id,
        "reply_thread_parent_user_login": msg.reply_thread_parent_user_login,
        "emotes": msg.emotes,
        "id": msg.id,
    }

    message["room"] = room
    message["user"] = user

    return json.dumps(message)


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

    def produce(self, topic, message, key):
        self.producer.produce(topic=topic, value=message, key=key)
        self.producer.flush()

    def consume(self, topic):
        self.consumer.subscribe([topic])
        return self.consumer


class ChatRoomJoiner:
    def __init__(self):
        self.total_message_processed = 0

        self.twitch_session = twitch_proxy.TwitchAPIConnection()
        self.kafka_client = KafkaClient()

        self.chat_topic = "twitch-chat-messages"

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

        self.message_counter = Counter(
            "streamer_message_count",
            "Number of messages per streamer",
            ["broadcaster_id"],
        )

    async def initialize_twitch(self):
        await self.twitch_session.authenticate()
        await self.twitch_session.initialize_chat(self.on_message)

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

    async def on_message(self, msg: ChatMessage):
        if not is_valid_message(msg):
            logging.warning(
                "Skipping message as it does not contain the necessary fields"
            )
            return

        self.total_message_processed += 1
        if self.total_message_processed % 100000 == 0:
            logging.info(f"Total messages processed: {self.total_message_processed}")

        message_fields = {
            "broadcaster_id": int(msg.room.room_id),
            "timestamp": msg.sent_timestamp,
            "message_id": str(uuid.UUID(msg.id)),
            "message": serialize_message(msg),
        }
        message = json.dumps(message_fields)

        logging.debug(
            f"Message {message_fields['message_id']} posted in chat room {message_fields['broadcaster_id']} at {message_fields['timestamp']}"
        )

        self.message_counter.labels(
            broadcaster_id=message_fields["broadcaster_id"]
        ).inc()

        try:
            self.kafka_client.produce(
                topic=self.chat_topic,
                message=message,
                key=str(message_fields["broadcaster_id"]),
            )

            logging.debug(
                f"Published message, {message_fields['message_id']}, which was posted in chat room {message_fields['broadcaster_id']} at {message_fields['timestamp']}, to Kafka"
            )
        except Exception as e:
            logging.error(f"Publishing message error: {e}")
            logging.error(
                f"Failed to publish message, {message_fields['message_id']}, which was posted in chat room {message_fields['broadcaster_id']} at {message_fields['timestamp']}, to Kafka"
            )


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
