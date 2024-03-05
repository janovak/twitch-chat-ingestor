import asyncio
import json
import sys
import uuid

import auth.secrets as secrets
import pika
from twitchAPI.chat import Chat, ChatMessage
from twitchAPI.oauth import UserAuthenticator
from twitchAPI.twitch import Twitch
from twitchAPI.type import AuthScope, ChatEvent


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


class TwitchAPIConnection:
    def __init__(self):
        self.session = None
        self.chat = None

        self.connection = pika.BlockingConnection(
            pika.URLParameters(secrets.get_cloudamqp_url())
        )
        self.channel = self.connection.channel()

        self.broadcaster_exchange = "broadcaster_fanout"
        self.channel.exchange_declare(self.broadcaster_exchange, exchange_type="fanout")

        self.channel.queue_declare(queue="chat_processing_queue", durable=True)

    def __del__(self):
        if self.chat:
            self.chat.stop()

        if self.session:
            # Check if event loop is running
            if asyncio.get_event_loop().is_running():
                # Schedule cleanup task in the event loop
                asyncio.ensure_future(self.cleanup())
            else:
                # If event loop is not running, run cleanup synchronously
                asyncio.run(self.cleanup())

        self.connection.close()

    async def cleanup(self):
        # Close the Twitch session
        await self.session.close()

    async def authenticate(self):
        self.session = await Twitch(
            secrets.get_twitch_api_client_id(), secrets.get_twitch_api_secret()
        )
        auth = UserAuthenticator(self.session, [AuthScope.CHAT_READ])
        token, refresh_token = await auth.authenticate()
        await self.session.set_user_authentication(
            token, [AuthScope.CHAT_READ], refresh_token
        )

    async def initialize_chat(self):
        self.chat = await Chat(self.session)
        self.chat.register_event(ChatEvent.MESSAGE, self.on_message)
        self.chat.start()

    async def join_chat_room(self, streamer_name):
        await self.chat.join_room(streamer_name)
        print(f"Joined {streamer_name}'s chat room")

    async def on_message(self, msg: ChatMessage):
        message_fields = {
            "broadcaster_id": int(msg.room.room_id),
            "timestamp": msg.sent_timestamp,
            "message_id": str(uuid.UUID(msg.id)),
            "message": serialize_message(msg),
        }
        message = json.dumps(message_fields)

        print(
            f"Message {message_fields['message_id']} posted in chat room {message_fields['broadcaster_id']} at {message_fields['timestamp']}"
        )

        self.channel.basic_publish(
            exchange="",
            routing_key="chat_processing_queue",
            body=message,
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )

    async def get_all_streamers(self):
        print("Retrieving all currently live streamers")
        await self.get_streamers(sys.maxint)

    async def get_top_streamers(self, n):
        print(f"Retrieving top {n} currently live streamers")
        await self.get_streamers(n)

    async def get_streamers(self, n):
        batch_size = min(n, 100)
        streamers = self.session.get_streams(first=batch_size, stream_type="live")

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
        async for s in streamers:
            streamer_list.append((int(s.user_id), s.user_login))
            counter += 1
            if len(streamer_list) == batch_size:
                tasks.append(asyncio.create_task(publish_batch(streamer_list.copy())))
                streamer_list.clear()
            if counter == n:
                break

        # Publish any remaining streamers if the total count is not a multiple of batch_size
        if streamer_list:
            counter += len(streamer_list)
            tasks.append(asyncio.create_task(publish_batch(streamer_list)))

        await asyncio.gather(*tasks)

        print(f"Published {counter} Ids in batches of {batch_size}")
