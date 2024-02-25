import asyncio
import json
import uuid

import auth.secrets as secrets
import pika
from pydispatch import dispatcher
from twitchAPI.chat import Chat, ChatMessage, EventData
from twitchAPI.oauth import UserAuthenticator
from twitchAPI.twitch import Twitch
from twitchAPI.type import AuthScope, ChatEvent

USER_SCOPE = [AuthScope.CHAT_READ]

CHAT_SIGNAL = "CHAT_SIGNAL"

STREAMERS = [
    "jynxzi",
    "kaicenat",
    "fps_shaka",
    "caseoh_",
    "ibai",
    "gamesdonequick",
    "illojuan",
    "auronplay",
    "gaules",
    "xqc",
]


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
        self.channel.queue_declare(queue="live_broadcasters_queue", durable=True)

    def __del__(self):
        self.connection.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *excinfo):
        if self.chat:
            self.chat.stop()
        if self.session:
            await self.session.close()

    async def authenticate(self):
        self.session = await Twitch(
            secrets.get_twitch_api_client_id(), secrets.get_twitch_api_secret()
        )
        auth = UserAuthenticator(self.session, USER_SCOPE)
        token, refresh_token = await auth.authenticate()
        await self.session.set_user_authentication(token, USER_SCOPE, refresh_token)

    async def join_chat(self):
        self.chat = await Chat(self.session)
        self.chat.register_event(ChatEvent.READY, self.on_ready)
        self.chat.register_event(ChatEvent.MESSAGE, self.on_message)
        self.chat.start()

    async def on_ready(self, ready_event: EventData):
        print("Bot is ready for work, joining channels")
        tasks = [ready_event.chat.join_room(streamer) for streamer in STREAMERS]
        await asyncio.gather(*tasks)

    async def on_message(self, msg: ChatMessage):
        dispatcher.send(
            signal=CHAT_SIGNAL,
            sender="TWITCH",
            broadcaster_id=int(msg.room.room_id),
            timestamp=msg.sent_timestamp,
            message_id=uuid.UUID(msg.id),
            message=serialize_message(msg),
        )

    async def get_all_streamers(self):
        print("Retreiving all currently live streamers")

        batch_size = 100
        streamers = self.session.get_streams(first=batch_size, stream_type="live")

        async def publish_batch(ids):
            message = json.dumps(ids)
            self.channel.basic_publish(
                exchange="",
                routing_key="live_broadcasters_queue",
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )

        counter = 0
        ids = []
        tasks = []
        async for s in streamers:
            ids.append(int(s.user_id))
            if len(ids) == batch_size:
                counter += batch_size
                tasks.append(asyncio.create_task(publish_batch(ids)))
                ids.clear()

        # Publish any remaining streamers if the total count is not a multiple of batch_size
        if ids:
            counter += len(ids)
            tasks.append(asyncio.create_task(publish_batch(ids)))

        asyncio.gather(*tasks)

        print(f"Published {counter} Ids in batches of {batch_size}")
