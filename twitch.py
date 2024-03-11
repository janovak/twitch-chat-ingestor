import asyncio
import json
import logging
import sys
import uuid
import utilities
import auth.secrets as secrets
import pika
from twitchAPI.chat import Chat, ChatMessage
from twitchAPI.oauth import UserAuthenticator
from twitchAPI.twitch import Twitch
from twitchAPI.type import AuthScope, ChatEvent


def is_valid_message(msg: ChatMessage):
    # Only validate the fields we a need to insert the message into the database
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


class TwitchAPIConnection:
    def __init__(self):
        self.twitch_session = None
        self.chat = None

        self.message_queue_connection = pika.BlockingConnection(
            pika.URLParameters(secrets.get_cloudamqp_url())
        )
        self.channel = self.message_queue_connection.channel()
        self.channel.confirm_delivery()

        # Create a fanout exchange to publish the broadcaster Ids to so that any service that needs
        # this information can bind a queue to this exchange
        self.broadcaster_exchange = "broadcaster_fanout"
        self.channel.exchange_declare(self.broadcaster_exchange, exchange_type="fanout")

        # Create a fanout exchange to publish the chat messages to so that any service that needs
        # this information can bind a queue to this exchange
        self.chat_exchange = "chat_fanout"
        self.channel.exchange_declare(self.chat_exchange, exchange_type="fanout")

    def __del__(self):
        self.close()

    def close(self):
        self.message_queue_connection.close()

        if self.chat:
            self.chat.stop()

        if self.twitch_session:
            if asyncio.get_event_loop().is_running():
                # Schedule cleanup task in the event loop
                asyncio.ensure_future(self.cleanup_async())
            else:
                # If event loop is not running, run cleanup synchronously
                asyncio.run(self.cleanup_async())

    async def cleanup_async(self):
        await self.twitch_session.close()

    async def authenticate(self):
        self.twitch_session = await Twitch(
            secrets.get_twitch_api_client_id(), secrets.get_twitch_api_secret()
        )
        auth = UserAuthenticator(self.twitch_session, [AuthScope.CHAT_READ])
        token, refresh_token = await auth.authenticate()
        await self.twitch_session.set_user_authentication(
            token, [AuthScope.CHAT_READ], refresh_token
        )

    async def initialize_chat(self):
        self.chat = await Chat(self.twitch_session)
        self.chat.register_event(ChatEvent.MESSAGE, self.on_message)
        self.chat.start()

    async def join_chat_room(self, streamer_name):
        await self.chat.join_room(streamer_name)
        logging.info(f"Joined {streamer_name}'s chat room")

    async def leave_chat_room(self, streamer_name):
        await self.chat.leave_room(streamer_name)
        logging.info(f"Left {streamer_name}'s chat room")

    async def on_message(self, msg: ChatMessage):
        if not is_valid_message(msg):
            logging.warning(
                "Skipping message as it does not contain the necessary fields"
            )
            return

        # Extract relevant fields from the message and serialize it to JSON
        message_fields = {
            "broadcaster_id": int(msg.room.room_id),
            "timestamp": msg.sent_timestamp,
            "message_id": str(uuid.UUID(msg.id)),
            "message": serialize_message(msg),
        }
        message = json.dumps(message_fields)

        logging.info(
            f"Message {message_fields['message_id']} posted in chat room {message_fields['broadcaster_id']} at {message_fields['timestamp']}"
        )

        try:
            self.channel.basic_publish(
                exchange=self.chat_exchange,
                routing_key="",
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )

            logging.info(
                f"Published message, {message_fields['message_id']}, which was posted in chat room {message_fields['broadcaster_id']} at {message_fields['timestamp']}, to the message queue"
            )
        except Exception as e:
            logging.error(f"Publishing message error: {e}")
            logging.error(
                f"Failed to publish message, {message_fields['message_id']}, which was posted in chat room {message_fields['broadcaster_id']} at {message_fields['timestamp']}, to the message queue"
            )

    async def get_all_streamers(self):
        logging.info("Retrieving all currently live streamers")
        await self.get_online_streamers(sys.maxint)

    async def get_top_streamers(self, n):
        logging.info(f"Retrieving top {n} currently live streamers")
        await self.get_online_streamers(n)

    async def get_online_streamers(self, batch_size):
        batch_size = min(batch_size, 100)
        streamers = self.twitch_session.get_streams(
            first=batch_size, stream_type="live"
        )

        # Publishes a JSON list of streamer Ids to the chat queue. The list has a maximum of batch_size Ids
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
        # Groups streamers into lists of 100 and then asyncronously publishes the list to the chat queue
        async for s in streamers:
            streamer_list.append((int(s.user_id), s.user_login))
            counter += 1
            if len(streamer_list) == batch_size:
                tasks.append(asyncio.create_task(publish_batch(streamer_list.copy())))
                streamer_list.clear()
            if counter == batch_size:
                break

        # Publish any remaining streamers if the total count is not a multiple of batch_size
        if streamer_list:
            counter += len(streamer_list)
            tasks.append(asyncio.create_task(publish_batch(streamer_list)))

        await asyncio.gather(*tasks)

        logging.info(f"Published {counter} Ids in batches of {batch_size}")
