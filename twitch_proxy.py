import asyncio
import logging
import auth.secrets as secrets
from twitchAPI.chat import Chat
from twitchAPI.twitch import Twitch
from twitchAPI.type import AuthScope, ChatEvent
from twitchAPI.helper import first
from confluent_kafka import Producer


class TwitchAPIConnection:
    def __init__(self):
        self.twitch_session = None
        self.chat = None

        self.producer = Producer({"bootstrap.servers": secrets.get_kafka_broker_url()})

    def __del__(self):
        self.close()

    def close(self):
        if self.chat:
            self.chat.stop()

        if self.twitch_session:
            if asyncio.get_event_loop().is_running():
                asyncio.ensure_future(self.cleanup_async())
            else:
                asyncio.run(self.cleanup_async())

        self.producer.flush()  # Ensure all messages are sent

    async def cleanup_async(self):
        await self.twitch_session.close()

    async def authenticate(self):
        self.twitch_session = await Twitch(
            secrets.get_twitch_api_client_id(), secrets.get_twitch_api_secret()
        )
        await self.twitch_session.set_user_authentication(
            secrets.get_twitch_access_token(),
            [AuthScope.CHAT_READ, AuthScope.CLIPS_EDIT],
            secrets.get_twitch_refresh_token(),
        )

    async def initialize_chat(self, callback):
        self.chat = await Chat(self.twitch_session)
        self.chat.register_event(ChatEvent.MESSAGE, callback)
        self.chat.start()

    async def join_chat_room(self, streamer_name):
        try:
            failed_to_join = await self.chat.join_room(streamer_name)
            if failed_to_join:
                logging.error(f"Failed to join {streamer_name}'s chat room.")
                return
        except asyncio.exceptions.CancelledError:
            logging.error(
                f"Cancellation exception while joining {streamer_name}'s chat room: {e}"
            )
            return
        except Exception as e:
            logging.error(f"Error joining {streamer_name}'s chat room: {e}")
            return
        logging.info(f"Joined {streamer_name}'s chat room")

    async def leave_chat_room(self, streamer_name):
        try:
            await self.chat.leave_room(streamer_name)
        except asyncio.exceptions.CancelledError as e:
            logging.error(
                f"Cancellation exception while {streamer_name}'s leaving chat room: {e}"
            )
            return
        except Exception as e:
            logging.error(f"Error leaving {streamer_name}'s chat room: {e}")
            return
        logging.info(f"Left {streamer_name}'s chat room")

    async def get_online_streamers(self, batch_size):
        logging.info(f"Retrieving currently live streamers")
        batch_size = min(batch_size, 100)
        streamers = self.twitch_session.get_streams(
            first=batch_size, stream_type="live"
        )
        return streamers

    async def create_clip(self, broadcaster_id):
        response = await self.twitch_session.create_clip(broadcaster_id)
        return response.id

    async def get_clip(self, clip_id):
        clip = await first(self.twitch_session.get_clips(clip_id=clip_id))
        return clip.id, clip.embed_url, clip.thumbnail_url
