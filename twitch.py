import json
from datetime import datetime
from twitchAPI.twitch import Twitch
from twitchAPI.oauth import UserAuthenticator
from twitchAPI.type import AuthScope, ChatEvent
from twitchAPI.chat import Chat, EventData, ChatMessage
import uuid
import auth.secrets as secrets
import asyncio
from pydispatch import dispatcher

USER_SCOPE = [AuthScope.CHAT_READ]

SIGNAL = 'CHAT_SIGNAL'

def serialize_message(msg: ChatMessage):
    room = {
        'name': msg.room.name,
        'is_emote_only': msg.room.is_emote_only,
        'is_subs_only': msg.room.is_subs_only,
        'is_followers_only': msg.room.is_followers_only,
        'is_unique_only': msg.room.is_unique_only,
        'follower_only_delay': msg.room.follower_only_delay,
        'room_id': msg.room.room_id,
        'slow': msg.room.slow
    }

    user = {
        'name': msg.user.name,
        'badge_info': msg.user.badge_info,
        'badges': msg.user.badges,
        'color': msg.user.color,
        'display_name': msg.user.display_name,
        'mod': msg.user.mod,
        'subscriber': msg.user.subscriber,
        'turbo': msg.user.turbo,
        'id': msg.user.id,
        'user_type': msg.user.user_type,
        'vip': msg.user.vip
    }

    message = {
        'text': msg.text,
        'is_me': msg.is_me,
        'bits': msg.bits,
        'sent_timestamp': msg.sent_timestamp,
        'reply_parent_msg_id': msg.reply_parent_msg_id,
        'reply_parent_user_id': msg.reply_parent_user_id,
        'reply_parent_user_login': msg.reply_parent_user_login,
        'reply_parent_display_name': msg.reply_parent_display_name,
        'reply_parent_msg_body': msg.reply_parent_msg_body,
        'reply_thread_parent_msg_id': msg.reply_thread_parent_msg_id,
        'reply_thread_parent_user_login': msg.reply_thread_parent_user_login,
        'emotes': msg.emotes,
        'id': msg.id
    }

    message['room'] = room
    message['user'] = user

    return json.dumps(message)

def get_month():
    return datetime.utcnow().strftime('%Y%m')

class TwitchAPIConnection:
    instance = None
    chat = None

    def __del__(self):
        self.cleanup()

    @classmethod
    async def init_connection(self):
        if self.instance is None:
            session = await Twitch(secrets.get_twitch_api_client_id(), secrets.get_twitch_api_secret())
            auth = UserAuthenticator(session, USER_SCOPE)
            token, refresh_token = await auth.authenticate()
            await session.set_user_authentication(token, USER_SCOPE, refresh_token)
            self.instance = session
        return self.instance

    async def join_chat(self):
        self.chat = await Chat(self.instance)
        self.chat.register_event(ChatEvent.READY, self.on_ready)
        self.chat.register_event(ChatEvent.MESSAGE, self.on_message)
        self.chat.start()

    def cleanup(self):
        if self.chat:
            self.chat.stop()
        if self.instance:
            self.instance.close()

    async def on_ready(self, ready_event: EventData):
        print('Bot is ready for work, joining channels')
        await ready_event.chat.join_room('sneakylol')

    async def on_message(self, msg: ChatMessage):
        print(msg.room.room_id, msg.sent_timestamp, msg.id)
        dispatcher.send(signal=SIGNAL, sender="TWITCH", broadcaster_id=int(msg.room.room_id), month=int(get_month()), timestamp=msg.sent_timestamp, message_id=uuid.UUID(msg.id), message=serialize_message(msg))
