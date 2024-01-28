from twitchAPI.type import ChatEvent
from twitchAPI.chat import Chat, EventData, ChatMessage, ChatSub
import asyncio
import sys
import uuid
import json
from datetime import datetime
import twitch
import astra_db

TARGET_CHANNEL = sys.argv[1]

# connect to chat database
database_session = astra_db.get_session('chat_data')

# this will be called when the event READY is triggered, which will be on bot start
async def on_ready(ready_event: EventData):
    print('Bot is ready for work, joining channels')
    await ready_event.chat.join_room(TARGET_CHANNEL)

def serialize_message(msg: ChatMessage):
    room = {}
    room['name'] = msg.room.name
    room['is_emote_only'] = msg.room.is_emote_only
    room['is_subs_only'] = msg.room.is_subs_only
    room['is_followers_only'] = msg.room.is_followers_only
    room['is_unique_only'] = msg.room.is_unique_only
    room['follower_only_delay'] = msg.room.follower_only_delay
    room['room_id'] = msg.room.room_id
    room['slow'] = msg.room.slow

    user = {}
    user['name'] = msg.user.name
    user['badge_info'] = msg.user.badge_info
    user['badges'] = msg.user.badges
    user['color'] = msg.user.color
    user['display_name'] = msg.user.display_name
    user['mod'] = msg.user.mod
    user['subscriber'] = msg.user.subscriber
    user['turbo'] = msg.user.turbo
    user['id'] = msg.user.id
    user['user_type'] = msg.user.user_type
    user['vip'] = msg.user.vip

    message = {}
    message['text'] = msg.text
    message['is_me'] = msg.is_me
    message['bits'] = msg.bits
    message['sent_timestamp'] = msg.sent_timestamp
    message['reply_parent_msg_id'] = msg.reply_parent_msg_id
    message['reply_parent_user_id'] = msg.reply_parent_user_id
    message['reply_parent_user_login'] = msg.reply_parent_user_login
    message['reply_parent_display_name'] = msg.reply_parent_display_name
    message['reply_parent_msg_body'] = msg.reply_parent_msg_body
    message['reply_thread_parent_msg_id'] = msg.reply_thread_parent_msg_id
    message['reply_thread_parent_user_login'] = msg.reply_thread_parent_user_login
    message['emotes'] = msg.emotes
    message['id'] = msg.id

    message['room'] = room
    message['user'] = user

    return json.dumps(message)

def get_month():
    return datetime.utcnow().strftime('%Y%m')

# this will be called whenever a message in a channel was send by either the bot OR another user
async def on_message(msg: ChatMessage):
    print(msg.room.room_id, msg.sent_timestamp, msg.id)
    database_session.execute(
        """
        INSERT INTO twitch_chat_by_broadcaster_and_timestamp (broadcaster_id, year_month, timestamp, message_id, message)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (int(msg.room.room_id), int(get_month()), msg.sent_timestamp, uuid.UUID(msg.id), serialize_message(msg))
    )

# this will be called whenever someone subscribes to a channel
async def on_sub(sub: ChatSub):
    pass

# this is where we set up the bot
async def run():
    # set up twitch api instance and add user authentication with some scopes
    twitch_session = await twitch.get_session()

    # create chat instance
    chat = await Chat(twitch_session)

    # register the handlers for the events you want

    # listen to when the bot is done starting up and ready to join channels
    chat.register_event(ChatEvent.READY, on_ready)
    # listen to chat messages
    chat.register_event(ChatEvent.MESSAGE, on_message)
    # listen to channel subscriptions
    chat.register_event(ChatEvent.SUB, on_sub)
    # there are more events, you can view them all in this documentation

    # we are done with our setup, lets start this bot up!
    chat.start()

    # lets run till we press enter in the console
    try:
        input('Press ENTER to stop\n')
    finally:
        # now we can close the chat bot and the twitch api client
        chat.stop()
        await twitch_session.close()

# lets run our setup
asyncio.run(run())