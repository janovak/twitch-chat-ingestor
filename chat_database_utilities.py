import logging
import uuid

import utilities


def serialize_chat_database_row(chat):
    return {
        "broadcaster_id": chat.broadcaster_id,
        "timestamp": chat.timestamp,
        "message_id": chat.message_id,
        "message": chat.message,
    }


def serialize_chat_database_rows(list_of_chats):
    return [serialize_chat_database_row(message) for message in list_of_chats]


def serialize_clip_database_row(clip):
    return {
        "embed_url": clip.embed_url,
        "thumbnail_url": clip.thumbnail_url,
    }


def serialize_clip_database_rows(list_of_chats):
    return [serialize_clip_database_row(message) for message in list_of_chats]


# Concatenate all elements of the primary key and base62 encode it so we have a URL safe string for pagination
def get_cursor(primary_key_elements):
    cursor = " ".join(str(item) for item in primary_key_elements)
    return utilities.base62_encode(cursor)


# Decode the cursor and return a tuple of elements that make up the primary key
def get_primary_key_elements(cursor):
    key_elements = utilities.base62_decode(cursor).split()
    if len(key_elements) != 4:
        logging.error(f"Cursor doesn't have 4 elements! cursor: {cursor}")
        return None

    broadcaster_id, month, timestamp, message_id = key_elements
    if (
        not broadcaster_id.isdigit()
        or not month.isdigit()
        or not timestamp.isdigit()
        or not utilities.is_guid(message_id)
    ):
        logging.error(
            f"One or more cursor fields are corrupt. broadcaster_id: {broadcaster_id}, month: {month}, timestamp: {timestamp}, message_id: {message_id}"
        )
        return None

    return (int(broadcaster_id), int(month), int(timestamp), uuid.UUID(message_id))
