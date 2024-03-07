import logging
import uuid
from datetime import datetime
from typing import Optional

import codec
import gen.grpc.chat_database.chat_database_pb2 as chat_database_pb2
import gen.grpc.chat_database.chat_database_pb2_grpc as chat_database_pb2_grpc
import grpc
from datetime_helpers import get_month
from flask import Flask, jsonify
from flask_parameter_validation import Query, Route, ValidateParameters

app = Flask(__name__)

# gRPC client to query chat database
grpc_channel = grpc.insecure_channel("localhost:50051")
grpc_client = chat_database_pb2_grpc.ChatDatabaseStub(grpc_channel)


def serialize_chat_database_row(chat):
    return {
        "broadcaster_id": chat.broadcaster_id,
        "timestamp": chat.timestamp,
        "message_id": chat.message_id,
        "message": chat.message,
    }


def serialize_chat_database_rows(list_of_chats):
    return [serialize_chat_database_row(message) for message in list_of_chats]


def is_guid(string):
    try:
        uuid.UUID(string)
        return True
    except ValueError:
        return False


# Concatenate all elements of the primary key and base62 encode it so we have URL safe string for pagination
def get_cursor(primary_key_elements):
    cursor = " ".join(str(item) for item in primary_key_elements)
    return codec.base62_encode(cursor)


# Decode cursor and return tuple of elements that make up the primary key
def get_primary_key_elements(cursor):
    key_elements = codec.base62_decode(cursor).split()
    if len(key_elements) != 4:
        logging.error(f"Cursor doesn't have 4 elements! cursor: {cursor}")
        return None

    broadcaster_id, month, timestamp, message_id = key_elements
    if (
        not broadcaster_id.isdigit()
        or not month.isdigit()
        or not timestamp.isdigit()
        or not is_guid(message_id)
    ):
        logging.error(
            f"One or more cursor fields are corrupt. broadcaster_id: {broadcaster_id}, month: {month}, timestamp: {timestamp}, message_id: {message_id}"
        )
        return None

    return (int(broadcaster_id), int(month), int(timestamp), uuid.UUID(message_id))


# Returns the messages sent in a given broadcaster's chat room between two timestamps
@app.route("/v1.0/<int:broadcaster_id>/chat", methods=["GET"])
@ValidateParameters()
def get_chats(
    broadcaster_id: int = Route(),
    start: datetime = Query(),
    end: datetime = Query(),
    after: Optional[str] = Query(),
    limit: Optional[int] = Query(min_int=1, max_int=100, default=20),
):
    logging.info(
        f"broadcaster_id: {broadcaster_id}, start: {start}, end: {end}, after: {after}, limit: {limit}"
    )

    # Ensure the cursor is valid and extract the timestamp so we know where we left off
    after_timestamp = 0
    if after is not None:
        cursor_elements = get_primary_key_elements(after)
        if cursor_elements is None:
            error = {"Invalid cursor": "Corrupt fields"}
            return error, 400

        cursor_broadcaster_id, cursor_month, cursor_timestamp, _ = cursor_elements

        # Validate the broadcaster id
        if cursor_broadcaster_id != broadcaster_id:
            logging.error(
                f"Broadcaster Id ({cursor_broadcaster_id}) in cursor doesn't match broadcaster Id ({broadcaster_id}) passed in."
            )
            error = {"Invalid cursor": "Cursor doesn't match the broadcaster Id."}
            return error, 400

        # Validate the timestamp and month fields match
        if get_month(cursor_timestamp) != cursor_month:
            error = {"Invalid cursor": "Cursor is invalid."}
            logging.error(
                f"Timestamp ({cursor_timestamp}) and month ({cursor_month}) in cursor don't match."
            )
            return error, 400

        after_timestamp = cursor_timestamp

    response = grpc_client.GetChats(
        chat_database_pb2.GetChatsRequest(
            broadcaster_id=broadcaster_id,
            start=int(start.timestamp() * 1000),
            end=int(end.timestamp() * 1000),
            after_timestamp=after_timestamp,
            limit=limit,
        )
    )
    list_of_rows = list(response.chats)

    if len(list_of_rows) <= limit:
        logging.info(f"Returning {len(list_of_rows)} chats")
        return jsonify({"messages": serialize_chat_database_rows(list_of_rows)})
    else:
        # GetChats returns one more element than asked for when we need to do pagination,
        # so omit the last row so we return the number of messages that were asked for.
        logging.info(f"Returning {len(list_of_rows[:-1])} chats")

        # Use the extra row to build the cursor we return to the user for use on subsequent calls.
        next_element = list_of_rows[-1]
        primary_key_elements = (
            next_element.broadcaster_id,
            get_month(next_element.timestamp),
            next_element.timestamp,
            next_element.message_id,
        )

        #'33569d6a-8a67-4e48-aa55-b11bf86e2268'

        return jsonify(
            {
                "messages": serialize_chat_database_rows(list_of_rows[:-1]),
                "cursor": get_cursor(primary_key_elements),
            }
        )


if __name__ == "__main__":
    logging.basicConfig(
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    app.run(debug=True)
