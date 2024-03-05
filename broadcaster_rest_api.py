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

grpc_channel = grpc.insecure_channel("localhost:50051")
grpc_client = chat_database_pb2_grpc.ChatDatabaseStub(grpc_channel)


def serialize_chat_database_row(chat):
    return {
        "broadcaster_id": chat.broadcaster_id,
        "timestamp": chat.timestamp,
        "message_id": chat.message_id,
        "message": chat.message,
    }


def get_cursor(primary_key_elements):
    cursor = " ".join(str(item) for item in primary_key_elements)
    return codec.base62_encode(cursor)


def get_primary_key(cursor):
    primary_key_string = codec.base62_decode(cursor)
    return tuple(primary_key_string.split())


@app.route("/v1.0/<int:broadcaster_id>/chat", methods=["GET"])
@ValidateParameters()
def get_chats(
    broadcaster_id: int = Route(),
    start: datetime = Query(),
    end: datetime = Query(),
    after: Optional[str] = Query(),
    limit: Optional[int] = Query(min_int=1, max_int=100, default=20),
):
    after_timestamp = 0
    if after is not None:
        broadcaster_id, year_month, timestamp, _ = get_primary_key(after)

        # Validate the broadcaster id
        if not broadcaster_id.isdigit() or int(broadcaster_id) != broadcaster_id:
            error = {"Invalid cursor": "Cursor doesn't match the broadcaster Id"}
            return error, 400

        # Validate the timestamp and month fields match
        if get_month(timestamp) != year_month:
            error = {"Invalid cursor": "Cursor doesn't match the timestamp"}
            return error, 400

        after_timestamp = int(timestamp)

    response = grpc_client.GetChats(
        chat_database_pb2.GetChatsRequest(
            broadcaster_id=broadcaster_id,
            start=int(start.timestamp() * 1000),
            end=int(end.timestamp() * 1000),
            after_timestamp=after_timestamp,
            limit=limit,
        )
    )

    row_list = list(response.chats)

    # TODO: Need to filter out UUIDs that preceed the UUID in 'after' for the exact same timestamp.
    # If first element's message id matchs the cursors message id, then proceed as normal. If not,
    # Make two more calls to the database. One for the exact timestamp in the cursor where we ask for
    # rows where message_id >= cursor.message_id and a second one for timestamps greater than cursor.timestamp
    # (instead of greater than or equal to). Then stitch the results together and return.

    if len(row_list) <= limit:
        return jsonify(
            {
                "messages": [
                    serialize_chat_database_row(message) for message in row_list[:-1]
                ]
            }
        )
    else:
        next_element = row_list[-1]
        primary_key_elements = (
            next_element.broadcaster_id,
            get_month(next_element.timestamp),
            next_element.timestamp,
            next_element.message_id,
        )

        return jsonify(
            {
                "messages": [
                    serialize_chat_database_row(message) for message in row_list[:-1]
                ],
                "cursor": get_cursor(primary_key_elements),
            }
        )


if __name__ == "__main__":
    app.run(debug=True)
