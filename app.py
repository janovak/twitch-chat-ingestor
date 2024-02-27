import json
from datetime import datetime
from typing import Optional

import codec
import grpc
import rest_api_pb2
import rest_api_pb2_grpc
from flask import Flask, jsonify
from flask_parameter_validation import Query, Route, ValidateParameters

app = Flask(__name__)

channel = grpc.insecure_channel("localhost:50051")
stub = rest_api_pb2_grpc.ChatDatabaseStub(channel)


def serialize_chat(chat):
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
        cursor_elements = get_primary_key(after)
        if (
            not cursor_elements[0].isdigit()
            or int(cursor_elements[0]) != broadcaster_id
        ):
            error = {"Invalid cursor": "Cursor doesn't match the broadcaster Id"}
            return error, 400
        # TODO: check if the year_month aligns with the timestamp
        after_timestamp = int(cursor_elements[2])

    response = stub.GetChats(
        rest_api_pb2.GetChatsRequest(
            broadcaster_id=broadcaster_id,
            start=int(start.timestamp() * 1000),
            end=int(end.timestamp() * 1000),
            after_timestamp=after_timestamp,
            limit=limit,
        )
    )

    row_list = list(response.chats)

    # TODO: need to filter out UUIDs that preceed the UUID in 'after' for the exact same timestamp

    if len(row_list) <= limit:
        return jsonify(
            {"messages": [serialize_chat(message) for message in row_list[:-1]]}
        )
    else:
        next_element = row_list[-1]
        primary_key_elements = (
            next_element.broadcaster_id,
            next_element.year_month,
            next_element.timestamp,
            next_element.message_id,
        )

        return jsonify(
            {
                "messages": [serialize_chat(message) for message in row_list[:-1]],
                "cursor": get_cursor(primary_key_elements),
            }
        )


if __name__ == "__main__":
    app.run(debug=True)
