import logging
from datetime import datetime
from typing import Optional

import gen.grpc.chat_database.chat_database_pb2 as chat_database_pb2
import gen.grpc.chat_database.chat_database_pb2_grpc as chat_database_pb2_grpc
import grpc
from chat_database_utilities import (
    get_cursor,
    get_primary_key_elements,
    serialize_chat_database_rows,
)
from datetime_helpers import get_month
from flask import Flask, jsonify
from flask_parameter_validation import Query, Route, ValidateParameters

app = Flask(__name__)

# gRPC client to query chat database
grpc_channel = grpc.insecure_channel("localhost:50051")
grpc_client = chat_database_pb2_grpc.ChatDatabaseStub(grpc_channel)


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

    start_milliseconds = int(start.timestamp() * 1000)
    end_milliseconds = int(end.timestamp() * 1000)

    # Ensure the cursor is valid and extract the timestamp so we know where we left off
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

        # Override the start time if we have a cursor since we want to pick up where the last request left off
        start = cursor_timestamp

    response = grpc_client.GetChats(
        chat_database_pb2.GetChatsRequest(
            broadcaster_id=broadcaster_id,
            start=start_milliseconds,
            end=end_milliseconds,
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
