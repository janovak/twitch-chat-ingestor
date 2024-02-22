from datetime import datetime
from typing import Optional

import astra_db
import codec
from flask import Flask, jsonify
from flask_parameter_validation import Query, Route, ValidateParameters

app = Flask(__name__)


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
        # check if the year_month aligns with the timestamp
        after_timestamp = int(cursor_elements[2])

    astra_session = astra_db.DatabaseConnection("chat_data")
    row_list = astra_session.get_chats(
        broadcaster_id, start, end, after_timestamp, limit
    )

    # TOOD: need to filter out UUIDs that preceed the UUID in 'after' for the exact same timestamp

    if len(row_list) <= limit:
        return jsonify({"messages": row_list})
    else:
        primary_key_elements = row_list[-1][:4]
        return jsonify(
            {"messages": row_list[:-1], "cursor": get_cursor(primary_key_elements)}
        )


if __name__ == "__main__":
    app.run(debug=True)
