import astra_db
import codec
from flask import Flask, jsonify, request

app = Flask(__name__)


def get_cursor(primary_key_elements):
    cursor = " ".join(str(item) for item in primary_key_elements)
    return codec.base62_encode(cursor)


def get_primary_key(cursor):
    primary_key_string = codec.base62_decode(cursor)
    return tuple(primary_key_string.split())


@app.route("/streamdata/api/v1.0/<broadcaster_id>/chat", methods=["GET"])
def get_chats(broadcaster_id):
    after = request.args.get("after")
    timestamp = 0
    if after is not None:
        # need to validate the other components of the cursor/primary key
        cursor_elements = get_primary_key(after)
        timestamp = cursor_elements[2]

    start = request.args.get("start")
    end = request.args.get("end")

    limit = int(request.args.get("limit", 20))
    limit = min(limit, 100)

    astra_session = astra_db.DatabaseConnection("chat_data")
    row_list = astra_session.get_chats(broadcaster_id, start, end, timestamp, limit)

    if len(row_list) <= limit:
        return jsonify({"messages": row_list})
    else:
        primary_key_elements = row_list[-1][:4]
        return jsonify(
            {"messages": row_list[:-1], "cursor": get_cursor(primary_key_elements)}
        )


if __name__ == "__main__":
    app.run(debug=True)
