import astra_db
from flask import Flask, jsonify, request

app = Flask(__name__)


# copied from chat GPT to test. clean up later
def encode_to_base62(string):
    # Define base62 characters
    characters = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    base = len(characters)

    # Convert each character to its ASCII value and then to base62 representation
    base62_encoded_string = ""
    for char in string:
        ascii_value = ord(char)
        base62_digit = ""
        while ascii_value > 0:
            remainder = ascii_value % base
            base62_digit = characters[remainder] + base62_digit
            ascii_value //= base
        base62_encoded_string += base62_digit

    return base62_encoded_string


# copied from chat GPT to test. clean up later
def decode_from_base62(base62_string):
    characters = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    base = len(characters)

    # Convert each base62 character to its corresponding ASCII value
    decoded_string = ""
    for char in base62_string:
        value = characters.index(char)
        decoded_char = ""
        while value > 0:
            remainder = value % 62
            decoded_char = chr(remainder) + decoded_char
            value //= 62
        decoded_string += decoded_char

    return decoded_string


def get_cursor(primary_key_elements):
    cursor = " ".join(str(item) for item in primary_key_elements)
    return encode_to_base62(cursor)


def get_primary_key(cursor):
    primary_key_string = decode_from_base62(cursor)
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
