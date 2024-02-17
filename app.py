import astra_db
from flask import Flask, jsonify, request

app = Flask(__name__)


@app.route("/streamdata/api/v1.0/test", methods=["GET"])
def test():
    print("test")
    broadcaster_id = request.args.get("broadcaster_id")
    print(broadcaster_id)
    return jsonify({"test": "1"})


@app.route("/streamdata/api/v1.0/chats", methods=["GET"])
def get_chats():
    broadcaster_id = request.args.get("broadcaster_id")
    start = request.args.get("start")
    end = request.args.get("end")

    astra_session = astra_db.DatabaseConnection("chat_data")
    messages = astra_session.get_chats(broadcaster_id, start, end)
    print(messages.one())
    print("1")

    return jsonify({"messages": messages.one()})


if __name__ == "__main__":
    app.run(debug=True)
