import json
import logging
import uuid
from collections import defaultdict

import auth.secrets as secrets
import pandas as pd
import time
import pika
from datetime_helpers import get_month


class ChatAnomalyDetector:
    def __init__(self):
        self.message_queue_connection = pika.BlockingConnection(
            pika.URLParameters(secrets.get_cloudamqp_url())
        )

        # All chat messages are published to the chat exchange, so we bind a queue to the exchange
        # so we can listen to all messages and process them for anomalies
        self.chat_exchange = "chat_fanout"
        self.channel.exchange_declare(self.chat_exchange, exchange_type="fanout")

        self.chat_queue = "chat_anomaly_detection_queue"
        self.channel.queue_declare(queue=self.chat_queue, durable=True)

        self.channel.queue_bind(exchange=self.chat_queue, queue=self.chat_queue)

        self.rolling_data_dict = defaultdict(
            lambda: pd.DataFrame(columns=["value"], index=[])
        )

        # Define the maximum duration to keep (e.g., 5 minutes)
        self.max_duration = pd.Timedelta(minutes=5)
        self.min_duration = pd.Timedelta(seconds=10)

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        self.message_queue_connection.close()

    def start_consuming_chats(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=self.chat_queue,
            on_message_callback=self.handle_chat_message,
        )
        logging.info("Start consuming chats from queue")
        self.channel.start_consuming()

    def handle_chat_message(self, ch, method, properties, body):
        message_fields = json.loads(body.decode())
        broadcaster_id = message_fields["broadcaster_id"]
        timestamp = message_fields["timestamp"]

        data = pd.DataFrame({"value": [1]}, index=[timestamp])
        # Append the new data to the existing DataFrame for the corresponding ID
        self.rolling_data_dict[broadcaster_id] = self.rolling_data_dict[
            broadcaster_id
        ].append(data)

        # Convert milliseconds since epoch to Timestamp objects

        # Remove old data beyond the maximum duration for the corresponding ID
        cutoff_time = pd.Timestamp.now() - self.max_duration
        self.rolling_data_dict[broadcaster_id] = self.rolling_data_dict[broadcaster_id][
            self.rolling_data_dict[broadcaster_id].index >= cutoff_time
        ]

        # Calculate the rolling mean for the corresponding ID
        rolling_mean_long = self.rolling_data_dict[broadcaster_id]["value"].mean()

        cutoff_time2 = pd.Timestamp.now() - self.min_duration
        self.rolling_data_dict[broadcaster_id] = self.rolling_data_dict[broadcaster_id][
            self.rolling_data_dict[broadcaster_id].index >= cutoff_time2
        ]

        rolling_mean_short = self.rolling_data_dict[broadcaster_id]["value"].mean()

        # Print the rolling mean for the corresponding ID
        logging.info(
            f"Rolling Mean (30-second window) for ID {broadcaster_id}: {rolling_mean_long}"
        )

        logging.info(
            f"Rolling Mean (10-second window) for ID {broadcaster_id}: {rolling_mean_short}"
        )

        if rolling_mean_short * 2 > rolling_mean_long:
            logging.info(f"{broadcaster_id} spiked")

        ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    logging.basicConfig(
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    session = ChatAnomalyDetector()
    session.start_consuming_chats()


if __name__ == "__main__":
    main()
