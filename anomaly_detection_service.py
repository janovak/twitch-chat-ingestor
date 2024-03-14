import json
import logging
from collections import defaultdict

import auth.secrets as secrets
import pika
from time_bucket_list import TimeBucketList


class AnomalyDetector:
    def __init__(self):
        self.message_queue_connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=secrets.get_cloudamqp_url())
        )
        self.channel = self.message_queue_connection.channel()

        # All chat messages are published to the chat exchange
        self.chat_exchange = "chat_fanout"
        self.channel.exchange_declare(self.chat_exchange, exchange_type="fanout")

        self.chat_queue = "chat_anomaly_detection_queue"
        self.channel.queue_declare(queue=self.chat_queue, durable=True)

        self.channel.queue_bind(exchange=self.chat_exchange, queue=self.chat_queue)

        self.anomaly_exchange = "anomaly_fanout"
        self.channel.exchange_declare(self.anomaly_exchange, exchange_type="fanout")

        def time_bucket_list_factory(bucket_size):
            return TimeBucketList(bucket_size)

        self.anomaly_detection_per_broadcaster = defaultdict(
            lambda: time_bucket_list_factory(bucket_size=5)
        )
        self.broadcaster_anomaly_cooldown = 30
        self.last_broadcaster_anomaly = defaultdict(int)

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        self.message_queue_connection.close()

    def start_consuming_chats(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=self.chat_queue, on_message_callback=self.handle_chat_message
        )
        logging.info("Start consuming chats from queue")
        self.channel.start_consuming()

    def handle_chat_message(self, ch, method, properties, body):
        message_fields = json.loads(body.decode())

        broadcaster_id = message_fields["broadcaster_id"]

        logging.info(
            f"Message, {message_fields['message_id']}, received in {broadcaster_id}'s chat room"
        )

        timestamp = message_fields["timestamp"] // 1000
        self.anomaly_detection_per_broadcaster[broadcaster_id].append(timestamp)

        # Only check for anomalies if we have at least 5 minutes (60 buckets * 5 second bucket size) of data
        if (
            self.anomaly_detection_per_broadcaster[broadcaster_id].size() > 60
            and self.anomaly_detection_per_broadcaster[
                broadcaster_id
            ].check_for_anomaly()
        ):
            if (
                timestamp - self.last_broadcaster_anomaly[broadcaster_id]
                > self.broadcaster_anomaly_cooldown
            ):
                logging.error(f"Anomaly detected in {broadcaster_id}'s chat room")
                self.last_broadcaster_anomaly[broadcaster_id] = timestamp

                message = json.dumps(
                    {"broadcaster_id": broadcaster_id, "timestamp": timestamp}
                )
                self.channel.basic_publish(
                    exchange=self.anomaly_exchange,
                    routing_key="",
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent
                    ),
                )
            else:
                logging.info(
                    f"Anomaly detected in {broadcaster_id}'s chat room, but we're in the cooldown period"
                )

        ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    logging.basicConfig(
        filemode="w",
        level=logging.WARNING,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    session = AnomalyDetector()
    session.start_consuming_chats()


if __name__ == "__main__":
    main()
