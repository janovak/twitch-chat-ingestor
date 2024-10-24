import json
import logging
import re
from collections import defaultdict
from prometheus_client import start_http_server, Counter
from confluent_kafka import Consumer, Producer, KafkaError

import auth.secrets as secrets
from time_bucket_list import TimeBucketList


class AnomalyDetector:
    def __init__(self):
        # Kafka Producer configuration
        self.producer_conf = {
            "bootstrap.servers": secrets.get_kafka_broker_url(),
            "client.id": "anomaly_detector",
        }
        self.producer = Producer(self.producer_conf)

        # Kafka Consumer configuration
        self.consumer_conf = {
            "bootstrap.servers": secrets.get_kafka_broker_url(),  # Change as necessary
            "group.id": "anomaly_detector_group",  # Consumer group ID
            "auto.offset.reset": "earliest",  # Start reading at the earliest message
        }
        self.consumer = Consumer(self.consumer_conf)
        self.consumer.subscribe(["twitch-chat-messages"])  # Subscribe to the topic

        self.anomaly_topic = "anomalies"

        def time_bucket_list_factory(bucket_size):
            return TimeBucketList(bucket_size)

        self.anomaly_detection_per_broadcaster = defaultdict(
            lambda: time_bucket_list_factory(bucket_size=5)
        )
        self.broadcaster_anomaly_cooldown = 30
        self.last_broadcaster_anomaly = defaultdict(int)

        self.total_message_count = 0

        self.anomaly_counter = Counter(
            "streamer_anomaly_count",
            "Number of anomalies per streamer",
            ["broadcaster_id"],
        )

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        self.consumer.close()
        self.producer.flush()  # Ensure all messages are sent

    def start_consuming_chats(self):
        logging.info("Start consuming chats from Kafka topic")
        try:
            while True:
                msg = self.consumer.poll(1.0)  # Wait for a message
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        continue
                    else:
                        logging.error(f"Error while consuming message: {msg.error()}")
                        continue

                message_fields = json.loads(msg.value().decode())
                self.handle_chat_message(message_fields)

        except KeyboardInterrupt:
            logging.info("Stopping consumer...")

    def handle_chat_message(self, message_fields):
        broadcaster_id = message_fields["broadcaster_id"]

        if self.total_message_count % 100000 == 0:
            logging.info(f"Messages received {self.total_message_count}")

        self.total_message_count += 1

        logging.debug(
            f"Message, {message_fields['message_id']}, received in {broadcaster_id}'s chat room"
        )

        def starts_with_valid_command(s):
            # '!' followed by an alphanumeric string and then any other character
            pattern = r"^![a-zA-Z0-9]+.*$"
            return bool(re.match(pattern, s))

        # Don't count commands in anomaly detection. We don't want to clip streamers doing giveaways, predictions, etc.
        message_text = json.loads(message_fields["message"])
        if starts_with_valid_command(message_text["text"]):
            return  # Do not process this message further

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
                logging.info(f"Anomaly detected in {broadcaster_id}'s chat room")
                self.last_broadcaster_anomaly[broadcaster_id] = timestamp

                message = json.dumps(
                    {"broadcaster_id": broadcaster_id, "timestamp": timestamp}
                )
                self.producer.produce(
                    self.anomaly_topic,
                    value=message,
                    callback=self.delivery_report,
                )

                self.anomaly_counter.labels(broadcaster_id=broadcaster_id).inc()
            else:
                logging.debug(
                    f"Anomaly detected in {broadcaster_id}'s chat room, but we're in the cooldown period"
                )

    def delivery_report(self, err, msg):
        """Delivery report for asynchronous produce() calls."""
        if err is not None:
            logging.error(f"Message delivery failed: {err}")
        else:
            logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def main():
    logging.basicConfig(
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    start_http_server(9200)

    session = AnomalyDetector()
    session.start_consuming_chats()


if __name__ == "__main__":
    main()
