import time
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import auth.secrets as secrets
from pydispatch import dispatcher

SIGNAL = "CHAT_SIGNAL"


class DatabaseConnection:
    def __init__(self, keyspace):
        self.session = self.get_session(keyspace)
        dispatcher.connect(
            self.handle_chat_message, signal=SIGNAL, sender=dispatcher.Any
        )

    def get_session(self, keyspace):
        auth_provider = PlainTextAuthProvider(
            secrets.get_chat_db_client_id(), secrets.get_chat_db_secret()
        )
        cluster = Cluster(
            cloud=secrets.get_astra_db_cloud_config(), auth_provider=auth_provider
        )
        session = cluster.connect(keyspace)
        return session

    def insert(self, broadcaster_id, month, timestamp, message_id, message):
        self.session.execute(
            """
            INSERT INTO twitch_chat_by_broadcaster_and_timestamp (broadcaster_id, year_month, timestamp, message_id, message)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (broadcaster_id, month, timestamp, message_id, message),
        )

    def handle_chat_message(
        self, broadcaster_id, month, timestamp, message_id, message
    ):
        self.insert(broadcaster_id, month, timestamp, message_id, message)
