import auth.secrets as secrets
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from pydispatch import dispatcher

SIGNAL = "CHAT_SIGNAL"


class DatabaseConnection:
    def __init__(self, keyspace):
        self.session = self.get_session(keyspace)
        # not thread safe at the moment
        self.batch = BatchStatement()
        self.batch_counter = 0
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

    def prepare(self, broadcaster_id, month, timestamp, message_id, message):
        statement = self.session.prepare(
            """
            INSERT INTO twitch_chat_by_broadcaster_and_timestamp (broadcaster_id, year_month, timestamp, message_id, message)
            VALUES (?, ?, ?, ?, ?)
            """
        )
        self.batch.add(
            statement, (broadcaster_id, month, timestamp, message_id, message)
        )
        self.batch_counter += 1

    def insert_batch(self):
        print("Inserting a batch of {} rows".format(self.batch_counter))
        self.session.execute(self.batch)
        self.batch.clear()
        self.batch_counter = 0

    def handle_chat_message(
        self, broadcaster_id, month, timestamp, message_id, message
    ):
        self.prepare(broadcaster_id, month, timestamp, message_id, message)
