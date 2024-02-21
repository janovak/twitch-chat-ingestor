import time

import auth.secrets as secrets
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType, tuple_factory
from pydispatch import dispatcher

SIGNAL = "CHAT_SIGNAL"


class DatabaseConnection:
    def __init__(self, keyspace):
        self.session = self.get_session(keyspace)
        # not thread safe at the moment
        self.batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        self.batch_counter = 0
        dispatcher.connect(
            self.handle_chat_message, signal=SIGNAL, sender=dispatcher.Any
        )

    def __del__(self):
        self.session.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, *members):
        self.session.shutdown()

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

    def get_month(self, timestamp):
        return time.strftime("%Y%m", time.gmtime(timestamp))

    def handle_chat_message(self, broadcaster_id, timestamp, message_id, message):
        self.prepare(
            broadcaster_id,
            int(self.get_month(timestamp)),
            timestamp,
            message_id,
            message,
        )

    def get_chats(self, broadcaster_id, start, end, after_timestamp, limit):
        self.session.row_factory = tuple_factory

        start_epoch = start.timestamp()
        year_month = int(self.get_month(start_epoch))

        start_epoch_milliseconds = int(start_epoch * 1000)
        end_epoch_milliseconds = int(end.timestamp() * 1000)

        start_epoch = max(start_epoch_milliseconds, after_timestamp)

        statement = self.session.prepare(
            """
            SELECT broadcaster_id, year_month, timestamp, message_id, message FROM twitch_chat_by_broadcaster_and_timestamp
            WHERE broadcaster_id=? AND year_month=? AND timestamp>=? AND timestamp<=?
            LIMIT ?
            """,
        )
        # TODO: need to handle timestamps that span multiple months
        rows = self.session.execute(
            statement,
            (
                broadcaster_id,
                year_month,
                start_epoch_milliseconds,
                end_epoch_milliseconds,
                limit + 1,
            ),
        )
        return list(rows)
