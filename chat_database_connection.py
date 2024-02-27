import time
import uuid

import auth.secrets as secrets
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import tuple_factory


class DatabaseConnection:
    def __init__(self, keyspace):
        self.session = self.get_session(keyspace)

    def __del__(self):
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

    def get_month(self, timestamp):
        return time.strftime("%Y%m", time.gmtime(timestamp))

    def insert_chats(self, broadcaster_id, timestamp, message_id, message):
        statement = self.session.prepare(
            """
            INSERT INTO twitch_chat_by_broadcaster_and_timestamp (broadcaster_id, year_month, timestamp, message_id, message)
            VALUES (?, ?, ?, ?, ?)
            """
        )

        self.session.execute(
            statement,
            (
                broadcaster_id,
                int(self.get_month(timestamp)),
                timestamp,
                message_id,  # uuid.UUID(message_id),
                message,
            ),
        )

    def get_chats(self, broadcaster_id, start, end, after_timestamp, limit):
        self.session.row_factory = tuple_factory

        year_month = int(self.get_month(start / 1000))

        start = max(start, after_timestamp)

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
                start,
                end,
                limit + 1,
            ),
        )

        return list(rows)
