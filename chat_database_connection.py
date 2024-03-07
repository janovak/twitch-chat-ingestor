import auth.secrets as secrets
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import tuple_factory
from datetime_helpers import get_month, get_next_month


class DatabaseConnection:
    def __init__(self, keyspace):
        auth_provider = PlainTextAuthProvider(
            secrets.get_chat_db_client_id(), secrets.get_chat_db_secret()
        )
        cluster = Cluster(
            cloud=secrets.get_astra_db_cloud_config(), auth_provider=auth_provider
        )
        self.session = cluster.connect(keyspace)

    def __del__(self):
        self.session.shutdown()

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
                get_month(timestamp),
                timestamp,
                message_id,
                message,
            ),
        )

    def get_chats(self, broadcaster_id, start, end, limit):
        self.session.row_factory = tuple_factory

        statement = self.session.prepare(
            """
            SELECT broadcaster_id, timestamp, message_id, message FROM twitch_chat_by_broadcaster_and_timestamp
            WHERE broadcaster_id=? AND year_month=? AND timestamp>=? AND timestamp<=?
            LIMIT ?
            """,
        )

        month = get_month(start)
        list_of_rows = []
        while len(list_of_rows) < limit + 1:
            list_of_rows.extend(
                list(
                    self.session.execute(
                        statement,
                        (
                            broadcaster_id,
                            month,
                            start,
                            end,
                            limit + 1 - len(list_of_rows),
                        ),
                    )
                )
            )
            month = get_next_month(month)

        return list_of_rows
