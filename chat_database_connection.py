import datetime

import auth.secrets as secrets
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import tuple_factory


def get_month(timestamp):
    return int(datetime.utcfromtimestamp(timestamp).strftime("%Y%m"))


def get_next_month(year_month):
    year = year_month // 100
    month = year_month % 100

    # Create a datetime object for the given year and month
    current_date = datetime(year, month, 1)

    # Calculate the start of the next month
    if month == 12:
        next_month = current_date.replace(year=year + 1, month=1)
    else:
        next_month = current_date.replace(month=month + 1)

    return int(next_month.strftime("%Y%m"))


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
                message_id,  # uuid.UUID(message_id),
                message,
            ),
        )

    def get_chats(self, broadcaster_id, start, end, after_timestamp, limit):
        self.session.row_factory = tuple_factory

        year_month = get_month(start / 1000)
        start = max(start, after_timestamp)

        statement = self.session.prepare(
            """
            SELECT broadcaster_id, year_month, timestamp, message_id, message FROM twitch_chat_by_broadcaster_and_timestamp
            WHERE broadcaster_id=? AND year_month=? AND timestamp>=? AND timestamp<=?
            LIMIT ?
            """,
        )

        list_of_rows = []
        while len(list_of_rows) < limit + 1:
            list_of_rows.append(
                list(
                    self.session.execute(
                        statement,
                        (
                            broadcaster_id,
                            year_month,
                            start,
                            end,
                            limit + 1 - len(list_of_rows),
                        ),
                    )
                )
            )
            year_month = get_next_month(year_month)

        return list_of_rows
