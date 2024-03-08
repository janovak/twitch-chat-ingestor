import logging

import auth.secrets as secrets
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy
from cassandra.query import BatchStatement, tuple_factory
from datetime_helpers import get_month, get_next_month


class DatabaseConnection:
    def __init__(self, keyspace):
        auth_provider = PlainTextAuthProvider(
            secrets.get_astra_client_id(), secrets.get_astra_secret()
        )
        cluster = Cluster(
            cloud=secrets.get_astra_cloud_config(), auth_provider=auth_provider
        )
        load_balancing_policy = TokenAwarePolicy(cluster.load_balancing_policy)
        cluster.set_load_balancing_policy(load_balancing_policy)
        self.session = cluster.connect(keyspace)

    def __del__(self):
        self.session.shutdown()

    def insert_chats(self, messages):
        logging.info(f"Inserting {len(messages)}")

        batch = BatchStatement(consistency_level="QUORUM", logged=False)

        statement = self.session.prepare(
            """
            INSERT INTO twitch_chat_by_broadcaster_and_timestamp (broadcaster_id, year_month, timestamp, message_id, message)
            VALUES (?, ?, ?, ?, ?)
            """
        )

        for m in messages:
            broadcaster_id, timestamp, message_id, message = m
            month = get_month(timestamp)
            batch.add(
                statement, (broadcaster_id, month, timestamp, message_id, message)
            )

        result = self.session.execute(batch)

        if result.errors:
            for error in result.errors:
                logging.error(
                    f"Error Code: {error.code}, Error Message: {error.message}"
                )
        else:
            logging.info("Messages inserted successfully!")

    def get_chats(self, broadcaster_id, start, end, limit):
        logging.info(
            f"Attempting to retieve chats using the parameters: broadcaster_id: {broadcaster_id}, start: {start}, end: {end}, limit: {limit}"
        )

        self.session.row_factory = tuple_factory

        statement = self.session.prepare(
            """
            SELECT broadcaster_id, timestamp, message_id, message FROM twitch_chat_by_broadcaster_and_timestamp
            WHERE broadcaster_id=? AND year_month=? AND timestamp>=? AND timestamp<=?
            LIMIT ?
            """,
        )

        month = get_month(start)
        end_month = get_month(end)
        list_of_rows = []
        # Because the start and end timestamps might span multiple partitions, we search for
        # messages on each partition where the messages in the specified time range might live.
        while len(list_of_rows) < limit and month <= end_month:
            result = self.session.execute(
                statement,
                (
                    broadcaster_id,
                    month,
                    start,
                    end,
                    limit - len(list_of_rows),
                ),
            )

            if result.errors:
                for error in result.errors:
                    logging.error(
                        f"Error Code: {error.code}, Error Message: {error.message}"
                    )
            else:
                logging.info(
                    f"Successfully retrieved {len(result)} rows from the {month} partition!"
                )

            # Include the result in the list to return
            list_of_rows.extend(list(result))
            # Get the next month in case we need to check the next partition for more messages
            month = get_next_month(month)

        logging.info(f"Returning {len(list_of_rows)} rows!")
        return list_of_rows
