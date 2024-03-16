import logging

import auth.secrets as secrets
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import BatchStatement, BatchType, ConsistencyLevel, tuple_factory
from datetime_helpers import get_month, get_next_month


class DatabaseConnection:
    def __init__(self, keyspace):
        auth_provider = PlainTextAuthProvider(
            secrets.get_astra_client_id(), secrets.get_astra_secret()
        )
        load_balancing_policy = TokenAwarePolicy(DCAwareRoundRobinPolicy())
        cluster = Cluster(
            cloud=secrets.get_astra_cloud_config(),
            auth_provider=auth_provider,
            load_balancing_policy=load_balancing_policy,
        )
        self.session = cluster.connect(keyspace)

    def __del__(self):
        self.close()

    def close(self):
        self.session.shutdown()

    def insert_chats(self, messages):
        logging.info(f"Inserting {len(messages)} message")

        batch = BatchStatement(
            consistency_level=ConsistencyLevel.QUORUM, batch_type=BatchType.UNLOGGED
        )

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

        try:
            self.session.execute(batch)
            logging.info("Messages inserted successfully")
            return True
        except Exception as e:
            logging.error(f"Exception: {e}")
            return False

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
            try:
                rows = list(
                    self.session.execute(
                        statement,
                        (
                            broadcaster_id,
                            month,
                            start,
                            end,
                            limit - len(list_of_rows),
                        ),
                    )
                )
                logging.info(
                    f"Successfully retrieved {len(rows)} rows from the {month} partition"
                )
            except Exception as e:
                logging.error(f"Exception: {e}")
                return False, []

            # Include the result in the list to return
            list_of_rows.extend(rows)
            # Get the next month in case we need to check the next partition for more messages
            month = get_next_month(month)

        logging.info(f"Returning {len(list_of_rows)} rows")
        return True, list_of_rows

    def insert_clip(self, clip_id, timestamp):
        logging.info(f"Inserting {clip_id}")

        statement = self.session.prepare(
            """
            INSERT INTO clips_by_timestamp (partition_key, timestamp, clip_id)
            VALUES (?, ?, ?)
            """
        )

        try:
            self.session.execute(
                statement,
                (
                    1,
                    timestamp,
                    clip_id,
                ),
            )
            logging.info("Clip inserted successfully")
            return True
        except Exception as e:
            logging.error(f"Exception: {e}")
            return False

    def get_clips(self, start, end):
        logging.info(f"Attempting to retrieve all clips {start} and {end}")

        self.session.row_factory = tuple_factory

        statement = self.session.prepare(
            """
            SELECT clip_id FROM clips_by_timestamp
            WHERE partition_key=1 AND timestamp>=? AND timestamp<=?
            """,
        )

        try:
            rows = list(
                self.session.execute(
                    statement,
                    (end, start),
                )
            )
            logging.info(f"Successfully retrieved {len(rows)} rows")
            return True, rows
        except Exception as e:
            logging.error(f"Exception: {e}")
            return False, []
