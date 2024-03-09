import logging

import auth.secrets as secrets
import psycopg2


class DatabaseConnection:
    def __init__(self):
        self.session = psycopg2.connect(secrets.get_neon_url())

    def __del__(self):
        self.close()

    def close(self):
        self.session.close()

    def insert_streamers(self, streamer_ids):
        logging.info(f"Inserting {len(streamer_ids)} streamer Ids")

        with self.session.cursor() as cursor:
            try:
                cursor.executemany(
                    "INSERT INTO Streamer (streamer_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    streamer_ids,
                )
                self.session.commit()
            except psycopg2.Error as e:
                logging.error(f"Error inserting streamers: {e}")

    def get_streamers(self):
        with self.session.cursor() as cursor:
            try:
                cursor.execute("SELECT streamer_id FROM Streamer")
                rows = cursor.fetchall()
            except psycopg2.Error as e:
                logging.error(f"Error executing query: {e}")
                return []

            logging.info(f"Retrieved {len(rows)} streamer Ids")

            return rows
