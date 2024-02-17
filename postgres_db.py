import auth.secrets as secrets
import psycopg2
from pydispatch import dispatcher

SIGNAL = "STREAMER_SIGNAL"


class NeonConnection:
    def __init__(self):
        self.session = psycopg2.connect(secrets.get_neon_url())
        dispatcher.connect(self.insert_streamers, signal=SIGNAL, sender=dispatcher.Any)

    def __del__(self):
        self.session.close()

    def insert_streamers(self, streamer_ids):
        print("Inserting ", len(streamer_ids), " rows")
        with self.session.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO streamer_ids (id) VALUES (%s) ON CONFLICT DO NOTHING",
                streamer_ids,
            )
            self.session.commit()

    def fetch_streamers(self):
        with self.session.cursor() as cursor:
            cursor.execute("SELECT * FROM streamer_ids")
            return cursor.fetchall()
