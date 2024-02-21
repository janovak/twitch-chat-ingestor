import auth.secrets as secrets
import psycopg2
from pydispatch import dispatcher
from bloom_filter2 import BloomFilter

SIGNAL = "STREAMER_SIGNAL"


class NeonConnection:
    def __init__(self):
        self.session = psycopg2.connect(secrets.get_neon_url())
        self.bloom_filter = BloomFilter(max_elements=10000000, error_rate=0.001)
        dispatcher.connect(self.insert_streamers, signal=SIGNAL, sender=dispatcher.Any)

    def __del__(self):
        self.session.close()

    def insert_streamers(self, streamer_ids):
        # right now we aren't handling the false positives returned by the bloom filter. i.e.
        # an item isn't in the database, but the bloom filter says it is, so we don't add it to the database
        # change this to get potential false positives and then check those against a cache.
        # the set of new ids is then the set not in the bloom filter + set not in cache
        new_ids = [id for id in streamer_ids if id[0] not in self.bloom_filter]
        print("Inserting", len(new_ids), "rows")
        with self.session.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO Streamer (streamer_id) VALUES (%s) ON CONFLICT DO NOTHING",
                new_ids,
            )
            self.session.commit()

    def fetch_streamers(self):
        with self.session.cursor() as cursor:
            cursor.execute("SELECT * FROM Streamer")
            return cursor.fetchall()

    def initialize_bloom_filter(self):
        all_known_streamers = self.fetch_streamers()
        for id in all_known_streamers:
            self.bloom_filter.add(id[0])
