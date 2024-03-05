from typing import List, Tuple

import auth.secrets as secrets
import psycopg2


class DatabaseConnection:
    def __init__(self) -> None:
        self.session: psycopg2.connection = psycopg2.connect(secrets.get_neon_url())

    def __del__(self) -> None:
        self.session.close()

    def insert_streamers(self, streamer_ids: List[Tuple[int]]) -> None:
        print(f"Received {len(streamer_ids)} streamer Ids")
        with self.session.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO Streamer (streamer_id) VALUES (%s) ON CONFLICT DO NOTHING",
                streamer_ids,
            )
            self.session.commit()

    def get_streamers(self) -> List[Tuple[int]]:
        with self.session.cursor() as cursor:
            cursor.execute("SELECT streamer_id FROM Streamer")
            return cursor.fetchall()
