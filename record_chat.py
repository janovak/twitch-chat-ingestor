import asyncio

import astra_db
import postgres_db
import twitch
from apscheduler.schedulers.background import BackgroundScheduler


async def run():
    scheduler = BackgroundScheduler()

    neon_session = postgres_db.NeonConnection()
    print("Total streamer count ", len(neon_session.fetch_streamers()))

    astra_session = astra_db.DatabaseConnection("chat_data")
    scheduler.add_job(astra_session.insert_batch, "interval", seconds=10)
    scheduler.start()

    async with twitch.TwitchAPIConnection() as session:
        await session.authenticate()
        await session.join_chat()
        await session.get_all_streamers()

    try:
        input("Press ENTER to stop\n")
    finally:
        scheduler.shutdown()


# lets run our setup
asyncio.run(run())
