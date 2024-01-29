import asyncio

import astra_db
import twitch
from apscheduler.schedulers.background import BackgroundScheduler

# connect to chat database
database_session = astra_db.DatabaseConnection("chat_data")


async def run():
    await twitch.TwitchAPIConnection().init_connection()
    await twitch.TwitchAPIConnection().join_chat()

    scheduler = BackgroundScheduler()
    scheduler.add_job(database_session.insert_batch, "interval", seconds=10)
    scheduler.start()

    try:
        input("Press ENTER to stop\n")
    finally:
        scheduler.shutdown()


# lets run our setup
asyncio.run(run())
