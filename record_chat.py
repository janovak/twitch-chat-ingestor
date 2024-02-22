import asyncio
import concurrent.futures

import astra_db
import postgres_db
import twitch
from apscheduler.schedulers.asyncio import AsyncIOScheduler


async def main():
    neon_session = postgres_db.NeonConnection()
    neon_session.initialize_bloom_filter()
    print("Total streamer count", len(neon_session.fetch_streamers()))

    # astra_session = astra_db.DatabaseConnection("chat_data")

    twitch_session = twitch.TwitchAPIConnection()
    await twitch_session.authenticate()
    await twitch_session.get_all_streamers()
    # await twitch_session.join_chat()

    async_scheduler = AsyncIOScheduler()
    # async_scheduler.add_job(astra_session.insert_batch, "interval", seconds=10)
    async_scheduler.add_job(twitch_session.get_all_streamers, "interval", minutes=10)
    async_scheduler.start()

    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, input, "Press enter to exit\n")


asyncio.get_event_loop().run_until_complete(main())
