import asyncio
import concurrent.futures

import astra_db
import postgres_db
import twitch
from apscheduler.schedulers.asyncio import AsyncIOScheduler


async def run():
    neon_session = postgres_db.NeonConnection()
    neon_session.initialize_bloom_filter()
    print("Total streamer count", len(neon_session.fetch_streamers()))

    astra_session = astra_db.DatabaseConnection("chat_data")

    async_scheduler = AsyncIOScheduler()
    async_scheduler.add_job(astra_session.insert_batch, "interval", seconds=10)
    async_scheduler.start()

    async with twitch.TwitchAPIConnection() as session:
        await session.authenticate()
        await session.join_chat()
        async_scheduler.add_job(session.get_all_streamers, "interval", hours=1)

    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, input, "Press enter to exit\n")


# lets run our setup
asyncio.run(run())
