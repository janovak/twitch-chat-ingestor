import asyncio
import concurrent.futures
import logging

import twitch
from apscheduler.schedulers.asyncio import AsyncIOScheduler


async def main():
    logging.basicConfig(
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    session = twitch.TwitchAPIConnection()
    await session.authenticate()

    # Twitch caches are 1 to 3 minutes stale, so it doesn't make sense to poll any more frequently than that
    scheduler = AsyncIOScheduler()
    scheduler.add_job(session.get_top_streamers, "interval", minutes=5, args=(1,))
    scheduler.start()

    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, input, "Press enter to exit\n")


asyncio.get_event_loop().run_until_complete(main())
