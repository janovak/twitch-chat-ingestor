import asyncio
import concurrent.futures

import twitch
from apscheduler.schedulers.asyncio import AsyncIOScheduler


async def main():
    session = twitch.TwitchAPIConnection()
    await session.authenticate()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(session.get_all_streamers, "interval", hours=1)
    scheduler.start()

    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, input, "Press enter to exit\n")


asyncio.get_event_loop().run_until_complete(main())
