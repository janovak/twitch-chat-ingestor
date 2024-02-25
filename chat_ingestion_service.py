import asyncio
import concurrent.futures
import twitch


async def main():
    session = twitch.TwitchAPIConnection()
    await session.authenticate()
    await session.join_chat()

    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, input, "Press enter to exit\n")


asyncio.get_event_loop().run_until_complete(main())
