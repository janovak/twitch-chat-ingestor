import asyncio
import sys
import twitch
import astra_db


from pydispatch import dispatcher




TARGET_CHANNEL = sys.argv[1]

# connect to chat database
database_session = astra_db.DatabaseConnection('chat_data')

async def run():
    await twitch.TwitchAPIConnection().init_connection()
    await twitch.TwitchAPIConnection().join_chat()
    
# lets run our setup
asyncio.run(run())