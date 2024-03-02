import asyncio
import concurrent.futures
import json

import auth.secrets as secrets
import pika
import redis
import twitch


class ChatRoomJoiner:
    def __init__(self):
        self.twitch = twitch.TwitchAPIConnection()
        twitch_authentication = asyncio.create_task(self.twitch.authenticate())

        self.cache = redis.Redis(
            host=secrets.get_redis_host_url(),
            port=secrets.get_redis_host_port(),
            password=secrets.get_redis_host_password(),
            db=0,
        )

        self.connection = pika.BlockingConnection(
            pika.URLParameters(secrets.get_cloudamqp_url())
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue="live_broadcasters_queue", durable=True)

        asyncio.gather(twitch_authentication)

    def __del__(self):
        self.connection.close()

    def start_consuming_streamers(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue="live_broadcasters_queue",
            on_message_callback=self.handle_live_streamers,
        )
        print(f"Start consuming streamers from queue")
        self.channel.start_consuming()

    def handle_live_streamers(self, ch, method, properties, body):
        streamers = json.loads(body.decode())

        print(f"Received {len(streamers)} live streamers")

        tasks = []
        for streamer in streamers:
            if not self.cache.sismember("live_broadcasters", streamer):
                tasks.append(asyncio.create_task(self.twitch.join_chat()))

        self.cache.sadd(*streamer)
        for streamer in streamers:
            self.cache.expire(streamer, 300)

        asyncio.gather(*tasks)

        ch.basic_ack(delivery_tag=method.delivery_tag)


async def main():
    joiner = ChatRoomJoiner()
    joiner.start_consuming_streamers()

    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, input, "Press enter to exit\n")


asyncio.get_event_loop().run_until_complete(main())
