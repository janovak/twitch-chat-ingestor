import aio_pika
import os
import signal
import asyncio
import concurrent.futures
import logging
import psutil
from apscheduler.schedulers.asyncio import AsyncIOScheduler


class ProcessManager:
    def __init__(self):
        self.pid = 0
        self.stdout = None
        self.stderr = None

    async def read_stream(self, stream):
        while True:
            line = await stream.readline()
            if not line:
                break
            logging.critical(f"Output: {line.decode().strip()}")

    async def start_process(self):
        process = await asyncio.create_subprocess_exec(
            "python3",
            "chat_listener_service.py",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Start reading from stdout and stderr asynchronously
        stdout_reader = self.read_stream(process.stdout)
        stderr_reader = self.read_stream(process.stderr)

        self.stdout = asyncio.create_task(stdout_reader)
        self.stderr = asyncio.create_task(stderr_reader)

        self.pid = process.pid

    async def kill_process(self):
        process = psutil.Process(self.pid)
        process.terminate()  # Example: terminate the subprocess

        try:
            await asyncio.gather(self.stdout, self.stderr)
        except asyncio.CancelledError:
            logging.critical("Task was cancelled")
        except Exception as e:
            logging.critical(f"{e}")

    async def monitor_process(self):
        connection = await aio_pika.connect_robust(
            "amqp://guest:guest@localhost:5672/", heartbeat=60
        )
        channel = await connection.channel()

        broadcaster_queue = await channel.declare_queue(
            "join_broadcaster_chat_queue", durable=True
        )

        scheduler = AsyncIOScheduler()
        scheduler.add_job(
            self.check_and_handle_queue_depth,
            "interval",
            minutes=2,
            args=(broadcaster_queue,),
        )
        scheduler.start()

    async def check_and_handle_queue_depth(self, queue_name):
        await queue_name.declare()
        if queue_name.declaration_result.message_count > 40:
            logging.critical(
                f"Killing {self.pid} because {queue_name.declaration_result.message_count} in queue"
            )
            await queue_name.purge()
            await self.kill_process()
            await self.start_process()


async def main():
    logging.basicConfig(
        filemode="w",
        level=logging.CRITICAL,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    manager = ProcessManager()
    await manager.start_process()
    await manager.monitor_process()

    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, input, "Press enter to exit\n")


asyncio.get_event_loop().run_until_complete(main())
