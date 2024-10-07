import logging
import threading
import os
from concurrent import futures

import gen.grpc.rate_limiter.rate_limiter_pb2 as rate_limiter_pb2
import gen.grpc.rate_limiter.rate_limiter_pb2_grpc as rate_limiter_pb2_grpc
import grpc


class Window:
    def __init__(self, timestamp=0, count=0):
        self.timestamp = timestamp
        self.count = count


class RateLimiterServicer(rate_limiter_pb2_grpc.RateLimiterServicer):
    def __init__(self, limit):
        self.counts_by_user_id = {}
        self.limit = limit
        # Only using a single lock at the moment since all requests are shared between user_ids
        self.lock = threading.Lock()

    def ConsumeToken(self, request, context):
        with self.lock:
            window = self.counts_by_user_id.get(request.id, Window())
            success = True

            # Start a new window if the previous timestamp is older than 30 seconds
            if request.timestamp - window.timestamp > 30:
                self.counts_by_user_id[request.id] = Window(request.timestamp, 1)
            # Increment counter since we're within tolerance
            elif window.count < self.limit:
                self.counts_by_user_id[request.id] = Window(
                    window.timestamp, window.count + 1
                )
            # Exceeded tolerance
            else:
                success = False

        return rate_limiter_pb2.ConsumeTokenResponse(success=success)


def serve():
    log_path = os.path.expandvars("$HOME/logs/rate_limiter_service.py")
    logging.basicConfig(
        filename=log_path,
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    rate_limiter_pb2_grpc.add_RateLimiterServicer_to_server(
        RateLimiterServicer(limit=20), server
    )
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
