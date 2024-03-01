from concurrent import futures

import grpc
import gen.grpc.streamer_database.streamer_database_pb2 as streamer_database_pb2
import gen.grpc.streamer_database.streamer_database_pb2_grpc as streamer_database_pb2_grpc
import streamer_database_connection


class StreamerDatabaseServicer(streamer_database_pb2_grpc.StreamerDatabaseServicer):
    def __init__(self):
        self.database = streamer_database_connection.DatabaseConnection()

    def InsertStreamers(self, request, context):
        streamer_ids = [streamer.streamer_id for streamer in request.streamers]
        self.database.insert_streamers(streamer_ids)

        return streamer_database_pb2.InsertStreamersRequest()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    streamer_database_pb2_grpc.add_StreamerDatabaseServicer_to_server(
        StreamerDatabaseServicer(), server
    )
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
