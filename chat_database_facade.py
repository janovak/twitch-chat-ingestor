import logging
from concurrent import futures

import chat_database_connection
import gen.grpc.chat_database.chat_database_pb2 as chat_database_pb2
import gen.grpc.chat_database.chat_database_pb2_grpc as chat_database_pb2_grpc
import grpc


class ChatDatabaseServicer(chat_database_pb2_grpc.ChatDatabaseServicer):
    def __init__(self):
        self.database = chat_database_connection.DatabaseConnection("chat_data")

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        self.database.close()

    def GetChats(self, request, context):
        logging.info(
            f"GetChats called with: broadcaster_id: {request.broadcaster_id}, start: {request.start}, end: {request.end}, limit: {request.limit}"
        )

        success, list_of_chats = self.database.get_chats(
            request.broadcaster_id,
            request.start,
            request.end,
            request.limit,
        )

        if success:
            logging.info(f"{len(list_of_chats)} messages returned by the database")
        else:
            logging.error(f"There was an error querying the database")

        # Repackage the chats from the database response and return the bundle back to the caller
        response = chat_database_pb2.GetChatsResponse()
        for broadcaster_id, timestamp, message_id, message in list_of_chats:
            response.chats.append(
                chat_database_pb2.Chat(
                    broadcaster_id=broadcaster_id,
                    timestamp=timestamp,
                    message_id=str(message_id),
                    message=message,
                )
            )

        return response

    def GetClips(self, request, context):
        logging.info(
            f"GetClips called with: start: {request.start}, end: {request.end}"
        )

        success, list_of_clips = self.database.get_clips(
            request.start,
            request.end,
        )

        if success:
            logging.info(f"{len(list_of_clips)} clips returned by the database")
        else:
            logging.error(f"There was an error querying the database")

        # Repackage the clips from the database response and return the bundle back to the caller
        response = chat_database_pb2.GetClipsResponse()
        for clip_id, embed_url, thumbnail_url in list_of_clips:
            response.clips.append(
                chat_database_pb2.Clip(
                    clip_id=clip_id, embed_url=embed_url, thumbnail_url=thumbnail_url
                )
            )

        return response


def serve():
    logging.basicConfig(
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    chat_database_pb2_grpc.add_ChatDatabaseServicer_to_server(
        ChatDatabaseServicer(), server
    )
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
