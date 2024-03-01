from concurrent import futures

import grpc
import gen.grpc.chat_database.chat_database_pb2 as chat_database_pb2
import gen.grpc.chat_database.chat_database_pb2_grpc as chat_database_pb2_grpc
import chat_database_connection


class ChatDatabaseServicer(chat_database_pb2_grpc.ChatDatabaseServicer):
    def __init__(self):
        self.database = chat_database_connection.DatabaseConnection("chat_data")

    def InsertChats(self, request, context):
        chats = request.chats

        rows = tuple(
            (chat.broadcaster_id, chat.timestamp, chat.message_id, chat.message)
            for chat in chats
        )

        self.database.insert_chats(rows)

        return chat_database_pb2.InsertChatsRequest()

    def GetChats(self, request, context):
        list_of_chats = self.database.get_chats(
            request.broadcaster_id,
            request.start,
            request.end,
            request.after_timestamp,
            request.limit,
        )

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


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    chat_database_pb2_grpc.add_ChatDatabaseServicer_to_server(
        ChatDatabaseServicer(), server
    )
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
