from concurrent import futures

import grpc
import rest_api_pb2
import rest_api_pb2_grpc
import chat_database_connection


class ChatDatabaseServicer(rest_api_pb2_grpc.ChatDatabaseServicer):
    def __init__(self):
        self.database = chat_database_connection.DatabaseConnection("chat_data")

    def InsertChats(self, request, context):
        chats = request.chats

        rows = tuple(
            (chat.broadcaster_id, chat.timestamp, chat.message_id, chat.message)
            for chat in chats
        )

        self.database.insert_chats(rows)

        return rest_api_pb2.InsertChatsRequest()

    def GetChats(self, request, context):
        list_of_chats = self.database.get_chats(
            request.broadcaster_id,
            request.start,
            request.end,
            request.after_timestamp,
            request.limit,
        )

        response = rest_api_pb2.GetChatsResponse()
        for broadcaster_id, year_month, timestamp, message_id, message in list_of_chats:
            response.chats.append(
                rest_api_pb2.Chat(
                    broadcaster_id=broadcaster_id,
                    year_month=year_month,
                    timestamp=timestamp,
                    message_id=str(message_id),
                    message=message,
                )
            )

        return response


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    rest_api_pb2_grpc.add_ChatDatabaseServicer_to_server(ChatDatabaseServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
