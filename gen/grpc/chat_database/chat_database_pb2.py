# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: gen/grpc/chat_database/chat_database.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n*gen/grpc/chat_database/chat_database.proto\x12\x0c\x63hatdatabase\"V\n\x04\x43hat\x12\x16\n\x0e\x62roadcaster_id\x18\x01 \x01(\r\x12\x11\n\ttimestamp\x18\x02 \x01(\x04\x12\x12\n\nmessage_id\x18\x03 \x01(\t\x12\x0f\n\x07message\x18\x04 \x01(\t\"T\n\x0fGetChatsRequest\x12\x16\n\x0e\x62roadcaster_id\x18\x01 \x01(\r\x12\r\n\x05start\x18\x02 \x01(\x04\x12\x0b\n\x03\x65nd\x18\x03 \x01(\x04\x12\r\n\x05limit\x18\x04 \x01(\r\"5\n\x10GetChatsResponse\x12!\n\x05\x63hats\x18\x01 \x03(\x0b\x32\x12.chatdatabase.Chat2[\n\x0c\x43hatDatabase\x12K\n\x08GetChats\x12\x1d.chatdatabase.GetChatsRequest\x1a\x1e.chatdatabase.GetChatsResponse\"\x00\x42<\n\x1ftwitchapiextended.chat.databaseB\x11\x43hatDatabaseProtoP\x01\xa2\x02\x03\x63\x64\x62\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'gen.grpc.chat_database.chat_database_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  _globals['DESCRIPTOR']._options = None
  _globals['DESCRIPTOR']._serialized_options = b'\n\037twitchapiextended.chat.databaseB\021ChatDatabaseProtoP\001\242\002\003cdb'
  _globals['_CHAT']._serialized_start=60
  _globals['_CHAT']._serialized_end=146
  _globals['_GETCHATSREQUEST']._serialized_start=148
  _globals['_GETCHATSREQUEST']._serialized_end=232
  _globals['_GETCHATSRESPONSE']._serialized_start=234
  _globals['_GETCHATSRESPONSE']._serialized_end=287
  _globals['_CHATDATABASE']._serialized_start=289
  _globals['_CHATDATABASE']._serialized_end=380
# @@protoc_insertion_point(module_scope)
