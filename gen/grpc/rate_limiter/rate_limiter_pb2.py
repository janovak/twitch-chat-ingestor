# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: gen/grpc/rate_limiter/rate_limiter.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n(gen/grpc/rate_limiter/rate_limiter.proto\x12\x0bratelimiter\"4\n\x13\x43onsumeTokenRequest\x12\n\n\x02id\x18\x01 \x01(\r\x12\x11\n\ttimestamp\x18\x02 \x01(\x04\"\'\n\x14\x43onsumeTokenResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\x32\x64\n\x0bRateLimiter\x12U\n\x0c\x43onsumeToken\x12 .ratelimiter.ConsumeTokenRequest\x1a!.ratelimiter.ConsumeTokenResponse\"\x00\x42\x39\n\x1etwitchchatingestor.ratelimiterB\x10RateLimiterProtoP\x01\xa2\x02\x02rlb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'gen.grpc.rate_limiter.rate_limiter_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  _globals['DESCRIPTOR']._options = None
  _globals['DESCRIPTOR']._serialized_options = b'\n\036twitchchatingestor.ratelimiterB\020RateLimiterProtoP\001\242\002\002rl'
  _globals['_CONSUMETOKENREQUEST']._serialized_start=57
  _globals['_CONSUMETOKENREQUEST']._serialized_end=109
  _globals['_CONSUMETOKENRESPONSE']._serialized_start=111
  _globals['_CONSUMETOKENRESPONSE']._serialized_end=150
  _globals['_RATELIMITER']._serialized_start=152
  _globals['_RATELIMITER']._serialized_end=252
# @@protoc_insertion_point(module_scope)
