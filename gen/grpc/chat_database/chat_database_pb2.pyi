from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Chat(_message.Message):
    __slots__ = ("broadcaster_id", "timestamp", "message_id", "message")
    BROADCASTER_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    broadcaster_id: int
    timestamp: int
    message_id: str
    message: str
    def __init__(self, broadcaster_id: _Optional[int] = ..., timestamp: _Optional[int] = ..., message_id: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class GetChatsRequest(_message.Message):
    __slots__ = ("broadcaster_id", "start", "end", "after_timestamp", "limit")
    BROADCASTER_ID_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    AFTER_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    broadcaster_id: int
    start: int
    end: int
    after_timestamp: int
    limit: int
    def __init__(self, broadcaster_id: _Optional[int] = ..., start: _Optional[int] = ..., end: _Optional[int] = ..., after_timestamp: _Optional[int] = ..., limit: _Optional[int] = ...) -> None: ...

class GetChatsResponse(_message.Message):
    __slots__ = ("chats",)
    CHATS_FIELD_NUMBER: _ClassVar[int]
    chats: _containers.RepeatedCompositeFieldContainer[Chat]
    def __init__(self, chats: _Optional[_Iterable[_Union[Chat, _Mapping]]] = ...) -> None: ...
