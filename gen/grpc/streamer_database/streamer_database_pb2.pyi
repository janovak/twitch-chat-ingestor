from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Streamer(_message.Message):
    __slots__ = ("streamer_id",)
    STREAMER_ID_FIELD_NUMBER: _ClassVar[int]
    streamer_id: int
    def __init__(self, streamer_id: _Optional[int] = ...) -> None: ...

class InsertStreamersRequest(_message.Message):
    __slots__ = ("streamers",)
    STREAMERS_FIELD_NUMBER: _ClassVar[int]
    streamers: _containers.RepeatedCompositeFieldContainer[Streamer]
    def __init__(self, streamers: _Optional[_Iterable[_Union[Streamer, _Mapping]]] = ...) -> None: ...

class InsertStreamersResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...
