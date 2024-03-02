from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class GetStreamersRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Streamer(_message.Message):
    __slots__ = ("streamer_id",)
    STREAMER_ID_FIELD_NUMBER: _ClassVar[int]
    streamer_id: int
    def __init__(self, streamer_id: _Optional[int] = ...) -> None: ...
