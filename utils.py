import base62
import uuid


def is_guid(string):
    try:
        uuid.UUID(string)
        return True
    except ValueError:
        return False


def base62_encode(string):
    return base62.encodebytes(string.encode())


def base62_decode(string):
    return base62.decodebytes(string).decode()
