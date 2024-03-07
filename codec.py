import base62


def base62_encode(string):
    return base62.encodebytes(string.encode())


def base62_decode(string):
    return base62.decodebytes(string).decode()
