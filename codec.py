base62_alphabet: str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def base62_encode(string: str) -> str:
    return encode(string, base62_alphabet)


def base62_decode(string: str) -> str:
    return decode(string, base62_alphabet)


def encode(string: str, alphabet: str) -> str:
    base = len(alphabet)

    # Convert each character to its ASCII value and then to base X representation
    encoded_string = ""
    for char in string:
        ascii_value = ord(char)
        new_base_digit = ""
        while ascii_value > 0:
            remainder = ascii_value % base
            new_base_digit = alphabet[remainder] + new_base_digit
            ascii_value //= base
        encoded_string += new_base_digit

    return encoded_string


def decode(encoded_string: str, alphabet: str) -> str:
    base = len(alphabet)

    # Convert each base X character to its corresponding ASCII value
    decoded_string = ""
    for char in encoded_string:
        value = alphabet.index(char)
        decoded_char = ""
        while value > 0:
            remainder = value % 62
            decoded_char = chr(remainder) + decoded_char
            value //= 62
        decoded_string += decoded_char

    return decoded_string
