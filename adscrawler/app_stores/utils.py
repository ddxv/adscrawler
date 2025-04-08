"""Shared utilities for app stores."""


def truncate_utf8_bytes(s, max_bytes=2700):
    encoded = s.encode("utf-8")
    if len(encoded) <= max_bytes:
        return s
    # Truncate bytes, then decode safely
    truncated = encoded[:max_bytes]
    while True:
        try:
            return truncated.decode("utf-8")
        except UnicodeDecodeError:
            truncated = truncated[:-1]
