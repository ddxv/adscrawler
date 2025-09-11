import base64
import hashlib

from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import PostgresCon
from adscrawler.dbcon.queries import query_sdk_keys

logger = get_logger(__name__)

try:
    C1 = CONFIG["applovin"]["C1"]
    C2 = CONFIG["applovin"]["C2"]
    CONST_A = base64.b64decode(CONFIG["applovin"]["CONST_A"])
    CONST_B = base64.b64decode(CONFIG["applovin"]["CONST_B"])
except Exception:
    logger.warning("No applovin config found")


def sha1_hex(b: bytes) -> str:
    return hashlib.sha1(b).hexdigest()


def base64_custom_decode(s: str) -> bytes:
    # Java used '+'->'-', '/'->'_', '='->'*' (and reverse on decode)
    std = s.replace("-", "+").replace("_", "/").replace("*", "=")
    return base64.b64decode(std)


def to_signed_64(n: int) -> int:
    """
    Emulates Java's 64-bit signed long from a Python integer.
    This is crucial for mimicking the exact behavior of the PRNG.
    """
    n &= 0xFFFFFFFFFFFFFFFF  # Mask to 64 bits
    if n & 0x8000000000000000:
        # If the sign bit is set, calculate the negative value.
        return n - 0x10000000000000000
    return n


def try_decompress(data: bytes):
    import gzip
    import zlib

    # Try gzip first (MAX uses gzip if length > threshold), then zlib variants, then none
    try:
        return gzip.decompress(data), "gzip"
    except Exception:
        pass
    for w in (zlib.MAX_WBITS, -zlib.MAX_WBITS, 31):
        try:
            return zlib.decompress(data, w), f"zlib({w})"
        except Exception:
            pass
    return data, "none"


def decode_from(blob: bytes, database_connection: PostgresCon) -> str | None:
    m = blob.split(b":")
    version = m[0]
    sha1_seen = m[1]
    sdk_postfix = m[2]
    payload = m[3]

    assert version in [b"1", b"2"], f"Invalid version: {version}"

    sdk_keys_df = query_sdk_keys(database_connection)

    keys = (
        sdk_keys_df[
            sdk_keys_df["applovin_sdk_key"].str.contains(sdk_postfix.decode("utf-8"))
        ]["applovin_sdk_key"]
        .unique()
        .tolist()
    )
    if len(keys) == 0:
        logger.error(f"No applovin sdk keys found for {sdk_postfix.decode('utf-8')}")
        return None
    if len(keys) > 1:
        logger.error(
            f"Multiple applovin sdk keys found for {sdk_postfix.decode('utf-8')}"
        )
        return None
    sdk_prefix32 = keys[0][:32]

    sha1_seen = sha1_seen.decode()

    if version == b"1":
        my_const = CONST_A
        assert sha1_hex(my_const) == sha1_hex(CONST_A), "Invalid sha1"
        found_text = decode_v1_from(payload, sdk_prefix32)
    elif version == b"2":
        my_const = CONST_B
        assert sha1_hex(my_const) == sha1_hex(CONST_B), "Invalid sha1"
        found_text = decode_v2_from(blob, sdk_prefix32)
    else:
        raise ValueError(f"Invalid version: {version}")

    if found_text:
        return found_text
    else:
        logger.error(f"Decode {version=} failed")
        return None


def decode_v1_from(user_input: bytes, sdk_prefix32: str) -> str | None:
    """
    Fixed V1 decoder based on careful analysis of the Java code
    """
    try:
        # Step 1: Decode custom base64
        raw_data = base64_custom_decode(user_input.decode("utf-8"))
    except Exception as e:
        return f"Error: Invalid Base64 string. {e}"
    if len(raw_data) <= 16:
        logger.debug("Error: Data is too short to contain an 8-byte seed.")
        return b""
    # Generate key from SDK prefix and CONST_A
    ckey = hashlib.sha256(CONST_A + sdk_prefix32.encode("utf-8")).digest()

    # The first 8 bytes are the encrypted seed
    encrypted_seed_bytes = bytearray(raw_data[:8])
    ciphertext = raw_data[8:]
    # Decrypt the seed by XORing with first 8 bytes of key
    for i in range(8):
        encrypted_seed_bytes[i] ^= ckey[i]
    # Convert to seed (little endian, unsigned)
    # seed = int.from_bytes(encrypted_seed_bytes, "little", signed=False)
    seed = int.from_bytes(encrypted_seed_bytes, "little")
    # Now decrypt the ciphertext
    decrypted_data = bytearray()

    # Process 8 bytes at a time (matching Java's 8-byte block processing)
    for block_start in range(0, len(ciphertext), 8):
        # Get current 8-byte block
        block_end = min(block_start + len(ciphertext), block_start + 8)

        # Generate PRNG value for this block position
        # The Java code uses block_start as the counter/offset
        counter = block_start
        x = seed + counter

        # First transformation: x = (x ^ (x >>> 33)) * C1
        x = to_signed_64((x ^ (x >> 33)) * C1)

        # Second transformation: x = (x ^ (x >>> 29)) * C2
        x = to_signed_64((x ^ (x >> 29)) * C2)

        # Final transformation: prng_val = x ^ (x >>> 32)
        prng_val = to_signed_64(x ^ (x >> 32))

        # Process each byte in this 8-byte block
        for byte_offset in range(8):
            abs_pos = block_start + byte_offset

            # Check if we have data at this position
            if abs_pos >= len(ciphertext):
                break

            # Get the cipher byte
            cipher_byte = ciphertext[abs_pos]

            # Get key byte (cycles through 32-byte key)
            key_byte = ckey[abs_pos % 32]

            # Get PRNG byte for this position within the block
            # Extract the appropriate byte from the 64-bit PRNG value
            shift_amount = byte_offset * 8
            prng_byte = (prng_val >> shift_amount) & 0xFF

            # Apply the triple XOR: cipher ^ key ^ prng
            decrypted_byte = cipher_byte ^ key_byte ^ prng_byte
            decrypted_data.append(decrypted_byte)
    # Try to decompress and decode
    plain, comp = try_decompress(decrypted_data)
    try:
        plain = plain.decode("utf-8")
    except Exception:
        logger.error("Decode V1 failed final decode to utf-8")
        return None
    return plain


def decode_v2_from(blob: bytes, sdk_prefix32: str) -> str | None:
    m = blob.split(b":", 3)
    payload = m[3]
    payload_start = len(b":".join(m[:3])) + 1  # +1 for the colon after sdk_prefix
    seed_enc_le = int.from_bytes(blob[payload_start + 8 : payload_start + 16], "little")
    payload = blob[payload_start + 16 :]
    digest = hashlib.sha256(CONST_B + sdk_prefix32.encode("utf-8")).digest()
    # Try several 64-bit derivations from the digest to XOR with seed_enc (robust across minor variants).
    candidates = []
    for off in (0, 8, 16, 24):
        # little and big endian interpretations
        candidates.append(int.from_bytes(digest[off : off + 8], "little"))
        candidates.append(int.from_bytes(digest[off : off + 8], "big"))
    # XOR of the four 8-byte chunks
    candidates.append(
        int.from_bytes(digest[0:8], "little")
        ^ int.from_bytes(digest[8:16], "little")
        ^ int.from_bytes(digest[16:24], "little")
        ^ int.from_bytes(digest[24:32], "little")
    )
    candidates.append(
        int.from_bytes(digest[0:8], "big")
        ^ int.from_bytes(digest[8:16], "big")
        ^ int.from_bytes(digest[16:24], "big")
        ^ int.from_bytes(digest[24:32], "big")
    )
    for kval in candidates:
        try:
            seed = seed_enc_le ^ kval
            dec = xor_permute(payload, seed, digest)
            plain, comp = try_decompress(dec)
            text = plain.decode("utf-8", errors="ignore").strip()
            # print(text[0:40])
            if text.startswith("{") or text.startswith("["):
                # print(text)
                return text
        except Exception:
            continue
    logger.error("Decode V2 failed")
    return None


def mix64(seed: int, b_index: int) -> int:
    """Reproduce the 64-bit mixing stream used in i4 (Little-Endian expansion per 8-byte stripe)."""
    MASK = (1 << 64) - 1
    cc1 = (1 << 64) + (C1)
    cc2 = (1 << 64) + (C2)
    z = (seed + b_index) & MASK
    x = (z ^ (z >> 33)) & MASK
    x = (x * cc1) & MASK
    x = (x ^ (x >> 29)) & MASK
    x = (x * cc2) & MASK
    x = (x ^ (x >> 32)) & MASK
    return x & MASK


def xor_permute(data: bytes, seed: int, key: bytes) -> bytes:
    """XOR payload with key[i%len] and bytes of mix64(seed, i) stream."""
    out = bytearray(data)
    cur = 0
    for i in range(len(out)):
        if (i % 8) == 0:
            cur = mix64(seed, i)
        ks_byte = (cur >> ((i % 8) * 8)) & 0xFF
        out[i] ^= key[i % len(key)] ^ ks_byte
    return bytes(out)
