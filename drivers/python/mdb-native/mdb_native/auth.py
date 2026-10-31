import hashlib
import hmac
import os
import base64
import re
from typing import Tuple, Optional

from mdb_native.connection import MdbConnection


def _parse_scram_attributes(data: str) -> dict:
    attrs = {}
    for part in data.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            attrs[k] = v
    return attrs


def sasl_start(
    conn: MdbConnection,
    mechanism: str,
    username: str,
    source: str,
) -> Tuple[bytes, Optional[int]]:
    nonce = base64.b64encode(os.urandom(24)).decode("ascii")
    first_bare = f"n={username},r={nonce}"
    first_message = f"n,,{first_bare}"
    first_msg_bytes = first_message.encode("utf-8")

    resp = conn.send_command(
        "saslStart",
        mechanism=mechanism,
        payload=first_msg_bytes,
    )
    conversation_id = resp.get("conversationId")
    server_payload = resp.get("payload", b"")
    if isinstance(server_payload, str):
        server_payload = server_payload.encode("utf-8")

    return server_payload, conversation_id


def sasl_continue(
    conn: MdbConnection,
    conversation_id: Optional[int],
    server_payload: bytes,
) -> Tuple[bool, bytes, Optional[int]]:
    resp = conn.send_command(
        "saslContinue",
        conversationId=conversation_id,
        payload=server_payload if isinstance(server_payload, bytes) else server_payload.encode("utf-8"),
    )
    done = resp.get("done", False)
    payload = resp.get("payload", b"")
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return done, payload, resp.get("conversationId")
