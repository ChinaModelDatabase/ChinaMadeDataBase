import socket
import struct
import threading
from typing import Any, Dict, List, Optional

from mdb_native.wire_protocol import WireProtocol, MSG_HEADER_SIZE, OpMsg

class MdbConnection:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 27017,
        timeout: float = 30.0,
    ) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self._sock: Optional[socket.socket] = None
        self._lock = threading.Lock()
        self._req_counter = 0
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected

    def _next_req_id(self) -> int:
        self._req_counter += 1
        return self._req_counter

    def connect(self) -> None:
        if self._connected:
            return
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(self.timeout)
        self._sock.connect((self.host, self.port))
        self._connected = True

    def close(self) -> None:
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None
            self._connected = False

    def _recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("Connection closed by remote")
            buf.extend(chunk)
        return bytes(buf)

    def _send_raw(self, data: bytes) -> None:
        self._sock.sendall(data)

    def _recv_response(self) -> OpMsg:
        header = self._recv_exact(MSG_HEADER_SIZE)
        msg_len = struct.unpack_from("<i", header, 0)[0]
        remaining = msg_len - MSG_HEADER_SIZE
        payload = self._recv_exact(remaining) if remaining > 0 else b""
        return WireProtocol.parse_response(header + payload)

    def send_command(
        self,
        cmd: str,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        with self._lock:
            req_id = self._next_req_id()
            body = {cmd: 1}
            body.update(kwargs)
            body_bytes = _encode_full_doc(body)
            msg = _build_simple_msg(req_id, body_bytes)
            self._send_raw(msg)
            resp = self._recv_response()
        _check_error(resp)
        return resp.body

    def send_command_with_docs(
        self,
        cmd: str,
        docs: List[Dict[str, Any]],
        seq_name: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        with self._lock:
            req_id = self._next_req_id()
            if seq_name is None:
                seq_name = cmd
            msg = WireProtocol.build_request(
                req_id=req_id,
                command=cmd,
                docs=docs,
                seq_name=seq_name,
            )
            self._send_raw(msg)
            resp = self._recv_response()
        _check_error(resp)
        return resp.body

def _encode_full_doc(doc: Dict[str, Any]) -> bytes:
    from mdb_native.bson import BsonEncoder
    return BsonEncoder.encode(doc)

def _build_simple_msg(req_id: int, body_bytes: bytes) -> bytes:
    from mdb_native.wire_protocol import OP_MSG
    flags = 0
    payload = struct.pack("<I", flags)
    payload += bytes([0])
    payload += body_bytes
    msg_len = MSG_HEADER_SIZE + len(payload)
    header = struct.pack("<iiii", msg_len, req_id, 0, OP_MSG)
    return header + payload

def _check_error(resp: OpMsg) -> None:
    if "ok" in resp.body and resp.body["ok"] != 1 and resp.body["ok"] != 1.0:
        errmsg = resp.body.get("errmsg", "Unknown error")
        code = resp.body.get("code", 0)
        raise RuntimeError(f"MongoDB error {code}: {errmsg}")
