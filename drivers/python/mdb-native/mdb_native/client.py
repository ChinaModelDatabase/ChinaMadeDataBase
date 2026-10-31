from typing import Any, Dict, List, Optional

from mdb_native.connection import MdbConnection
from mdb_native.database import MdbDatabase

class MdbClient:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 27017,
        timeout: float = 30.0,
        username: Optional[str] = None,
        password: Optional[str] = None,
        auth_source: str = "admin",
        auth_mechanism: str = "SCRAM-SHA-256",
    ) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.username = username
        self.password = password
        self.auth_source = auth_source
        self.auth_mechanism = auth_mechanism
        self._conn = MdbConnection(host, port, timeout)

    def connect(self) -> None:
        self._conn.connect()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "MdbClient":
        self.connect()
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def database(self, name: str) -> MdbDatabase:
        return MdbDatabase(self._conn, name)

    def list_databases(self) -> List[Dict[str, Any]]:
        resp = self._conn.send_command("listDatabases")
        return resp.get("databases", [])

    def server_status(self) -> Dict[str, Any]:
        resp = self._conn.send_command("serverStatus")
        return resp

    def ping(self) -> bool:
        resp = self._conn.send_command("ping")
        return resp.get("ok") == 1 or resp.get("ok") == 1.0

    def authenticate(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        source: Optional[str] = None,
        mechanism: Optional[str] = None,
    ) -> bool:
        username = username or self.username
        password = password or self.password
        source = source or self.auth_source
        mechanism = mechanism or self.auth_mechanism

        if not username or not password:
            raise ValueError("Username and password required")

        from mdb_native.auth import sasl_start, sasl_continue

        first_msg, conversation_id = sasl_start(
            self._conn, mechanism, username, source
        )
        done = False
        payload = first_msg

        while not done:
            done, payload, conv_id = sasl_continue(
                self._conn, conversation_id, payload
            )
            if conv_id:
                conversation_id = conv_id

        return True
