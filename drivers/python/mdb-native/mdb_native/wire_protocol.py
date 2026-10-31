import struct
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from mdb_native.bson import BsonEncoder, BsonDecoder

OP_MSG = 2013

MSG_HEADER_SIZE = 16
SECTION_KIND_BODY = 0
SECTION_KIND_DOCUMENT_SEQUENCE = 1

@dataclass
class OpMsg:
    body: Dict[str, Any] = field(default_factory=dict)
    documents: List[Dict[str, Any]] = field(default_factory=list)
    request_id: int = 0
    response_to: int = 0
    more_to_come: bool = False

class WireProtocol:
    @staticmethod
    def build_request(
        req_id: int,
        command: str,
        docs: Optional[List[Dict[str, Any]]] = None,
        seq_name: Optional[str] = None,
        request_id: Optional[int] = None,
        flags: int = 0,
    ) -> bytes:
        body_doc: Dict[str, Any] = {command: 1}
        if docs is None:
            docs = []
        if docs and seq_name is None:
            seq_name = command

        checksum_present = (flags & 1) != 0
        if checksum_present:
            flags &= ~1

        body_bytes = BsonEncoder.encode(body_doc)
        sections = bytearray()
        sections.append(SECTION_KIND_BODY)
        sections.extend(body_bytes)

        if docs and seq_name:
            seq_header = bytearray()
            seq_header.extend(struct.pack("<i", 0))
            seq_header.extend(seq_name.encode("utf-8"))
            seq_header.append(0x00)
            for doc in docs:
                seq_header.extend(BsonEncoder.encode(doc))
            struct.pack_into("<i", seq_header, 0, len(seq_header))
            sections.append(SECTION_KIND_DOCUMENT_SEQUENCE)
            sections.extend(seq_header)

        flag_bytes = struct.pack("<I", flags)
        payload = flag_bytes + bytes(sections)
        msg_len = MSG_HEADER_SIZE + len(payload)

        header = struct.pack("<iiii", msg_len, req_id, 0, OP_MSG)
        msg = header + payload

        if checksum_present:
            from mdb_native.crc32c import crc32c
            checksum = crc32c(msg)
            msg += struct.pack("<I", checksum)

        return msg

    @staticmethod
    def parse_response(data: bytes) -> OpMsg:
        if len(data) < MSG_HEADER_SIZE:
            raise ValueError("Response too short for header")

        msg_len, req_id, resp_to, op_code = struct.unpack_from("<iiii", data, 0)

        if op_code != OP_MSG:
            raise ValueError(f"Unexpected opCode: {op_code}")

        flags = struct.unpack_from("<I", data, 16)[0]
        more_to_come = (flags & 2) != 0

        offset = MSG_HEADER_SIZE + 4
        body: Dict[str, Any] = {}
        documents: List[Dict[str, Any]] = []

        while offset < msg_len:
            section_kind = data[offset]
            offset += 1

            if section_kind == SECTION_KIND_BODY:
                doc_len = struct.unpack_from("<i", data, offset)[0]
                body = BsonDecoder.decode(data[offset:offset + doc_len])
                offset += doc_len

            elif section_kind == SECTION_KIND_DOCUMENT_SEQUENCE:
                seq_size = struct.unpack_from("<i", data, offset)[0]
                seq_end = offset + seq_size
                offset += 4
                while offset < seq_end and data[offset] != 0x00:
                    offset += 1
                offset += 1
                while offset < seq_end - 1:
                    doc_len = struct.unpack_from("<i", data, offset)[0]
                    doc = BsonDecoder.decode(data[offset:offset + doc_len])
                    documents.append(doc)
                    offset += doc_len
                offset = seq_end
            else:
                raise ValueError(f"Unknown section kind: {section_kind}")

        return OpMsg(
            body=body,
            documents=documents,
            request_id=req_id,
            response_to=resp_to,
            more_to_come=more_to_come,
        )
