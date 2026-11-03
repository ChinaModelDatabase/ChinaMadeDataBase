import { crc32c } from './crc32c';
import { BsonEncoder, BsonDecoder, BsonDocument } from './bson';

// ─── Constants ─────────────────────────────────────────────────────────────────

export const HEADER_SIZE = 16;
export const OP_MSG = 2013;

// Section kinds
export const SECTION_BODY = 0;
export const SECTION_DOCUMENT_SEQUENCE = 1;

export const CHECKSUM_PRESENT = 1 << 0;
export const MORE_TO_COME = 1 << 1;

// 兼容别名：驱动对外文档与测试用的是 `FLAG_*` 前缀。
export const FLAG_CHECKSUM = CHECKSUM_PRESENT;
export const FLAG_MORE_TO_COME = MORE_TO_COME;

// ─── Flags (bitmask) ──────────────────────────────────────────────────────────

export interface OpMsgFlags {
  checksumPresent: boolean;
  moreToCome: boolean;
  exhaustAllowed: boolean;
}

// ─── Section ───────────────────────────────────────────────────────────────────

export interface BodySection {
  kind: 0;
  document: BsonDocument;
}

export interface DocSequenceSection {
  kind: 1;
  identifier: string;
  documents: BsonDocument[];
}

export type Section = BodySection | DocSequenceSection;

// ─── Full message ──────────────────────────────────────────────────────────────

export interface OpMsgMessage {
  requestID: number;
  responseTo: number;
  flags: OpMsgFlags;
  sections: Section[];
  checksum?: number;
}

// ─── Builder ───────────────────────────────────────────────────────────────────

let nextRequestId = 1;

export function nextReqId(): number {
  return nextRequestId++;
}

const encoder = new BsonEncoder(4096);

export function buildOpMsg(msg: OpMsgMessage): Buffer {
  // Pre-calculate total size
  let payloadSize = 0;

  for (const section of msg.sections) {
    if (section.kind === 0) {
      const encoded = encoder.encode(section.document);
      payloadSize += 1 + encoded.length; // kind(u8) + doc（自带长度前缀）
    } else {
      const docsBuf = encodeDocSequence(section.identifier, section.documents);
      payloadSize += docsBuf.length;
    }
  }

  const hasChecksum = msg.flags.checksumPresent;
  const totalSize = HEADER_SIZE + 4 + payloadSize + (hasChecksum ? 4 : 0);

  const buf = Buffer.allocUnsafe(totalSize);
  let off = 0;

  // Header
  buf.writeInt32LE(totalSize, off); off += 4;
  buf.writeInt32LE(msg.requestID, off); off += 4;
  buf.writeInt32LE(msg.responseTo, off); off += 4;
  buf.writeInt32LE(OP_MSG, off); off += 4;

  // Flags
  let flags = 0;
  if (msg.flags.checksumPresent) flags |= CHECKSUM_PRESENT;
  if (msg.flags.moreToCome) flags |= MORE_TO_COME;
  if (msg.flags.exhaustAllowed) flags |= 1 << 16;
  buf.writeInt32LE(flags, off); off += 4;

  // Sections
  for (const section of msg.sections) {
    if (section.kind === 0) {
      const encoded = encoder.encode(section.document);
      // OP_MSG 的 body 段就是 kind(0) + 文档本身（文档自带 4 字节长度前缀），
      // 中间**没有**额外的 size 字段——多写一个就会和自己的解析器错位。
      buf[off++] = 0; // kind = 0
      encoded.copy(buf, off); off += encoded.length;
    } else {
      const docsBuf = encodeDocSequence(section.identifier, section.documents);
      docsBuf.copy(buf, off);
      off += docsBuf.length;
    }
  }

  // Checksum (CRC-32C of everything before the checksum field)
  if (hasChecksum) {
    const crc = crc32c(buf.subarray(0, off));
    buf.writeUInt32LE(crc, off);
    off += 4;
  }

  return buf;
}

function encodeDocSequence(identifier: string, documents: BsonDocument[]): Buffer {
  const idBytes = Buffer.from(identifier, 'utf8');
  // kind(1) + size(4) + idBytes(NUL 结尾的 cstring) + docs
  //
  // 注意标识符是 **cstring**，没有额外的长度字段：多写一个 i32 就会和
  // 标准实现（含 MDB 服务端）错开 4 字节。
  const innerParts: Buffer[] = [];
  let innerSize = 4 + idBytes.length + 1; // size 字段自身 + cstring

  for (const doc of documents) {
    const d = encoder.encode(doc);
    innerParts.push(d);
    innerSize += d.length;
  }

  const size = 1 + innerSize; // kind byte + innerSize
  const buf = Buffer.allocUnsafe(size);
  let off = 0;
  buf[off++] = 1; // kind = document sequence
  buf.writeInt32LE(innerSize, off); off += 4;
  idBytes.copy(buf, off); off += idBytes.length;
  buf[off++] = 0x00; // NUL terminator for identifier
  for (const part of innerParts) {
    part.copy(buf, off);
    off += part.length;
  }
  return buf;
}

// ─── Parser ────────────────────────────────────────────────────────────────────

export function parseOpMsg(buf: Buffer): OpMsgMessage {
  if (buf.length < HEADER_SIZE) {
    throw new Error(`Message too short for header: ${buf.length}`);
  }

  let off = 0;
  const msgLen = buf.readInt32LE(off); off += 4;
  // 声明长度与实际长度不一致说明分帧错位，继续解析只会读到垃圾；
  // 必须在这里拦下（服务端对同一问题也这么做）。
  if (msgLen !== buf.length) {
    throw new Error(`Message length mismatch: header says ${msgLen}, buffer has ${buf.length}`);
  }
  const requestID = buf.readInt32LE(off); off += 4;
  const responseTo = buf.readInt32LE(off); off += 4;
  const opCode = buf.readInt32LE(off); off += 4;

  if (opCode !== OP_MSG) {
    throw new Error(`Expected OP_MSG (2013), got opCode ${opCode}`);
  }

  // Flags
  const flagsRaw = buf.readInt32LE(off); off += 4;
  const flags: OpMsgFlags = {
    checksumPresent: (flagsRaw & CHECKSUM_PRESENT) !== 0,
    moreToCome: (flagsRaw & MORE_TO_COME) !== 0,
    exhaustAllowed: (flagsRaw & (1 << 16)) !== 0,
  };

  const end = flags.checksumPresent ? buf.length - 4 : buf.length;
  const sections: Section[] = [];

  while (off < end) {
    const kind = buf[off++];
    if (kind === 0) {
      const docSize = buf.readInt32LE(off); off += 4;
      const doc = new BsonDecoder(buf.subarray(off - 4, off - 4 + docSize)).decode();
      off += docSize - 4;
      sections.push({ kind: 0, document: doc });
    } else if (kind === 1) {
      const seqSize = buf.readInt32LE(off); off += 4;
      const seqEnd = off + seqSize - 4;
      // 标识符是 NUL 结尾的 cstring，没有长度前缀字段。
      let idEnd = off;
      while (idEnd < seqEnd && buf[idEnd] !== 0x00) {
        idEnd++;
      }
      const identifier = buf.toString('utf8', off, idEnd);
      off = idEnd + 1;
      const docs: BsonDocument[] = [];
      while (off < seqEnd) {
        const docSize = buf.readInt32LE(off);
        const doc = new BsonDecoder(buf.subarray(off, off + docSize)).decode();
        off += docSize;
        docs.push(doc);
      }
      off = seqEnd;
      sections.push({ kind: 1, identifier, documents: docs });
    } else {
      throw new Error(`Unknown section kind: ${kind}`);
    }
  }

  let checksum: number | undefined;
  if (flags.checksumPresent) {
    checksum = buf.readUInt32LE(buf.length - 4);
  }

  return { requestID, responseTo, flags, sections, checksum };
}

// ─── Helpers ───────────────────────────────────────────────────────────────────

export function buildHello(dbName: string): OpMsgMessage {
  return {
    requestID: nextReqId(),
    responseTo: 0,
    flags: { checksumPresent: false, moreToCome: false, exhaustAllowed: false },
    sections: [
      {
        kind: 0,
        document: {
          hello: 1,
          $db: dbName,
        },
      },
    ],
  };
}

export function buildIsMaster(dbName: string): OpMsgMessage {
  return buildHello(dbName);
}

export function buildOpMsgSingle(doc: BsonDocument, requestId?: number): OpMsgMessage {
  return {
    requestID: requestId ?? nextReqId(),
    responseTo: 0,
    flags: { checksumPresent: false, moreToCome: false, exhaustAllowed: false },
    sections: [{ kind: 0, document: doc }],
  };
}

// ─── 面向使用者的门面 ──────────────────────────────────────────────────────────
//
// 底层 `buildOpMsg` / `parseOpMsg` 用的是"消息对象"，而调用方（含测试）真正
// 想要的是"给我一个 Buffer / 帮我解出 body"，所以这里补一层薄封装。

export const WireProtocol = {
  /** 组装一条 OP_MSG 请求；`documents` 非空时追加 document sequence 段。 */
  buildRequest(
    requestId: number,
    body: BsonDocument,
    documents?: BsonDocument[],
    identifier = 'documents',
  ): Buffer {
    const sections: Section[] = [{ kind: 0, document: body }];
    if (documents && documents.length > 0) {
      sections.push({ kind: 1, identifier, documents });
    }
    return buildOpMsg({
      requestID: requestId,
      responseTo: 0,
      flags: { checksumPresent: false, moreToCome: false, exhaustAllowed: false },
      sections,
    });
  },

  /** 解析应答，直接给出 requestId / responseTo / moreToCome / body。 */
  parseResponse(buf: Buffer): {
    requestId: number;
    responseTo: number;
    moreToCome: boolean;
    body: BsonDocument;
    sections: Section[];
  } {
    const msg = parseOpMsg(buf);
    const bodySection = msg.sections.find(
      (section): section is BodySection => section.kind === 0,
    );
    return {
      requestId: msg.requestID,
      responseTo: msg.responseTo,
      moreToCome: msg.flags.moreToCome,
      body: bodySection ? bodySection.document : {},
      sections: msg.sections,
    };
  },

  generateRequestId(): number {
    return nextReqId();
  },
};
