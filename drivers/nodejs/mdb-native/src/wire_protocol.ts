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
      payloadSize += 1 + 4 + encoded.length; // kind(u8) + size(i32) + doc
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
      buf[off++] = 0; // kind = 0
      buf.writeInt32LE(encoded.length, off); off += 4;
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
  // kind(1) + size(4) + idLen(4) + idBytes + NUL + docs
  const innerParts: Buffer[] = [];
  let innerSize = 4 + idBytes.length + 1; // size includes itself + idCString

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
  buf.writeInt32LE(idBytes.length + 1, off); off += 4;
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
    throw new Error(`Buffer too small for header: ${buf.length}`);
  }

  let off = 0;
  const msgLen = buf.readInt32LE(off); off += 4;
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
      const idLen = buf.readInt32LE(off); off += 4;
      const identifier = buf.toString('utf8', off, off + idLen - 1);
      off += idLen;
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
