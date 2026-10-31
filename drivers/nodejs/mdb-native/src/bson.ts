import { ObjectId } from './objectid';

// ─── BSON Element Types ────────────────────────────────────────────────────────

export const enum BsonType {
  DOUBLE = 0x01,
  STRING = 0x02,
  DOCUMENT = 0x03,
  ARRAY = 0x04,
  OBJECT_ID = 0x07,
  BOOL = 0x08,
  DATETIME = 0x09,
  NULL = 0x0a,
  INT32 = 0x10,
  INT64 = 0x12,
}

// ─── Generic BSON value type ───────────────────────────────────────────────────

export type BsonValue =
  | number
  | string
  | boolean
  | null
  | ObjectId
  | Date
  | BsonDocument
  | BsonValue[]
  | Int32
  | Int64;

export interface BsonDocument {
  [key: string]: BsonValue;
}

export class Int32 {
  constructor(public readonly value: number) {}
  toJSON(): number { return this.value; }
}

export class Int64 {
  constructor(public readonly value: bigint) {}
  toJSON(): string { return this.value.toString(); }
}

// ─── Encoder ───────────────────────────────────────────────────────────────────

export class BsonEncoder {
  private buf: Buffer;
  private pos: number;

  constructor(initialSize = 256) {
    this.buf = Buffer.allocUnsafe(initialSize);
    this.pos = 0;
  }

  private ensureSpace(n: number): void {
    while (this.pos + n > this.buf.length) {
      const next = Buffer.allocUnsafe(this.buf.length * 2);
      this.buf.copy(next);
      this.buf = next;
    }
  }

  private writeU8(v: number): void {
    this.ensureSpace(1);
    this.buf[this.pos++] = v & 0xff;
  }

  private writeI32(v: number): void {
    this.ensureSpace(4);
    this.buf.writeInt32LE(v, this.pos);
    this.pos += 4;
  }

  private writeU64(v: bigint): void {
    this.ensureSpace(8);
    this.buf.writeBigInt64LE(v, this.pos);
    this.pos += 8;
  }

  private writeCString(s: string): void {
    const bytes = Buffer.from(s, 'utf8');
    this.ensureSpace(bytes.length + 1);
    bytes.copy(this.buf, this.pos);
    this.pos += bytes.length;
    this.buf[this.pos++] = 0x00;
  }

  private writeString(s: string): void {
    const bytes = Buffer.from(s, 'utf8');
    this.writeI32(bytes.length + 1);
    this.ensureSpace(bytes.length + 1);
    bytes.copy(this.buf, this.pos);
    this.pos += bytes.length;
    this.buf[this.pos++] = 0x00;
  }

  private writeElement(name: string, value: BsonValue): void {
    if (value === null || value === undefined) {
      this.writeU8(BsonType.NULL);
      this.writeCString(name);
    } else if (typeof value === 'number') {
      if (Number.isInteger(value) && value >= -0x80000000 && value < 0x80000000) {
        this.writeU8(BsonType.INT32);
        this.writeCString(name);
        this.writeI32(value);
      } else {
        this.writeU8(BsonType.DOUBLE);
        this.writeCString(name);
        this.ensureSpace(8);
        this.buf.writeDoubleLE(value, this.pos);
        this.pos += 8;
      }
    } else if (typeof value === 'string') {
      this.writeU8(BsonType.STRING);
      this.writeCString(name);
      this.writeString(value);
    } else if (typeof value === 'boolean') {
      this.writeU8(BsonType.BOOL);
      this.writeCString(name);
      this.writeU8(value ? 1 : 0);
    } else if (value instanceof ObjectId) {
      this.writeU8(BsonType.OBJECT_ID);
      this.writeCString(name);
      this.ensureSpace(12);
      value.toBuffer().copy(this.buf, this.pos);
      this.pos += 12;
    } else if (value instanceof Date) {
      this.writeU8(BsonType.DATETIME);
      this.writeCString(name);
      this.writeU64(BigInt(value.getTime()));
    } else if (value instanceof Int32) {
      this.writeU8(BsonType.INT32);
      this.writeCString(name);
      this.writeI32(value.value);
    } else if (value instanceof Int64) {
      this.writeU8(BsonType.INT64);
      this.writeCString(name);
      this.writeU64(value.value);
    } else if (Array.isArray(value)) {
      this.writeU8(BsonType.ARRAY);
      this.writeCString(name);
      const arrDoc: BsonDocument = {};
      for (let i = 0; i < value.length; i++) {
        arrDoc[String(i)] = value[i];
      }
      this.writeDocument(arrDoc);
    } else if (typeof value === 'object') {
      this.writeU8(BsonType.DOCUMENT);
      this.writeCString(name);
      this.writeDocument(value as BsonDocument);
    } else {
      throw new Error(`Unsupported BSON value type: ${typeof value}`);
    }
  }

  writeDocument(doc: BsonDocument): void {
    const sizePos = this.pos;
    this.writeI32(0);

    for (const key of Object.keys(doc)) {
      this.writeElement(key, doc[key]);
    }
    this.writeU8(0x00);

    this.buf.writeInt32LE(this.pos - sizePos, sizePos);
  }

  encode(doc: BsonDocument): Buffer {
    this.pos = 0;
    this.writeDocument(doc);
    return this.buf.subarray(0, this.pos);
  }

  encodeInto(doc: BsonDocument, target: Buffer, offset = 0): Buffer {
    const encoded = this.encode(doc);
    encoded.copy(target, offset);
    return target.subarray(offset, offset + encoded.length);
  }
}

// ─── Decoder ───────────────────────────────────────────────────────────────────

export class BsonDecoder {
  private buf: Buffer;
  private pos: number;

  constructor(buf: Buffer) {
    this.buf = buf;
    this.pos = 0;
  }

  private readU8(): number {
    return this.buf[this.pos++];
  }

  private readI32(): number {
    const v = this.buf.readInt32LE(this.pos);
    this.pos += 4;
    return v;
  }

  private readU64(): bigint {
    const v = this.buf.readBigInt64LE(this.pos);
    this.pos += 8;
    return v;
  }

  private readF64(): number {
    const v = this.buf.readDoubleLE(this.pos);
    this.pos += 8;
    return v;
  }

  private readCString(): string {
    const start = this.pos;
    while (this.buf[this.pos] !== 0x00) this.pos++;
    const s = this.buf.toString('utf8', start, this.pos);
    this.pos++;
    return s;
  }

  private readString(): string {
    const len = this.readI32();
    const s = this.buf.toString('utf8', this.pos, this.pos + len - 1);
    this.pos += len;
    return s;
  }

  private readDocument(): BsonDocument {
    const docSize = this.readI32();
    const end = this.pos + docSize - 4;
    const doc: BsonDocument = {};

    while (this.pos < end) {
      const type = this.readU8();
      if (type === 0x00) break;
      const name = this.readCString();
      doc[name] = this.readValue(type);
    }
    this.pos = end;
    return doc;
  }

  private readValue(type: number): BsonValue {
    switch (type) {
      case BsonType.DOUBLE:
        return this.readF64();
      case BsonType.STRING:
        return this.readString();
      case BsonType.DOCUMENT:
        return this.readDocument();
      case BsonType.ARRAY: {
        const arrDoc = this.readDocument();
        const keys = Object.keys(arrDoc).map(Number).sort((a, b) => a - b);
        return keys.map((k) => arrDoc[String(k)]);
      }
      case BsonType.OBJECT_ID: {
        const id = ObjectId.fromBuffer(this.buf, this.pos);
        this.pos += 12;
        return id;
      }
      case BsonType.BOOL:
        return this.readU8() !== 0;
      case BsonType.DATETIME: {
        const ms = Number(this.readU64());
        return new Date(ms);
      }
      case BsonType.NULL:
        return null;
      case BsonType.INT32:
        return this.readI32();
      case BsonType.INT64:
        return new Int64(this.readU64());
      default:
        throw new Error(`Unknown BSON type: 0x${type.toString(16)}`);
    }
  }

  decode(): BsonDocument {
    this.pos = 0;
    return this.readDocument();
  }

  decodeAt(offset: number): BsonDocument {
    this.pos = offset;
    return this.readDocument();
  }
}
