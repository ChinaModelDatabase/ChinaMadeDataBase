import { randomBytes } from 'crypto';

export class ObjectId {
  static readonly SIZE = 12;

  private readonly data: Buffer;

  constructor(buffer?: Buffer, offset?: number) {
    if (buffer) {
      this.data = Buffer.allocUnsafe(12);
      buffer.copy(this.data, 0, offset ?? 0, (offset ?? 0) + 12);
    } else {
      this.data = randomBytes(12);
      const ts = Math.floor(Date.now() / 1000);
      this.data[0] = (ts >>> 24) & 0xff;
      this.data[1] = (ts >>> 16) & 0xff;
      this.data[2] = (ts >>> 8) & 0xff;
      this.data[3] = ts & 0xff;
    }
  }

  static fromHexString(hex: string): ObjectId {
    if (hex.length !== 24) {
      throw new Error(`Invalid ObjectId hex string length: ${hex.length}`);
    }
    return new ObjectId(Buffer.from(hex, 'hex'));
  }

  static fromBuffer(buf: Buffer, offset = 0): ObjectId {
    return new ObjectId(buf, offset);
  }

  toBuffer(): Buffer {
    return this.data;
  }

  toHexString(): string {
    return this.data.toString('hex');
  }

  get timestamp(): number {
    return (
      ((this.data[0] << 24) |
        (this.data[1] << 16) |
        (this.data[2] << 8) |
        this.data[3]) >>>
      0
    );
  }

  equals(other: ObjectId): boolean {
    return this.data.equals(other.data);
  }

  toString(): string {
    return `ObjectId("${this.toHexString()}")`;
  }

  toJSON(): string {
    return this.toHexString();
  }
}
