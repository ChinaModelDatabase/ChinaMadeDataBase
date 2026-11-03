import { randomBytes } from 'crypto';

export class ObjectId {
  static readonly SIZE = 12;

  private readonly data: Buffer;

  /**
   * 可以从 Buffer 或 24 位十六进制串构造——驱动（和 mongosh）两种都给，
   * 只支持 Buffer 会让上层到处手写 `Buffer.from(hex, 'hex')`。
   */
  constructor(input?: Buffer | string, offset?: number) {
    if (typeof input === 'string') {
      this.data = ObjectId.hexToBuffer(input);
    } else if (input) {
      // 字节不够就是坏输入：`copy` 只会静默补零，之后这个 ObjectId 会带着
      // 尾巴上的 0 写进 BSON，比直接报错难查得多。
      const start = offset ?? 0;
      if (input.length < start + ObjectId.SIZE) {
        throw new Error(
          `Invalid ObjectId buffer length: ${input.length}, need ${start + ObjectId.SIZE}`,
        );
      }
      this.data = Buffer.allocUnsafe(ObjectId.SIZE);
      input.copy(this.data, 0, start, start + ObjectId.SIZE);
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

  /** 24 位十六进制串才是合法 ObjectId。 */
  static isValid(hex: string): boolean {
    return /^[0-9a-fA-F]{24}$/.test(hex);
  }

  private static hexToBuffer(hex: string): Buffer {
    if (!ObjectId.isValid(hex)) {
      throw new Error(`Invalid ObjectId hex string: ${hex}`);
    }
    return Buffer.from(hex, 'hex');
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

  /** 与官方驱动一致：字符串形式就是 24 位十六进制，方便再喂回构造函数。 */
  toString(): string {
    return this.toHexString();
  }

  toJSON(): string {
    return this.toHexString();
  }
}
