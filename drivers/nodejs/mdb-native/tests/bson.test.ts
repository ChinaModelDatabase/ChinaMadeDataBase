import { BsonEncoder, BsonDecoder } from '../src/bson';
import { ObjectId } from '../src/objectid';

describe('BSON', () => {
  describe('BsonEncoder', () => {
    it('should encode empty document', () => {
      const encoder = new BsonEncoder();
      const buf = encoder.encode({});
      expect(buf.length).toBe(5); // 4 bytes size + 1 null terminator
      expect(buf.readInt32LE(0)).toBe(5);
      expect(buf[4]).toBe(0); // null terminator
    });

    it('should encode int32', () => {
      const encoder = new BsonEncoder();
      const buf = encoder.encode({ x: 42 });
      const decoder = new BsonDecoder(buf);
      const doc = decoder.decode();
      expect(doc.x).toBe(42);
    });

    it('should encode negative int32', () => {
      const encoder = new BsonEncoder();
      const buf = encoder.encode({ x: -100 });
      const decoder = new BsonDecoder(buf);
      const doc = decoder.decode();
      expect(doc.x).toBe(-100);
    });

    it('should encode double', () => {
      const encoder = new BsonEncoder();
      const buf = encoder.encode({ x: 3.14 });
      const decoder = new BsonDecoder(buf);
      const doc = decoder.decode();
      expect(doc.x).toBeCloseTo(3.14);
    });

    it('should encode string', () => {
      const encoder = new BsonEncoder();
      const buf = encoder.encode({ name: 'hello' });
      const decoder = new BsonDecoder(buf);
      const doc = decoder.decode();
      expect(doc.name).toBe('hello');
    });

    it('should encode boolean true', () => {
      const encoder = new BsonEncoder();
      const buf = encoder.encode({ flag: true });
      const decoder = new BsonDecoder(buf);
      const doc = decoder.decode();
      expect(doc.flag).toBe(true);
    });

    it('should encode boolean false', () => {
      const encoder = new BsonEncoder();
      const buf = encoder.encode({ flag: false });
      const decoder = new BsonDecoder(buf);
      const doc = decoder.decode();
      expect(doc.flag).toBe(false);
    });

    it('should encode null', () => {
      const encoder = new BsonEncoder();
      const buf = encoder.encode({ val: null });
      const decoder = new BsonDecoder(buf);
      const doc = decoder.decode();
      expect(doc.val).toBeNull();
    });

    it('should encode ObjectId', () => {
      const id = new ObjectId();
      const encoder = new BsonEncoder();
      const buf = encoder.encode({ _id: id });
      const decoder = new BsonDecoder(buf);
      const doc = decoder.decode();
      expect(doc._id).toBeInstanceOf(ObjectId);
      expect(doc._id.equals(id)).toBe(true);
    });

    it('should encode Date', () => {
      const date = new Date('2024-01-01T00:00:00Z');
      const encoder = new BsonEncoder();
      const buf = encoder.encode({ created: date });
      const decoder = new BsonDecoder(buf);
      const doc = decoder.decode();
      expect(doc.created).toBeInstanceOf(Date);
      expect(doc.created.getTime()).toBe(date.getTime());
    });

    it('should encode nested document', () => {
      const encoder = new BsonEncoder();
      const buf = encoder.encode({ nested: { a: 1, b: 'two' } });
      const decoder = new BsonDecoder(buf);
      const doc = decoder.decode();
      expect(doc.nested).toEqual({ a: 1, b: 'two' });
    });

    it('should encode array', () => {
      const encoder = new BsonEncoder();
      const buf = encoder.encode({ arr: [1, 2, 3] });
      const decoder = new BsonDecoder(buf);
      const doc = decoder.decode();
      expect(doc.arr).toEqual([1, 2, 3]);
    });

    it('should encode empty array', () => {
      const encoder = new BsonEncoder();
      const buf = encoder.encode({ arr: [] });
      const decoder = new BsonDecoder(buf);
      const doc = decoder.decode();
      expect(doc.arr).toEqual([]);
    });

    it('should encode mixed types', () => {
      const encoder = new BsonEncoder();
      const doc = {
        str: 'hello',
        num: 42,
        float: 1.5,
        bool: true,
        nil: null,
        obj: { nested: true },
        arr: [1, 'two'],
        id: new ObjectId(),
        date: new Date(),
      };
      const buf = encoder.encode(doc);
      const decoder = new BsonDecoder(buf);
      const decoded = decoder.decode();
      expect(decoded.str).toBe('hello');
      expect(decoded.num).toBe(42);
      expect(decoded.float).toBeCloseTo(1.5);
      expect(decoded.bool).toBe(true);
      expect(decoded.nil).toBeNull();
      expect(decoded.obj).toEqual({ nested: true });
      expect(decoded.arr).toEqual([1, 'two']);
      expect(decoded.id).toBeInstanceOf(ObjectId);
      expect(decoded.date).toBeInstanceOf(Date);
    });

    it('should encode large int32 values', () => {
      const encoder = new BsonEncoder();
      const buf = encoder.encode({ max: 2147483647, min: -2147483648 });
      const decoder = new BsonDecoder(buf);
      const doc = decoder.decode();
      expect(doc.max).toBe(2147483647);
      expect(doc.min).toBe(-2147483648);
    });

    it('should encode bigint as int64', () => {
      const encoder = new BsonEncoder();
      const buf = encoder.encode({ big: BigInt(9007199254740993) });
      const decoder = new BsonDecoder(buf);
      const doc = decoder.decode();
      expect(doc.big).toBe(BigInt(9007199254740993));
    });
  });

  describe('BsonDecoder', () => {
    it('should decode empty document', () => {
      const buf = Buffer.from([5, 0, 0, 0, 0]);
      const decoder = new BsonDecoder(buf);
      const doc = decoder.decode();
      expect(doc).toEqual({});
    });

    it('should decode document with string', () => {
      const encoder = new BsonEncoder();
      const buf = encoder.encode({ key: 'value' });
      const decoder = new BsonDecoder(buf);
      const doc = decoder.decode();
      expect(doc.key).toBe('value');
    });

    it('should handle multiple calls to decode', () => {
      const encoder = new BsonEncoder();
      const buf1 = encoder.encode({ a: 1 });
      const buf2 = encoder.encode({ b: 2 });

      const decoder = new BsonDecoder(buf1);
      expect(decoder.decode()).toEqual({ a: 1 });

      decoder.decode(buf2);
      expect(decoder.decode()).toEqual({ b: 2 });
    });
  });

  describe('ObjectId', () => {
    it('should create 12-byte buffer', () => {
      const id = new ObjectId();
      expect(id.toBuffer().length).toBe(12);
    });

    it('should have 24-char hex string', () => {
      const id = new ObjectId();
      expect(id.toString().length).toBe(24);
    });

    it('should be equal to another ObjectId with same bytes', () => {
      const buf = Buffer.alloc(12);
      buf.fill(0xff);
      const id1 = new ObjectId(buf);
      const id2 = new ObjectId(buf);
      expect(id1.equals(id2)).toBe(true);
    });

    it('should not be equal to different ObjectId', () => {
      const id1 = new ObjectId();
      const id2 = new ObjectId();
      expect(id1.equals(id2)).toBe(false);
    });

    it('should create from hex string', () => {
      const id = new ObjectId();
      const hex = id.toString();
      const id2 = ObjectId.fromHexString(hex);
      expect(id.equals(id2)).toBe(true);
    });

    it('should throw on invalid hex length', () => {
      expect(() => new ObjectId('abc')).toThrow();
    });

    it('should throw on invalid buffer length', () => {
      expect(() => new ObjectId(Buffer.alloc(10))).toThrow();
    });

    it('should generate unique IDs', () => {
      const ids = new Set<string>();
      for (let i = 0; i < 100; i++) {
        ids.add(new ObjectId().toString());
      }
      expect(ids.size).toBe(100);
    });
  });
});
