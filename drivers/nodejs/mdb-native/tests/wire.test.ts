import { WireProtocol, OP_MSG, FLAG_CHECKSUM, FLAG_MORE_TO_COME } from '../src/wire_protocol';
import { BsonEncoder } from '../src/bson';

describe('WireProtocol', () => {
  describe('buildRequest', () => {
    it('should build a valid OP_MSG request', () => {
      const reqId = 1;
      const body = { ping: 1 };
      const msg = WireProtocol.buildRequest(reqId, body);

      // Check header
      expect(msg.readInt32LE(0)).toBe(msg.length); // msgLen
      expect(msg.readInt32LE(4)).toBe(reqId); // reqId
      expect(msg.readInt32LE(8)).toBe(0); // responseTo
      expect(msg.readInt32LE(12)).toBe(OP_MSG); // opCode

      // Check flags
      expect(msg.readInt32LE(16)).toBe(0); // flags

      // Check kind byte
      expect(msg[20]).toBe(0); // kind 0 = body
    });

    it('should include body document', () => {
      const body = { insert: 'test', ordered: true };
      const msg = WireProtocol.buildRequest(1, body);

      // Body starts after header (16) + flags (4) + kind (1) = 21
      const bodyBuf = msg.slice(21);
      const decoder = new (require('../src/bson').BsonDecoder)(bodyBuf);
      const doc = decoder.decode();
      expect(doc.insert).toBe('test');
      expect(doc.ordered).toBe(true);
    });

    it('should build request with document sequence', () => {
      const body = { insert: 'test', ordered: true };
      const docs = [{ name: 'doc1' }, { name: 'doc2' }];
      const msg = WireProtocol.buildRequest(1, body, docs, 'test.test');

      // Should be larger than just body
      const bodyOnlyMsg = WireProtocol.buildRequest(1, body);
      expect(msg.length).toBeGreaterThan(bodyOnlyMsg.length);

      // Should have kind 1 section
      // Find kind 1 byte after body section
      let offset = 21;
      const bodySize = msg.readInt32LE(offset);
      offset += bodySize;
      expect(offset).toBeLessThan(msg.length);
      expect(msg[offset]).toBe(1); // kind 1
    });

    it('should produce correct message length', () => {
      const body = { test: 1 };
      const msg = WireProtocol.buildRequest(1, body);
      expect(msg.readInt32LE(0)).toBe(msg.length);
    });
  });

  describe('parseResponse', () => {
    it('should parse a simple response', () => {
      // Build a mock response
      const encoder = new BsonEncoder();
      const body = { ok: 1 };
      const bodyBuf = encoder.encode(body);

      const flags = 0;
      const msgLen = 16 + 4 + 1 + bodyBuf.length;
      const msg = Buffer.alloc(msgLen);
      msg.writeInt32LE(msgLen, 0);
      msg.writeInt32LE(100, 4); // requestId
      msg.writeInt32LE(1, 8); // responseTo
      msg.writeInt32LE(OP_MSG, 12);
      msg.writeInt32LE(flags, 16);
      msg[20] = 0; // kind 0
      bodyBuf.copy(msg, 21);

      const result = WireProtocol.parseResponse(msg);
      expect(result.requestId).toBe(100);
      expect(result.responseTo).toBe(1);
      expect(result.moreToCome).toBe(false);
      expect(result.body.ok).toBe(1);
    });

    it('should parse response with moreToCome flag', () => {
      const encoder = new BsonEncoder();
      const body = { ok: 1 };
      const bodyBuf = encoder.encode(body);

      const flags = FLAG_MORE_TO_COME;
      const msgLen = 16 + 4 + 1 + bodyBuf.length;
      const msg = Buffer.alloc(msgLen);
      msg.writeInt32LE(msgLen, 0);
      msg.writeInt32LE(100, 4);
      msg.writeInt32LE(1, 8);
      msg.writeInt32LE(OP_MSG, 12);
      msg.writeInt32LE(flags, 16);
      msg[20] = 0;
      bodyBuf.copy(msg, 21);

      const result = WireProtocol.parseResponse(msg);
      expect(result.moreToCome).toBe(true);
    });

    it('should reject messages with wrong opCode', () => {
      const msg = Buffer.alloc(16);
      msg.writeInt32LE(16, 0);
      msg.writeInt32LE(1, 4);
      msg.writeInt32LE(0, 8);
      msg.writeInt32LE(2001, 12); // OP_INSERT, not OP_MSG

      expect(() => WireProtocol.parseResponse(msg)).toThrow('Expected OP_MSG');
    });

    it('should reject messages with wrong length', () => {
      const msg = Buffer.alloc(20);
      msg.writeInt32LE(100, 0); // claims 100 bytes but only has 20
      msg.writeInt32LE(1, 4);
      msg.writeInt32LE(0, 8);
      msg.writeInt32LE(OP_MSG, 12);

      expect(() => WireProtocol.parseResponse(msg)).toThrow('Message length mismatch');
    });

    it('should reject messages too short for header', () => {
      const msg = Buffer.alloc(10);
      expect(() => WireProtocol.parseResponse(msg)).toThrow('too short');
    });
  });

  describe('round-trip', () => {
    it('should build and parse a request', () => {
      const body = { ping: 1 };
      const msg = WireProtocol.buildRequest(42, body);
      // Simulate response with same requestId as responseTo
      const response = simulateResponse(msg, { ok: 1 });

      const parsed = WireProtocol.parseResponse(response);
      expect(parsed.responseTo).toBe(42);
      expect(parsed.body.ok).toBe(1);
    });
  });
});

function simulateResponse(request: Buffer, body: any): Buffer {
  const encoder = new BsonEncoder();
  const bodyBuf = encoder.encode(body);

  const reqId = request.readInt32LE(4);
  const msgLen = 16 + 4 + 1 + bodyBuf.length;
  const msg = Buffer.alloc(msgLen);
  msg.writeInt32LE(msgLen, 0);
  msg.writeInt32LE(999, 4); // response requestId
  msg.writeInt32LE(reqId, 8); // responseTo = original reqId
  msg.writeInt32LE(OP_MSG, 12);
  msg.writeInt32LE(0, 16); // flags
  msg[20] = 0; // kind 0
  bodyBuf.copy(msg, 21);
  return msg;
}
