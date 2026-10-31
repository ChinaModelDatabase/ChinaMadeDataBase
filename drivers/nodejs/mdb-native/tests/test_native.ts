import { BSON, ObjectId } from "../src/bson";
import { WireProtocol } from "../src/wire_protocol";
import { CRC32C } from "../src/crc32c";

function assert(condition: boolean, message: string) {
  if (!condition) {
    throw new Error(`ASSERT FAILED: ${message}`);
  }
}

function assertEq(a: any, b: any, message: string) {
  if (JSON.stringify(a) !== JSON.stringify(b)) {
    throw new Error(`ASSERT FAILED: ${message} - expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}`);
  }
}

function testObjectId() {
  console.log("Testing ObjectId...");

  const id1 = new ObjectId();
  assert(id1.toBuffer().length === 12, "ObjectId should be 12 bytes");
  assert(id1.toHexString().length === 24, "ObjectId hex should be 24 chars");
  assert(ObjectId.isValid(id1.toHexString()), "should be valid");

  const id2 = new ObjectId(id1.toHexString());
  assert(id1.equals(id2), "equal objects should be equal");
  assert(id1.toHexString() === id2.toHexString(), "hex strings should match");

  const id3 = new ObjectId();
  assert(!id1.equals(id3), "different objects should not be equal");

  const id4 = new ObjectId("000000000000000000000000");
  assert(id4.toHexString() === "000000000000000000000000", "should preserve hex string");

  console.log("  ObjectId: PASS");
}

function testBsonEncodeDecodePrimitives() {
  console.log("Testing BSON primitives...");

  const doc1 = { a: 42 };
  const buf1 = BSON.encode(doc1);
  const dec1 = BSON.decode(buf1);
  assertEq(dec1.a, 42, "int32 encode/decode");

  const doc2 = { a: "hello" };
  const buf2 = BSON.encode(doc2);
  const dec2 = BSON.decode(buf2);
  assertEq(dec2.a, "hello", "string encode/decode");

  const doc3 = { a: true, b: false };
  const buf3 = BSON.encode(doc3);
  const dec3 = BSON.decode(buf3);
  assertEq(dec3.a, true, "bool true encode/decode");
  assertEq(dec3.b, false, "bool false encode/decode");

  const doc4 = { a: null };
  const buf4 = BSON.encode(doc4);
  const dec4 = BSON.decode(buf4);
  assertEq(dec4.a, null, "null encode/decode");

  const doc5 = { a: 3.14159 };
  const buf5 = BSON.encode(doc5);
  const dec5 = BSON.decode(buf5);
  assertEq(dec5.a, 3.14159, "double encode/decode");

  const doc6 = { a: BigInt(1234567890123) };
  const buf6 = BSON.encode(doc6);
  const dec6 = BSON.decode(buf6);
  assertEq(dec6.a, 1234567890123, "int64 encode/decode");

  console.log("  Primitives: PASS");
}

function testBsonObjectId() {
  console.log("Testing BSON ObjectId...");

  const id = new ObjectId();
  const doc = { _id: id, name: "test" };
  const buf = BSON.encode(doc);
  const dec = BSON.decode(buf);

  assert(dec._id instanceof ObjectId, "should decode as ObjectId");
  assert(dec._id.equals(id), "ObjectId should match");

  console.log("  BSON ObjectId: PASS");
}

function testBsonNestedDocument() {
  console.log("Testing BSON nested document...");

  const doc = {
    name: "outer",
    nested: {
      a: 1,
      b: "inner",
    },
  };
  const buf = BSON.encode(doc);
  const dec = BSON.decode(buf);

  assertEq(dec.name, "outer", "outer name");
  assertEq(dec.nested.a, 1, "nested int");
  assertEq(dec.nested.b, "inner", "nested string");

  console.log("  Nested document: PASS");
}

function testBsonArray() {
  console.log("Testing BSON array...");

  const doc = { arr: [1, 2, 3, "four", true, null] };
  const buf = BSON.encode(doc);
  const dec = BSON.decode(buf);

  assertEq(dec.arr, [1, 2, 3, "four", true, null], "array encode/decode");

  console.log("  Array: PASS");
}

function testBsonBinary() {
  console.log("Testing BSON binary...");

  const data = Buffer.from([0x01, 0x02, 0x03, 0xff]);
  const doc = { bin: { subtype: 0, data } };
  const buf = BSON.encode(doc);
  const dec = BSON.decode(buf);

  assert(Buffer.isBuffer(dec.bin.data), "should be buffer");
  assert(dec.bin.data.equals(data), "binary data should match");
  assertEq(dec.bin.subtype, 0, "subtype should match");

  console.log("  Binary: PASS");
}

function testBsonDatetime() {
  console.log("Testing BSON datetime...");

  const now = new Date();
  const doc = { dt: now };
  const buf = BSON.encode(doc);
  const dec = BSON.decode(buf);

  // Note: datetime is stored as milliseconds since epoch as int64
  // On decode we get the number back
  assertEq(typeof dec.dt, "number", "datetime should decode as number");

  console.log("  Datetime: PASS");
}

function testBsonLargeDocument() {
  console.log("Testing BSON large document...");

  const doc: any = {};
  for (let i = 0; i < 100; i++) {
    doc[`key_${i}`] = `value_${i}`;
  }
  const buf = BSON.encode(doc);
  const dec = BSON.decode(buf);

  for (let i = 0; i < 100; i++) {
    assertEq(dec[`key_${i}`], `value_${i}`, `key_${i}`);
  }

  console.log("  Large document: PASS");
}

function testBsonEmptyDocument() {
  console.log("Testing BSON empty document...");

  const doc = {};
  const buf = BSON.encode(doc);
  const dec = BSON.decode(buf);

  assert(Object.keys(dec).length === 0, "should be empty");

  console.log("  Empty document: PASS");
}

function testWireProtocolBuildParse() {
  console.log("Testing Wire Protocol build/parse...");

  const reqId = 12345;
  const body = { ping: 1, $db: "admin" };
  const packet = WireProtocol.buildRequest(reqId, body);

  assert(packet.length >= 16, "packet should have header");

  const msgLen = packet.readInt32LE(0);
  assertEq(msgLen, packet.length, "message length should match");

  const opCode = packet.readInt32LE(12);
  assertEq(opCode, 2013, "opcode should be OP_MSG");

  const parsed = WireProtocol.parseResponse(packet);
  assertEq(parsed.requestId, reqId, "requestId should match");
  assertEq(parsed.responseTo, 0, "responseTo should be 0 for request");
  assertEq(parsed.body.ping, 1, "body should match");
  assertEq(parsed.body.$db, "admin", "db field");

  console.log("  Wire Protocol: PASS");
}

function testWireProtocolWithDocs() {
  console.log("Testing Wire Protocol with document sequence...");

  const reqId = 99999;
  const body = { insert: "test", ordered: true, $db: "mydb" };
  const docs = [{ a: 1 }, { b: 2 }];

  const packet = WireProtocol.buildRequest(reqId, body, docs, "documents");

  const parsed = WireProtocol.parseResponse(packet);
  assertEq(parsed.requestId, reqId, "requestId");
  assertEq(parsed.body.insert, "test", "body insert");
  assertEq(parsed.documents.length, 2, "should have 2 docs");
  assertEq(parsed.documents[0].a, 1, "first doc");
  assertEq(parsed.documents[1].b, 2, "second doc");

  console.log("  Wire Protocol with docs: PASS");
}

function testWireProtocolRequestId() {
  console.log("Testing Wire Protocol request ID generation...");

  const ids = new Set<number>();
  for (let i = 0; i < 1000; i++) {
    ids.add(WireProtocol.generateRequestId());
  }
  assert(ids.size > 900, "should generate mostly unique IDs");

  console.log("  Request ID generation: PASS");
}

function testCRC32C() {
  console.log("Testing CRC32C...");

  const data1 = Buffer.from("123456789");
  const crc1 = CRC32C.calculate(data1);
  assertEq(crc1, 0xe3069283, "CRC32C of '123456789'");

  const data2 = Buffer.from([]);
  const crc2 = CRC32C.calculate(data2);
  assertEq(crc2, 0x00000000, "CRC32C of empty buffer");

  const data3 = Buffer.from("hello");
  const crc3 = CRC32C.calculate(data3);
  assert(typeof crc3 === "number", "should be number");

  console.log("  CRC32C: PASS");
}

function testCRC32CSeed() {
  console.log("Testing CRC32C with seed...");

  const data = Buffer.from("test");
  const crc1 = CRC32C.calculate(data);
  const crc2 = CRC32C.calculateWithSeed(data, 0xffffffff);

  assert(typeof crc1 === "number", "should be number");
  assert(typeof crc2 === "number", "should be number");

  console.log("  CRC32C seeded: PASS");
}

function testBsonKeyOrder() {
  console.log("Testing BSON key order preservation...");

  const doc = { z: 1, a: 2, m: 3 };
  const buf = BSON.encode(doc);
  const dec = BSON.decode(buf);
  const keys = Object.keys(dec);

  assertEq(keys, ["z", "a", "m"], "key order should be preserved");

  console.log("  Key order: PASS");
}

function testBsonSpecialChars() {
  console.log("Testing BSON special characters...");

  const doc = {
    key1: "hello world",
    key2: "",
    "key.with.dots": 1,
    "key with spaces": 2,
    "key\nwith\nnewlines": 3,
  };
  const buf = BSON.encode(doc);
  const dec = BSON.decode(buf);

  assertEq(dec.key1, "hello world", "normal string");
  assertEq(dec.key2, "", "empty string");
  assertEq(dec["key.with.dots"], 1, "dots in key");
  assertEq(dec["key with spaces"], 2, "spaces in key");
  assertEq(dec["key\nwith\nnewlines"], 3, "newlines in key");

  console.log("  Special chars: PASS");
}

function testBsonLargeIntegers() {
  console.log("Testing BSON large integers...");

  const doc = {
    smallInt: 42,
    largeInt: 2147483647,
    negativeInt: -2147483648,
    bigInt: BigInt("9007199254740991"),
  };
  const buf = BSON.encode(doc);
  const dec = BSON.decode(buf);

  assertEq(dec.smallInt, 42, "small int");
  assertEq(dec.largeInt, 2147483647, "max int32");
  assertEq(dec.negativeInt, -2147483648, "min int32");
  assertEq(dec.bigInt, 9007199254740991, "large int");

  console.log("  Large integers: PASS");
}

// Run all tests
function runAllTests() {
  console.log("\n=== MDB Native Driver Tests ===\n");

  testObjectId();
  testBsonEncodeDecodePrimitives();
  testBsonObjectId();
  testBsonNestedDocument();
  testBsonArray();
  testBsonBinary();
  testBsonDatetime();
  testBsonLargeDocument();
  testBsonEmptyDocument();
  testWireProtocolBuildParse();
  testWireProtocolWithDocs();
  testWireProtocolRequestId();
  testCRC32C();
  testCRC32CSeed();
  testBsonKeyOrder();
  testBsonSpecialChars();
  testBsonLargeIntegers();

  console.log("\n=== All tests passed! ===\n");
}

runAllTests();
