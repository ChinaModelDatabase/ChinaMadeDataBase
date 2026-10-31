package mdb

import (
	"encoding/binary"
	"testing"
)

func TestBSONEncodeDecode(t *testing.T) {
	doc := map[string]interface{}{
		"name":   "test",
		"age":    int32(25),
		"score":  99.5,
		"active": true,
		"data":   nil,
	}

	encoded, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if len(encoded) < 5 {
		t.Fatal("Encoded data too short")
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded["name"] != "test" {
		t.Errorf("name = %v, want 'test'", decoded["name"])
	}
	if decoded["age"] != int32(25) {
		t.Errorf("age = %v, want 25", decoded["age"])
	}
	if decoded["score"] != 99.5 {
		t.Errorf("score = %v, want 99.5", decoded["score"])
	}
	if decoded["active"] != true {
		t.Errorf("active = %v, want true", decoded["active"])
	}
	if decoded["data"] != nil {
		t.Errorf("data = %v, want nil", decoded["data"])
	}
}

func TestBSONNestedDocument(t *testing.T) {
	doc := map[string]interface{}{
		"nested": map[string]interface{}{
			"key": "value",
		},
	}

	encoded, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	nested, ok := decoded["nested"].(map[string]interface{})
	if !ok {
		t.Fatalf("nested is not a map, got %T", decoded["nested"])
	}
	if nested["key"] != "value" {
		t.Errorf("nested.key = %v, want 'value'", nested["key"])
	}
}

func TestBSONInt64(t *testing.T) {
	doc := map[string]interface{}{
		"big": int64(1 << 40),
	}

	encoded, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded["big"] != int64(1<<40) {
		t.Errorf("big = %v, want %d", decoded["big"], int64(1<<40))
	}
}

func TestBSONObjectID(t *testing.T) {
	id := NewObjectID()
	if len(id) != 12 {
		t.Errorf("ObjectID length = %d, want 12", len(id))
	}
	s := id.String()
	if len(s) != 24 {
		t.Errorf("ObjectID string length = %d, want 24", len(s))
	}
}

func TestBSONArray(t *testing.T) {
	doc := map[string]interface{}{
		"arr": []interface{}{int32(1), "two", true},
	}

	encoded, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	arr, ok := decoded["arr"].([]interface{})
	if !ok {
		t.Fatalf("arr is not a slice, got %T", decoded["arr"])
	}
	if len(arr) != 3 {
		t.Errorf("arr length = %d, want 3", len(arr))
	}
}

func TestBSONBinary(t *testing.T) {
	doc := map[string]interface{}{
		"bin": Binary{Data: []byte{1, 2, 3}, SubType: 0},
	}

	encoded, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	bin, ok := decoded["bin"].(Binary)
	if !ok {
		t.Fatalf("bin is not Binary, got %T", decoded["bin"])
	}
	if len(bin.Data) != 3 || bin.Data[0] != 1 {
		t.Errorf("bin.Data = %v, want [1 2 3]", bin.Data)
	}
}

func TestCRC32C(t *testing.T) {
	data := []byte("123456789")
	checksum := CRC32C(data)
	if checksum == 0 {
		t.Error("CRC32C returned zero")
	}
	// Same data should give same checksum
	checksum2 := CRC32C(data)
	if checksum != checksum2 {
		t.Errorf("CRC32C not deterministic: %d != %d", checksum, checksum2)
	}
	// Different data should give different checksum
	data2 := []byte("123456780")
	checksum3 := CRC32C(data2)
	if checksum == checksum3 {
		t.Error("Different data gave same CRC32C")
	}
}

func TestWireProtocolBuildParse(t *testing.T) {
	body := map[string]interface{}{
		"ping": 1,
	}

	reqId := int32(42)
	data, err := BuildRequest(reqId, body, nil, "")
	if err != nil {
		t.Fatalf("BuildRequest failed: %v", err)
	}

	if len(data) < HeaderSize {
		t.Fatalf("Request too short: %d bytes", len(data))
	}

	// Verify header
	msgLen := int32(binary.LittleEndian.Uint32(data[0:4]))
	if int(msgLen) != len(data) {
		t.Errorf("msgLen = %d, data len = %d", msgLen, len(data))
	}
	gotReqId := int32(binary.LittleEndian.Uint32(data[4:8]))
	if gotReqId != reqId {
		t.Errorf("reqId = %d, want %d", gotReqId, reqId)
	}
	opCode := int32(binary.LittleEndian.Uint32(data[12:16]))
	if opCode != OpMsgCode {
		t.Errorf("opCode = %d, want %d", opCode, OpMsgCode)
	}

	// Parse as response (set respTo = reqId)
	parsed, err := ParseResponse(data)
	if err != nil {
		t.Fatalf("ParseResponse failed: %v", err)
	}

	if parsed.Body == nil {
		t.Fatal("Body is nil")
	}
	if parsed.RequestId != reqId {
		t.Errorf("RequestId = %d, want %d", parsed.RequestId, reqId)
	}
}

func TestWireProtocolWithDocs(t *testing.T) {
	body := map[string]interface{}{
		"insert": "test",
		"ordered": true,
	}
	docs := []map[string]interface{}{
		{"name": "doc1"},
		{"name": "doc2"},
	}

	data, err := BuildRequest(1, body, docs, "documents")
	if err != nil {
		t.Fatalf("BuildRequest with docs failed: %v", err)
	}

	if len(data) < HeaderSize {
		t.Fatalf("Request too short: %d bytes", len(data))
	}

	parsed, err := ParseResponse(data)
	if err != nil {
		t.Fatalf("ParseResponse failed: %v", err)
	}

	if parsed.Body == nil {
		t.Fatal("Body is nil")
	}
	if len(parsed.Documents) != 2 {
		t.Errorf("Documents count = %d, want 2", len(parsed.Documents))
	}
}

func TestBSONEmptyDocument(t *testing.T) {
	doc := map[string]interface{}{}

	encoded, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode empty doc failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode empty doc failed: %v", err)
	}

	if len(decoded) != 0 {
		t.Errorf("decoded doc has %d keys, want 0", len(decoded))
	}
}
