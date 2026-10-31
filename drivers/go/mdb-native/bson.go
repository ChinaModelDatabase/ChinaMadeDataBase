package mdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

// BSON type constants.
const (
	BSONTypeDouble   byte = 0x01
	BSONTypeString   byte = 0x02
	BSONTypeDocument byte = 0x03
	BSONTypeArray    byte = 0x04
	BSONTypeBinary   byte = 0x05
	BSONTypeObjectID byte = 0x07
	BSONTypeBool     byte = 0x08
	BSONTypeDateTime byte = 0x09
	BSONTypeNull     byte = 0x0A
	BSONTypeInt32    byte = 0x10
	BSONTypeInt64    byte = 0x12
)

// ObjectID represents a 12-byte MongoDB ObjectId.
type ObjectID [12]byte

// NewObjectID generates a new ObjectID.
func NewObjectID() ObjectID {
	var id ObjectID
	t := uint32(time.Now().Unix())
	id[0] = byte(t >> 24)
	id[1] = byte(t >> 16)
	id[2] = byte(t >> 8)
	id[3] = byte(t)
	// 5 random bytes
	for i := 4; i < 9; i++ {
		id[i] = byte(time.Now().UnixNano() >> uint((i-4)*8))
	}
	// 3 incrementing bytes
	id[9] = 0
	id[10] = 0
	id[11] = 1
	return id
}

// String returns the hex string representation.
func (id ObjectID) String() string {
	return fmt.Sprintf("%x", [12]byte(id))
}

// Encode encodes a Go map to BSON binary.
func Encode(doc map[string]interface{}) ([]byte, error) {
	var buf bytes.Buffer
	// Reserve 4 bytes for size
	sizeBuf := make([]byte, 4)
	buf.Write(sizeBuf)

	if err := encodeElements(&buf, doc); err != nil {
		return nil, err
	}
	buf.WriteByte(0x00)

	// Write final size
	data := buf.Bytes()
	size := int32(len(data))
	binary.LittleEndian.PutUint32(data[0:4], uint32(size))

	return data, nil
}

// encodeElements encodes all elements of a map into the buffer.
func encodeElements(buf *bytes.Buffer, doc map[string]interface{}) error {
	for key, val := range doc {
		if err := encodeElement(buf, key, val); err != nil {
			return err
		}
	}
	return nil
}

func encodeElement(buf *bytes.Buffer, name string, val interface{}) error {
	var typ byte
	var raw []byte
	var err error

	switch v := val.(type) {
	case float64:
		typ = BSONTypeDouble
		raw = make([]byte, 8)
		binary.LittleEndian.PutUint64(raw, math.Float64bits(v))
	case string:
		typ = BSONTypeString
		strBytes := []byte(v)
		intBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(intBuf, uint32(len(strBytes)+1))
		raw = append(intBuf, strBytes...)
		raw = append(raw, 0)
	case map[string]interface{}:
		typ = BSONTypeDocument
		embedded, err := Encode(v)
		if err != nil {
			return err
		}
		raw = embedded
	case []interface{}:
		typ = BSONTypeArray
		arrMap := make(map[string]interface{})
		for i, item := range v {
			arrMap[fmt.Sprintf("%d", i)] = item
		}
		embedded, err := Encode(arrMap)
		if err != nil {
			return err
		}
		raw = embedded
	case Binary:
		typ = BSONTypeBinary
		intBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(intBuf, uint32(len(v.Data)))
		raw = append(intBuf, v.SubType)
		raw = append(raw, v.Data...)
	case ObjectID:
		typ = BSONTypeObjectID
		raw = v[:]
	case bool:
		typ = BSONTypeBool
		if v {
			raw = []byte{1}
		} else {
			raw = []byte{0}
		}
	case time.Time:
		typ = BSONTypeDateTime
		raw = make([]byte, 8)
		binary.LittleEndian.PutUint64(raw, uint64(v.UnixNano()/int64(time.Millisecond)))
	case nil:
		typ = BSONTypeNull
		raw = []byte{}
	case int32:
		typ = BSONTypeInt32
		raw = make([]byte, 4)
		binary.LittleEndian.PutUint32(raw, uint32(v))
	case int:
		typ = BSONTypeInt32
		raw = make([]byte, 4)
		binary.LittleEndian.PutUint32(raw, uint32(v))
	case int64:
		typ = BSONTypeInt64
		raw = make([]byte, 8)
		binary.LittleEndian.PutUint64(raw, uint64(v))
	default:
		return fmt.Errorf("bson: unsupported type %T", val)
	}

	buf.WriteByte(typ)
	buf.WriteString(name)
	buf.WriteByte(0)
	buf.Write(raw)
	return nil
}

// Binary represents a BSON binary value.
type Binary struct {
	Data    []byte
	SubType byte
}

// Decode decodes a BSON byte slice into a Go map.
func Decode(data []byte) (map[string]interface{}, error) {
	if len(data) < 5 {
		return nil, errors.New("bson: data too short")
	}
	size := int32(binary.LittleEndian.Uint32(data[0:4]))
	if int(size) != len(data) {
		return nil, fmt.Errorf("bson: size mismatch: header=%d actual=%d", size, len(data))
	}
	return decodeDocument(bytes.NewReader(data[4:]))
}

func decodeDocument(r *bytes.Reader) (map[string]interface{}, error) {
	doc := make(map[string]interface{})
	for {
		b, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if b == 0x00 {
			break
		}
		name, err := readCString(r)
		if err != nil {
			return nil, err
		}
		val, err := decodeValue(r, b)
		if err != nil {
			return nil, err
		}
		doc[name] = val
	}
	return doc, nil
}

func readCString(r *bytes.Reader) (string, error) {
	var buf bytes.Buffer
	for {
		b, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		if b == 0x00 {
			break
		}
		buf.WriteByte(b)
	}
	return buf.String(), nil
}

func readInt32(r *bytes.Reader) (int32, error) {
	b := make([]byte, 4)
	_, err := io.ReadFull(r, b)
	if err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(b)), nil
}

func readInt64(r *bytes.Reader) (int64, error) {
	b := make([]byte, 8)
	_, err := io.ReadFull(r, b)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b)), nil
}

func readFloat64(r *bytes.Reader) (float64, error) {
	b := make([]byte, 8)
	_, err := io.ReadFull(r, b)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(b)), nil
}

func decodeValue(r *bytes.Reader, typ byte) (interface{}, error) {
	switch typ {
	case BSONTypeDouble:
		return readFloat64(r)
	case BSONTypeString:
		size, err := readInt32(r)
		if err != nil {
			return nil, err
		}
		strBytes := make([]byte, size-1)
		_, err = io.ReadFull(r, strBytes)
		if err != nil {
			return nil, err
		}
		r.ReadByte() // trailing null
		return string(strBytes), nil
	case BSONTypeDocument:
		size, err := readInt32(r)
		if err != nil {
			return nil, err
		}
		docBytes := make([]byte, size)
		docBytes[0] = byte(size)
		docBytes[1] = byte(size >> 8)
		docBytes[2] = byte(size >> 16)
		docBytes[3] = byte(size >> 24)
		_, err = io.ReadFull(r, docBytes[4:])
		if err != nil {
			return nil, err
		}
		return Decode(docBytes)
	case BSONTypeArray:
		size, err := readInt32(r)
		if err != nil {
			return nil, err
		}
		arrBytes := make([]byte, size)
		arrBytes[0] = byte(size)
		arrBytes[1] = byte(size >> 8)
		arrBytes[2] = byte(size >> 16)
		arrBytes[3] = byte(size >> 24)
		_, err = io.ReadFull(r, arrBytes[4:])
		if err != nil {
			return nil, err
		}
		doc, err := Decode(arrBytes)
		if err != nil {
			return nil, err
		}
		// Convert ordered map to slice
		length := len(doc)
		result := make([]interface{}, length)
		for i := 0; i < length; i++ {
			result[i] = doc[fmt.Sprintf("%d", i)]
		}
		return result, nil
	case BSONTypeBinary:
		size, err := readInt32(r)
		if err != nil {
			return nil, err
		}
		subType, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		binData := make([]byte, size)
		_, err = io.ReadFull(r, binData)
		if err != nil {
			return nil, err
		}
		return Binary{Data: binData, SubType: subType}, nil
	case BSONTypeObjectID:
		var id ObjectID
		_, err := io.ReadFull(r, id[:])
		if err != nil {
			return nil, err
		}
		return id, nil
	case BSONTypeBool:
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		return b != 0, nil
	case BSONTypeDateTime:
		millis, err := readInt64(r)
		if err != nil {
			return nil, err
		}
		return time.Unix(0, millis*int64(time.Millisecond)), nil
	case BSONTypeNull:
		return nil, nil
	case BSONTypeInt32:
		return readInt32(r)
	case BSONTypeInt64:
		return readInt64(r)
	default:
		return nil, fmt.Errorf("bson: unsupported type 0x%02X", typ)
	}
}

// EncodeArray encodes a slice to BSON binary.
func EncodeArray(arr []interface{}) ([]byte, error) {
	return Encode(arr)
}
