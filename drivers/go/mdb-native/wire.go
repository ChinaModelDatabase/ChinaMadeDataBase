package mdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// OP_MSG opCode.
const OpMsgCode int32 = 2013

// Header sizes and flags.
const (
	HeaderSize      = 16
	MsgFlagChecksum = 1 << 0
	MsgFlagMoreToCome = 1 << 1
)

// OpMsg represents a MongoDB OP_MSG.
type OpMsg struct {
	Body       map[string]interface{}
	Documents  []map[string]interface{}
	RequestId  int32
	ResponseTo int32
	MoreToCome bool
}

// BuildRequest builds an OP_MSG request as raw bytes.
// body is the main section document.
// docs holds optional document sequences.
func BuildRequest(reqId int32, body map[string]interface{}, docs []map[string]interface{}, seqName string) ([]byte, error) {
	var buf bytes.Buffer

	// Body section as single-doc kind=0
	bodyBytes, err := Encode(body)
	if err != nil {
		return nil, fmt.Errorf("wire: encode body: %w", err)
	}

	// header + flagBits + kind byte + body
	totalSize := HeaderSize + 4 + 1 + len(bodyBytes)

	var docSeqBytes []byte
	if len(docs) > 0 && seqName != "" {
		// Section kind=1: document sequence
		// Sequence header: size(4) + cstring(seqName) + documents
		var seqBuf bytes.Buffer
		seqBuf.WriteString(seqName)
		seqBuf.WriteByte(0)
		for _, doc := range docs {
			d, err := Encode(doc)
			if err != nil {
				return nil, fmt.Errorf("wire: encode seq doc: %w", err)
			}
			seqBuf.Write(d)
		}
		innerBytes := seqBuf.Bytes()
		docSeqBytes = make([]byte, 4+len(innerBytes))
		binary.LittleEndian.PutUint32(docSeqBytes[0:4], uint32(4+len(innerBytes)))
		copy(docSeqBytes[4:], innerBytes)

		totalSize += 1 + len(docSeqBytes) // kind byte + docSeqBytes
	}

	// Write header placeholder
	header := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], uint32(totalSize))
	binary.LittleEndian.PutUint32(header[4:8], uint32(reqId))
	binary.LittleEndian.PutUint32(header[8:12], 0) // responseTo
	binary.LittleEndian.PutUint32(header[12:16], uint32(OpMsgCode))
	buf.Write(header)

	// Flag bits
	buf.Write([]byte{0, 0, 0, 0})

	// Section kind=0
	buf.WriteByte(0x00)
	buf.Write(bodyBytes)

	// Section kind=1
	if docSeqBytes != nil {
		buf.WriteByte(0x01)
		buf.Write(docSeqBytes)
	}

	return buf.Bytes(), nil
}

// ParseResponse parses a raw OP_MSG response.
func ParseResponse(data []byte) (*OpMsg, error) {
	if len(data) < HeaderSize {
		return nil, fmt.Errorf("wire: response too short: %d bytes", len(data))
	}

	msgLen := int32(binary.LittleEndian.Uint32(data[0:4]))
	reqId := int32(binary.LittleEndian.Uint32(data[4:8]))
	respTo := int32(binary.LittleEndian.Uint32(data[8:12]))
	opCode := int32(binary.LittleEndian.Uint32(data[12:16]))

	if int(msgLen) > len(data) {
		return nil, fmt.Errorf("wire: msgLen %d > data length %d", msgLen, len(data))
	}
	if opCode != OpMsgCode {
		return nil, fmt.Errorf("wire: unexpected opCode %d, expected %d", opCode, OpMsgCode)
	}

	msg := &OpMsg{
		RequestId:  reqId,
		ResponseTo: respTo,
	}

	r := bytes.NewReader(data[HeaderSize:msgLen])

	// Flag bits
	flagBytes := make([]byte, 4)
	if _, err := io.ReadFull(r, flagBytes); err != nil {
		return nil, fmt.Errorf("wire: read flags: %w", err)
	}
	flags := binary.LittleEndian.Uint32(flagBytes)
	msg.MoreToCome = flags&MsgFlagMoreToCome != 0

	for r.Len() > 0 {
		kind, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("wire: read kind: %w", err)
		}

		switch kind {
		case 0x00:
			// Single document section
			// Read the BSON size to know how much to consume
			sizeBytes := make([]byte, 4)
			if _, err := io.ReadFull(r, sizeBytes); err != nil {
				return nil, fmt.Errorf("wire: read doc size: %w", err)
			}
			size := int(binary.LittleEndian.Uint32(sizeBytes))
			docBytes := make([]byte, size)
			copy(docBytes, sizeBytes)
			_, err = io.ReadFull(r, docBytes[4:])
			if err != nil {
				return nil, fmt.Errorf("wire: read doc body: %w", err)
			}
			doc, err := Decode(docBytes)
			if err != nil {
				return nil, fmt.Errorf("wire: decode doc: %w", err)
			}
			msg.Body = doc

		case 0x01:
			// Document sequence section
			sizeBytes := make([]byte, 4)
			if _, err := io.ReadFull(r, sizeBytes); err != nil {
				return nil, fmt.Errorf("wire: read seq size: %w", err)
			}
			seqSize := int(binary.LittleEndian.Uint32(sizeBytes))
			seqBytes := make([]byte, seqSize-4)
			_, err = io.ReadFull(r, seqBytes)
			if err != nil {
				return nil, fmt.Errorf("wire: read seq body: %w", err)
			}
			// Parse sequence: cstring name + documents
			seqReader := bytes.NewReader(seqBytes)
			_, err = readCStringFromReader(seqReader)
			if err != nil {
				return nil, fmt.Errorf("wire: read seq name: %w", err)
			}
			for seqReader.Len() > 0 {
				sizeBytes2 := make([]byte, 4)
				if _, err := io.ReadFull(seqReader, sizeBytes2); err != nil {
					break
				}
				docSize := int(binary.LittleEndian.Uint32(sizeBytes2))
				docBytes := make([]byte, docSize)
				copy(docBytes, sizeBytes2)
				_, err = io.ReadFull(seqReader, docBytes[4:])
				if err != nil {
					return nil, fmt.Errorf("wire: read seq doc: %w", err)
				}
				doc, err := Decode(docBytes)
				if err != nil {
					return nil, fmt.Errorf("wire: decode seq doc: %w", err)
				}
				msg.Documents = append(msg.Documents, doc)
			}

		default:
			return nil, fmt.Errorf("wire: unknown section kind %d", kind)
		}
	}

	return msg, nil
}

func readCStringFromReader(r *bytes.Reader) (string, error) {
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
