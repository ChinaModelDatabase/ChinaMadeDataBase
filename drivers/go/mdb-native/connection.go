package mdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

// MdbConnection is a raw TCP connection to MDB with message framing.
type MdbConnection struct {
	conn   net.Conn
	mu     sync.Mutex
	nextId int32
}

// Dial connects to an MDB server.
func Dial(host string, port int) (*MdbConnection, error) {
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("mdb: dial %s: %w", addr, err)
	}
	return &MdbConnection{conn: conn, nextId: 1}, nil
}

// Close closes the connection.
func (c *MdbConnection) Close() error {
	return c.conn.Close()
}

// NextRequestId returns an auto-incrementing request ID.
func (c *MdbConnection) NextRequestId() int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextId
	c.nextId++
	return id
}

// SendCommand sends a command and reads the response.
func (c *MdbConnection) SendCommand(cmd map[string]interface{}) (map[string]interface{}, error) {
	reqId := c.NextRequestId()
	data, err := BuildRequest(reqId, cmd, nil, "")
	if err != nil {
		return nil, err
	}
	if err := c.writeMessage(data); err != nil {
		return nil, err
	}
	resp, err := c.readMessage()
	if err != nil {
		return nil, err
	}
	msg, err := ParseResponse(resp)
	if err != nil {
		return nil, err
	}
	return msg.Body, nil
}

// SendCommandWithDocs sends a command with document sequences.
func (c *MdbConnection) SendCommandWithDocs(cmd map[string]interface{}, docs []map[string]interface{}, seqName string) (map[string]interface{}, error) {
	reqId := c.NextRequestId()
	data, err := BuildRequest(reqId, cmd, docs, seqName)
	if err != nil {
		return nil, err
	}
	if err := c.writeMessage(data); err != nil {
		return nil, err
	}
	resp, err := c.readMessage()
	if err != nil {
		return nil, err
	}
	msg, err := ParseResponse(resp)
	if err != nil {
		return nil, err
	}
	return msg.Body, nil
}

// writeMessage writes a length-prefixed message to the connection.
func (c *MdbConnection) writeMessage(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.conn.Write(data)
	return err
}

// readMessage reads a length-prefixed message from the connection.
func (c *MdbConnection) readMessage() ([]byte, error) {
	// Read 4-byte length header
	var lenBuf [4]byte
	_, err := io.ReadFull(c.conn, lenBuf[:])
	if err != nil {
		return nil, fmt.Errorf("mdb: read length: %w", err)
	}
	msgLen := binary.LittleEndian.Uint32(lenBuf[:])
	if msgLen < 4 || msgLen > 48*1024*1024 {
		return nil, fmt.Errorf("mdb: invalid message length %d", msgLen)
	}

	// Read remaining bytes
	data := make([]byte, msgLen)
	copy(data[:4], lenBuf[:])
	_, err = io.ReadFull(c.conn, data[4:])
	if err != nil {
		return nil, fmt.Errorf("mdb: read message body: %w", err)
	}
	return data, nil
}
