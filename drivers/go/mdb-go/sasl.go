package mdb

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"

	native "github.com/mdb-official/mdb-native"
)

// SCRAM-SHA-256 认证。
//
// 服务端只在有账号后才要求认证，但连接串里带凭据是常态，所以驱动必须
// 自己实现握手：mongodb-cr/scram 的口令派生规则是
// `SaltedPassword = PBKDF2(HMAC(password), salt, i)`，
// 客户端证明 `p = ClientKey XOR HMAC(StoredKey, AuthMessage)`。
func scramSHA256(db *native.MdbDatabase, username, password string) error {
	nonce, err := randomNonce()
	if err != nil {
		return err
	}
	clientFirst := "n,,n=" + username + ",r=" + nonce

	start, err := db.Command(map[string]interface{}{
		"saslStart": 1,
		"mechanism": "SCRAM-SHA-256",
		"payload":   native.Binary{Data: []byte(clientFirst), SubType: 0},
		"options":   map[string]interface{}{"skipEmptyExchange": true},
	})
	if err != nil {
		return fmt.Errorf("mdb: saslStart: %w", err)
	}
	if err := commandErr(start); err != nil {
		return fmt.Errorf("mdb: saslStart: %w", err)
	}
	conversationID, _ := start["conversationId"].(int32)
	if conversationID == 0 {
		// 服务端可能用 int64/uint32 编码，这里按数值类型取。
		switch v := start["conversationId"].(type) {
		case int64:
			conversationID = int32(v)
		case float64:
			conversationID = int32(v)
		case uint32:
			conversationID = int32(v)
		}
	}
	serverFirst, err := payloadString(start)
	if err != nil {
		return err
	}

	// 解析 r= / s= / i=
	var serverNonce, saltB64 string
	var iterations int
	for _, part := range strings.Split(serverFirst, ",") {
		switch {
		case strings.HasPrefix(part, "r="):
			serverNonce = part[2:]
		case strings.HasPrefix(part, "s="):
			saltB64 = part[2:]
		case strings.HasPrefix(part, "i="):
			fmt.Sscanf(part[2:], "%d", &iterations)
		}
	}
	if serverNonce == "" || saltB64 == "" || iterations <= 0 {
		return fmt.Errorf("mdb: malformed server-first-message: %q", serverFirst)
	}
	if !strings.HasPrefix(serverNonce, nonce) {
		return fmt.Errorf("mdb: server nonce does not extend client nonce")
	}
	salt, err := base64.StdEncoding.DecodeString(saltB64)
	if err != nil {
		return fmt.Errorf("mdb: bad salt: %w", err)
	}

	salted := pbkdf2SHA256([]byte(password), salt, iterations)
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)

	withoutProof := "c=biws,r=" + serverNonce
	authMessage := "n=" + username + ",r=" + nonce + "," + serverFirst + "," + withoutProof
	clientSignature := hmacSHA256(storedKey[:], []byte(authMessage))
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ clientSignature[i]
	}
	clientFinal := withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)

	cont, err := db.Command(map[string]interface{}{
		"saslContinue":   1,
		"conversationId": conversationID,
		"payload":        native.Binary{Data: []byte(clientFinal), SubType: 0},
	})
	if err != nil {
		return fmt.Errorf("mdb: saslContinue: %w", err)
	}
	if err := commandErr(cont); err != nil {
		return fmt.Errorf("mdb: saslContinue: %w", err)
	}

	// 服务端要收到最后一条空 payload 的 saslContinue 才算完成握手。
	if done, _ := cont["done"].(bool); !done {
		doneReply, err := db.Command(map[string]interface{}{
			"saslContinue":   1,
			"conversationId": conversationID,
			"payload":        native.Binary{Data: []byte{}, SubType: 0},
		})
		if err != nil {
			return fmt.Errorf("mdb: saslContinue (final): %w", err)
		}
		if err := commandErr(doneReply); err != nil {
			return fmt.Errorf("mdb: saslContinue (final): %w", err)
		}
	}
	return nil
}

func payloadString(reply map[string]interface{}) (string, error) {
	switch v := reply["payload"].(type) {
	case native.Binary:
		return string(v.Data), nil
	case []byte:
		return string(v), nil
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("mdb: missing binary payload in reply")
	}
}

// commandErr 把 `{ok: 0, errmsg: ...}` 变成 Go error。
func commandErr(reply map[string]interface{}) error {
	if ok, _ := reply["ok"].(float64); ok == 1 {
		return nil
	}
	msg, _ := reply["errmsg"].(string)
	if msg == "" {
		msg = "command failed"
	}
	return fmt.Errorf("mdb: %s", msg)
}

func randomNonce() (string, error) {
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("mdb: generate nonce: %w", err)
	}
	return base64.RawStdEncoding.EncodeToString(b), nil
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

// pbkdf2SHA256 标准库没有 sha256 版本的 pbkdf2，这里按 RFC 2898 展开。
func pbkdf2SHA256(password, salt []byte, iter int) []byte {
	// HMAC_SHA256(password, salt || INT32_BE(i))，逐块异或，取前 32 字节。
	out := make([]byte, 32)
	block := make([]byte, len(salt)+4)
	copy(block, salt)
	block[len(salt)+0] = 0
	block[len(salt)+1] = 0
	block[len(salt)+2] = 0
	block[len(salt)+3] = 1

	u := hmacSHA256(password, block)
	t := make([]byte, len(u))
	copy(t, u)
	for i := 1; i < iter; i++ {
		u = hmacSHA256(password, u)
		for j := range t {
			t[j] ^= u[j]
		}
	}
	copy(out, t)
	return out
}
