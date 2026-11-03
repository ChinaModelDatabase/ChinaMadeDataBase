package mdb

import (
	"fmt"
	"strings"

	native "github.com/mdb-official/mdb-native"
)

// Document 是驱动里的文档类型：底层就是 map，与 wire 层完全一致，
// 因此这个模块不需要任何第三方 BSON 库（也就不需要 go.sum）。
type Document = map[string]interface{}

// binarySubtype4 构造 UUID 子类型的二进制值（lsid 要求 subtype 4）。
//
// 必须是**真正的 BSON binary**：写成 extended JSON 的 `{$binary: ...}`
// 会被当成普通子文档，服务端就找不到会话了。
func binarySubtype4(data []byte) native.Binary {
	return native.Binary{Data: data, SubType: 4}
}

func joinStrings(parts []string, sep string) string { return strings.Join(parts, sep) }

// E 是有序文档的一个字段。
type E struct {
	Key   string
	Value interface{}
}

// D 是有序文档：索引键这类场景必须保序，map 会丢失顺序。
type D []E

// Map 转成 Document（键重复时后者覆盖）。
func (d D) Map() Document {
	out := make(Document, len(d))
	for _, e := range d {
		out[e.Key] = e.Value
	}
	return out
}

// Ascending 生成升序索引键。
func Ascending(keys ...string) D {
	return keySpec(keys, 1)
}

// Descending 生成降序索引键。
func Descending(keys ...string) D {
	return keySpec(keys, -1)
}

// TextIndex 生成文本索引键。
func TextIndex(keys ...string) D {
	return keySpec(keys, "text")
}

// GeoSphereIndex 生成 2dsphere 索引键。
func GeoSphereIndex(keys ...string) D {
	return keySpec(keys, "2dsphere")
}

// HashedIndex 生成哈希索引键。
func HashedIndex(keys ...string) D {
	return keySpec(keys, "hashed")
}

func keySpec(keys []string, value interface{}) D {
	d := make(D, len(keys))
	for i, k := range keys {
		d[i] = E{Key: k, Value: value}
	}
	return d
}

// toDocument 把调用方传入的 filter / doc 归一化为 Document。
//
// 支持 nil（空文档）、Document、map[string]interface{}、D（保序文档）。
func toDocument(v interface{}) (Document, error) {
	switch t := v.(type) {
	case nil:
		return Document{}, nil
	// Document 就是 map[string]interface{}：两个 case 写在一起会重复。
	case map[string]interface{}:
		if t == nil {
			return Document{}, nil
		}
		return Document(t), nil
	case D:
		return t.Map(), nil
	default:
		return nil, fmt.Errorf("mdb: unsupported document type %T", v)
	}
}

// toDocumentSlice 把 []interface{} 归一化成文档切片（insertMany 用）。
func toDocumentSlice(docs []interface{}) ([]map[string]interface{}, error) {
	out := make([]map[string]interface{}, 0, len(docs))
	for _, d := range docs {
		doc, err := toDocument(d)
		if err != nil {
			return nil, err
		}
		out = append(out, doc)
	}
	return out, nil
}

// toPipeline 把聚合管道归一化成文档切片。
func toPipeline(pipeline interface{}) ([]map[string]interface{}, error) {
	switch t := pipeline.(type) {
	case nil:
		return nil, nil
	case []map[string]interface{}:
		return t, nil
	case []interface{}:
		return toDocumentSlice(t)
	case D:
		return []map[string]interface{}{t.Map()}, nil
	default:
		return nil, fmt.Errorf("mdb: unsupported pipeline type %T", pipeline)
	}
}
