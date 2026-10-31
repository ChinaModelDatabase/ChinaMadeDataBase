package mdb

import "go.mongodb.org/mongo-driver/bson"

// Ascending creates an ascending index key specification for the given fields.
func Ascending(keys ...string) bson.D {
	d := make(bson.D, len(keys))
	for i, key := range keys {
		d[i] = bson.E{Key: key, Value: 1}
	}
	return d
}

// Descending creates a descending index key specification for the given fields.
func Descending(keys ...string) bson.D {
	d := make(bson.D, len(keys))
	for i, key := range keys {
		d[i] = bson.E{Key: key, Value: -1}
	}
	return d
}

// TextIndex creates a text index key specification for the given fields.
func TextIndex(keys ...string) bson.D {
	d := make(bson.D, len(keys))
	for i, key := range keys {
		d[i] = bson.E{Key: key, Value: "text"}
	}
	return d
}

// GeoSphereIndex creates a 2dsphere index key specification for the given fields.
func GeoSphereIndex(keys ...string) bson.D {
	d := make(bson.D, len(keys))
	for i, key := range keys {
		d[i] = bson.E{Key: key, Value: "2dsphere"}
	}
	return d
}

// HashedIndex creates a hashed index key specification for the given fields.
func HashedIndex(keys ...string) bson.D {
	d := make(bson.D, len(keys))
	for i, key := range keys {
		d[i] = bson.E{Key: key, Value: "hashed"}
	}
	return d
}
