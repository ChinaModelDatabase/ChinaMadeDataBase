// Package mdb provides a Go driver for MDB (Xinchuang Document Database).
//
// MDB is wire-protocol compatible with MongoDB 7.0. This driver wraps the
// official mongo-go-driver with MDB-specific extensions, providing a
// familiar API for Go developers while leveraging MDB's high-performance
// storage engine.
//
// Basic usage:
//
//	client, err := mdb.NewClient("localhost", 27017)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close(context.Background())
//
//	db := client.Database("mydb")
//	coll := db.Collection("mycollection")
//
// Quick start with authentication:
//
//	client, err := mdb.NewClientWithAuth("localhost", 27017, "admin", "password", "admin")
package mdb
