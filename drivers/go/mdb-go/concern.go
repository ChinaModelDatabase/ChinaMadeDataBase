package mdb

// WriteConcern describes the level of durability for write operations.
type WriteConcern struct {
	Level string
}

// ReadConcern describes the level of isolation for read operations.
type ReadConcern struct {
	Level string
}

// Level returns the write concern level string.
func (wc WriteConcern) Level() string {
	return wc.Level
}

// Level returns the read concern level string.
func (rc ReadConcern) Level() string {
	return rc.Level
}

// Common write concern levels.
var (
	// WriteConcernAcknowledged requests acknowledgment that write operations
	// have been written to the primary MongoDB server.
	WriteConcernAcknowledged = WriteConcern{Level: "acknowledged"}

	// WriteConcernMajority requests acknowledgment that write operations have
	// been propagated to the majority of the servers in the replica set.
	WriteConcernMajority = WriteConcern{Level: "majority"}
)

// Common read concern levels.
var (
	// ReadConcernLocal returns the node's most recent data. This provides the
	// lowest latency reads but data may be rolled back on failover.
	ReadConcernLocal = ReadConcern{Level: "local"}

	// ReadConcernMajority returns data acknowledged by a majority of servers.
	ReadConcernMajority = ReadConcern{Level: "majority"}

	// ReadConcernLinearizable returns data that is acknowledged by a majority
	// of servers and can only be read after the most recent committed write.
	ReadConcernLinearizable = ReadConcern{Level: "linearizable"}

	// ReadConcernSnapshot returns data from a snapshot of the data at a point
	// in time. Only available on multi-document transactions.
	ReadConcernSnapshot = ReadConcern{Level: "snapshot"}
)
