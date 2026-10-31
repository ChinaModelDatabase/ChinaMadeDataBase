package com.mdb.driver;

/**
 * Write concern levels for MDB write operations.
 *
 * <p>Controls the guarantee that MongoDB provides when reporting on the success of a write operation.</p>
 */
public enum MdbWriteConcern {

    /** Errors are not reported by the server. */
    UNACKNOWLEDGED(com.mongodb.WriteConcern.UNACKNOWLEDGED),

    /** Errors are reported by the server. */
    ACKNOWLEDGED(com.mongodb.WriteConcern.ACKNOWLEDGED),

    /** Errors are reported by the server; waits for majority of replica set members. */
    MAJORITY(com.mongodb.WriteConcern.MAJORITY);

    private final com.mongodb.WriteConcern underlying;

    MdbWriteConcern(com.mongodb.WriteConcern underlying) {
        this.underlying = underlying;
    }

    /**
     * Returns the underlying MongoDB WriteConcern.
     *
     * @return the MongoDB WriteConcern
     */
    public com.mongodb.WriteConcern toWriteConcern() {
        return underlying;
    }
}
