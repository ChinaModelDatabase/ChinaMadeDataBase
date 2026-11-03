package com.mdb.driver;

import org.bson.conversions.Bson;
import com.mongodb.client.model.IndexOptions;

/**
 * Builder for MDB index options.
 *
 * <p>Provides a fluent API for constructing index creation options.</p>
 *
 * <pre>{@code
 * MdbIndexOptions options = new MdbIndexOptions()
 *     .unique(true)
 *     .sparse(true)
 *     .expireAfterSeconds(3600)
 *     .name("my_index");
 *
 * collection.createIndex(ascending("field"), options.build());
 * }</pre>
 */
public class MdbIndexOptions {

    private boolean unique = false;
    private boolean sparse = false;
    private int expireAfterSeconds = -1;
    private String name = null;
    private boolean background = false;

    /**
     * Sets whether the index should enforce a uniqueness constraint.
     *
     * @param unique true to create a unique index
     * @return this builder
     */
    public MdbIndexOptions unique(boolean unique) {
        this.unique = unique;
        return this;
    }

    /**
     * Sets whether the index should only reference documents that have the indexed field.
     *
     * @param sparse true to create a sparse index
     * @return this builder
     */
    public MdbIndexOptions sparse(boolean sparse) {
        this.sparse = sparse;
        return this;
    }

    /**
     * Sets the time-to-live in seconds for a TTL index. Documents older than this
     * age are automatically removed.
     *
     * @param seconds the number of seconds before a document expires
     * @return this builder
     */
    public MdbIndexOptions expireAfterSeconds(int seconds) {
        this.expireAfterSeconds = seconds;
        return this;
    }

    /**
     * Sets the name of the index.
     *
     * @param name the index name
     * @return this builder
     */
    public MdbIndexOptions name(String name) {
        this.name = name;
        return this;
    }

    /**
     * Sets whether the index should be built in the background.
     *
     * @param background true to build in background
     * @return this builder
     */
    public MdbIndexOptions background(boolean background) {
        this.background = background;
        return this;
    }

    /**
     * Builds the MongoDB {@link IndexOptions} from the configured values.
     *
     * @return a new IndexOptions instance
     */
    public IndexOptions build() {
        IndexOptions opts = new IndexOptions();
        if (unique) {
            opts.unique(true);
        }
        if (sparse) {
            opts.sparse(true);
        }
        if (expireAfterSeconds >= 0) {
            opts.expireAfter((long) expireAfterSeconds, java.util.concurrent.TimeUnit.SECONDS);
        }
        if (name != null) {
            opts.name(name);
        }
        if (background) {
            opts.background(true);
        }
        return opts;
    }
}
