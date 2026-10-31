import { IndexSpecification, Document, CreateIndexesOptions } from "mongodb";
import { CreateIndexOptions } from "./types";

/**
 * Index direction constants.
 */
export const ASC = 1 as const;
export const DESC = -1 as const;
export const GEO2D = "2d" as const;
export const GEOSPHERE = "2dsphere" as const;
export const HASHED = "hashed" as const;
export const TEXT = "text" as const;

/**
 * Create a simple ascending index specification.
 * @param field - The field name to index.
 */
export function ascending(field: string): IndexSpecification {
  return { [field]: ASC } as IndexSpecification;
}

/**
 * Create a simple descending index specification.
 * @param field - The field name to index.
 */
export function descending(field: string): IndexSpecification {
  return { [field]: DESC } as IndexSpecification;
}

/**
 * Create a compound index specification.
 * @param fields - Array of [fieldName, direction] tuples.
 */
export function compoundIndex(...fields: [string, 1 | -1 | "2d" | "2dsphere" | "hashed" | "text"][]): IndexSpecification {
  const spec: Document = {};
  for (const [field, dir] of fields) {
    spec[field] = dir;
  }
  return spec as IndexSpecification;
}

/**
 * Create a text index specification for full-text search.
 * @param field - The field to create a text index on.
 */
export function textIndex(field: string): IndexSpecification {
  return { [field]: TEXT } as IndexSpecification;
}

/**
 * Create a geospatial 2d index specification.
 * @param field - The field to index for 2d queries.
 */
export function geo2dIndex(field: string): IndexSpecification {
  return { [field]: GEO2D } as IndexSpecification;
}

/**
 * Create a geospatial 2dsphere index specification.
 * @param field - The field to index for 2dsphere queries.
 */
export function geoSphereIndex(field: string): IndexSpecification {
  return { [field]: GEOSPHERE } as IndexSpecification;
}

/**
 * Create a hashed index specification for hash-based sharding.
 * @param field - The field to hash.
 */
export function hashedIndex(field: string): IndexSpecification {
  return { [field]: HASHED } as IndexSpecification;
}

/**
 * Create a TTL index specification for automatic document expiration.
 * @param field - The date field for TTL.
 * @param expireAfterSeconds - Seconds after which the document expires.
 */
export function ttlIndex(field: string, expireAfterSeconds: number): CreateIndexOptions {
  return {
    expireAfterSeconds,
  };
}

/**
 * Create a unique index specification.
 * @param field - The field that must be unique.
 */
export function uniqueIndex(field: string): CreateIndexOptions {
  return {
    unique: true,
  };
}

/**
 * Create a sparse index specification (only indexes documents with the field).
 * @param field - The field to create a sparse index on.
 */
export function sparseIndex(field: string): CreateIndexOptions {
  return {
    sparse: true,
  };
}

/**
 * Create a partial filter expression for a partial index.
 * @param filter - The filter expression for partial index.
 */
export function partialFilter(filter: Document): CreateIndexOptions {
  return {
    partialFilterExpression: filter,
  };
}

/**
 * Build an index name from a specification following MongoDB conventions.
 * @param keys - The index keys specification.
 */
export function buildIndexName(keys: IndexSpecification): string {
  const parts: string[] = [];
  const spec = keys as Document;
  for (const [key, value] of Object.entries(spec)) {
    parts.push(`${key}_${value}`);
  }
  return parts.join("_");
}

/**
 * Common MDB-specific index helpers.
 */
export const MdbIndexes = {
  /**
   * Create an index on the _id field (already exists by default).
   */
  id: () => ascending("_id"),

  /**
   * Create a compound index on createdAt and _id for time-series queries.
   */
  createdAt: () => compoundIndex(["createdAt", DESC], ["_id", DESC]),

  /**
   * Create a text index on common search fields.
   */
  fullText: (...fields: string[]) => {
    const spec: Document = {};
    for (const field of fields) {
      spec[field] = TEXT;
    }
    return spec as IndexSpecification;
  },

  /**
   * Create a TTL index on an expiration field.
   */
  ttl: (field: string, seconds: number) => ttlIndex(field, seconds),
} as const;
