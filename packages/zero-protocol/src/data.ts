import {jsonSchema} from '../../shared/src/json-schema.js';
import * as v from '../../shared/src/valita.js';

export const valueSchema = v.union(jsonSchema, v.undefined());

export const rowSchema = v.record(valueSchema);

/**
 * The data types that Zero can represent are limited by two things:
 *
 * 1. The underlying Replicache sync layer currently can only represent JSON
 *    types. This could possibly be expanded in the future, but we do want to be
 *    careful of adding encoding overhead. By using JSON, we are taking
 *    advantage of IndexedDB’s fast native JSValue [de]serialization which has
 *    historically been a perf advantage for us.
 *
 * 2. IDs in Zero need to be comparable because we use them for sorting and row
 *    identity. We could expand the set of allowed value types (to include,
 *    i.e., Objects) but we would then need to restrict IDs to only comparable
 *    types.
 *
 * These two facts leave us with the following allowed types. Zero's replication
 * layer must convert other types into these for tables to be used with Zero.
 *
 * For developer convenience we also allow `undefined`, which we treat
 * equivalently to `null`.
 */
export type Value = v.Infer<typeof valueSchema>;

/**
 * A Row is represented as a JS Object.
 *
 * We do everything in IVM as loosely typed values because these pipelines are
 * going to be constructed at runtime by other code, so type-safety can't buy us
 * anything.
 *
 * Also since the calling code on the client ultimately wants objects to work
 * with we end up with a lot less copies by using objects throughout.
 */
export type Row = v.Infer<typeof rowSchema>;
