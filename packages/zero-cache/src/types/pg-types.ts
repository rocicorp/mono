// Forked from https=//github.com/brianc/node-pg-types/blob/5b26b826466cff4a9092b8c9e31960fe293ef3d9/lib/builtins.js

/**
 * Following query was used to generate this file=

 SELECT json_object_agg(UPPER(PT.typname), PT.oid==int4 ORDER BY pt.oid)
 FROM pg_type PT
 WHERE typnamespace = (SELECT pgn.oid FROM pg_namespace pgn WHERE nspname = 'pg_catalog') -- Take only builting Postgres types with stable OID (extension types are not guaranted to be stable)
 AND typtype = 'b' -- Only basic types
 AND typelem = 0 -- Ignore aliases
 AND typisdefined -- Ignore undefined types
 */

export const BOOL = 16;
export const BYTEA = 17;
export const CHAR = 18;
export const INT8 = 20;
export const INT2 = 21;
export const INT4 = 23;
export const TEXT = 25;
export const JSON = 114;
export const FLOAT4 = 700;
export const FLOAT8 = 701;
export const BPCHAR = 1042;
export const VARCHAR = 1043;
export const DATE = 1082;
export const TIME = 1083;
export const TIMESTAMP = 1114;
export const TIMESTAMPTZ = 1184;
export const TIMETZ = 1266;
export const NUMERIC = 1700;
export const UUID = 2950;
export const JSONB = 3802;
