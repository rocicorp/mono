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
export const REGPROC = 24;
export const TEXT = 25;
export const OID = 26;
export const TID = 27;
export const XID = 28;
export const CID = 29;
export const JSON = 114;
export const XML = 142;
export const PG_NODE_TREE = 194;
export const SMGR = 210;
export const PATH = 602;
export const POLYGON = 604;
export const CIDR = 650;
export const FLOAT4 = 700;
export const FLOAT8 = 701;
export const ABSTIME = 702;
export const RELTIME = 703;
export const TINTERVAL = 704;
export const CIRCLE = 718;
export const MACADDR8 = 774;
export const MONEY = 790;
export const MACADDR = 829;
export const INET = 869;
export const ACLITEM = 1033;
export const BPCHAR = 1042;
export const VARCHAR = 1043;
export const DATE = 1082;
export const TIME = 1083;
export const TIMESTAMP = 1114;
export const TIMESTAMPTZ = 1184;
export const INTERVAL = 1186;
export const TIMETZ = 1266;
export const BIT = 1560;
export const VARBIT = 1562;
export const NUMERIC = 1700;
export const REFCURSOR = 1790;
export const REGPROCEDURE = 2202;
export const REGOPER = 2203;
export const REGOPERATOR = 2204;
export const REGCLASS = 2205;
export const REGTYPE = 2206;
export const UUID = 2950;
export const TXID_SNAPSHOT = 2970;
export const PG_LSN = 3220;
export const PG_NDISTINCT = 3361;
export const PG_DEPENDENCIES = 3402;
export const TSVECTOR = 3614;
export const TSQUERY = 3615;
export const GTSVECTOR = 3642;
export const REGCONFIG = 3734;
export const REGDICTIONARY = 3769;
export const JSONB = 3802;
export const REGNAMESPACE = 4089;
export const REGROLE = 4096;
