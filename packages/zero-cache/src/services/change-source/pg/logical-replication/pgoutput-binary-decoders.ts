import {
  decodeArrayValues,
  decodeDate,
  decodeNumeric,
  decodeTime,
  decodeTimestamp,
  decodeTimeTZ,
  decodeUUID,
} from '../../../../db/pg-copy-binary.ts';
import {
  BOOL,
  BPCHAR,
  BYTEA,
  CHAR,
  DATE,
  FLOAT4,
  FLOAT8,
  INT2,
  INT4,
  INT8,
  JSON as JSON_OID,
  JSONB,
  NUMERIC,
  TEXT,
  TIME,
  TIMESTAMP,
  TIMESTAMPTZ,
  TIMETZ,
  UUID,
  VARCHAR,
} from '../../../../types/pg-types.ts';
import type {PostgresDB} from '../../../../types/pg.ts';

export type BinaryDecoder = (buf: Buffer) => unknown;

export interface BinaryDecoders {
  getBinaryDecoder(typeOID: number): BinaryDecoder | undefined;
}

// Well-known array OIDs in PostgreSQL pg_type
const KNOWN_ARRAY_OIDS: readonly number[] = [
  1000, // _bool
  1001, // _bytea
  1002, // _char
  1005, // _int2
  1007, // _int4
  1009, // _text
  1014, // _bpchar
  1015, // _varchar
  1016, // _int8
  1021, // _float4
  1022, // _float8
  1115, // _timestamp
  1182, // _date
  1183, // _time
  1185, // _timestamptz
  1231, // _numeric
  1270, // _timetz
  199, // _json
  2951, // _uuid
  3807, // _jsonb
];

/**
 * Creates static decoders for standard PostgreSQL data types and built-in arrays.
 */
export function createStaticBinaryDecoders(): Map<number, BinaryDecoder> {
  const decoders = new Map<number, BinaryDecoder>();

  // Boolean: 1 byte (0 = false, 1 = true). Returns native boolean for RowValue.
  decoders.set(BOOL, buf => buf[0] !== 0);

  // Integers: Big-endian binary signed integers.
  decoders.set(INT2, buf => buf.readInt16BE(0));
  decoders.set(INT4, buf => buf.readInt32BE(0));
  decoders.set(INT8, buf => buf.readBigInt64BE(0));

  // Floats: IEEE 754 Big-endian.
  decoders.set(FLOAT4, buf => buf.readFloatBE(0));
  decoders.set(FLOAT8, buf => buf.readDoubleBE(0));

  // Textual: UTF-8 strings.
  const decodeText: BinaryDecoder = buf => buf.toString('utf8');
  decoders.set(TEXT, decodeText);
  decoders.set(VARCHAR, decodeText);
  decoders.set(BPCHAR, decodeText);
  decoders.set(CHAR, decodeText);

  // UUID: 16 bytes raw hex.
  decoders.set(UUID, buf => decodeUUID(buf));

  // Bytea: raw byte array.
  decoders.set(BYTEA, buf => new Uint8Array(buf));

  // JSON: Raw UTF-8 JSON string.
  decoders.set(JSON_OID, decodeText);

  // JSONB: 1-byte version header (0x01) followed by UTF-8 JSON string.
  decoders.set(JSONB, buf => buf.toString('utf8', 1));

  // Temporal: Milliseconds since Unix epoch.
  decoders.set(TIMESTAMP, buf => decodeTimestamp(buf));
  decoders.set(TIMESTAMPTZ, buf => decodeTimestamp(buf));
  decoders.set(DATE, buf => decodeDate(buf));
  decoders.set(TIME, buf => decodeTime(buf));
  decoders.set(TIMETZ, buf => decodeTimeTZ(buf));

  // Numeric: Decoded into JS number.
  decoders.set(NUMERIC, buf => decodeNumeric(buf));

  // Standard Arrays: Parsed into JS arrays.
  const decodeArray: BinaryDecoder = buf => decodeArrayValues(buf);
  for (const arrayOid of KNOWN_ARRAY_OIDS) {
    decoders.set(arrayOid, decodeArray);
  }

  return decoders;
}

/**
 * Creates a `BinaryDecoders` instance with static decoders for standard types.
 */
export function createStaticBinaryDecodersInstance(): BinaryDecoders {
  const decoders = createStaticBinaryDecoders();
  return {
    getBinaryDecoder: typeOID => decoders.get(typeOID),
  };
}

/**
 * Fetches enum and array type metadata from PostgreSQL `pg_type` and returns
 * a `BinaryDecoders` instance containing decoders for standard and custom types.
 */
export async function getBinaryDecoders(
  db: PostgresDB,
): Promise<BinaryDecoders> {
  const decoders = createStaticBinaryDecoders();

  try {
    const customTypes = await db<
      {oid: number; typtype: string; typelem: number}[]
    >`
      SELECT oid, typtype, typelem
      FROM pg_type
      WHERE typtype = 'e' OR typelem > 0
    `;

    for (const {oid, typtype, typelem} of customTypes) {
      if (typtype === 'e') {
        // Enums in PG binary logical replication are sent as UTF-8 strings.
        decoders.set(oid, buf => buf.toString('utf8'));
      } else if (typelem > 0 && !decoders.has(oid)) {
        // Custom array types (e.g. arrays of enums or domains).
        decoders.set(oid, buf => decodeArrayValues(buf));
      }
    }
  } catch {
    // If pg_type query fails (e.g. permission or mocked db), static decoders remain intact.
  }

  return {
    getBinaryDecoder: typeOID => decoders.get(typeOID),
  };
}
