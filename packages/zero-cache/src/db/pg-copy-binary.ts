import {stringify} from '../../../shared/src/bigint-json.ts';
import type {LiteValueType} from '../types/lite.ts';
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
  JSONB,
  NUMERIC,
  TEXT,
  TIME,
  TIMESTAMP,
  TIMESTAMPTZ,
  TIMETZ,
  UUID,
  VARCHAR,
} from '../types/pg-types.ts';
import {JSON as JSON_OID} from '../types/pg-types.ts';
import * as PostgresTypeClass from './postgres-type-class-enum.ts';
import type {ColumnSpec} from './specs.ts';

// PostgreSQL COPY binary format signature: "PGCOPY\n\xff\r\n\0"
const PGCOPY_SIGNATURE = Buffer.from([
  0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00,
]);

const HEADER_MIN_SIZE = 11 + 4 + 4; // signature + flags + extension length

// PostgreSQL epoch is 2000-01-01T00:00:00Z.
// Offset from Unix epoch (1970-01-01) in milliseconds.
const PG_EPOCH_UNIX_MILLIS = 946_684_800_000;

// Days from Unix epoch (1970-01-01) to PG epoch (2000-01-01).
const PG_EPOCH_UNIX_DAYS = 10_957;

const MS_PER_DAY = 86_400_000;

// Sentinel values for infinity in PG binary format (as hi/lo int32 pairs).
const PG_TIMESTAMP_INF_HI = 0x7fffffff;
const PG_TIMESTAMP_INF_LO = 0xffffffff;
const PG_TIMESTAMP_NEG_INF_HI = -0x80000000; // readInt32BE of 0x80000000
const PG_TIMESTAMP_NEG_INF_LO = 0;
const PG_DATE_INFINITY = 0x7fffffff;
const PG_DATE_NEG_INFINITY = -0x80000000;

/**
 * Streaming parser for PostgreSQL `COPY ... TO STDOUT WITH (FORMAT binary)`.
 *
 * Analogous to {@link import('./pg-copy.ts').TsvParser} but for binary format.
 * Yields `Buffer | null` per field (null = SQL NULL).
 *
 * The caller tracks column position the same way as with TsvParser.
 */
export class BinaryCopyParser {
  #buffer: Buffer = Buffer.alloc(0);
  #offset = 0;
  #headerParsed = false;
  #fieldsRemaining = 0; // fields left in current tuple (0 = need new tuple header)

  *parse(chunk: Buffer): Iterable<Buffer | null> {
    this.#append(chunk);

    if (!this.#headerParsed) {
      if (!this.#tryParseHeader()) {
        return;
      }
    }

    for (;;) {
      // If we're at the start of a tuple, read the field count.
      if (this.#fieldsRemaining === 0) {
        if (this.#remaining() < 2) {
          break;
        }
        const fieldCount = this.#buffer.readInt16BE(this.#offset);
        if (fieldCount === -1) {
          // Trailer marker — end of data.
          break;
        }
        this.#offset += 2;
        this.#fieldsRemaining = fieldCount;
      }

      // Parse fields within the current tuple.
      while (this.#fieldsRemaining > 0) {
        if (this.#remaining() < 4) {
          // Not enough data for field length — wait for next chunk.
          this.#compact();
          return;
        }
        const fieldLen = this.#buffer.readInt32BE(this.#offset);
        this.#offset += 4;

        if (fieldLen === -1) {
          // NULL field.
          yield null;
        } else {
          if (this.#remaining() < fieldLen) {
            // Not enough data for field value — rewind past the length
            // we just read and wait for more data.
            this.#offset -= 4;
            this.#compact();
            return;
          }
          yield this.#buffer.subarray(this.#offset, this.#offset + fieldLen);
          this.#offset += fieldLen;
        }
        this.#fieldsRemaining--;
      }
    }

    this.#compact();
  }

  #remaining(): number {
    return this.#buffer.length - this.#offset;
  }

  #append(chunk: Buffer): void {
    if (this.#buffer.length === this.#offset) {
      // Fully consumed — replace.
      this.#buffer = chunk;
      this.#offset = 0;
    } else {
      // Concatenate unconsumed remainder with new chunk.
      this.#buffer = Buffer.concat([
        this.#buffer.subarray(this.#offset),
        chunk,
      ]);
      this.#offset = 0;
    }
  }

  #compact(): void {
    if (this.#offset > 0) {
      this.#buffer = this.#buffer.subarray(this.#offset);
      this.#offset = 0;
    }
  }

  #tryParseHeader(): boolean {
    if (this.#remaining() < HEADER_MIN_SIZE) {
      return false;
    }

    // Validate signature.
    for (let i = 0; i < PGCOPY_SIGNATURE.length; i++) {
      if (this.#buffer[this.#offset + i] !== PGCOPY_SIGNATURE[i]) {
        throw new Error('Invalid PGCOPY binary signature');
      }
    }
    this.#offset += 11;

    // Flags (int32) — currently only bit 16 (has OID column) is defined.
    // We don't use OID columns, so just skip.
    const flags = this.#buffer.readInt32BE(this.#offset);
    this.#offset += 4;
    if (flags !== 0) {
      throw new Error(`Unsupported PGCOPY flags: ${flags}`);
    }

    // Extension area length (int32).
    const extensionLen = this.#buffer.readInt32BE(this.#offset);
    this.#offset += 4;

    // Skip extension data if present.
    if (extensionLen > 0) {
      if (this.#remaining() < extensionLen) {
        // Rewind and wait for more data.
        this.#offset -= HEADER_MIN_SIZE;
        return false;
      }
      this.#offset += extensionLen;
    }

    this.#headerParsed = true;
    return true;
  }
}

// ---- Binary Type Decoders ----

export type BinaryDecoder = (buf: Buffer) => LiteValueType;

type BinaryColumnSpec = Pick<
  ColumnSpec,
  'dataType' | 'pgTypeClass' | 'elemPgTypeClass'
> & {typeOID: number};

const KNOWN_BINARY_OIDS = new Set([
  BOOL,
  INT2,
  INT4,
  INT8,
  FLOAT4,
  FLOAT8,
  TEXT,
  VARCHAR,
  BPCHAR,
  CHAR,
  UUID,
  BYTEA,
  JSON_OID,
  JSONB,
  TIMESTAMP,
  TIMESTAMPTZ,
  DATE,
  TIME,
  TIMETZ,
  NUMERIC,
]);

/**
 * Returns true if the column's binary format is known and can be decoded
 * natively. For columns where this returns false, the COPY SELECT should
 * cast the column to `::text` so PG sends the text representation inside
 * the binary frame.
 */
export function hasBinaryDecoder(spec: BinaryColumnSpec): boolean {
  if (spec.elemPgTypeClass !== null && spec.elemPgTypeClass !== undefined) {
    return true; // Array types
  }
  if (spec.pgTypeClass === PostgresTypeClass.Enum) {
    return true; // Enums are sent as UTF-8 text in binary format
  }
  return KNOWN_BINARY_OIDS.has(spec.typeOID);
}

/** Decoder for columns cast to `::text` in the COPY SELECT. */
export const textCastDecoder: BinaryDecoder = buf => buf.toString('utf8');

/**
 * Creates a specialized binary decoder for the given column spec.
 * The returned function converts a raw COPY binary field `Buffer`
 * directly to a `LiteValueType`, bypassing text parsing entirely.
 *
 * Only call this for columns where {@link hasBinaryDecoder} returns true.
 * For other columns, cast to `::text` in the SELECT and use
 * {@link textCastDecoder}.
 */
export function makeBinaryDecoder(spec: BinaryColumnSpec): BinaryDecoder {
  const {typeOID, pgTypeClass, elemPgTypeClass} = spec;

  // Array types: elemPgTypeClass is non-null for arrays.
  if (elemPgTypeClass !== null && elemPgTypeClass !== undefined) {
    return buf => decodeArray(buf);
  }

  // Enum types: binary representation is UTF-8 text.
  if (pgTypeClass === PostgresTypeClass.Enum) {
    return buf => buf.toString('utf8');
  }

  switch (typeOID) {
    case BOOL:
      return buf => (buf[0] ? 1 : 0);
    case INT2:
      return buf => buf.readInt16BE(0);
    case INT4:
      return buf => buf.readInt32BE(0);
    case INT8:
      return buf => buf.readBigInt64BE(0);
    case FLOAT4:
      return buf => buf.readFloatBE(0);
    case FLOAT8:
      return buf => buf.readDoubleBE(0);
    case TEXT:
    case VARCHAR:
    case BPCHAR:
    case CHAR:
      return buf => buf.toString('utf8');
    case UUID:
      return buf => decodeUUID(buf);
    case BYTEA:
      return buf => Uint8Array.prototype.slice.call(buf) as Uint8Array;
    case JSON_OID:
      return buf => buf.toString('utf8');
    case JSONB:
      // JSONB binary format has a 1-byte version prefix (currently 0x01).
      return buf => buf.toString('utf8', 1);
    case TIMESTAMP:
    case TIMESTAMPTZ:
      return buf => decodeTimestamp(buf);
    case DATE:
      return buf => decodeDate(buf);
    case TIME:
      return buf => decodeTime(buf);
    case TIMETZ:
      return buf => decodeTimeTZ(buf);
    case NUMERIC:
      return buf => decodeNumeric(buf);
    default:
      throw new Error(
        `No binary decoder for type OID ${typeOID}. ` +
          `Use hasBinaryDecoder() to check before calling makeBinaryDecoder().`,
      );
  }
}

// ---- Individual Decoders (exported for testing) ----

/**
 * UUID: 16 bytes → "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
 */
export function decodeUUID(buf: Buffer): string {
  const hex = buf.toString('hex');
  return (
    hex.substring(0, 8) +
    '-' +
    hex.substring(8, 12) +
    '-' +
    hex.substring(12, 16) +
    '-' +
    hex.substring(16, 20) +
    '-' +
    hex.substring(20, 32)
  );
}

/**
 * TIMESTAMP / TIMESTAMPTZ: int64 microseconds since PG epoch (2000-01-01 UTC)
 * → floating-point milliseconds since Unix epoch.
 *
 * Matches the output of `timestampToFpMillis()` in `types/pg.ts`.
 *
 * Uses Number arithmetic (avoiding BigInt) for speed. The microsecond value
 * fits safely in a Number for all practical dates (up to ~year 285,000).
 */
export function decodeTimestamp(buf: Buffer): number {
  const hi = buf.readInt32BE(0);
  const lo = buf.readUInt32BE(4);
  if (hi === PG_TIMESTAMP_INF_HI && lo === PG_TIMESTAMP_INF_LO) return Infinity;
  if (hi === PG_TIMESTAMP_NEG_INF_HI && lo === PG_TIMESTAMP_NEG_INF_LO) {
    return -Infinity;
  }
  const microseconds = hi * 0x100000000 + lo;
  return microseconds / 1000 + PG_EPOCH_UNIX_MILLIS;
}

/**
 * DATE: int32 days since PG epoch (2000-01-01) → millis since Unix epoch at
 * UTC midnight. Matches `dateToUTCMidnight()` in `types/pg.ts`.
 */
export function decodeDate(buf: Buffer): number {
  const pgDays = buf.readInt32BE(0);
  if (pgDays === PG_DATE_INFINITY) return Infinity;
  if (pgDays === PG_DATE_NEG_INFINITY) return -Infinity;
  return (pgDays + PG_EPOCH_UNIX_DAYS) * MS_PER_DAY;
}

/**
 * TIME: int64 microseconds since midnight → milliseconds since midnight.
 * Matches `postgresTimeToMilliseconds()` in `types/pg.ts`.
 *
 * Max value is 86,400,000,000 (~8.6e10), well within Number.MAX_SAFE_INTEGER.
 */
export function decodeTime(buf: Buffer): number {
  const hi = buf.readInt32BE(0);
  const lo = buf.readUInt32BE(4);
  const micros = hi * 0x100000000 + lo;
  return Math.trunc(micros / 1000);
}

/**
 * TIMETZ: int64 microseconds since midnight + int32 timezone offset in seconds.
 * PG stores the offset with inverted sign from ISO (POSIX convention):
 * positive = west of UTC, negative = east of UTC.
 * UTC = local_time + pg_offset.
 * → UTC milliseconds since midnight.
 *
 * Max value ~1.3e11 microseconds, well within Number.MAX_SAFE_INTEGER.
 */
export function decodeTimeTZ(buf: Buffer): number {
  const hi = buf.readInt32BE(0);
  const lo = buf.readUInt32BE(4);
  const localMicros = hi * 0x100000000 + lo;
  const tzOffsetSeconds = buf.readInt32BE(8);
  const utcMicros = localMicros + tzOffsetSeconds * 1_000_000;
  let ms = Math.trunc(utcMicros / 1000);
  // Normalize to [0, MS_PER_DAY).
  if (ms < 0 || ms >= MS_PER_DAY) {
    ms = ((ms % MS_PER_DAY) + MS_PER_DAY) % MS_PER_DAY;
  }
  return ms;
}

// NUMERIC binary format constants.
const NUMERIC_NEG = 0x4000;
const NUMERIC_NAN = 0xc000;
const NUMERIC_PINF = 0xd000;
const NUMERIC_NINF = 0xf000;
const NBASE = 10_000;

/**
 * NUMERIC: variable-length binary format.
 * Header: {ndigits: int16, weight: int16, sign: int16, dscale: int16}
 * Followed by ndigits x int16 base-10000 digits.
 *
 * Converts to a JS `number` (matching the text path's `Number(x)` behavior).
 */
export function decodeNumeric(buf: Buffer): number {
  const ndigits = buf.readInt16BE(0);
  const weight = buf.readInt16BE(2);
  const sign = buf.readUInt16BE(4);
  // const dscale = buf.readInt16BE(6); // display scale, not needed for value

  if (sign === NUMERIC_NAN) {
    return NaN;
  }
  if (sign === NUMERIC_PINF) {
    return Infinity;
  }
  if (sign === NUMERIC_NINF) {
    return -Infinity;
  }
  if (ndigits === 0) {
    return 0;
  }

  // Accumulate base-10000 digits into an integer, then do a single
  // division at the end. Repeated `scale /= NBASE` accumulates
  // floating-point error (e.g. 9900 * 0.0001 = 0.9900000000000001).
  // A single division lets IEEE 754 round to the nearest double,
  // matching the text path's `Number("0.99")` behavior.
  //
  // For numerics with many digits (ndigits > 3), intVal can exceed
  // MAX_SAFE_INTEGER. In that case, fall back to building a string
  // and using Number() to match the text path exactly.
  if (ndigits > 3) {
    return decodeNumericViaString(buf, ndigits, weight, sign);
  }

  let intVal = 0;
  for (let i = 0; i < ndigits; i++) {
    intVal = intVal * NBASE + buf.readInt16BE(8 + i * 2);
  }

  // weight indicates the power-of-NBASE of the first digit.
  // shift is how many base-10000 positions to divide by.
  const shift = ndigits - weight - 1;
  let result;
  if (shift > 0) {
    result = intVal / NBASE ** shift;
  } else if (shift < 0) {
    result = intVal * NBASE ** -shift;
  } else {
    result = intVal;
  }
  return sign === NUMERIC_NEG ? -result : result;
}

/**
 * Fallback for numerics with many base-10000 digits where accumulating
 * into an integer would exceed MAX_SAFE_INTEGER. Builds the decimal
 * string and uses Number() to match the text path exactly.
 */
function decodeNumericViaString(
  buf: Buffer,
  ndigits: number,
  weight: number,
  sign: number,
): number {
  // Number of base-10000 digit groups before the decimal point.
  const intGroups = weight + 1;

  let str = '';
  for (let i = 0; i < ndigits; i++) {
    const digit = buf.readInt16BE(8 + i * 2);
    if (i === intGroups) {
      str = str || '0';
      str += '.';
    }
    str += i === 0 ? String(digit) : String(digit).padStart(4, '0');
  }

  // Append trailing zero groups if the integer part extends beyond ndigits.
  if (intGroups > ndigits) {
    str += '0'.repeat((intGroups - ndigits) * 4);
  }

  return Number((sign === NUMERIC_NEG ? '-' : '') + str);
}

/**
 * Array: binary format.
 *
 * Header:
 *   int32 ndim       — number of dimensions (0 for empty array)
 *   int32 flags      — 0 or 1 (has-nulls)
 *   int32 elem_oid   — OID of element type
 *   Per dimension:
 *     int32 dim_size  — number of elements in this dimension
 *     int32 dim_lb    — lower bound (usually 1)
 *
 * Then for each element (in row-major order):
 *   int32 length     — -1 for NULL, otherwise byte length
 *   bytes            — element data
 *
 * Result is JSON.stringify'd for storage in SQLite (matching text path behavior).
 */
export function decodeArray(buf: Buffer): string {
  let offset = 0;

  const ndim = buf.readInt32BE(offset);
  offset += 4;
  // skip flags (has-nulls)
  offset += 4;
  const elemOid = buf.readInt32BE(offset);
  offset += 4;

  if (ndim === 0) {
    return '[]';
  }

  // Read dimension sizes.
  const dims: number[] = [];
  for (let d = 0; d < ndim; d++) {
    dims.push(buf.readInt32BE(offset));
    offset += 4;
    // skip lower bound
    offset += 4;
  }

  const elemDecoder = makeElementDecoder(elemOid);

  // Recursively build the nested array structure.
  function readDimension(dim: number): unknown[] {
    const size = dims[dim];
    const arr: unknown[] = [];
    for (let i = 0; i < size; i++) {
      if (dim < ndim - 1) {
        arr.push(readDimension(dim + 1));
      } else {
        // Leaf dimension — read element.
        const elemLen = buf.readInt32BE(offset);
        offset += 4;
        if (elemLen === -1) {
          arr.push(null);
        } else {
          arr.push(elemDecoder(buf.subarray(offset, offset + elemLen)));
          offset += elemLen;
        }
      }
    }
    return arr;
  }

  const result = readDimension(0);
  return stringify(result);
}

/**
 * Creates a decoder for array elements. Array elements use the same
 * binary encoding as scalar columns, but we need to map the element
 * OID to the right decoder. Returns JS values (not LiteValueType)
 * since the result will be JSON.stringify'd.
 */
function makeElementDecoder(elemOid: number): (buf: Buffer) => unknown {
  switch (elemOid) {
    case BOOL:
      return buf => (buf[0] ? true : false);
    case INT2:
      return buf => buf.readInt16BE(0);
    case INT4:
      return buf => buf.readInt32BE(0);
    case INT8:
      return buf => {
        const val = buf.readBigInt64BE(0);
        // Use number if it fits safely, otherwise bigint for JSON.
        return val >= Number.MIN_SAFE_INTEGER && val <= Number.MAX_SAFE_INTEGER
          ? Number(val)
          : val;
      };
    case FLOAT4:
      return buf => buf.readFloatBE(0);
    case FLOAT8:
      return buf => buf.readDoubleBE(0);
    case TEXT:
    case VARCHAR:
    case BPCHAR:
    case CHAR:
      return buf => buf.toString('utf8');
    case UUID:
      return buf => decodeUUID(buf);
    case JSON_OID:
      return buf => JSON.parse(buf.toString('utf8'));
    case JSONB:
      return buf => JSON.parse(buf.toString('utf8', 1));
    case TIMESTAMP:
    case TIMESTAMPTZ:
      return buf => decodeTimestamp(buf);
    case DATE:
      return buf => decodeDate(buf);
    case TIME:
      return buf => decodeTime(buf);
    case TIMETZ:
      return buf => decodeTimeTZ(buf);
    case NUMERIC:
      return buf => decodeNumeric(buf);
    default:
      return buf => buf.toString('utf8');
  }
}
