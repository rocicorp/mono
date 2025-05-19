import {PreciseDate} from '@google-cloud/precise-date';
import {OID} from '@postgresql-typed/oids';
import {LogContext} from '@rocicorp/logger';
import postgres, {type Notice, type PostgresType} from 'postgres';
import {randInt} from '../../../shared/src/rand.ts';
import type {ValueType} from '../../../zero-protocol/src/client-schema.ts';
import {BigIntJSON, type JSONValue} from './bigint-json.ts';
import {
  DATE,
  JSON,
  JSONB,
  NUMERIC,
  TIMESTAMP,
  TIMESTAMPTZ,
} from './pg-types.ts';

// exported for testing.
export function timestampToFpMillis(timestamp: string): number {
  // Convert from PG's time string, e.g. "1999-01-08 12:05:06+00" to "Z"
  // format expected by PreciseDate.
  timestamp = timestamp.replace(' ', 'T');
  const positiveOffset = timestamp.includes('+');
  const tzSplitIndex = positiveOffset
    ? timestamp.lastIndexOf('+')
    : timestamp.indexOf('-', timestamp.indexOf('T'));
  const timezoneOffset =
    tzSplitIndex === -1 ? undefined : timestamp.substring(tzSplitIndex);
  const tsWithoutTimezone =
    (tzSplitIndex === -1 ? timestamp : timestamp.substring(0, tzSplitIndex)) +
    'Z';

  try {
    // PreciseDate does not return microsecond precision unless the provided
    // timestamp is in UTC time so we need to add the timezone offset back in.
    const fullTime = new PreciseDate(tsWithoutTimezone).getFullTime();
    const millis = Number(fullTime / 1_000_000n);
    const nanos = Number(fullTime % 1_000_000n);
    const ret = millis + nanos * 1e-6; // floating point milliseconds

    // add back in the timezone offset
    if (timezoneOffset) {
      const [hours, minutes] = timezoneOffset.split(':');
      const offset =
        Math.abs(Number(hours)) * 60 + (minutes ? Number(minutes) : 0);
      const offsetMillis = offset * 60 * 1_000;
      // If it is a positive offset, we subtract the offset from the UTC
      // because we passed in the "local time" as if it was UTC.
      // The opposite is true for negative offsets.
      return positiveOffset ? ret - offsetMillis : ret + offsetMillis;
    }
    return ret;
  } catch (e) {
    throw new Error(`Error parsing ${timestamp}`, {cause: e});
  }
}

function serializeTimestamp(val: unknown): string {
  switch (typeof val) {
    case 'string':
      return val; // Let Postgres parse it
    case 'number': {
      if (Number.isInteger(val)) {
        return new PreciseDate(val).toISOString();
      }
      // Convert floating point to bigint nanoseconds.
      const nanoseconds =
        1_000_000n * BigInt(Math.trunc(val)) +
        BigInt(Math.trunc((val % 1) * 1e6));
      return new PreciseDate(nanoseconds).toISOString();
    }
    // Note: Don't support bigint inputs until we decide what the semantics are (e.g. micros vs nanos)
    // case 'bigint':
    //   return new PreciseDate(val).toISOString();
    default:
      if (val instanceof Date) {
        return val.toISOString();
      }
  }
  throw new Error(`Unsupported type "${typeof val}" for timestamp: ${val}`);
}

function dateToUTCMidnight(date: string): number {
  const d = new Date(date);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

/**
 * The (javascript) types of objects that can be returned by our configured
 * Postgres clients. For initial-sync, these comes from the postgres.js client:
 *
 * https://github.com/porsager/postgres/blob/master/src/types.js
 *
 * and for the replication stream these come from the the node-postgres client:
 *
 * https://github.com/brianc/node-pg-types/blob/master/lib/textParsers.js
 */
export type PostgresValueType = JSONValue | Uint8Array;

/**
 * Configures types for the Postgres.js client library (`postgres`).
 *
 * @param jsonAsString Keep JSON / JSONB values as strings instead of parsing.
 */
export const postgresTypeConfig = (
  jsonAsString?: 'json-as-string' | undefined,
) => ({
  // Type the type IDs as `number` so that Typescript doesn't complain about
  // referencing external types during type inference.
  types: {
    bigint: postgres.BigInt,
    json: {
      to: JSON,
      from: [JSON, JSONB],
      serialize: BigIntJSON.stringify,
      parse: jsonAsString ? (x: string) => x : BigIntJSON.parse,
    },
    // Timestamps are converted to PreciseDate objects.
    timestamp: {
      to: TIMESTAMP,
      from: [TIMESTAMP, TIMESTAMPTZ],
      serialize: serializeTimestamp,
      parse: timestampToFpMillis,
    },
    // The DATE type is stored directly as the PG normalized date string.
    date: {
      to: DATE,
      from: [DATE],
      serialize: (x: string | Date) =>
        (x instanceof Date ? x : new Date(x)).toISOString(),
      parse: dateToUTCMidnight,
    },
    // Returns a `js` number which can lose precision for large numbers.
    // JS number is 53 bits so this should generally not occur.
    // An API will be provided for users to override this type.
    numeric: {
      to: NUMERIC,
      from: [NUMERIC],
      serialize: (x: number) => String(x), // pg expects a string
      parse: (x: string | number) => Number(x),
    },
  },
});

export type PostgresDB = postgres.Sql<{
  bigint: bigint;
  json: JSONValue;
}>;

export type PostgresTransaction = postgres.TransactionSql<{
  bigint: bigint;
  json: JSONValue;
}>;

export function pgClient(
  lc: LogContext,
  connectionURI: string,
  options?: postgres.Options<{
    bigint: PostgresType<bigint>;
    json: PostgresType<JSONValue>;
  }>,
  jsonAsString?: 'json-as-string',
): PostgresDB {
  const onnotice = (n: Notice) => {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html#PLPGSQL-STATEMENTS-RAISE
    switch (n.severity) {
      case 'NOTICE':
        return; // silenced
      case 'DEBUG':
        lc.debug?.(n);
        return;
      case 'WARNING':
      case 'EXCEPTION':
        lc.error?.(n);
        return;
      case 'LOG':
      case 'INFO':
      default:
        lc.info?.(n);
    }
  };
  const url = new URL(connectionURI);
  const sslFlag =
    url.searchParams.get('ssl') ?? url.searchParams.get('sslmode') ?? 'prefer';

  let ssl: boolean | 'prefer' | {rejectUnauthorized: boolean};
  if (sslFlag === 'disable' || sslFlag === 'false') {
    ssl = false;
  } else if (sslFlag === 'no-verify') {
    ssl = {rejectUnauthorized: false};
  } else {
    ssl = sslFlag as 'prefer';
  }

  // Set connections to expire between 5 and 10 minutes to free up state on PG.
  const maxLifetimeSeconds = randInt(5 * 60, 10 * 60);
  return postgres(connectionURI, {
    ...postgresTypeConfig(jsonAsString),
    onnotice,
    ['max_lifetime']: maxLifetimeSeconds,
    ssl,
    ...options,
  });
}

export const typeNameByOID: Record<number, string> = Object.freeze(
  Object.fromEntries(
    Object.entries(OID).map(([name, oid]) => [
      oid,
      name.startsWith('_') ? `${name.substring(1)}[]` : name,
    ]),
  ),
);

export const pgToZqlNumericTypeMap = Object.freeze({
  'smallint': 'number',
  'integer': 'number',
  'int': 'number',
  'int2': 'number',
  'int4': 'number',
  'int8': 'number',
  'bigint': 'number',
  'smallserial': 'number',
  'serial': 'number',
  'serial2': 'number',
  'serial4': 'number',
  'serial8': 'number',
  'bigserial': 'number',
  'decimal': 'number',
  'numeric': 'number',
  'real': 'number',
  'double precision': 'number',
  'float': 'number',
  'float4': 'number',
  'float8': 'number',
});

export function isPgNumberType(pgType: string) {
  return Object.hasOwn(pgToZqlNumericTypeMap, formatTypeForLookup(pgType));
}

export const pgToZqlStringTypeMap = Object.freeze({
  'bpchar': 'string',
  'character': 'string',
  'character varying': 'string',
  'text': 'string',
  'uuid': 'string',
  'varchar': 'string',
});

export function isPgStringType(pgType: string) {
  return Object.hasOwn(pgToZqlStringTypeMap, formatTypeForLookup(pgType));
}

export const pgToZqlTypeMap = Object.freeze({
  // Numeric types
  ...pgToZqlNumericTypeMap,

  // Date/Time types
  'date': 'number',
  'timestamp': 'number',
  'timestamptz': 'number',
  'timestamp with time zone': 'number',
  'timestamp without time zone': 'number',

  // String types
  ...pgToZqlStringTypeMap,

  // Boolean types
  'bool': 'boolean',
  'boolean': 'boolean',

  'json': 'json',
  'jsonb': 'json',

  // TODO: Add support for these.
  // 'bytea':
});

export function dataTypeToZqlValueType(
  pgType: string,
  isEnum: boolean,
  isArray: boolean,
): ValueType | undefined {
  // We treat pg arrays as JSON values.
  if (isArray) {
    return 'json';
  }

  const valueType = (pgToZqlTypeMap as Record<string, ValueType>)[
    formatTypeForLookup(pgType)
  ];
  if (valueType === undefined) {
    if (isEnum) {
      return 'string';
    }
  }
  return valueType;
}

// Strips args (i.e. (32) in char(32)) and lowercases.
function formatTypeForLookup(pgType: string): string {
  const startOfArgs = pgType.indexOf('(');
  if (startOfArgs === -1) {
    return pgType.toLocaleLowerCase();
  }
  return pgType.toLocaleLowerCase().substring(0, startOfArgs);
}
