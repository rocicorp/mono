import {PreciseDate} from '@google-cloud/precise-date';
import {OID} from '@postgresql-typed/oids';
import {LogContext} from '@rocicorp/logger';
import pg from 'pg';
import postgres, {type Notice, type PostgresType} from 'postgres';
import array from 'postgres-array';
import {BigIntJSON, type JSONValue} from './bigint-json.js';

const {
  types: {builtins, setTypeParser},
} = pg;

const TIMESTAMP_TYPES = [builtins.TIMESTAMP, builtins.TIMESTAMPTZ];

const TIMESTAMP_ARRAYS = [1115 /* timestamp[] */, 1185 /* timestamptz[] */];

const builtinsDATEARRAY = 1182;

const builtinsINT8ARRAY = 1016; // No definition in builtins for int8[]

/** Registers types for the 'pg' library used by `pg-logical-replication`. */
export function registerPostgresTypeParsers() {
  setTypeParser(builtins.INT8, val => BigInt(val));
  setTypeParser(builtinsINT8ARRAY, val => array.parse(val, val => BigInt(val)));

  // For pg-logical-replication we convert timestamps directly to microseconds
  // to facilitate serializing them in the Change stream.
  for (const type of TIMESTAMP_TYPES) {
    setTypeParser(type, timestampToFpMillis);
  }
  // Timestamps are converted to epoch microseconds via the PreciseDate object.
  for (const type of TIMESTAMP_ARRAYS) {
    setTypeParser(type, val => array.parse(val, timestampToFpMillis));
  }
  // Store dates as the epoch milliseconds at UTC midnight of the date.
  setTypeParser(builtins.DATE, dateToUTCMidnight);
  setTypeParser(builtinsDATEARRAY, val => array.parse(val, dateToUTCMidnight));
}

const WITH_HH_MM_TIMEZONE = /[+-]\d\d:\d\d$/;
const WITH_HH_TIMEZONE = /[+-]\d\d$/;

// exported for testing.
export function timestampToFpMillis(timestamp: string): number {
  // Convert from PG's time string, e.g. "1999-01-08 12:05:06+00" to "Z"
  // format expected by PreciseDate.
  let ts = timestamp.replace(' ', 'T');
  if (ts.match(WITH_HH_TIMEZONE)) {
    if (ts.endsWith('+00')) {
      // Using 'Z' provides microsecond precision with PreciseDate.
      ts = ts.replace('+00', 'Z');
    } else {
      ts += ':00'; // PG's timezone offset "HH" needs to be converted to "HH:MM"
    }
  } else if (ts.match(WITH_HH_MM_TIMEZONE)) {
    // Using 'Z' provides microsecond precision with PreciseDate.
    ts = ts.replace('+00:00', 'Z');
  } else {
    ts += 'Z';
  }
  try {
    const fullTime = new PreciseDate(ts).getFullTime();
    const millis = Number(fullTime / 1_000_000n);
    const nanos = Number(fullTime % 1_000_000n);
    return millis + nanos * 1e-6; // floating point milliseconds
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

/** Configures types for the Postgres.js client library (`postgres`). */
export const postgresTypeConfig = () => ({
  // Type the type IDs as `number` so that Typescript doesn't complain about
  // referencing external types during type inference.
  types: {
    bigint: postgres.BigInt,
    json: {
      to: builtins.JSON as number,
      from: [builtins.JSON, builtins.JSONB] as number[],
      serialize: BigIntJSON.stringify,
      parse: BigIntJSON.parse,
    },
    // Timestamps are converted to PreciseDate objects.
    timestamp: {
      to: builtins.TIMESTAMP as number,
      from: TIMESTAMP_TYPES as number[],
      serialize: serializeTimestamp,
      parse: timestampToFpMillis,
    },
    // The DATE type is stored directly as the PG normalized date string.
    date: {
      to: builtins.DATE as number,
      from: [builtins.DATE] as number[],
      serialize: (x: string | Date) =>
        (x instanceof Date ? x : new Date(x)).toISOString(),
      parse: dateToUTCMidnight,
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
  return postgres(connectionURI, {
    ...postgresTypeConfig(),
    onnotice,
    ssl: 'prefer',
    ...options,
  });
}

export const typeNameByOID: Record<number, string> = Object.fromEntries(
  Object.entries(OID).map(([name, oid]) => [
    oid,
    name.startsWith('_') ? `${name.substring(1)}[]` : name,
  ]),
);

Object.freeze(typeNameByOID);
