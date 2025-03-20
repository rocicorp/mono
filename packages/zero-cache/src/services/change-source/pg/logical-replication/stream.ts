import type {LogContext} from '@rocicorp/logger';
import {defu} from 'defu';
import postgres, {type Options, type PostgresType} from 'postgres';
import {assert} from '../../../../../../shared/src/asserts.ts';
import {mapValues} from '../../../../../../shared/src/objects.ts';
import {type PostgresDB} from '../../../../types/pg.ts';
import {pipe, type Sink, type Source} from '../../../../types/streams.ts';
import {Subscription} from '../../../../types/subscription.ts';
import {fromBigInt} from '../lsn.ts';
import {PgoutputParser} from './pgoutput-parser.ts';
import type {Message} from './pgoutput.types.ts';

// Arbitrary array type to test if the PostgresDB client has fetched types.
const INT4_ARRAY_TYPE = 1007;

export type StreamMessage = [lsn: bigint, Message | {tag: 'keepalive'}];

export async function subscribe(
  lc: LogContext,
  db: PostgresDB,
  slot: string,
  publications: string[],
  lsn: bigint,
  applicationName = 'zero-replicator',
): Promise<{messages: Source<StreamMessage>; acks: Sink<bigint>}> {
  const session = postgres(
    defu(
      {
        max: 1,
        ['fetch_types']: false, // Necessary for the streaming protocol
        ['idle_timeout']: null,
        ['max_lifetime']: null as unknown as number,
        connection: {
          ['application_name']: applicationName,
          replication: 'database', // https://www.postgresql.org/docs/current/protocol-replication.html
        },
      },
      // ParsedOptions are technically compatible with Options, but happen
      // to not be typed that way. The postgres.js author does an equivalent
      // merge of ParsedOptions and Options here:
      // https://github.com/porsager/postgres/blob/089214e85c23c90cf142d47fb30bd03f42874984/src/subscribe.js#L13
      db.options as unknown as Options<Record<string, PostgresType>>,
    ),
  );

  const lsnString = fromBigInt(lsn);
  const stream = session
    .unsafe(
      `START_REPLICATION SLOT "${slot}" LOGICAL ${lsnString} (
        proto_version '1', 
        publication_names '${publications}',
        messages 'true'
      )`,
    )
    .execute();
  const [readable, writable] = await Promise.all([
    stream.readable(),
    stream.writable(),
  ]);
  lc.debug?.(`started replication stream at ${slot} from ${lsnString}`);

  const typeParsers = await getTypeParsers(lc, db);
  const parser = new PgoutputParser(typeParsers);
  const messages = Subscription.create<StreamMessage>({
    cleanup: () => readable.destroyed || readable.destroy(),
  });

  pipe(readable, messages, buffer => parseStreamMessage(lc, buffer, parser));

  return {
    messages,
    acks: {push: (lsn: bigint) => writable.write(makeAck(lsn))},
  };
}

function parseStreamMessage(
  lc: LogContext,
  buffer: Buffer,
  parser: PgoutputParser,
): StreamMessage | null {
  // https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-XLOGDATA
  if (buffer[0] !== 0x77 && buffer[0] !== 0x6b) {
    lc.warn?.('Unknown message', buffer[0]);
    return null;
  }
  const lsn = buffer.readBigUInt64BE(1);
  return buffer[0] === 0x77 // XLogData
    ? [lsn, parser.parse(buffer.subarray(25))]
    : buffer.readInt8(17) // Primary keepalive message: shouldRespond
    ? [lsn, {tag: 'keepalive'}]
    : null;
}

// https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-STANDBY-STATUS-UPDATE
function makeAck(lsn: bigint): Buffer {
  const microNow = BigInt(Date.now() - Date.UTC(2000, 0, 1)) * BigInt(1000);

  const x = Buffer.alloc(34);
  x[0] = 'r'.charCodeAt(0);
  x.writeBigInt64BE(lsn, 1);
  x.writeBigInt64BE(lsn, 9);
  x.writeBigInt64BE(lsn, 17);
  x.writeBigInt64BE(microNow, 25);
  return x;
}

// postgres.js has default type parsers with user-defined overrides
// configurable per-client (see `postgresTypeConfig` in types/pg.ts).
//
// From these, the postgres.js client will automatically derive parsers
// for array versions of these types, provided that the client was
// configured with `fetch_types: true` (which is the default).
//
// A replication session (with `database: replication`), however, does
// not support this type fetching, so it is done on a connection from
// a default client.
async function getTypeParsers(lc: LogContext, db: PostgresDB) {
  if (!db.options.parsers[INT4_ARRAY_TYPE]) {
    assert(db.options.fetch_types, `Supplied db must fetch_types`);
    lc.debug?.('fetching array types');

    // Execute a query to ensure that fetchArrayTypes() gets executed:
    // https://github.com/porsager/postgres/blob/089214e85c23c90cf142d47fb30bd03f42874984/src/connection.js#L536
    await db`SELECT 1`.simple();
    assert(
      db.options.parsers[INT4_ARRAY_TYPE],
      `array types not fetched ${Object.keys(db.options.parsers)}`,
    );
  }
  return mapValues(db.options.parsers, parse => {
    // The postgres.js library tags parsers for array types with an `array: true` field.
    // https://github.com/porsager/postgres/blob/089214e85c23c90cf142d47fb30bd03f42874984/src/connection.js#L760
    const isArrayType = (parse as unknown as {array?: boolean}).array;

    // And then skips the first character when parsing the string,
    // e.g. '{1,2,3}` it will parse `1,2,3}`.
    // https://github.com/porsager/postgres/blob/089214e85c23c90cf142d47fb30bd03f42874984/src/connection.js#L496
    return isArrayType ? (val: string) => parse(val.substring(1)) : parse;
  });
}
