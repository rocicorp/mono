import {PG_OBJECT_IN_USE} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import {defu} from 'defu';
import postgres, {type Options, type PostgresType} from 'postgres';
import {assert} from '../../../../../../shared/src/asserts.ts';
import {mapValues} from '../../../../../../shared/src/objects.ts';
import {sleep} from '../../../../../../shared/src/sleep.ts';
import {type PostgresDB} from '../../../../types/pg.ts';
import {pipe, type Sink, type Source} from '../../../../types/streams.ts';
import {Subscription} from '../../../../types/subscription.ts';
import {fromBigInt} from '../lsn.ts';
import {PgoutputParser} from './pgoutput-parser.ts';
import type {Message} from './pgoutput.types.ts';

const DEFAULT_RETRIES_IF_REPLICATION_SLOT_ACTIVE = 5;

// Postgres will send keepalives every 30 seconds before timing out
// a wal_sender. It is possible that these keepalives are not received
// if there is back-pressure in the replication stream. To keep the
// connection alive anyway, explicitly send keepalives if none have been sent.
const MANUAL_KEEPALIVE_TIMEOUT = 32_000;

export type StreamMessage = [lsn: bigint, Message | {tag: 'keepalive'}];

export async function subscribe(
  lc: LogContext,
  db: PostgresDB,
  slot: string,
  publications: string[],
  lsn: bigint,
  retriesIfReplicationSlotActive = DEFAULT_RETRIES_IF_REPLICATION_SLOT_ACTIVE,
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

  const [readable, writable] = await startReplicationStream(
    lc,
    session,
    slot,
    publications,
    lsn,
    retriesIfReplicationSlotActive + 1,
  );

  let lastAckTime = Date.now();
  function sendAck(lsn: bigint) {
    writable.write(makeAck(lsn));
    lastAckTime = Date.now();
  }
  const ackTimer = setInterval(() => {
    if (Date.now() - lastAckTime > MANUAL_KEEPALIVE_TIMEOUT) {
      lc.warn?.(`sending postgres keepalive (replication stream backed up?)`);
      sendAck(0n);
    }
  }, MANUAL_KEEPALIVE_TIMEOUT / 5);

  const typeParsers = await getTypeParsers(lc, db);
  const parser = new PgoutputParser(typeParsers);
  const messages = Subscription.create<StreamMessage>({
    cleanup: () => {
      readable.destroyed || readable.destroy();
      clearInterval(ackTimer);
      return session.end();
    },
  });

  pipe(readable, messages, buffer => parseStreamMessage(lc, buffer, parser));

  return {
    messages,
    acks: {push: sendAck},
  };
}

async function startReplicationStream(
  lc: LogContext,
  session: postgres.Sql,
  slot: string,
  publications: string[],
  lsn: bigint,
  maxAttempts: number,
) {
  for (let i = 0; i < maxAttempts; i++) {
    try {
      const stream = session
        .unsafe(
          `START_REPLICATION SLOT "${slot}" LOGICAL ${fromBigInt(lsn)} (
        proto_version '1', 
        publication_names '${publications}',
        messages 'true'
      )`,
        )
        .execute();
      return await Promise.all([stream.readable(), stream.writable()]);
    } catch (e) {
      if (
        // error: replication slot "zero_slot_change_source_test_id" is active for PID 268
        e instanceof postgres.PostgresError &&
        e.code === PG_OBJECT_IN_USE
      ) {
        // The freeing up of the replication slot is not transactional;
        // sometimes it takes time for Postgres to consider the slot
        // inactive.
        lc.warn?.(`attempt ${i + 1}: ${String(e)}`, e);
        await sleep(10);
      } else {
        throw e;
      }
    }
  }
  throw new Error(
    `exceeded max attempts (${maxAttempts}) to start the Postgres stream`,
  );
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

// Arbitrary array type to test if the PostgresDB client has fetched types.
const INT4_ARRAY_TYPE = 1007;

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
    // e.g. an array parser will parse '{1,2,3}' from '1,2,3}'.
    // https://github.com/porsager/postgres/blob/089214e85c23c90cf142d47fb30bd03f42874984/src/connection.js#L496
    return isArrayType ? (val: string) => parse(val.substring(1)) : parse;
  });
}
