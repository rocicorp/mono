import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import type postgres from 'postgres';
import {assert} from 'shared/src/asserts.js';
import {
  MutationType,
  type CRUDMutation,
  type CreateOp,
  type DeleteOp,
  type Mutation,
  type SetOp,
  type UpdateOp,
} from 'zero-protocol/src/push.js';
import type {PostgresDB, PostgresTransaction} from '../../types/pg.js';
import type {Service} from '../service.js';

// An error encountered processing a mutation.
// Returned back to application for display to user.
export type MutationError = string;

export interface Mutagen {
  processMutation(mutation: Mutation): Promise<MutationError | undefined>;
}

export class MutagenService implements Mutagen, Service {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #upstream: PostgresDB;
  readonly #stopped = resolver();

  constructor(lc: LogContext, clientGroupID: string, upstream: PostgresDB) {
    this.id = clientGroupID;
    this.#lc = lc
      .withContext('component', 'Mutagen')
      .withContext('serviceID', this.id);
    this.#upstream = upstream;
  }

  processMutation(mutation: Mutation): Promise<MutationError | undefined> {
    return processMutation(this.#lc, this.#upstream, this.id, mutation);
  }

  run(): Promise<void> {
    return this.#stopped.promise;
  }

  stop(): Promise<void> {
    this.#stopped.resolve();
    return Promise.resolve();
  }
}

export async function processMutation(
  lc: LogContext | undefined,
  db: PostgresDB,
  clientGroupID: string,
  mutation: Mutation,
): Promise<MutationError | undefined> {
  assert(
    mutation.type === MutationType.CRUD,
    'Only CRUD mutations are supported',
  );
  lc = lc?.withContext('mutationID', mutation.id);
  lc = lc?.withContext('processMutation');
  lc?.debug?.('Process mutation start', mutation);
  const start = Date.now();
  try {
    try {
      await db.begin(tx =>
        processMutationWithTx(lc, tx, clientGroupID, mutation, false),
      );
    } catch (firstError) {
      // Mutations can fail for a variety of reasons:
      //
      // - application error
      // - network/db error
      // - zero bug
      //
      // For application errors what we want is to re-run the mutation in
      // "error mode", which skips the actual mutation and just updates the
      // lastMutationID. Then return the error to the app.
      //
      // However, it's hard to tell the difference between application errors
      // and the other types.
      //
      // A reasonable policy ends up being to just retry every mutation once
      // in error mode. If the error mode mutation succeeds then we assume it
      // was an application error and return the error to the app. Otherwise,
      // we know it was something internal and we log it.
      //
      // This is not 100% correct - there are theoretical cases where we
      // return an internal error to the app that shouldn't have been. But it
      // would have to be a crazy coincidence: we'd have to have a network
      // error on the first attempt that resolves by the second attempt.
      //
      // One might ask why not try/catch just the calls to the mutators and
      // consider those application errors. That is actually what we do in
      // Replicache:
      //
      // https://github.com/rocicorp/todo-row-versioning/blob/9a0a79dc2d2de32c4fac61b5d1634bd9a9e66b7c/server/src/push.ts#L131
      //
      // We don't do it here because:
      //
      // 1. It's still not perfect. It's hard to isolate SQL errors in
      //    mutators due to app developer mistakes from SQL errors due to
      //    Zero mistakes.
      // 2. It's not possible to do this with the pg library we're using in
      //    Zero anyway: https://github.com/porsager/postgres/issues/455.
      //
      // Personally I think this simple retry policy is nice.
      lc?.error?.(
        'Got error running mutation, re-running in error mode',
        firstError,
      );
      await db.begin(tx =>
        processMutationWithTx(lc, tx, clientGroupID, mutation, true),
      );
      lc?.debug?.('Ran mutation successfully in error mode');
      return String(firstError);
    }
    return undefined;
  } catch (e) {
    lc?.error?.('Process mutation error', e);
    throw e;
  } finally {
    lc?.debug?.('Process mutation complete in', Date.now() - start);
  }
}

async function processMutationWithTx(
  lc: LogContext | undefined,
  tx: PostgresTransaction,
  clientGroupID: string,
  mutation: CRUDMutation,
  errorMode: boolean,
) {
  const lastMutationID = await readLastMutationID(
    tx,
    clientGroupID,
    mutation.clientID,
  );
  const expectedMutationID = lastMutationID + 1n;

  if (mutation.id < expectedMutationID) {
    lc?.debug?.(
      `Ignoring mutation with ID ${mutation.id} as it was already processed. Expected: ${expectedMutationID}`,
    );
    return;
  } else if (mutation.id > expectedMutationID) {
    throw new Error(
      `Mutation ID was out of order. Expected: ${expectedMutationID} received: ${mutation.id}`,
    );
  }

  if (!errorMode) {
    const {ops} = mutation.args[0];
    const queryPromises: Promise<unknown>[] = [];

    for (const op of ops) {
      switch (op.op) {
        case 'create':
          queryPromises.push(getCreateSQL(tx, op).execute());
          break;
        case 'set':
          queryPromises.push(getSetSQL(tx, op).execute());
          break;
        case 'update':
          queryPromises.push(getUpdateSQL(tx, op).execute());
          break;
        case 'delete':
          queryPromises.push(getDeleteSQL(tx, op).execute());
          break;
        default:
          op satisfies never;
      }
    }

    // All the CRUD operations were dispatched serially (above).
    // Now wait for their completion and then update `lastMutationID`.
    await Promise.all(queryPromises);
  }

  await writeLastMutationID(
    tx,
    clientGroupID,
    mutation.clientID,
    expectedMutationID,
  );
}

export function getCreateSQL(
  tx: postgres.TransactionSql,
  create: CreateOp,
): postgres.PendingQuery<postgres.Row[]> {
  const table = create.entityType;
  const {id, value} = create;

  const valueWithIdColumns = {
    ...value,
    ...id,
  };

  return tx`INSERT INTO ${tx(table)} ${tx(valueWithIdColumns)}`;
}

export function getSetSQL(
  tx: postgres.TransactionSql,
  set: SetOp,
): postgres.PendingQuery<postgres.Row[]> {
  const table = set.entityType;
  const {id, value} = set;

  return tx`
    INSERT INTO ${tx(table)} ${tx({...value, ...id})}
    ON CONFLICT (${tx(Object.keys(id))})
    DO UPDATE SET ${tx(value)}
  `;
}

function getUpdateSQL(
  tx: postgres.TransactionSql,
  update: UpdateOp,
): postgres.PendingQuery<postgres.Row[]> {
  const table = update.entityType;
  const {id, partialValue} = update;

  return tx`UPDATE ${tx(table)} SET ${tx(partialValue)} WHERE ${tx(id)}`;
}

function getDeleteSQL(
  tx: postgres.TransactionSql,
  deleteOp: DeleteOp,
): postgres.PendingQuery<postgres.Row[]> {
  const table = deleteOp.entityType;
  const {id} = deleteOp;

  const conditions = [];
  for (const [key, value] of Object.entries(id)) {
    if (conditions.length > 0) {
      conditions.push(tx`AND`);
    }
    conditions.push(tx`${tx(key)} = ${value}`);
  }

  return tx`DELETE FROM ${tx(table)} WHERE ${conditions}`;
}

export async function readLastMutationID(
  tx: postgres.TransactionSql,
  clientGroupID: string,
  clientID: string,
): Promise<bigint> {
  const rows = await tx`
    SELECT "lastMutationID" FROM zero.clients 
    WHERE "clientGroupID" = ${clientGroupID} AND "clientID" = ${clientID}`;
  if (rows.length === 0) {
    return 0n;
  }
  return rows[0].lastMutationID;
}

function writeLastMutationID(
  tx: PostgresTransaction,
  clientGroupID: string,
  clientID: string,
  nextMutationID: bigint,
) {
  return tx`
    INSERT INTO zero.clients ("clientGroupID", "clientID", "lastMutationID")
    VALUES (${clientGroupID}, ${clientID}, ${nextMutationID})
    ON CONFLICT ("clientGroupID", "clientID")
    DO UPDATE SET "lastMutationID" = ${nextMutationID}
  `;
}
