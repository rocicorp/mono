import {PG_SERIALIZATION_FAILURE} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import postgres from 'postgres';
import {assert, unreachable} from 'shared/src/asserts.js';
import {Mode} from 'zero-cache/src/db/transaction-pool.js';
import {ErrorKind} from 'zero-protocol';
import {
  MutationType,
  type CRUDMutation,
  type CreateOp,
  type DeleteOp,
  type Mutation,
  type SetOp,
  type UpdateOp,
} from 'zero-protocol/src/push.js';
import {ErrorForClient} from '../../types/error-for-client.js';
import type {PostgresDB, PostgresTransaction} from '../../types/pg.js';
import type {Service} from '../service.js';
import {AuthorizationConfig} from '../../config/zero-config.js';

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
  readonly #authorizationConfig: AuthorizationConfig;

  constructor(
    lc: LogContext,
    clientGroupID: string,
    upstream: PostgresDB,
    authorizationConfig: AuthorizationConfig,
  ) {
    this.id = clientGroupID;
    this.#lc = lc
      .withContext('component', 'Mutagen')
      .withContext('serviceID', this.id);
    this.#upstream = upstream;
    this.#authorizationConfig = authorizationConfig;
  }

  processMutation(mutation: Mutation): Promise<MutationError | undefined> {
    return processMutation(
      this.#lc,
      this.#upstream,
      this.id,
      mutation,
      this.#authorizationConfig,
    );
  }

  run(): Promise<void> {
    return this.#stopped.promise;
  }

  stop(): Promise<void> {
    this.#stopped.resolve();
    return Promise.resolve();
  }
}

const MAX_SERIALIZATION_ATTEMPTS = 2;

export async function processMutation(
  lc: LogContext | undefined,
  db: PostgresDB,
  clientGroupID: string,
  mutation: Mutation,
  _authorizationConfig: AuthorizationConfig,
  onTxStart?: () => void, // for testing
): Promise<MutationError | undefined> {
  assert(
    mutation.type === MutationType.CRUD,
    'Only CRUD mutations are supported',
  );
  lc = lc?.withContext('mutationID', mutation.id);
  lc = lc?.withContext('processMutation');
  lc?.debug?.('Process mutation start', mutation);

  let result: MutationError | undefined;

  const start = Date.now();
  try {
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
    let errorMode = false;
    for (let i = 0; i < MAX_SERIALIZATION_ATTEMPTS; i++) {
      try {
        await db.begin(Mode.SERIALIZABLE, tx => {
          onTxStart?.();
          return processMutationWithTx(tx, clientGroupID, mutation, errorMode);
        });
        if (errorMode) {
          lc?.debug?.('Ran mutation successfully in error mode');
        }
        break;
      } catch (e) {
        if (e instanceof MutationAlreadyProcessedError) {
          lc?.debug?.(e.message);
          return undefined;
        }
        if (e instanceof ErrorForClient || errorMode) {
          lc?.error?.('Process mutation error', e);
          throw e;
        }
        if (
          e instanceof postgres.PostgresError &&
          e.code === PG_SERIALIZATION_FAILURE
        ) {
          lc?.info?.(i < MAX_SERIALIZATION_ATTEMPTS ? `Retrying` : '', e);
          continue; // Retry up to MAX_SERIALIZATION_ATTEMPTS.
        }
        result = String(e);
        if (errorMode) {
          break;
        }
        lc?.error?.('Got error running mutation, re-running in error mode', e);
        errorMode = true;
        i--;
      }
    }
  } finally {
    lc?.debug?.('Process mutation complete in', Date.now() - start);
  }
  return result;
}

async function processMutationWithTx(
  tx: PostgresTransaction,
  clientGroupID: string,
  mutation: CRUDMutation,
  errorMode: boolean,
) {
  const queryPromises: Promise<unknown>[] = [
    incrementLastMutationID(tx, clientGroupID, mutation.clientID, mutation.id),
  ];

  if (!errorMode) {
    const {ops} = mutation.args[0];

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
          unreachable(op);
      }
    }
  }

  // Note: An error thrown from any Promise aborts the entire transaction.
  await Promise.all(queryPromises);
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

async function incrementLastMutationID(
  tx: PostgresTransaction,
  clientGroupID: string,
  clientID: string,
  receivedMutationID: number,
) {
  const [{lastMutationID}] = await tx<{lastMutationID: bigint}[]>`
    INSERT INTO zero.clients as current ("clientGroupID", "clientID", "lastMutationID")
    VALUES (${clientGroupID}, ${clientID}, ${1})
    ON CONFLICT ("clientGroupID", "clientID")
    DO UPDATE SET "lastMutationID" = current."lastMutationID" + 1
    RETURNING "lastMutationID"
  `;

  // ABORT if the resulting lastMutationID is not equal to the receivedMutationID.
  if (receivedMutationID < lastMutationID) {
    throw new MutationAlreadyProcessedError(
      clientID,
      receivedMutationID,
      lastMutationID,
    );
  } else if (receivedMutationID > lastMutationID) {
    throw new ErrorForClient([
      'error',
      ErrorKind.InvalidPush,
      `Push contains unexpected mutation id ${receivedMutationID} for client ${clientID}. Expected mutation id ${lastMutationID.toString()}.`,
    ]);
  }
}

class MutationAlreadyProcessedError extends Error {
  constructor(clientID: string, received: number, actual: bigint) {
    super(
      `Ignoring mutation from ${clientID} with ID ${received} as it was already processed. Expected: ${actual}`,
    );
    assert(received < actual);
  }
}
