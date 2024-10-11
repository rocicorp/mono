import {PG_SERIALIZATION_FAILURE} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import type {JWTPayload} from 'jose';
import postgres from 'postgres';
import {assert, unreachable} from '../../../../shared/src/asserts.js';
import * as v from '../../../../shared/src/valita.js';
import {ErrorKind} from '../../../../zero-protocol/src/mod.js';
import {
  primaryKeyValueSchema,
  type PrimaryKeyValue,
} from '../../../../zero-protocol/src/primary-key.js';
import {
  MutationType,
  type CRUDMutation,
  type CreateOp,
  type DeleteOp,
  type Mutation,
  type SetOp,
  type UpdateOp,
} from '../../../../zero-protocol/src/push.js';
import {Database} from '../../../../zqlite/src/db.js';
import {type ZeroConfig} from '../../config/zero-config.js';
import {Mode} from '../../db/transaction-pool.js';
import {ErrorForClient} from '../../types/error-for-client.js';
import type {PostgresDB, PostgresTransaction} from '../../types/pg.js';
import {throwErrorForClientIfSchemaVersionNotSupported} from '../../types/schema-versions.js';
import {SlidingWindowLimiter} from '../limiter/sliding-window-limiter.js';
import type {Service} from '../service.js';
import {WriteAuthorizerImpl, type WriteAuthorizer} from './write-authorizer.js';

// An error encountered processing a mutation.
// Returned back to application for display to user.
export type MutationError = [
  kind: ErrorKind.MutationFailed | ErrorKind.MutationRateLimited,
  desc: string,
];

export interface Mutagen {
  processMutation(
    mutation: Mutation,
    authData: JWTPayload,
    schemaVersion: number,
  ): Promise<MutationError | undefined>;
}

export class MutagenService implements Mutagen, Service {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #upstream: PostgresDB;
  readonly #shardID: string;
  readonly #stopped = resolver();
  readonly #replica: Database;
  readonly #writeAuthorizer: WriteAuthorizer;
  readonly #limiter: SlidingWindowLimiter | undefined;

  constructor(
    lc: LogContext,
    shardID: string,
    clientGroupID: string,
    upstream: PostgresDB,
    config: ZeroConfig,
  ) {
    this.id = clientGroupID;
    this.#lc = lc
      .withContext('component', 'mutagen')
      .withContext('serviceID', this.id);
    this.#upstream = upstream;
    this.#shardID = shardID;
    this.#replica = new Database(this.#lc, config.replicaDBFile, {
      readonly: true,
      fileMustExist: true,
    });
    this.#writeAuthorizer = new WriteAuthorizerImpl(
      this.#lc,
      config,
      this.#replica,
      clientGroupID,
    );

    if (config.rateLimit) {
      this.#limiter = new SlidingWindowLimiter(
        config.rateLimit.mutationTransactions.windowMs,
        config.rateLimit.mutationTransactions.maxTransactions,
      );
    }
  }

  processMutation(
    mutation: Mutation,
    authData: JWTPayload,
    schemaVersion: number,
  ): Promise<MutationError | undefined> {
    if (this.#limiter?.canDo() === false) {
      return Promise.resolve([
        ErrorKind.MutationRateLimited,
        'Rate limit exceeded',
      ]);
    }
    return processMutation(
      this.#lc,
      authData,
      this.#upstream,
      this.#shardID,
      this.id,
      mutation,
      this.#writeAuthorizer,
      schemaVersion,
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
  authData: JWTPayload,
  db: PostgresDB,
  shardID: string,
  clientGroupID: string,
  mutation: Mutation,
  writeAuthorizer: WriteAuthorizer,
  schemaVersion: number,
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
          return processMutationWithTx(
            tx,
            authData,
            shardID,
            clientGroupID,
            schemaVersion,
            mutation,
            errorMode,
            writeAuthorizer,
          );
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
        result = [ErrorKind.MutationFailed, String(e)];
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
  authData: JWTPayload,
  shardID: string,
  clientGroupID: string,
  schemaVersion: number,
  mutation: CRUDMutation,
  errorMode: boolean,
  authorizer: WriteAuthorizer,
) {
  const tasks: (() => Promise<unknown>)[] = [];

  if (!errorMode) {
    const {ops} = mutation.args[0];

    for (const [i, op] of ops.entries()) {
      if (tasks.length !== i) {
        // Some mutation was not allowed. No need to visit the rest.
        break;
      }
      switch (op.op) {
        case 'create':
          if (authorizer.canInsert(authData, op)) {
            tasks.push(() => getCreateSQL(tx, op).execute());
          }
          break;
        case 'set':
          if (authorizer.canUpsert(authData, op)) {
            tasks.push(() => getSetSQL(tx, op).execute());
          }
          break;
        case 'update':
          if (authorizer.canUpdate(authData, op)) {
            tasks.push(() => getUpdateSQL(tx, op).execute());
          }
          break;
        case 'delete':
          if (authorizer.canDelete(authData, op)) {
            tasks.push(() => getDeleteSQL(tx, op).execute());
          }
          break;
        default:
          unreachable(op);
      }
    }

    // If not all mutations are allowed, don't do any of them.
    // This is to prevent partial application of mutations.
    if (tasks.length < ops.length) {
      tasks.length = 0; // Clear all tasks.
    }
  }

  // Confirm the mutation even though it may have been blocked by the authorizer.
  // Authorizer blocking a mutation is not an error but the correct result of the mutation.
  tasks.unshift(() =>
    checkSchemaVersionAndIncrementLastMutationID(
      tx,
      shardID,
      clientGroupID,
      schemaVersion,
      mutation.clientID,
      mutation.id,
    ),
  );

  // Note: An error thrown from any Promise aborts the entire transaction.
  await Promise.all(tasks.map(task => task()));
}

export function getCreateSQL(
  tx: postgres.TransactionSql,
  create: CreateOp,
): postgres.PendingQuery<postgres.Row[]> {
  return tx`INSERT INTO ${tx(create.tableName)} ${tx(create.value)}`;
}

export function getSetSQL(
  tx: postgres.TransactionSql,
  set: SetOp,
): postgres.PendingQuery<postgres.Row[]> {
  const {tableName, primaryKey, value} = set;
  return tx`
    INSERT INTO ${tx(tableName)} ${tx(value)}
    ON CONFLICT (${tx(primaryKey)})
    DO UPDATE SET ${tx(value)}
  `;
}

function getUpdateSQL(
  tx: postgres.TransactionSql,
  update: UpdateOp,
): postgres.PendingQuery<postgres.Row[]> {
  const table = update.tableName;
  const {primaryKey, value} = update;
  const id: Record<string, PrimaryKeyValue> = {};
  for (const key of primaryKey) {
    id[key] = v.parse(value[key], primaryKeyValueSchema);
  }
  return tx`UPDATE ${tx(table)} SET ${tx(value)} WHERE ${tx(id)}`;
}

function getDeleteSQL(
  tx: postgres.TransactionSql,
  deleteOp: DeleteOp,
): postgres.PendingQuery<postgres.Row[]> {
  const {tableName, primaryKey, value} = deleteOp;

  const conditions = [];
  for (const key of primaryKey) {
    if (conditions.length > 0) {
      conditions.push(tx`AND`);
    }
    conditions.push(tx`${tx(key)} = ${value[key]}`);
  }

  return tx`DELETE FROM ${tx(tableName)} WHERE ${conditions}`;
}

async function checkSchemaVersionAndIncrementLastMutationID(
  tx: PostgresTransaction,
  shardID: string,
  clientGroupID: string,
  schemaVersion: number,
  clientID: string,
  receivedMutationID: number,
) {
  const lastMutationIdPromise = tx<{lastMutationID: bigint}[]>`
    INSERT INTO zero.clients as current ("shardID", "clientGroupID", "clientID", "lastMutationID")
    VALUES (${shardID}, ${clientGroupID}, ${clientID}, ${1})
    ON CONFLICT ("shardID", "clientGroupID", "clientID")
    DO UPDATE SET "lastMutationID" = current."lastMutationID" + 1
    RETURNING "lastMutationID"
  `.execute();

  const supportedVersionRangePromise = tx<
    {
      minSupportedVersion: number;
      maxSupportedVersion: number;
    }[]
  >`SELECT "minSupportedVersion", "maxSupportedVersion" FROM zero."schemaVersions"`.execute();

  const [{lastMutationID}] = await lastMutationIdPromise;

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

  const supportedVersionRange = await supportedVersionRangePromise;
  assert(supportedVersionRange.length === 1);
  throwErrorForClientIfSchemaVersionNotSupported(
    schemaVersion,
    supportedVersionRange[0],
  );
}

class MutationAlreadyProcessedError extends Error {
  constructor(clientID: string, received: number, actual: bigint) {
    super(
      `Ignoring mutation from ${clientID} with ID ${received} as it was already processed. Expected: ${actual}`,
    );
    assert(received < actual);
  }
}
