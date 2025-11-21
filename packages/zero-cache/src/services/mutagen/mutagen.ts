import {PG_SERIALIZATION_FAILURE} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import type {JWTPayload} from 'jose';
import postgres from 'postgres';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import * as v from '../../../../shared/src/valita.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import * as MutationType from '../../../../zero-protocol/src/mutation-type-enum.ts';
import {
  primaryKeyValueSchema,
  type PrimaryKeyValue,
} from '../../../../zero-protocol/src/primary-key.ts';
import {
  type CRUDMutation,
  type DeleteOp,
  type InsertOp,
  type Mutation,
  type UpdateOp,
  type UpsertOp,
} from '../../../../zero-protocol/src/push.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import type {DatabaseStorage} from '../../../../zqlite/src/database-storage.ts';
import {
  WriteAuthorizerImpl,
  type WriteAuthorizer,
} from '../../auth/write-authorizer.ts';
import {type ZeroConfig} from '../../config/zero-config.ts';
import * as Mode from '../../db/mode-enum.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import {recordMutation} from '../../server/anonymous-otel-start.ts';
import {ErrorForClient} from '../../types/error-for-client.ts';
import type {PostgresDB, PostgresTransaction} from '../../types/pg.ts';
import {throwErrorForClientIfSchemaVersionNotSupported} from '../../types/schema-versions.ts';
import {appSchema, upstreamSchema, type ShardID} from '../../types/shards.ts';
import {SlidingWindowLimiter} from '../limiter/sliding-window-limiter.ts';
import type {RefCountedService, Service} from '../service.ts';
import {MutationAlreadyProcessedError} from './error.ts';

// An error encountered processing a mutation.
// Returned back to application for display to user.
export type MutationError = [
  kind: ErrorKind.MutationFailed | ErrorKind.MutationRateLimited,
  desc: string,
];

export interface Mutagen extends RefCountedService {
  processMutation(
    mutation: Mutation,
    authData: JWTPayload | undefined,
    schemaVersion: number | undefined,
    customMutatorsEnabled: boolean,
  ): Promise<MutationError | undefined>;
}

export class MutagenService implements Mutagen, Service {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #upstream: PostgresDB;
  readonly #shard: ShardID;
  readonly #stopped = resolver();
  readonly #replica: Database;
  readonly #writeAuthorizer: WriteAuthorizerImpl;
  readonly #limiter: SlidingWindowLimiter | undefined;
  #refCount = 0;
  #isStopped = false;

  readonly #crudMutations = getOrCreateCounter(
    'mutation',
    'crud',
    'Number of CRUD mutations processed',
  );

  constructor(
    lc: LogContext,
    shard: ShardID,
    clientGroupID: string,
    upstream: PostgresDB,
    config: ZeroConfig,
    writeAuthzStorage: DatabaseStorage,
  ) {
    this.id = clientGroupID;
    this.#lc = lc;
    this.#upstream = upstream;
    this.#shard = shard;
    this.#replica = new Database(this.#lc, config.replica.file, {
      fileMustExist: true,
    });
    this.#writeAuthorizer = new WriteAuthorizerImpl(
      this.#lc,
      config,
      this.#replica,
      shard.appID,
      clientGroupID,
      writeAuthzStorage,
    );

    if (config.perUserMutationLimit.max !== undefined) {
      this.#limiter = new SlidingWindowLimiter(
        config.perUserMutationLimit.windowMs,
        config.perUserMutationLimit.max,
      );
    }
  }

  ref() {
    assert(!this.#isStopped, 'MutagenService is already stopped');
    ++this.#refCount;
  }

  unref() {
    assert(!this.#isStopped, 'MutagenService is already stopped');
    --this.#refCount;
    if (this.#refCount <= 0) {
      void this.stop();
    }
  }

  hasRefs(): boolean {
    return this.#refCount > 0;
  }

  processMutation(
    mutation: Mutation,
    authData: JWTPayload | undefined,
    schemaVersion: number | undefined,
    customMutatorsEnabled = false,
  ): Promise<MutationError | undefined> {
    if (this.#limiter?.canDo() === false) {
      return Promise.resolve([
        ErrorKind.MutationRateLimited,
        'Rate limit exceeded',
      ]);
    }
    this.#crudMutations.add(1, {
      clientGroupID: this.id,
    });
    return processMutation(
      this.#lc,
      authData,
      this.#upstream,
      this.#shard,
      this.id,
      mutation,
      this.#writeAuthorizer,
      schemaVersion,
      undefined,
      customMutatorsEnabled,
    );
  }

  run(): Promise<void> {
    return this.#stopped.promise;
  }

  stop(): Promise<void> {
    if (this.#isStopped) {
      return this.#stopped.promise;
    }
    this.#writeAuthorizer.destroy();
    this.#isStopped = true;
    this.#stopped.resolve();
    return this.#stopped.promise;
  }
}

const MAX_SERIALIZATION_ATTEMPTS = 10;

export async function processMutation(
  lc: LogContext,
  authData: JWTPayload | undefined,
  db: PostgresDB,
  shard: ShardID,
  clientGroupID: string,
  mutation: Mutation,
  writeAuthorizer: WriteAuthorizer,
  schemaVersion: number | undefined,
  onTxStart?: () => void | Promise<void>, // for testing
  customMutatorsEnabled = false,
): Promise<MutationError | undefined> {
  assert(
    mutation.type === MutationType.CRUD,
    'Only CRUD mutations are supported',
  );
  lc = lc.withContext('mutationID', mutation.id);
  lc = lc.withContext('processMutation');
  lc.debug?.('Process mutation start', mutation);

  // Record mutation processing attempt for telemetry (regardless of success/failure)
  recordMutation('crud');

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
        await db.begin(Mode.SERIALIZABLE, async tx => {
          // Simulates a concurrent request for testing. In production this is a noop.
          const done = onTxStart?.();
          try {
            return await processMutationWithTx(
              lc,
              tx,
              authData,
              shard,
              clientGroupID,
              schemaVersion,
              mutation,
              errorMode,
              writeAuthorizer,
            );
          } finally {
            await done;
          }
        });
        if (errorMode) {
          lc.debug?.('Ran mutation successfully in error mode');
        }
        break;
      } catch (e) {
        if (e instanceof MutationAlreadyProcessedError) {
          lc.debug?.(e.message);
          // Don't double-count already processed mutations, but they were counted above
          return undefined;
        }
        if (
          e instanceof ErrorForClient &&
          !errorMode &&
          e.errorBody.kind === ErrorKind.InvalidPush &&
          customMutatorsEnabled &&
          i < 2
        ) {
          // We're temporarily supporting custom mutators AND CRUD mutators at the same time.
          // This can create a lot of OOO mutation errors since we do not know when the API server
          // has applied a custom mutation before moving on to process CRUD mutations.
          // The temporary workaround (since CRUD is being deprecated) is to retry the mutation
          // after a small delay. Users are not expected to be running both CRUD and Custom mutators.
          // They should migrate completely to custom mutators.
          lc.info?.(
            'Both CRUD and Custom mutators are being used at once. This is supported for now but IS NOT RECOMMENDED. Migrate completely to custom mutators.',
            e,
          );
          await new Promise(resolve => setTimeout(resolve, 100));
          continue;
        }
        if (e instanceof ErrorForClient || errorMode) {
          lc.error?.('Process mutation error', e);
          throw e;
        }
        if (
          e instanceof postgres.PostgresError &&
          e.code === PG_SERIALIZATION_FAILURE
        ) {
          lc.info?.(`attempt ${i + 1}: ${String(e)}`, e);
          continue; // Retry up to MAX_SERIALIZATION_ATTEMPTS.
        }
        result = [ErrorKind.MutationFailed, String(e)];
        if (errorMode) {
          break;
        }
        lc.error?.('Got error running mutation, re-running in error mode', e);
        errorMode = true;
        i--;
      }
    }
  } finally {
    lc.debug?.('Process mutation complete in', Date.now() - start);
  }
  return result;
}

export async function processMutationWithTx(
  lc: LogContext,
  tx: PostgresTransaction,
  authData: JWTPayload | undefined,
  shard: ShardID,
  clientGroupID: string,
  schemaVersion: number | undefined,
  mutation: CRUDMutation,
  errorMode: boolean,
  authorizer: WriteAuthorizer,
) {
  const tasks: (() => Promise<unknown>)[] = [];

  async function execute(stmt: postgres.PendingQuery<postgres.Row[]>) {
    try {
      return await stmt.execute();
    } finally {
      const q = stmt as unknown as Query;
      lc.debug?.(`${q.string}: ${JSON.stringify(q.parameters)}`);
    }
  }

  authorizer.reloadPermissions();

  if (!errorMode) {
    const {ops} = mutation.args[0];
    const normalizedOps = authorizer.normalizeOps(ops);
    const [canPre, canPost] = await Promise.all([
      authorizer.canPreMutation(authData, normalizedOps),
      authorizer.canPostMutation(authData, normalizedOps),
    ]);
    if (canPre && canPost) {
      for (const op of ops) {
        switch (op.op) {
          case 'insert':
            tasks.push(() => execute(getInsertSQL(tx, op)));
            break;
          case 'upsert':
            tasks.push(() => execute(getUpsertSQL(tx, op)));
            break;
          case 'update':
            tasks.push(() => execute(getUpdateSQL(tx, op)));
            break;
          case 'delete':
            tasks.push(() => execute(getDeleteSQL(tx, op)));
            break;
          default:
            unreachable(op);
        }
      }
    }
  }

  // Confirm the mutation even though it may have been blocked by the authorizer.
  // Authorizer blocking a mutation is not an error but the correct result of the mutation.
  tasks.unshift(() =>
    checkSchemaVersionAndIncrementLastMutationID(
      tx,
      shard,
      clientGroupID,
      schemaVersion,
      mutation.clientID,
      mutation.id,
    ),
  );

  // Note: An error thrown from any Promise aborts the entire transaction.
  await Promise.all(tasks.map(task => task()));
}

export function getInsertSQL(
  tx: postgres.TransactionSql,
  create: InsertOp,
): postgres.PendingQuery<postgres.Row[]> {
  return tx`INSERT INTO ${tx(create.tableName)} ${tx(create.value)}`;
}

export function getUpsertSQL(
  tx: postgres.TransactionSql,
  set: UpsertOp,
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
  return tx`UPDATE ${tx(table)} SET ${tx(value)} WHERE ${Object.entries(
    id,
  ).flatMap(([key, value], i) =>
    i ? [tx`AND`, tx`${tx(key)} = ${value}`] : tx`${tx(key)} = ${value}`,
  )}`;
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
  shard: ShardID,
  clientGroupID: string,
  schemaVersion: number | undefined,
  clientID: string,
  receivedMutationID: number,
) {
  const [[{lastMutationID}], supportedVersionRange] = await Promise.all([
    tx<{lastMutationID: bigint}[]>`
    INSERT INTO ${tx(upstreamSchema(shard))}.clients 
      as current ("clientGroupID", "clientID", "lastMutationID")
          VALUES (${clientGroupID}, ${clientID}, ${1})
      ON CONFLICT ("clientGroupID", "clientID")
      DO UPDATE SET "lastMutationID" = current."lastMutationID" + 1
      RETURNING "lastMutationID"
  `,
    schemaVersion === undefined
      ? undefined
      : tx<
          {
            minSupportedVersion: number;
            maxSupportedVersion: number;
          }[]
        >`SELECT "minSupportedVersion", "maxSupportedVersion" 
        FROM ${tx(appSchema(shard))}."schemaVersions"`,
  ]);

  // ABORT if the resulting lastMutationID is not equal to the receivedMutationID.
  if (receivedMutationID < lastMutationID) {
    throw new MutationAlreadyProcessedError(
      clientID,
      receivedMutationID,
      lastMutationID,
    );
  } else if (receivedMutationID > lastMutationID) {
    throw new ErrorForClient({
      kind: ErrorKind.InvalidPush,
      message: `Push contains unexpected mutation id ${receivedMutationID} for client ${clientID}. Expected mutation id ${lastMutationID.toString()}.`,
    });
  }

  if (schemaVersion !== undefined && supportedVersionRange !== undefined) {
    assert(supportedVersionRange.length === 1);
    throwErrorForClientIfSchemaVersionNotSupported(
      schemaVersion,
      supportedVersionRange[0],
    );
  }
}

// The slice of information from the Query object in Postgres.js that gets logged for debugging.
// https://github.com/porsager/postgres/blob/f58cd4f3affd3e8ce8f53e42799672d86cd2c70b/src/connection.js#L219
type Query = {string: string; parameters: object[]};
