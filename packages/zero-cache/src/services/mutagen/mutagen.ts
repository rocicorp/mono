import {assert} from 'shared/src/asserts.js';
import type {
  Mutation,
  CRUDMutation,
  CreateOp,
  SetOp,
  UpdateOp,
  DeleteOp,
} from 'zero-protocol/src/push.js';
import type {PostgresDB, PostgresTransaction} from '../../types/pg.js';
import type postgres from 'postgres';
import type {LogContext} from '@rocicorp/logger';

export async function processMutation(
  lc: LogContext | undefined,
  db: PostgresDB,
  mutation: Mutation,
) {
  assert(mutation.name === '_zero_crud', 'Only CRUD mutations are supported');
  lc = lc?.withContext('mutationID', mutation.id);
  lc = lc?.withContext('processMutation');
  lc?.debug?.('Process mutation start', mutation);
  try {
    const start = Date.now();
    await db.begin(async tx => {
      await processMutationWithTx(lc, tx, mutation as CRUDMutation);
    });
    lc?.withContext('mutationTiming', Date.now() - start);
    lc?.debug?.('Process mutation complete');
  } catch (e) {
    lc?.error?.('Process mutation error', e);
    throw e;
  }
}

async function processMutationWithTx(
  lc: LogContext | undefined,
  tx: PostgresTransaction,
  mutation: CRUDMutation,
) {
  const lastMutationID = await readLastMutationID(tx, mutation.clientID);
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
  await writeLastMutationID(tx, mutation.clientID, expectedMutationID);
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
  clientID: string,
): Promise<bigint> {
  const rows =
    await tx`SELECT "lastMutationID" FROM zero.clients WHERE "clientID" = ${clientID}`;
  if (rows.length === 0) {
    return 0n;
  }
  return rows[0].lastMutationID;
}

function writeLastMutationID(
  tx: PostgresTransaction,
  clientID: string,
  nextMutationID: bigint,
) {
  return tx`
    INSERT INTO zero.clients ("clientID", "lastMutationID")
    VALUES (${clientID}, ${nextMutationID})
    ON CONFLICT ("clientID")
    DO UPDATE SET "lastMutationID" = ${nextMutationID}
  `;
}
