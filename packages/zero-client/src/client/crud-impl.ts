import type {ReadonlyJSONObject} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {
  type DeleteOp,
  type InsertOp,
  type UpdateOp,
  type UpsertOp,
} from '../../../zero-protocol/src/push.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {consume} from '../../../zql/src/ivm/stream.ts';
import type {IVMSourceBranch} from './ivm-branch.ts';
import {toPrimaryKeyString} from './keys.ts';
import type {WriteTransaction} from './replicache-types.ts';
export type {TableMutator} from '../../../zql/src/mutate/crud.ts';

function defaultOptionalFieldsToNull(
  schema: TableSchema,
  value: ReadonlyJSONObject,
): ReadonlyJSONObject {
  let rv = value;
  for (const name in schema.columns) {
    if (rv[name] === undefined) {
      rv = {...rv, [name]: null};
    }
  }
  return rv;
}

export async function insert(
  tx: WriteTransaction,
  arg: InsertOp,
  schema: Schema,
  ivmBranch: IVMSourceBranch | undefined,
  mutationName: string,
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.tableName,
    schema.tables[arg.tableName].primaryKey,
    arg.value,
  );
  if (!(await tx.has(key))) {
    const val = defaultOptionalFieldsToNull(
      schema.tables[arg.tableName],
      arg.value,
    );
    await tx.set(key, val);
    if (ivmBranch) {
      consume(
        must(ivmBranch.getSource(arg.tableName)).push(
          {
            type: 'add',
            row: arg.value,
          },
          {reason: {type: 'mutation', name: mutationName}},
        ),
      );
    }
  }
}

export async function upsert(
  tx: WriteTransaction,
  arg: InsertOp | UpsertOp,
  schema: Schema,
  ivmBranch: IVMSourceBranch | undefined,
  mutationName: string,
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.tableName,
    schema.tables[arg.tableName].primaryKey,
    arg.value,
  );
  if (await tx.has(key)) {
    await update(tx, {...arg, op: 'update'}, schema, ivmBranch, mutationName);
  } else {
    await insert(tx, {...arg, op: 'insert'}, schema, ivmBranch, mutationName);
  }
}

export async function update(
  tx: WriteTransaction,
  arg: UpdateOp,
  schema: Schema,
  ivmBranch: IVMSourceBranch | undefined,
  mutationName: string,
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.tableName,
    schema.tables[arg.tableName].primaryKey,
    arg.value,
  );
  const prev = await tx.get(key);
  if (prev === undefined) {
    return;
  }
  const updateVal = arg.value;
  const next = {...(prev as ReadonlyJSONObject)};
  for (const k in updateVal) {
    if (updateVal[k] !== undefined) {
      next[k] = updateVal[k];
    }
  }
  await tx.set(key, next);
  if (ivmBranch) {
    consume(
      must(ivmBranch.getSource(arg.tableName)).push(
        {
          type: 'edit',
          oldRow: prev as Row,
          row: next,
        },
        {reason: {type: 'mutation', name: mutationName}},
      ),
    );
  }
}

async function deleteImpl(
  tx: WriteTransaction,
  arg: DeleteOp,
  schema: Schema,
  ivmBranch: IVMSourceBranch | undefined,
  mutationName: string,
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.tableName,
    schema.tables[arg.tableName].primaryKey,
    arg.value,
  );
  const prev = await tx.get(key);
  if (prev === undefined) {
    return;
  }
  await tx.del(key);
  if (ivmBranch) {
    consume(
      must(ivmBranch.getSource(arg.tableName)).push(
        {
          type: 'remove',
          row: prev as Row,
        },
        {reason: {type: 'mutation', name: mutationName}},
      ),
    );
  }
}

export {deleteImpl as delete};
