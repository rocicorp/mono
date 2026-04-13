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
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from '../../../zql/src/ivm/source.ts';
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
          makeSourceChangeAdd(arg.value),
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
): Promise<void> {
  const key = toPrimaryKeyString(
    arg.tableName,
    schema.tables[arg.tableName].primaryKey,
    arg.value,
  );
  if (await tx.has(key)) {
    await update(tx, {...arg, op: 'update'}, schema, ivmBranch);
  } else {
    await insert(tx, {...arg, op: 'insert'}, schema, ivmBranch);
  }
}

export async function update(
  tx: WriteTransaction,
  arg: UpdateOp,
  schema: Schema,
  ivmBranch: IVMSourceBranch | undefined,
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
  const update = arg.value;
  const next = {...(prev as ReadonlyJSONObject)};
  for (const k in update) {
    if (update[k] !== undefined) {
      next[k] = update[k];
    }
  }
  await tx.set(key, next);
  if (ivmBranch) {
    consume(
      must(ivmBranch.getSource(arg.tableName)).push(
        makeSourceChangeEdit(next, prev as Row),
      ),
    );
  }
}

async function deleteImpl(
  tx: WriteTransaction,
  arg: DeleteOp,
  schema: Schema,
  ivmBranch: IVMSourceBranch | undefined,
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
        makeSourceChangeRemove(prev as Row),
      ),
    );
  }
}

export {deleteImpl as delete};
