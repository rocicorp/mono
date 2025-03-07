import {must} from '../../../shared/src/must.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {ClientID} from '../types/client-state.ts';
import {deleteImpl, insertImpl, updateImpl, upsertImpl} from './crud.ts';
import type {WriteTransaction} from './replicache-types.ts';
import type {IVMSourceBranch} from './ivm-branch.ts';
import type {
  DeleteID,
  InsertValue,
  SchemaCRUD,
  SchemaQuery,
  TableCRUD,
  TransactionBase,
  UpdateValue,
  UpsertValue,
} from '../../../zql/src/mutate/custom.ts';
import {
  WriteTransactionImpl,
  zeroData,
} from '../../../replicache/src/transactions.ts';

/**
 * An instance of this is passed to custom mutator implementations and
 * allows reading and writing to the database and IVM at the head
 * at which the mutator is being applied.
 */
export interface Transaction<S extends Schema> extends TransactionBase<S> {
  readonly location: 'client';
  readonly reason: 'optimistic' | 'rebase';
}

/**
 * The shape which a user's custom mutator definitions must conform to.
 */
export type CustomMutatorDefs<S extends Schema> = {
  readonly [Table in keyof S['tables']]?: {
    readonly [key: string]: CustomMutatorImpl<S>;
  };
} & {
  // The user is not required to associate mutators with tables.
  // Maybe that have some other arbitrary way to namespace.
  [namespace: string]: {
    [key: string]: CustomMutatorImpl<S>;
  };
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type CustomMutatorImpl<S extends Schema, TArgs = any> = (
  tx: Transaction<S>,
  // TODO: many args. See commit: 52657c2f934b4a458d628ea77e56ce92b61eb3c6 which did have many args.
  // The issue being that it will be a protocol change to support varargs.
  args: TArgs,
) => Promise<void>;

/**
 * The shape exposed on the `Zero.mutate` instance.
 * The signature of a custom mutator takes a `transaction` as its first arg
 * but the user does not provide this arg when calling the mutator.
 *
 * This utility strips the `tx` arg from the user's custom mutator signatures.
 */
export type MakeCustomMutatorInterfaces<
  S extends Schema,
  MD extends CustomMutatorDefs<S>,
> = {
  readonly [Table in keyof MD]: {
    readonly [P in keyof MD[Table]]: MakeCustomMutatorInterface<
      S,
      MD[Table][P]
    >;
  };
};

export type MakeCustomMutatorInterface<
  S extends Schema,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  F,
> = F extends (tx: Transaction<S>, ...args: infer Args) => Promise<void>
  ? (...args: Args) => Promise<void>
  : never;

export class TransactionImpl implements Transaction<Schema> {
  constructor(repTx: WriteTransaction, schema: Schema) {
    const castedRepTx = repTx as WriteTransactionImpl;
    must(repTx.reason === 'initial' || repTx.reason === 'rebase');
    this.clientID = repTx.clientID;
    this.mutationID = repTx.mutationID;
    this.reason = repTx.reason === 'initial' ? 'optimistic' : 'rebase';
    // ~ Note: we will likely need to leverage proxies one day to create
    // ~ crud mutators and queries on demand for users with very large schemas.
    this.mutate = makeSchemaCRUD(
      schema,
      repTx,
      castedRepTx[zeroData] as undefined | IVMSourceBranch,
    );
    this.query = {};
  }

  readonly clientID: ClientID;
  readonly mutationID: number;
  readonly reason: 'optimistic' | 'rebase';
  readonly location = 'client';
  readonly mutate: SchemaCRUD<Schema>;
  readonly query: SchemaQuery<Schema>;
}

export function makeReplicacheMutator(
  mutator: CustomMutatorImpl<Schema>,
  schema: Schema,
) {
  return (repTx: WriteTransaction, args: ReadonlyJSONValue): Promise<void> => {
    const tx = new TransactionImpl(repTx, schema);
    return mutator(tx, args);
  };
}

function makeSchemaCRUD(
  schema: Schema,
  tx: WriteTransaction,
  ivmBranch: IVMSourceBranch | undefined,
) {
  const mutate: Record<string, TableCRUD<TableSchema>> = {};
  for (const [name] of Object.entries(schema.tables)) {
    mutate[name] = makeTableCRUD(schema, name, tx, ivmBranch);
  }
  return mutate;
}

function makeTableCRUD(
  schema: Schema,
  tableName: string,
  tx: WriteTransaction,
  ivmBranch: IVMSourceBranch | undefined,
) {
  const table = must(schema.tables[tableName]);
  const {primaryKey} = table;
  return {
    insert: (value: InsertValue<TableSchema>) =>
      insertImpl(
        tx,
        {op: 'insert', tableName, primaryKey, value},
        schema,
        ivmBranch,
      ),
    upsert: (value: UpsertValue<TableSchema>) =>
      upsertImpl(
        tx,
        {op: 'upsert', tableName, primaryKey, value},
        schema,
        ivmBranch,
      ),
    update: (value: UpdateValue<TableSchema>) =>
      updateImpl(
        tx,
        {op: 'update', tableName, primaryKey, value},
        schema,
        ivmBranch,
      ),
    delete: (id: DeleteID<TableSchema>) =>
      deleteImpl(
        tx,
        {op: 'delete', tableName, primaryKey, value: id},
        schema,
        ivmBranch,
      ),
  };
}
