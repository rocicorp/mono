import {must} from '../../../shared/src/must.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {ClientID} from '../types/client-state.ts';
import {deleteImpl, insertImpl, updateImpl, upsertImpl} from './crud.ts';
import type {WriteTransaction} from './replicache-types.ts';
import type {IVMSourceBranch, IVMSourceRepo} from './ivm-source-repo.ts';
import {newQuery} from '../../../zql/src/query/query-impl.ts';
import type {Query} from '../../../zql/src/query/query.ts';
import {ZeroContext} from './context.ts';
import type {
  CustomMutatorImpl,
  DeleteID,
  InsertValue,
  SchemaCRUD,
  SchemaQuery,
  TableCRUD,
  Transaction,
  TransactionReason,
  UpdateValue,
  UpsertValue,
} from '../../../zql/src/mutate/custom.ts';

export class TransactionImpl implements Transaction<Schema> {
  constructor(
    repTx: WriteTransaction,
    schema: Schema,
    ivmSourceRepo: IVMSourceRepo,
  ) {
    must(repTx.reason === 'initial' || repTx.reason === 'rebase');
    this.clientID = repTx.clientID;
    this.mutationID = repTx.mutationID;
    this.reason = repTx.reason === 'initial' ? 'optimistic' : 'rebase';
    // ~ Note: we will likely need to leverage proxies one day to create
    // ~ crud mutators and queries on demand for users with very large schemas.
    this.mutate = makeSchemaCRUD(
      schema,
      repTx,
      // Mutators do not write to the main IVM sources during optimistic mutations
      // so we pass undefined here.
      // ExperimentalWatch handles updating main.
      this.reason === 'optimistic' ? undefined : ivmSourceRepo.rebase,
    );
    this.query = makeSchemaQuery(
      schema,
      this.reason === 'optimistic' ? ivmSourceRepo.main : ivmSourceRepo.rebase,
    );
  }

  readonly clientID: ClientID;
  readonly mutationID: number;
  readonly reason: TransactionReason;
  readonly mutate: SchemaCRUD<Schema>;
  readonly query: SchemaQuery<Schema>;
}

export function makeReplicacheMutator(
  mutator: CustomMutatorImpl<Schema>,
  schema: Schema,
  ivmSourceRepo: IVMSourceRepo,
) {
  return (repTx: WriteTransaction, args: ReadonlyJSONValue): Promise<void> => {
    const tx = new TransactionImpl(repTx, schema, ivmSourceRepo);
    return mutator(tx, args);
  };
}

function makeSchemaQuery(schema: Schema, ivmBranch: IVMSourceBranch) {
  const rv = {} as Record<string, Query<Schema, string>>;
  const context = new ZeroContext(
    ivmBranch,
    () => () => {},
    applyViewUpdates => applyViewUpdates(),
  );

  for (const name of Object.keys(schema.tables)) {
    rv[name] = newQuery(context, schema, name);
  }

  return rv as SchemaQuery<Schema>;
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
