import {expectTypeOf, test} from 'vitest';
import type {
  DefaultContext,
  DefaultSchema,
  DefaultTypes,
  DefaultWrappedTransaction,
  InferTransactionFromDbProvider,
} from './default-types.ts';
import type {Schema} from './schema.ts';

type CustomSchema = Schema & {readonly name: 'custom'};
type CustomContext = {userId: string};
type WrappedTx = {
  readonly dbTransaction: {readonly wrappedTransaction: {readonly foo: 'bar'}};
};

type DbProvider = {
  transaction: <R>(callback: (tx: WrappedTx) => R) => Promise<R>;
};

interface RegisteredTypes extends DefaultTypes {
  schema: CustomSchema;
  context: CustomContext;
  dbProvider: DbProvider;
}

test('DefaultSchema uses registered schema and falls back to Schema', () => {
  expectTypeOf<DefaultSchema<RegisteredTypes>>().toEqualTypeOf<CustomSchema>();
  expectTypeOf<DefaultSchema>().toEqualTypeOf<Schema>();
});

test('DefaultContext uses registered context with undefined and defaults to unknown', () => {
  expectTypeOf<DefaultContext<RegisteredTypes>>().toEqualTypeOf<{
    readonly userId: string;
  }>();
  expectTypeOf<DefaultContext>().toEqualTypeOf<unknown>();
});

test('InferTransactionFromDbProvider extracts transaction argument type', () => {
  expectTypeOf<
    InferTransactionFromDbProvider<DbProvider>
  >().toEqualTypeOf<WrappedTx>();
});

test('DefaultWrappedTransaction picks wrapped transaction and reports errors', () => {
  expectTypeOf<DefaultWrappedTransaction<RegisteredTypes>>().toEqualTypeOf<
    WrappedTx['dbTransaction']['wrappedTransaction']
  >();

  expectTypeOf<
    DefaultWrappedTransaction<{
      readonly dbProvider: {
        readonly incorrect: 'db provider is incorrectly typed';
      };
    }>
  >().toExtend<{
    error: `The \`dbProvider\` type you have registered with \`declare module '@rocicorp/zero'\` is incorrect.`;
  }>();
  expectTypeOf<DefaultWrappedTransaction>().toEqualTypeOf<unknown>();
});
