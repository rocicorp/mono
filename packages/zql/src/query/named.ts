import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {SchemaQuery} from '../mutate/custom.ts';
import type {Query} from './query.ts';

export type NamedQuery<
  S extends Schema,
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<S, keyof S['tables'] & string>,
> = (tx: SchemaQuery<S>, ...args: TArg) => TReturnQuery;

export type CustomQueryID = {
  name: string;
  args: ReadonlyArray<ReadonlyJSONValue>;
};

type NamedQueryImpl<
  S extends Schema,
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<S, keyof S['tables'] & string>,
> = (tx: SchemaQuery<S>, ...arg: TArg) => TReturnQuery;

export function query<
  S extends Schema,
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<S, keyof S['tables'] & string>,
>(
  _s: S,
  name: string,
  fn: NamedQueryImpl<S, TArg, TReturnQuery>,
): NamedQuery<S, TArg, TReturnQuery> {
  return function queryWrapper(tx: SchemaQuery<S>, ...args: TArg) {
    return fn(tx, ...args).nameAndArgs(name, args);
  } as NamedQuery<S, TArg, TReturnQuery>;
}

query.bindTo =
  <S extends Schema>(s: S) =>
  <
    TArg extends ReadonlyArray<ReadonlyJSONValue>,
    TReturnQuery extends Query<S, keyof S['tables'] & string>,
  >(
    name: string,
    fn: NamedQueryImpl<S, TArg, TReturnQuery>,
  ): NamedQuery<S, TArg, TReturnQuery> =>
    query(s, name, fn);
